//! Postgres tests for `services::org_deletion` (control 0387).
//!
//! `RELINFER_TEST_DATABASE_URL`-gated like every billing pg suite; without it
//! each test prints a skip line and returns. A skip is NOT a pass.
//!
//! **The job and every request-path call run as `coralswarm_app`** (RLS on,
//! 0083's revokes, 0387's approval trigger). Fixtures are seeded and asserted
//! through the owner pool. Stripe is the recording `MockStripe`; the tenant
//! data plane, the export store, Clerk and email are local fakes. The
//! destroy steps that need the provisioner role and a real tenant database
//! (routing, credential, database) are in `backend/tests/org_destroy_pg.rs`;
//! here the destroyer is absent, so a deletion that reaches `routing` parks.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::billing::test_support::*;

// ── fakes ────────────────────────────────────────────────────────────────

#[derive(Default)]
struct FakeData {
    exports: AtomicUsize,
    forgotten: Mutex<Vec<String>>,
    /// Test hook: while building an export, another worker re-claims this
    /// deletion (a fresh lease token), as when our lease expired mid-step.
    steal: Mutex<Option<(PgPool, Uuid)>>,
}

#[async_trait]
impl OrgDataPlane for FakeData {
    async fn source_manifest(&self, _d: &OrgDeletion) -> Result<Value, String> {
        Ok(json!({"atoms": 3, "by_source_type": {"slack": {"atoms": 3, "sources": 2}}}))
    }
    async fn export_bundle(&self, d: &OrgDeletion, _scope: &ExportScope) -> Result<Vec<u8>, String> {
        self.exports.fetch_add(1, Ordering::SeqCst);
        let steal = self.steal.lock().unwrap().clone();
        if let Some((pool, id)) = steal {
            sqlx::query("UPDATE org_deletions SET lease_id = gen_random_uuid(), lease_until = now() + interval '120 seconds' WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
        }
        Ok(format!("{{\"kind\":\"manifest\",\"org\":\"{}\"}}\n", d.clerk_org_id).into_bytes())
    }
    async fn forget_tenant(&self, org: &str, _account: Uuid) {
        self.forgotten.lock().unwrap().push(org.to_string());
    }
}

/// Stores the "ciphertext" as (key, plaintext) so a test can check the
/// recorded wrapped key really opens what was stored.
#[derive(Default)]
struct FakeExports {
    objects: Mutex<HashMap<String, ([u8; 32], Vec<u8>)>>,
    deleted: Mutex<Vec<String>>,
}

#[async_trait]
impl ExportStore for FakeExports {
    async fn put(&self, key: &str, plaintext: &[u8], export_key: &[u8; 32]) -> Result<(), String> {
        self.objects.lock().unwrap().insert(key.into(), (*export_key, plaintext.to_vec()));
        Ok(())
    }
    async fn get(&self, key: &str, export_key: &[u8; 32]) -> Result<Vec<u8>, String> {
        match self.objects.lock().unwrap().get(key) {
            Some((k, v)) if k == export_key => Ok(v.clone()),
            Some(_) => Err("tag mismatch".into()),
            None => Err("not found".into()),
        }
    }
    async fn delete(&self, key: &str) -> Result<(), String> {
        self.objects.lock().unwrap().remove(key);
        self.deleted.lock().unwrap().push(key.into());
        Ok(())
    }
}

#[derive(Default)]
struct FakeDirectory {
    deleted: Mutex<Vec<String>>,
}

#[async_trait]
impl OrgDirectory for FakeDirectory {
    async fn delete_org(&self, org: &str) -> Result<(), String> {
        self.deleted.lock().unwrap().push(org.into());
        Ok(())
    }
}

#[derive(Default)]
struct FakeMailer {
    sent: Mutex<Vec<Mail>>,
    /// Addresses whose sends fail (a mail outage for one recipient).
    failing: Mutex<Vec<String>>,
}

#[async_trait]
impl Mailer for FakeMailer {
    async fn send(&self, mail: Mail) -> Result<(), String> {
        if self.failing.lock().unwrap().contains(&mail.to) {
            return Err("provider down".into());
        }
        self.sent.lock().unwrap().push(mail);
        Ok(())
    }
}

const KEK: [u8; 32] = [0x42; 32];

/// A pool connected as `coralswarm_app`.
async fn app_pool(owner: &PgPool) -> PgPool {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;
    // Cluster-wide role: concurrent tests race on it ("tuple concurrently
    // updated"), so retry.
    for attempt in 0..10 {
        match sqlx::query("ALTER ROLE coralswarm_app WITH LOGIN PASSWORD 'app_test_pw'").execute(owner).await {
            Ok(_) => break,
            Err(e) if attempt < 9 => {
                let _ = e;
                tokio::time::sleep(std::time::Duration::from_millis(50 * (attempt + 1))).await;
            }
            Err(e) => panic!("ALTER ROLE coralswarm_app: {e}"),
        }
    }
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    let opts = PgConnectOptions::from_str(&url).unwrap().username("coralswarm_app").password("app_test_pw");
    PgPoolOptions::new().max_connections(4).connect_with(opts).await.unwrap()
}

struct Rig {
    owner: PgPool,
    app: PgPool,
    stripe: MockStripe,
    data: FakeData,
    exports: FakeExports,
    directory: FakeDirectory,
    mailer: FakeMailer,
    approval_first_n: i64,
}

impl Rig {
    async fn new(owner: &PgPool) -> Self {
        // The database's own approval threshold (owner-writable only) is the
        // floor the worker cannot go under; this suite exercises both sides
        // with the per-run `approval_first_n`, so open the floor.
        sqlx::query("UPDATE org_deletion_policy SET approval_first_n = 0 WHERE approval_first_n <> 0")
            .execute(owner)
            .await
            .unwrap();
        let stripe = MockStripe::spawn().await;
        // No open Checkout, no subscription at the customer, unless a test says so.
        stripe.respond("GET", "/v1/checkout/sessions", 200, json!({"data": [], "has_more": false}));
        stripe.respond("GET", "/v1/subscriptions", 200, json!({"data": [], "has_more": false}));
        Self {
            owner: owner.clone(),
            app: app_pool(owner).await,
            stripe,
            data: FakeData::default(),
            exports: FakeExports::default(),
            directory: FakeDirectory::default(),
            mailer: FakeMailer::default(),
            approval_first_n: 0,
        }
    }
    fn ctx_with(&self, crash_after: Option<CrashHook>) -> Ctx<'_> {
        Ctx {
            control: &self.app,
            stripe: Some(&*self.stripe.client),
            directory: Some(&self.directory),
            data: &self.data,
            exports: Some(&self.exports),
            destroyer: None,
            mailer: &self.mailer,
            kek: Some(KEK),
            web_base: "https://app.example.test",
            approval_first_n: self.approval_first_n,
            crash_after,
        }
    }
    /// Run THIS deletion until it parks (ticks claim the earliest due row,
    /// which in a shared database may be another test's: run it directly).
    async fn drive(&self, id: Uuid) -> RunEnd {
        self.drive_with(id, None).await
    }
    async fn drive_with(&self, id: Uuid, crash_after: Option<CrashHook>) -> RunEnd {
        let d = leased(&self.owner, id).await;
        run(&self.ctx_with(crash_after), d).await
    }
}

struct Org {
    account: Uuid,
    org: String,
    creator: Uuid,
    creator_sub: String,
    admin: Uuid,
    member: Uuid,
    member_sub: String,
}

async fn user(pool: &PgPool) -> (Uuid, String) {
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let id: Uuid = sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
        .bind(format!("{}@org-deletion.test", Uuid::new_v4().simple()))
        .bind(&sub)
        .fetch_one(pool)
        .await
        .unwrap();
    (id, sub)
}

async fn member(pool: &PgPool, account: Uuid, user: Uuid, role: &str) {
    let tier: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, $3, $4)")
        .bind(account)
        .bind(user)
        .bind(role)
        .bind(tier)
        .execute(pool)
        .await
        .unwrap();
}

/// A self-serve team org: creator (admin, `accounts.owner_user_id`), a
/// second admin, a member, a `ready` provisioning job and, when `routed`, an
/// active routing row.
async fn selfserve_org(pool: &PgPool, routed: bool) -> Org {
    let (creator, creator_sub) = user(pool).await;
    let (admin, _) = user(pool).await;
    let (m, member_sub) = user(pool).await;
    let org = format!("org_{}", Uuid::new_v4().simple());
    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
        .bind(creator)
        .bind(&org)
        .fetch_one(pool)
        .await
        .unwrap();
    member(pool, account, creator, "admin").await;
    member(pool, account, admin, "admin").await;
    member(pool, account, m, "member").await;
    sqlx::query(
        "INSERT INTO tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref, step, status, provider, \
                provider_database_id, database_create_requested_at, credential_stored_at, routing_inserted_at, ready_at) \
         VALUES ($1, $2, 'o-' || left(replace($2::text, '-', ''), 16), \
                 'TENANT_DB_O_' || upper(left(replace($2::text, '-', ''), 16)), 'done', 'ready', 'local', \
                 'coralswarm_tenant_o_' || left(replace($2::text, '-', ''), 16), now(), now(), now(), now())",
    )
    .bind(&org)
    .bind(account)
    .execute(pool)
    .await
    .unwrap();
    if routed {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx, account).await.unwrap();
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
             SELECT clerk_org_id, account_id, slug, db_secret_ref, 'active' FROM tenant_provisioning_jobs WHERE clerk_org_id = $1",
        )
        .bind(&org)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    Org { account, org, creator, creator_sub, admin, member: m, member_sub }
}

async fn by_id(pool: &PgPool, id: Uuid) -> OrgDeletion {
    sqlx::query_as("SELECT * FROM org_deletions WHERE id = $1").bind(id).fetch_one(pool).await.unwrap()
}

/// What `org_deletion_claim` does for one row: a fresh fencing token and a
/// live lease. Returns the row as the worker would hold it.
async fn leased(owner: &PgPool, id: Uuid) -> OrgDeletion {
    sqlx::query_as(
        "UPDATE org_deletions SET lease_id = gen_random_uuid(), lease_until = now() + interval '120 seconds' \
          WHERE id = $1 RETURNING *",
    )
    .bind(id)
    .fetch_one(owner)
    .await
    .unwrap()
}

async fn end_grace(owner: &PgPool, id: Uuid) {
    sqlx::query("UPDATE org_deletions SET grace_ends_at = now() - interval '1 second', next_attempt_at = now(), lease_until = NULL WHERE id = $1")
        .bind(id)
        .execute(owner)
        .await
        .unwrap();
}

async fn request(rig: &Rig, o: &Org) -> OrgDeletion {
    request_by_owner(&rig.app, o.creator, &o.org, "Acme", "Acme").await.unwrap().deletion
}

fn sub_json(id: &str, status: &str) -> Value {
    subscription_json(id, status, &[("si_1", "price_standard", 2)], false, None, None)
}

/// A Stripe mock whose subscription flips to `canceled` once DELETEd.
fn stripe_cancels(mock: &MockStripe, sub: &str) {
    let canceled = Arc::new(AtomicBool::new(false));
    let (g, d) = (canceled.clone(), canceled);
    let (gs, ds) = (sub.to_string(), sub.to_string());
    let path = format!("/v1/subscriptions/{sub}");
    mock.respond_with("GET", &path, move |_| {
        (200, sub_json(&gs, if g.load(Ordering::SeqCst) { "canceled" } else { "active" }))
    });
    mock.respond_with("DELETE", &path, move |_| {
        d.store(true, Ordering::SeqCst);
        (200, sub_json(&ds, "canceled"))
    });
}

fn claims(sub: &str, org: &str) -> crate::clerk::ClerkClaims {
    serde_json::from_value(json!({"sub": sub, "org_id": org, "exp": 0})).unwrap()
}

/// What the auth door does with this token: `Ok` = admitted, `Err(code)`.
async fn door(app: &PgPool, sub: &str, org: &str, deletion_path: bool) -> Result<(), String> {
    match crate::middleware::auth::normalize_personal_org_claim_for(app, claims(sub, org), false, deletion_path).await {
        Ok(_) => Ok(()),
        Err(resp) => Err(body_json(resp).await["code"].as_str().unwrap_or("?").to_string()),
    }
}

// ── tests ────────────────────────────────────────────────────────────────

/// Only the in-app owner may ask, only for a self-serve org, only with the
/// org's name typed; a second request is the same deletion.
#[tokio::test]
async fn only_the_owner_may_request_and_only_for_a_self_serve_org() {
    let Some(owner) = pg_test_pool("org_deletion::owner_only").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let (stranger, _) = user(&owner).await;

    let as_member = request_by_owner(&rig.app, o.member, &o.org, "Acme", "Acme").await;
    let as_other_admin = request_by_owner(&rig.app, o.admin, &o.org, "Acme", "Acme").await;
    let as_stranger = request_by_owner(&rig.app, stranger, &o.org, "Acme", "Acme").await;
    let wrong_name = request_by_owner(&rig.app, o.creator, &o.org, "Acme Inc", "Acme").await;
    let first = request_by_owner(&rig.app, o.creator, &o.org, " acme ", "Acme").await.unwrap();
    let again = request_by_owner(&rig.app, o.creator, &o.org, "", "Acme").await.unwrap();

    // A concierge org (no provisioning job) is refused.
    let (concierge, concierge_owner) = seed_org_account(&owner, "owner").await;
    let concierge_org: String = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(concierge)
        .fetch_one(&owner)
        .await
        .unwrap();
    let concierge_try = request_by_owner(&rig.app, concierge_owner, &concierge_org, "x", "x").await;
    // A personal org is refused.
    let (_, personal_user, personal_org) = seed_personal_org_account(&owner).await;
    let personal_try = request_by_owner(&rig.app, personal_user, &personal_org, "x", "x").await;

    assert!(matches!(as_member, Err(RequestError::NotOwner)), "{as_member:?}");
    assert!(matches!(as_other_admin, Err(RequestError::NotOwner)), "another admin is not the owner");
    assert!(matches!(as_stranger, Err(RequestError::NotFound)));
    assert!(matches!(wrong_name, Err(RequestError::NameMismatch)));
    assert!(first.created);
    assert_eq!(first.deletion.state, "requested");
    assert_eq!(first.deletion.initiated_by, "owner");
    assert_eq!(first.deletion.requested_by_user_id, Some(o.creator));
    assert!(!again.created, "a second request joins the live one");
    assert_eq!(again.deletion.id, first.deletion.id);
    assert!(matches!(concierge_try, Err(RequestError::NotSelfServe)), "{concierge_try:?}");
    assert!(matches!(personal_try, Err(RequestError::PersonalOrg)), "{personal_try:?}");
}

/// Decision 12: the subscription ends NOW, with no proration and no final
/// invoice, keyed `org-deleted-cancel:{sub}`; a crash after Stripe answered
/// re-runs the step without a second write, and so does #1538's handler
/// arriving later (same read-first contract, same key).
#[tokio::test]
async fn the_subscription_is_cancelled_immediately_once_with_the_shared_key() {
    let Some(owner) = pg_test_pool("org_deletion::stripe_once").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let sub = format!("sub_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, o.account, &format!("cus_{}", Uuid::new_v4().simple()), Some(&sub), "active").await;
    stripe_cancels(&rig.stripe, &sub);
    let path = format!("/v1/subscriptions/{sub}");

    let d = request(&rig, &o).await;
    // Crash right after Stripe answered, before the checkpoint.
    let crash: CrashHook = Arc::new(|s: &str| s == "stripe_cancel");
    let crashed = rig.drive_with(d.id, Some(crash)).await;
    let after_crash = by_id(&owner, d.id).await;
    // Resume.
    let resumed = rig.drive(d.id).await;
    let after = by_id(&owner, d.id).await;
    // #1538's `organization.deleted` (or a replay) runs the same cancel.
    let late = cancel_org_subscription_now(&rig.app, Some(&*rig.stripe.client), o.account).await.unwrap();
    let deletes = rig.stripe.calls("DELETE", &path);

    assert_eq!(crashed, RunEnd::Crashed);
    assert!(!after_crash.has("stripe_cancel"));
    assert_eq!(resumed, RunEnd::Waiting, "parks for the grace period");
    assert_eq!(after.state, "grace");
    assert!(after.has("trial_end") && after.has("stripe_cancel") && after.has("notify_requested"));
    assert_eq!(after.stripe_outcome.as_deref(), Some("ended_now"), "the first run's outcome is kept");
    assert_eq!(late, CancelOutcome::AlreadyEnded);
    assert_eq!(deletes.len(), 1, "exactly one Stripe cancel across the crash and the webhook");
    assert_eq!(deletes[0].idempotency_key.as_deref(), Some(deleted_org_cancel_key(&sub).as_str()));
    assert_eq!(deletes[0].form.get("prorate").map(String::as_str), Some("false"));
    assert_eq!(deletes[0].form.get("invoice_now").map(String::as_str), Some("false"));
    assert_eq!(
        rig.mailer.sent.lock().unwrap().len(),
        3,
        "the requester's email plus one notice each for the other two members"
    );
}

/// Members are locked out from the moment the deletion is requested; the
/// owner reaches only the deletion routes; nothing destructive has run.
#[tokio::test]
async fn members_are_locked_out_and_the_owner_reaches_only_the_deletion_routes() {
    let Some(owner) = pg_test_pool("org_deletion::lockout").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;

    let before = door(&rig.app, &o.member_sub, &o.org, false).await;
    let d = request(&rig, &o).await;
    let member_any = door(&rig.app, &o.member_sub, &o.org, false).await;
    let owner_any = door(&rig.app, &o.creator_sub, &o.org, false).await;
    let owner_deletion_route = door(&rig.app, &o.creator_sub, &o.org, true).await;
    rig.drive(d.id).await;
    let in_grace = door(&rig.app, &o.member_sub, &o.org, false).await;
    let still_member: i64 = sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
        .bind(o.account)
        .fetch_one(&owner)
        .await
        .unwrap();
    let revoked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM org_revocations WHERE clerk_org_id = $1)")
        .bind(&o.org)
        .fetch_one(&owner)
        .await
        .unwrap();

    assert_eq!(before, Ok(()));
    assert_eq!(member_any, Err("org_deletion_pending".into()));
    assert_eq!(owner_any, Err("org_deletion_pending".into()), "the owner too, outside the deletion routes");
    assert_eq!(owner_deletion_route, Ok(()));
    assert_eq!(in_grace, Err("org_deletion_pending".into()));
    assert_eq!(still_member, 3, "revocation happens after grace, not at request");
    assert!(!revoked);
}

/// Undo during grace restores access but NOT the subscription; undo while
/// the subscription is still being cancelled is refused (retry), and undo
/// is the owner's alone.
#[tokio::test]
async fn undo_restores_access_but_not_the_subscription() {
    let Some(owner) = pg_test_pool("org_deletion::undo").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let sub = format!("sub_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, o.account, &format!("cus_{}", Uuid::new_v4().simple()), Some(&sub), "active").await;
    stripe_cancels(&rig.stripe, &sub);

    let d = request(&rig, &o).await;
    let too_early = cancel_by_owner(&rig.app, o.creator, &o.org).await.unwrap();
    rig.drive(d.id).await;
    let by_member = cancel_by_owner(&rig.app, o.member, &o.org).await.unwrap();
    let by_admin = cancel_by_owner(&rig.app, o.admin, &o.org).await.unwrap();
    let undone = cancel_by_owner(&rig.app, o.creator, &o.org).await.unwrap();
    let again = cancel_by_owner(&rig.app, o.creator, &o.org).await.unwrap();
    let after = by_id(&owner, d.id).await;
    let door_after = door(&rig.app, &o.member_sub, &o.org, false).await;
    let deletes = rig.stripe.calls("DELETE", &format!("/v1/subscriptions/{sub}")).len();
    let posts = rig.stripe.requests().iter().filter(|r| r.method == "POST").count();
    // A fresh request after the undo starts a new deletion.
    let fresh = request_by_owner(&rig.app, o.creator, &o.org, "Acme", "Acme").await.unwrap();

    assert_eq!(too_early, CancelResult::NotYet);
    assert_eq!(by_member, CancelResult::NotFound);
    assert_eq!(by_admin, CancelResult::NotFound);
    assert_eq!(undone, CancelResult::Canceled);
    assert_eq!(again, CancelResult::NotFound, "nothing live to undo any more");
    assert_eq!(after.state, "canceled");
    assert_eq!(after.canceled_by.as_deref(), Some("owner"));
    assert_eq!(door_after, Ok(()), "access is restored");
    assert_eq!(deletes, 1, "the subscription was ended at request time");
    assert_eq!(posts, 0, "undo never re-creates or resumes the subscription: the org resubscribes");
    assert!(fresh.created && fresh.deletion.id != d.id);
}

/// Decision 13: an operator must approve before anything is destroyed (the
/// first N deletions); the serving role cannot approve its own deletion,
/// even by writing the column; a pre-approval during grace is honoured.
#[tokio::test]
async fn operator_approval_is_required_and_only_an_owner_role_can_give_it() {
    let Some(owner) = pg_test_pool("org_deletion::approval").await else { return };
    let mut rig = Rig::new(&owner).await;
    rig.approval_first_n = i64::MAX;
    let o = selfserve_org(&owner, false).await;
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    end_grace(&owner, d.id).await;
    let first = rig.drive(d.id).await;
    let waiting = by_id(&owner, d.id).await;
    let second = rig.drive(d.id).await;

    let app_approve = approve(&rig.app, d.id, "app").await;
    let mut tx = crate::tenancy::scope::begin_account_scoped(&rig.app, o.account).await.unwrap();
    let app_write = sqlx::query("UPDATE org_deletions SET approved_at = now(), approved_by = 'app' WHERE id = $1")
        .bind(d.id)
        .execute(&mut *tx)
        .await;
    drop(tx);
    let mut tx = crate::tenancy::scope::begin_account_scoped(&rig.app, o.account).await.unwrap();
    let app_relax = sqlx::query("UPDATE org_deletions SET approval_required = false WHERE id = $1")
        .bind(d.id)
        .execute(&mut *tx)
        .await;
    drop(tx);
    let mut tx = crate::tenancy::scope::begin_account_scoped(&rig.app, o.account).await.unwrap();
    let app_skip = sqlx::query("UPDATE org_deletions SET state = 'destroying', destroying_since = now() WHERE id = $1")
        .bind(d.id)
        .execute(&mut *tx)
        .await;
    drop(tx);
    let still: OrgDeletion = by_id(&owner, d.id).await;

    let ok = approve(&owner, d.id, "Alex").await.unwrap();
    let twice = approve(&owner, d.id, "Alex").await.unwrap();
    let run = rig.drive(d.id).await;
    let after = by_id(&owner, d.id).await;

    assert_eq!(first, RunEnd::Waiting);
    assert_eq!(waiting.state, "awaiting_approval");
    assert_eq!(waiting.approval_required, Some(true));
    assert_eq!(second, RunEnd::Waiting);
    assert!(app_approve.is_err(), "the serving role cannot approve");
    assert!(app_write.is_err(), "nor write the approval columns");
    assert!(app_relax.is_err(), "nor relax a decided requirement");
    assert!(app_skip.is_err(), "nor skip to destroying without approval");
    assert_eq!(still.state, "awaiting_approval");
    assert!(still.approved_at.is_none());
    assert_eq!(ok, "approved");
    assert_eq!(twice, "already_approved");
    assert_eq!(run, RunEnd::Waiting, "parks at `routing`: no provisioner in this suite");
    assert_eq!(after.state, "destroying");
    assert_eq!(after.approved_by.as_deref(), Some("Alex"));
    assert!(after.has("revoke_access") && after.has("manifest") && after.has("export"));
    assert_eq!(after.next_step(), Some("routing"));

    // Pre-approval during grace: the end of grace goes straight to destroying.
    let o2 = selfserve_org(&owner, false).await;
    let d2 = request(&rig, &o2).await;
    rig.drive(d2.id).await;
    assert_eq!(approve(&owner, d2.id, "Alex").await.unwrap(), "approved");
    end_grace(&owner, d2.id).await;
    rig.drive(d2.id).await;
    assert_eq!(by_id(&owner, d2.id).await.state, "destroying");

    // Relaxed (ORG_DELETION_APPROVAL_FIRST_N=0): no approval needed.
    rig.approval_first_n = 0;
    let o3 = selfserve_org(&owner, false).await;
    let d3 = request(&rig, &o3).await;
    rig.drive(d3.id).await;
    end_grace(&owner, d3.id).await;
    rig.drive(d3.id).await;
    let d3 = by_id(&owner, d3.id).await;
    assert_eq!(d3.state, "destroying");
    assert_eq!(d3.approval_required, Some(false));
}

/// After grace: access is revoked (members' rows, `org_revocations`), the
/// manifest and the 30-day export are written; the recorded wrapped key
/// opens the stored export; nothing has touched the members' own personal
/// accounts.
#[tokio::test]
async fn revocation_and_export_happen_after_grace_and_the_export_key_opens_the_object() {
    let Some(owner) = pg_test_pool("org_deletion::export").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let personal: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
        .bind(o.member)
        .fetch_one(&owner)
        .await
        .unwrap();
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    end_grace(&owner, d.id).await;
    rig.drive(d.id).await;
    let after = by_id(&owner, d.id).await;
    let members: i64 = sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
        .bind(o.account)
        .fetch_one(&owner)
        .await
        .unwrap();
    let personal_left: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts WHERE id = $1")
        .bind(personal)
        .fetch_one(&owner)
        .await
        .unwrap();
    let read = read_export(&rig.exports, &KEK, None, &after).await.unwrap();

    assert_eq!(after.state, "destroying");
    assert_eq!(members, 0, "memberships dropped by revoke_org_access");
    assert_eq!(personal_left, 1, "a member's personal account is untouched");
    assert_eq!(after.source_manifest.as_ref().unwrap()["atoms"], 3);
    assert!(after.export_object_key.as_deref().unwrap().starts_with(&format!("{}/{}/", o.org, d.id)));
    assert!(after.export_expires_at.unwrap() > Utc::now() + chrono::Duration::days(29));
    assert_eq!(after.export_sha256.as_deref(), Some(sha256_hex(&read).as_str()));
    assert!(String::from_utf8(read).unwrap().contains(&o.org));
    assert_eq!(rig.data.exports.load(Ordering::SeqCst), 1);
}

/// Day 30: the export object is deleted and its wrapped key destroyed; the
/// founder's download then fails.
#[tokio::test]
async fn the_export_and_its_key_are_destroyed_at_day_30() {
    let Some(owner) = pg_test_pool("org_deletion::retention").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    end_grace(&owner, d.id).await;
    rig.drive(d.id).await;
    sqlx::query("UPDATE org_deletions SET export_expires_at = now() - interval '1 minute' WHERE id = $1")
        .bind(d.id)
        .execute(&owner)
        .await
        .unwrap();
    let n = destroy_expired_exports(&rig.app, Some(&rig.exports)).await.unwrap();
    let after = by_id(&owner, d.id).await;
    let read = read_export(&rig.exports, &KEK, None, &after).await;

    assert!(n >= 1);
    assert!(after.export_key_wrapped.is_none());
    assert!(after.export_destroyed_at.is_some());
    assert!(rig.exports.deleted.lock().unwrap().contains(&after.export_object_key.clone().unwrap()));
    assert!(read.is_err());
}

/// Decision 4: 30 days unpaid enters the same flow (system as requester,
/// same grace). A concierge org, a paid org and an org whose unpaid clock
/// is younger are left alone. Undoing an unpaid deletion gives 7 days to pay.
/// `org_deletion_unpaid_forget` is SECURITY DEFINER (it bypasses the table's
/// RLS), so it re-applies the isolation itself: the serving role can only
/// reset the clock of the account it has entered.
#[tokio::test]
async fn the_app_role_cannot_reset_another_accounts_unpaid_clock() {
    let Some(owner) = pg_test_pool("org_deletion::unpaid_forget").await else { return };
    let rig = Rig::new(&owner).await;
    let mine = selfserve_org(&owner, false).await;
    let theirs = selfserve_org(&owner, false).await;
    for o in [&mine, &theirs] {
        sqlx::query("INSERT INTO org_unpaid_watch (account_id, clerk_org_id) VALUES ($1, $2)")
            .bind(o.account)
            .bind(&o.org)
            .execute(&owner)
            .await
            .unwrap();
    }
    let watched = |account: Uuid| {
        let owner = owner.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM org_unpaid_watch WHERE account_id = $1")
                .bind(account)
                .fetch_one(&owner)
                .await
                .unwrap()
        }
    };

    // No account entered: nothing is reset.
    let none: i32 = sqlx::query_scalar("SELECT org_deletion_unpaid_forget($1)").bind(theirs.account).fetch_one(&rig.app).await.unwrap();
    // Entered as `mine`, aiming at `theirs`: nothing is reset.
    let mut tx = crate::tenancy::scope::begin_account_scoped(&rig.app, mine.account).await.unwrap();
    let cross: i32 = sqlx::query_scalar("SELECT org_deletion_unpaid_forget($1)").bind(theirs.account).fetch_one(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!((none, cross), (0, 0));
    assert_eq!(watched(theirs.account).await, 1, "another account's clock survives");

    // The worker path (enters the account, then forgets it) still works.
    assert_eq!(unpaid_forget(&rig.app, mine.account).await.unwrap(), 1);
    assert_eq!(watched(mine.account).await, 0);
    assert_eq!(watched(theirs.account).await, 1);
}

#[tokio::test]
async fn an_org_unpaid_for_30_days_enters_deletion_automatically() {
    let Some(owner) = pg_test_pool("org_deletion::unpaid").await else { return };
    let rig = Rig::new(&owner).await;
    let unpaid = selfserve_org(&owner, false).await;
    let young = selfserve_org(&owner, false).await;
    let paid = selfserve_org(&owner, false).await;
    let (concierge, concierge_user) = seed_org_account(&owner, "owner").await;
    let unpaid_sub = format!("sub_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, unpaid.account, &format!("cus_{}", Uuid::new_v4().simple()), Some(&unpaid_sub), "past_due").await;
    stripe_cancels(&rig.stripe, &unpaid_sub);
    for (acct, status) in [(young.account, "canceled"), (paid.account, "active"), (concierge, "past_due")] {
        seed_subscription(&owner, acct, &format!("cus_{}", Uuid::new_v4().simple()), Some(&format!("sub_{}", Uuid::new_v4().simple())), status).await;
    }
    // First pass only starts the clocks.
    let _ = unpaid_entry(&rig.app, Some(&*rig.stripe.client)).await.unwrap();
    let first = live_for_account(&owner, unpaid.account).await.unwrap();
    let watched: Vec<Uuid> = sqlx::query_scalar("SELECT account_id FROM org_unpaid_watch WHERE account_id = ANY($1)")
        .bind(vec![unpaid.account, young.account, paid.account, concierge])
        .fetch_all(&owner)
        .await
        .unwrap();
    sqlx::query("UPDATE org_unpaid_watch SET unpaid_since = now() - interval '31 days' WHERE account_id = $1")
        .bind(unpaid.account)
        .execute(&owner)
        .await
        .unwrap();
    let _ = unpaid_entry(&rig.app, Some(&*rig.stripe.client)).await.unwrap();
    let entered = live_for_account(&owner, unpaid.account).await;
    let young_live = live_for_account(&owner, young.account).await.unwrap();
    let paid_live = live_for_account(&owner, paid.account).await.unwrap();

    assert!(first.is_none(), "nothing is due on the first sight");
    assert!(watched.contains(&unpaid.account) && watched.contains(&young.account));
    assert!(!watched.contains(&paid.account), "a paid org is not watched");
    assert!(!watched.contains(&concierge), "a concierge org is never auto-deleted");
    let d = entered.unwrap().expect("unpaid org entered deletion");
    assert_eq!(d.initiated_by, "unpaid");
    assert_eq!(d.requested_by_user_id, None);
    assert!(d.grace_ends_at > Utc::now() + chrono::Duration::days(6), "same 7-day grace");
    assert!(young_live.is_none());
    assert!(paid_live.is_none());

    // The owner undoes it during grace: 7 days to pay before it re-enters.
    rig.drive(d.id).await;
    assert_eq!(cancel_by_owner(&rig.app, unpaid.creator, &unpaid.org).await.unwrap(), CancelResult::Canceled);
    let _ = unpaid_entry(&rig.app, Some(&*rig.stripe.client)).await.unwrap();
    assert!(live_for_account(&owner, unpaid.account).await.unwrap().is_none(), "not re-entered inside the window");
    let not_before: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT not_before FROM org_unpaid_watch WHERE account_id = $1")
        .bind(unpaid.account)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert!(not_before.unwrap() > Utc::now() + chrono::Duration::days(6));
    let _ = concierge_user;
}

/// #1557: a trialing subscription is mirrored as `active`
/// (`map_subscription_status`), so the unpaid sweep must never watch it, and
/// must not enter it even after a long time. Guards against a future
/// `trialing` mirror value being read as unpaid.
#[tokio::test]
async fn a_trialing_org_is_never_watched_or_entered_by_the_unpaid_sweep() {
    let Some(owner) = pg_test_pool("org_deletion::unpaid_trialing").await else { return };
    let rig = Rig::new(&owner).await;
    let trialing = selfserve_org(&owner, false).await;
    let mirrored = crate::routes::stripe_webhook::map_subscription_status("trialing");
    seed_subscription(
        &owner,
        trialing.account,
        &format!("cus_{}", Uuid::new_v4().simple()),
        Some(&format!("sub_{}", Uuid::new_v4().simple())),
        mirrored,
    )
    .await;
    let _ = unpaid_entry(&rig.app, Some(&*rig.stripe.client)).await.unwrap();
    let watched: i64 = sqlx::query_scalar("SELECT count(*) FROM org_unpaid_watch WHERE account_id = $1")
        .bind(trialing.account)
        .fetch_one(&owner)
        .await
        .unwrap();
    let _ = unpaid_entry(&rig.app, Some(&*rig.stripe.client)).await.unwrap();
    let live = live_for_account(&owner, trialing.account).await.unwrap();
    assert_eq!(mirrored, "active");
    assert_eq!(watched, 0, "a trialing org has no unpaid clock");
    assert!(live.is_none(), "a trialing org never enters deletion");
}

/// `organization.deleted` arriving MID-FLOW (an operator deletes the org in
/// the Clerk dashboard during grace). Main's arm runs `revoke_org_access`;
/// #1538 adds the trial end and the keyed cancel. All of it finds the work
/// already done: no Stripe write. Undo becomes impossible, and the job still
/// reaches the destroy step, where `revoke_access` converges.
#[tokio::test]
async fn organization_deleted_mid_flow_is_a_no_op_for_the_job() {
    let Some(owner) = pg_test_pool("org_deletion::webhook_mid_flow").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let sub = format!("sub_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, o.account, &format!("cus_{}", Uuid::new_v4().simple()), Some(&sub), "active").await;
    stripe_cancels(&rig.stripe, &sub);
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;

    // The webhook, as main runs it, then #1538's billing half.
    crate::clerk_provision::revoke_org_access(&rig.app, &o.org).await.unwrap();
    let again = cancel_org_subscription_now(&rig.app, Some(&*rig.stripe.client), o.account).await.unwrap();
    let undo = cancel_by_owner(&rig.app, o.creator, &o.org).await.unwrap();

    end_grace(&owner, d.id).await;
    rig.drive(d.id).await;
    let after = by_id(&owner, d.id).await;
    // A redelivery after the job revoked: still converges.
    crate::clerk_provision::revoke_org_access(&rig.app, &o.org).await.unwrap();
    let _ = adopt_clerk_deletions(&rig.app).await.unwrap();
    let live = live_for_account(&owner, o.account).await.unwrap().unwrap();

    assert_eq!(again, CancelOutcome::AlreadyEnded);
    assert_eq!(rig.stripe.calls("DELETE", &format!("/v1/subscriptions/{sub}")).len(), 1);
    assert_eq!(undo, CancelResult::NotFound, "the owner is no longer a member");
    assert_eq!(after.state, "destroying");
    assert!(after.has("revoke_access"));
    assert_eq!(live.id, d.id, "an org with a live deletion is never adopted as a second one");
    assert_eq!(live.initiated_by, "owner");
}

/// Decision 13: an org deleted in the Clerk dashboard (revoked, no in-app
/// deletion) is adopted with the same grace for its tenant data, and its
/// deletion cannot be undone in-app (the membership is already gone).
#[tokio::test]
async fn a_clerk_dashboard_delete_is_adopted_with_grace_for_the_tenant() {
    let Some(owner) = pg_test_pool("org_deletion::adopt").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, true).await;
    crate::clerk_provision::revoke_org_access(&rig.app, &o.org).await.unwrap();
    let _ = adopt_clerk_deletions(&rig.app).await.unwrap();
    let first = live_for_account(&owner, o.account).await.unwrap().expect("adopted");
    let _ = adopt_clerk_deletions(&rig.app).await.unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM org_deletions WHERE account_id = $1")
        .bind(o.account)
        .fetch_one(&owner)
        .await
        .unwrap();
    rig.drive(first.id).await;
    let d = by_id(&owner, first.id).await;

    assert_eq!(rows, 1, "adopted once");
    assert_eq!(d.initiated_by, "clerk_dashboard");
    assert_eq!(d.state, "grace");
    assert!(d.grace_ends_at > Utc::now() + chrono::Duration::days(6));
    assert!(!d.can_cancel());
}

/// The `done` row is the tombstone: a later request, sweep or adoption for
/// the org is a no-op, and the gate state is `done` (no lockout code: the
/// org is revoked, so the membership check refuses first).
#[tokio::test]
async fn the_done_row_is_a_tombstone_for_every_later_entry() {
    let Some(owner) = pg_test_pool("org_deletion::tombstone").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, true).await;
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    end_grace(&owner, d.id).await;
    rig.drive(d.id).await;
    // What the provisioner steps (tested in backend/tests/org_destroy_pg.rs)
    // leave behind, stamped as owner so the job can finish here.
    sqlx::query(
        "UPDATE org_deletions SET completed_steps = completed_steps || ARRAY['routing','credential','database']::text[] \
          WHERE id = $1",
    )
    .bind(d.id)
    .execute(&owner)
    .await
    .unwrap();
    let end = rig.drive(d.id).await;
    let done = by_id(&owner, d.id).await;
    let state = state_for_org(&rig.app, &o.org).await.unwrap();
    let again = request_by_owner(&rig.app, o.creator, &o.org, "Acme", "Acme").await;
    let _ = adopt_clerk_deletions(&rig.app).await.unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM org_deletions WHERE account_id = $1")
        .bind(o.account)
        .fetch_one(&owner)
        .await
        .unwrap();

    assert_eq!(end, RunEnd::Done);
    assert_eq!(done.state, "done");
    assert!(done.done_at.is_some());
    assert_eq!(rig.directory.deleted.lock().unwrap().as_slice(), &[o.org.clone()]);
    assert_eq!(state.as_deref(), Some("done"));
    assert!(lockout_code("done").is_none());
    assert!(matches!(again, Err(RequestError::NotFound)), "no longer a member: {again:?}");
    assert_eq!(rows, 1, "the tombstone stops a second adoption");
}

// ═══════════════════════════════════════════════════════════════════════════
// Security-review fixes (database-enforced state machine, lease fencing,
// billing that cannot stay live, shared access predicate)
// ═══════════════════════════════════════════════════════════════════════════

/// Run ONE statement as `coralswarm_app` inside `account`'s scope, on a
/// connection that is rolled back afterwards, so the real row is never
/// changed. `setup` runs first as the owner (e.g. to raise the approval
/// threshold for this transaction only). Returns the database error code on
/// refusal.
async fn as_app(owner: &PgPool, account: Uuid, setup: &[&str], sql: &str) -> Result<u64, String> {
    let mut tx = owner.begin().await.unwrap();
    for q in setup {
        sqlx::query(q).execute(&mut *tx).await.unwrap();
    }
    sqlx::query("SET LOCAL ROLE coralswarm_app").execute(&mut *tx).await.unwrap();
    sqlx::query("SELECT set_config('app.account_id', $1, true)")
        .bind(account.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    let r = sqlx::query(sql).execute(&mut *tx).await;
    let _ = tx.rollback().await;
    r.map(|r| r.rows_affected()).map_err(|e| {
        e.as_database_error().and_then(|d| d.code().map(|c| c.to_string())).unwrap_or_else(|| e.to_string())
    })
}

const RAISE_THRESHOLD: &str = "UPDATE org_deletion_policy SET approval_first_n = 1000000";

/// HIGH 2: the serving role cannot skip grace, skip approval, decide that no
/// approval is needed before N deletions have completed, or rewrite the
/// account, the org or the grace of a deletion; and it cannot start a
/// deletion that is not a plain `requested` one with the full grace. Each
/// bypass is tried as a non-owner role and must be refused by the database
/// itself; the legitimate statement beside it is accepted.
#[tokio::test]
async fn the_database_refuses_every_bypass_by_the_serving_role() {
    let Some(owner) = pg_test_pool("org_deletion::db_guard").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let other = selfserve_org(&owner, false).await;
    let d = request(&rig, &o).await;
    let id = d.id;
    // Request phase done: `grace`, grace NOT over.
    rig.drive(id).await;
    assert_eq!(by_id(&owner, id).await.state, "grace");
    let upd = |set: &str| format!("UPDATE org_deletions SET {set} WHERE id = '{id}'");

    // --- during grace ---
    for (what, set) in [
        ("skip grace to destroying with no approval", "state = 'destroying', approval_required = false, destroying_since = now()"),
        ("skip grace to awaiting_approval", "state = 'awaiting_approval', approval_required = true"),
        ("pull grace_ends_at into the past", "grace_ends_at = now() - interval '1 day'"),
        ("shorten grace but stay in it", "grace_ends_at = now() + interval '1 minute'"),
        ("decide approval_required = false from NULL", "approval_required = false"),
        ("rewrite the Clerk org", "clerk_org_id = 'org_somebody_else'"),
        ("rewrite created_at", "created_at = now() - interval '30 days'"),
        ("rewrite the initiator", "initiated_by = 'unpaid'"),
        ("jump to done", "state = 'done', done_at = now(), destroying_since = now()"),
        ("jump back to requested", "state = 'requested'"),
        ("remove a completed step", "completed_steps = '{}'"),
    ] {
        let r = as_app(&owner, o.account, &[], &upd(set)).await;
        assert_eq!(r, Err("42501".to_string()), "{what}: {r:?}");
    }
    let move_account = as_app(&owner, o.account, &[], &upd(&format!("account_id = '{}'", other.account))).await;
    assert!(move_account.is_err(), "re-pointing the row at another account: {move_account:?}");
    // Legitimate: the owner's undo, and a harmless bookkeeping write.
    assert_eq!(as_app(&owner, o.account, &[], &upd("state = 'canceled', canceled_at = now()")).await, Ok(1));
    assert_eq!(as_app(&owner, o.account, &[], &upd("last_error = 'x', attempts = attempts + 1")).await, Ok(1));

    // --- grace over (an operator moves the clock; the serving role could not) ---
    end_grace(&owner, id).await;
    let leave = "state = 'destroying', approval_required = false, destroying_since = now()";
    // Fewer than N deletions are done: NULL -> false is refused.
    let early = as_app(&owner, o.account, &[RAISE_THRESHOLD], &upd(leave)).await;
    assert_eq!(early, Err("42501".to_string()), "approval cannot be waived before N deletions completed: {early:?}");
    let early2 = as_app(&owner, o.account, &[RAISE_THRESHOLD], &upd("state = 'awaiting_approval', approval_required = false")).await;
    assert_eq!(early2, Err("42501".to_string()), "{early2:?}");
    // No approval, though required.
    let unapproved = as_app(&owner, o.account, &[], &upd("state = 'destroying', approval_required = true, destroying_since = now()")).await;
    assert!(unapproved.is_err(), "destroying without the approval it needs: {unapproved:?}");
    // The legitimate worker statements pass.
    assert_eq!(
        as_app(&owner, o.account, &[RAISE_THRESHOLD], &upd("state = 'awaiting_approval', approval_required = true")).await,
        Ok(1),
        "grace over -> awaiting_approval"
    );
    assert_eq!(
        as_app(&owner, o.account, &[], &upd(leave)).await,
        Ok(1),
        "once N deletions are done (here N = 0) the requirement may be waived"
    );
    // awaiting_approval -> destroying without approved_at, and approving itself.
    sqlx::query("UPDATE org_deletions SET state = 'awaiting_approval', approval_required = true WHERE id = $1")
        .bind(id)
        .execute(&owner)
        .await
        .unwrap();
    let skip = as_app(&owner, o.account, &[], &upd("state = 'destroying', destroying_since = now()")).await;
    assert!(skip.is_err(), "awaiting_approval -> destroying needs an operator's approval: {skip:?}");
    let self_approve = as_app(&owner, o.account, &[], &upd("approved_at = now(), approved_by = 'app'")).await;
    assert_eq!(self_approve, Err("42501".to_string()));
    let back = as_app(&owner, o.account, &[], &upd("state = 'grace'")).await;
    assert_eq!(back, Err("42501".to_string()), "no way back to grace: {back:?}");

    // --- INSERT ---
    let ins = |org: &str, acct: Uuid, extra_cols: &str, extra_vals: &str, grace: &str| {
        format!(
            "INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by, grace_ends_at{extra_cols}) \
             VALUES ('{acct}', '{org}', 'owner', {grace}{extra_vals})"
        )
    };
    let fresh = selfserve_org(&owner, false).await;
    let ok_grace = "now() + interval '7 days'";
    assert_eq!(as_app(&owner, fresh.account, &[], &ins(&fresh.org, fresh.account, "", "", ok_grace)).await, Ok(1));
    for (what, sql) in [
        ("a grace shorter than 7 days", ins(&fresh.org, fresh.account, "", "", "now() + interval '1 day'")),
        ("an already-destroying row", ins(&fresh.org, fresh.account, ", state, destroying_since, approval_required", ", 'destroying', now(), false", ok_grace)),
        ("a pre-approved row", ins(&fresh.org, fresh.account, ", approved_at, approved_by", ", now(), 'app'", ok_grace)),
        ("approval_required pre-decided", ins(&fresh.org, fresh.account, ", approval_required", ", false", ok_grace)),
        ("a back-dated created_at", ins(&fresh.org, fresh.account, ", created_at", ", now() - interval '30 days'", "now() - interval '23 days'")),
        ("an account that is not the org's", ins(&fresh.org, other.account, "", "", ok_grace)),
    ] {
        let r = as_app(&owner, fresh.account, &[], &sql).await;
        assert!(r.is_err(), "{what} must be refused: {r:?}");
    }
    // The owner role (an operator DSN) is exempt: it can extend a grace.
    assert!(sqlx::query("UPDATE org_deletions SET grace_ends_at = now() + interval '9 days' WHERE id = $1")
        .bind(id)
        .execute(&owner)
        .await
        .is_ok());
}

/// The approval threshold is the database's: `org_deletion_policy` (seeded
/// 10) is unreadable and unwritable by the serving role, and the waiver is
/// only possible once that many deletions are `done`. The worker's
/// environment value can only raise it (`max(env, db)`).
#[tokio::test]
async fn the_approval_threshold_lives_in_a_table_the_serving_role_cannot_touch() {
    let Some(owner) = pg_test_pool("org_deletion::threshold_floor").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    end_grace(&owner, d.id).await;
    // The floor is raised in a transaction this test never commits: the
    // worker below sees the committed value (0), so emulate the decision the
    // way production takes it, through the function the worker calls.
    let mut tx = owner.begin().await.unwrap();
    sqlx::query(RAISE_THRESHOLD).execute(&mut *tx).await.unwrap();
    let floor: i32 = sqlx::query_scalar("SELECT org_deletion_approval_first_n()").fetch_one(&mut *tx).await.unwrap();
    let may_relax: bool = sqlx::query_scalar("SELECT org_deletion_may_relax_approval()").fetch_one(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(floor, 1_000_000);
    assert!(!may_relax, "with fewer than N deletions done the requirement cannot be waived");
    // And the app role cannot read or write the policy table at all.
    let read = as_app(&owner, o.account, &[], "SELECT 1 FROM org_deletion_policy").await;
    let write = as_app(&owner, o.account, &[], "UPDATE org_deletion_policy SET approval_first_n = 0").await;
    assert_eq!(read, Err("42501".to_string()));
    assert_eq!(write, Err("42501".to_string()));
}

/// MEDIUM 6: the claim mints a fencing token; a worker whose lease was taken
/// over writes nothing more, mid-step or between steps, and two exports racing
/// leave exactly one recorded key that opens the one stored object.
#[tokio::test]
async fn a_worker_that_lost_its_lease_cannot_write_and_two_exports_race_safely() {
    let Some(owner) = pg_test_pool("org_deletion::lease_fence").await else { return };
    // The claim function mints a token (rolled back: no other test's row is held).
    let mut tx = owner.begin().await.unwrap();
    let claimed: Option<(Option<Uuid>, Option<DateTime<Utc>>)> =
        sqlx::query_as("SELECT lease_id, lease_until FROM org_deletion_claim(120)").fetch_optional(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    if let Some((token, until)) = claimed {
        assert!(token.is_some() && until.unwrap() > Utc::now(), "a claim carries a fresh token and lease");
    }

    let mut rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    end_grace(&owner, d.id).await;

    // (1) Between steps: A holds a token, then B re-claims. A stops at once.
    let a = leased(&owner, d.id).await;
    let b = leased(&owner, d.id).await;
    assert_ne!(a.lease_id, b.lease_id);
    let stale_run = run(&rig.ctx_with(None), a.clone()).await;
    let after = by_id(&owner, d.id).await;
    assert_eq!(stale_run, RunEnd::LeaseLost);
    assert_eq!(after.state, "grace", "the stale worker did not even leave grace");
    assert_eq!(mark_step_done(&rig.app, &a, "trial_end").await, Err(LEASE_LOST.to_string()));
    assert_eq!(renew_lease(&rig.app, &a).await, Err(LEASE_LOST.to_string()));
    let _ = record_failure(&rig.app, &a, "stale worker error").await;
    let _ = release(&rig.app, &a, Duration::ZERO, Some("stale")).await;
    let untouched = by_id(&owner, d.id).await;
    assert!(untouched.last_error.is_none(), "a stale worker's failure/release writes nothing");
    assert_eq!(untouched.lease_id, b.lease_id);

    // (2) Mid-export: A builds its bundle, B takes the lease while A is
    // still working. A's upload happens, its key is NOT recorded, and its
    // orphan object is deleted; B's export is the recorded one.
    rig.approval_first_n = 0;
    let a = leased(&owner, d.id).await;
    *rig.data.steal.lock().unwrap() = Some((owner.clone(), d.id));
    let racing = run(&rig.ctx_with(None), a).await;
    *rig.data.steal.lock().unwrap() = None;
    let mid = by_id(&owner, d.id).await;
    assert_eq!(racing, RunEnd::LeaseLost);
    assert!(mid.export_key_wrapped.is_none() && mid.export_object_key.is_none(), "A recorded nothing");
    assert!(rig.exports.objects.lock().unwrap().is_empty(), "A's orphan object was removed");
    assert_eq!(rig.exports.deleted.lock().unwrap().len(), 1);

    let b_row = by_id(&owner, d.id).await; // carries the token the steal minted
    let finished = run(&rig.ctx_with(None), b_row).await;
    let fin = by_id(&owner, d.id).await;
    assert_ne!(finished, RunEnd::LeaseLost, "B, the current holder, proceeds");
    let key_b = fin.export_object_key.clone().expect("B's export is recorded");
    assert_eq!(rig.exports.objects.lock().unwrap().len(), 1);
    assert!(rig.exports.deleted.lock().unwrap().iter().all(|k| *k != key_b));
    assert!(read_export(&rig.exports, &KEK, None, &fin).await.is_ok(), "the recorded key opens the one stored object");

    // (3) A late writer holding an old token cannot replace B's key either.
    let late = sqlx::query(
        "UPDATE org_deletions SET export_object_key = 'x/y/z', export_key_wrapped = '\\x00' \
          WHERE id = $1 AND lease_id = $2 AND export_key_wrapped IS NULL",
    )
    .bind(d.id)
    .bind(Uuid::new_v4())
    .execute(&owner)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(late, 0);
    assert_eq!(by_id(&owner, d.id).await.export_object_key, Some(key_b));
}

// ── MEDIUM 4: billing cannot stay live ───────────────────────────────────

/// (a) the customer's open Checkout sessions are expired at request time;
/// (d) our stored `canceled` copy is not believed: Stripe is asked, and a
/// subscription it still reports live is ended.
#[tokio::test]
async fn request_time_expires_open_checkouts_and_does_not_trust_a_stored_canceled_copy() {
    let Some(owner) = pg_test_pool("org_deletion::billing_request_time").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let sub = format!("sub_{}", Uuid::new_v4().simple());
    let cus = format!("cus_{}", Uuid::new_v4().simple());
    // Our mirror says canceled (a lagging copy); Stripe says otherwise.
    seed_subscription(&owner, o.account, &cus, Some(&sub), "canceled").await;
    stripe_cancels(&rig.stripe, &sub);
    rig.stripe.respond("GET", "/v1/checkout/sessions", 200, json!({"data": [{"id": "cs_open_1"}, {"id": "cs_open_2"}], "has_more": false}));
    rig.stripe.respond("POST", "/v1/checkout/sessions/cs_open_1/expire", 200, json!({"id": "cs_open_1", "status": "expired"}));
    rig.stripe.respond("POST", "/v1/checkout/sessions/cs_open_2/expire", 400, json!({"error": {"message": "already completed"}}));

    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    let after = by_id(&owner, d.id).await;
    let listed = rig.stripe.calls("GET", "/v1/checkout/sessions");
    let deletes = rig.stripe.calls("DELETE", &format!("/v1/subscriptions/{sub}"));

    assert_eq!(after.state, "grace");
    assert_eq!(listed[0].query.get("customer"), Some(&cus));
    assert_eq!(listed[0].query.get("status").map(String::as_str), Some("open"));
    assert_eq!(rig.stripe.calls("POST", "/v1/checkout/sessions/cs_open_1/expire").len(), 1);
    assert_eq!(rig.stripe.calls("POST", "/v1/checkout/sessions/cs_open_2/expire").len(), 1, "a refused expire is tolerated");
    assert_eq!(after.stripe_outcome.as_deref(), Some("ended_now"), "the stored `canceled` was not believed");
    assert_eq!(deletes.len(), 1);
}

/// (b) the destroying-phase sweep lists the customer's subscriptions AT
/// STRIPE and ends every one that is not ended (keyed, no proration), even
/// ones our mirror never heard of; ended ones are left alone.
#[tokio::test]
async fn the_destroying_sweep_ends_every_live_subscription_the_customer_has_at_stripe() {
    let Some(owner) = pg_test_pool("org_deletion::billing_sweep").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let cus = format!("cus_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, o.account, &cus, None, "canceled").await;
    let (live, past_due, gone) = ("sub_live_late", "sub_past_due", "sub_already_gone");
    rig.stripe.respond(
        "GET",
        "/v1/subscriptions",
        200,
        json!({"data": [
            {"id": live, "status": "active"},
            {"id": past_due, "status": "past_due"},
            {"id": gone, "status": "canceled"},
        ], "has_more": false}),
    );
    for sub in [live, past_due] {
        rig.stripe.respond("DELETE", &format!("/v1/subscriptions/{sub}"), 200, sub_json(sub, "canceled"));
    }
    let d = request(&rig, &o).await;
    let ctx = rig.ctx_with(None);

    let step = step_stripe_sweep(&ctx, &d).await.unwrap();

    assert_eq!(step, StepResult::Done);
    for sub in [live, past_due] {
        let calls = rig.stripe.calls("DELETE", &format!("/v1/subscriptions/{sub}"));
        assert_eq!(calls.len(), 1, "{sub}");
        assert_eq!(calls[0].idempotency_key.as_deref(), Some(deleted_org_cancel_key(sub).as_str()));
        assert_eq!(calls[0].form.get("prorate").map(String::as_str), Some("false"));
        assert_eq!(calls[0].form.get("invoice_now").map(String::as_str), Some("false"));
    }
    assert!(rig.stripe.calls("DELETE", &format!("/v1/subscriptions/{gone}")).is_empty());
    // It is a real step: the first destroying step, before any data is touched.
    let i = |s: &str| STEPS.iter().position(|x| *x == s).unwrap();
    assert!(i("stripe_sweep") < i("revoke_access") && i("stripe_sweep") < i("database"));
    assert_eq!(i("stripe_sweep"), FIRST_DESTRUCTIVE_STEP);
    assert!(is_destructive("stripe_sweep"));
}

fn webhook_event(id: &str, ty: &str, customer: &str, object: Value) -> (String, String) {
    use hmac::{Hmac, Mac};
    let mut o = object;
    o["customer"] = json!(customer);
    let body = json!({"id": id, "type": ty, "data": {"object": o}}).to_string();
    let t = Utc::now().timestamp();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(b"whsec_org_deletion_test").unwrap();
    mac.update(format!("{t}.").as_bytes());
    mac.update(body.as_bytes());
    (body, format!("t={t},v1={}", hex::encode(mac.finalize().into_bytes())))
}

async fn post_webhook(state: &Arc<crate::AppState>, body: String, sig: String) -> (axum::http::StatusCode, Value) {
    use axum::response::IntoResponse;
    let mut h = axum::http::HeaderMap::new();
    h.insert("stripe-signature", sig.parse().unwrap());
    let resp = crate::routes::stripe_webhook::stripe_webhook(axum::extract::State(state.clone()), h, body.into())
        .await
        .into_response();
    let status = resp.status();
    (status, body_json(resp).await)
}

/// (c) `checkout.session.completed` and `customer.subscription.created` for
/// an org with a live deletion (or a revocation) cancel the subscription on
/// the spot instead of activating it; an ordinary org is untouched.
#[tokio::test]
async fn checkout_and_subscription_created_webhooks_cancel_for_a_deleting_org() {
    let Some(owner) = pg_test_pool("org_deletion::billing_webhook").await else { return };
    let rig = Rig::new(&owner).await;
    std::env::set_var("STRIPE_WEBHOOK_SECRET", "whsec_org_deletion_test");
    let mut state = crate::test_app_state_owned_with_db_and_store(rig.app.clone(), None);
    state.stripe = Some(rig.stripe.client.clone());
    let state = Arc::new(state);

    let o = selfserve_org(&owner, false).await;
    let cus = format!("cus_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, o.account, &cus, None, "canceled").await;
    let ordinary = billing_block_reason(&rig.app, o.account).await.unwrap();
    let d = request(&rig, &o).await;
    let requested = billing_block_reason(&rig.app, o.account).await.unwrap();
    for sub in ["sub_checkout_late", "sub_created_late"] {
        rig.stripe.respond("DELETE", &format!("/v1/subscriptions/{sub}"), 200, sub_json(sub, "canceled"));
    }

    let (b, s) = webhook_event(
        "evt_co_1",
        "checkout.session.completed",
        &cus,
        json!({"id": "cs_late", "mode": "subscription", "subscription": "sub_checkout_late", "payment_intent": null}),
    );
    let (c_status, c_body) = post_webhook(&state, b, s).await;
    let (b, s) = webhook_event("evt_sc_1", "customer.subscription.created", &cus, json!({"id": "sub_created_late", "status": "active"}));
    let (s_status, s_body) = post_webhook(&state, b, s).await;
    let late_checkout = rig.stripe.calls("DELETE", "/v1/subscriptions/sub_checkout_late");
    let late_created = rig.stripe.calls("DELETE", "/v1/subscriptions/sub_created_late");
    let no_session_refetch = rig.stripe.calls("GET", "/v1/checkout/sessions/cs_late");

    // An org revoked in Clerk with no in-app deletion is blocked too.
    let r = selfserve_org(&owner, false).await;
    let r_cus = format!("cus_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, r.account, &r_cus, None, "canceled").await;
    crate::clerk_provision::revoke_org_access(&rig.app, &r.org).await.unwrap();
    let revoked = billing_block_reason(&rig.app, r.account).await.unwrap();
    rig.stripe.respond("DELETE", "/v1/subscriptions/sub_revoked_late", 200, sub_json("sub_revoked_late", "canceled"));
    let (b, s) = webhook_event("evt_sc_2", "customer.subscription.created", &r_cus, json!({"id": "sub_revoked_late"}));
    let (r_status, _) = post_webhook(&state, b, s).await;

    // An ordinary org's subscription.created is accepted and ignored.
    let n = selfserve_org(&owner, false).await;
    let n_cus = format!("cus_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, n.account, &n_cus, None, "canceled").await;
    let (b, s) = webhook_event("evt_sc_3", "customer.subscription.created", &n_cus, json!({"id": "sub_ordinary"}));
    let (n_status, n_body) = post_webhook(&state, b, s).await;
    std::env::remove_var("STRIPE_WEBHOOK_SECRET");

    assert_eq!(ordinary, None);
    assert_eq!(requested.as_deref(), Some("org_deletion_requested"));
    assert_eq!(revoked.as_deref(), Some("org_revoked"));
    assert_eq!((c_status, c_body["refused"].as_str()), (axum::http::StatusCode::OK, Some("org_deleting")));
    assert_eq!((s_status, s_body["refused"].as_str()), (axum::http::StatusCode::OK, Some("org_deleting")));
    assert_eq!(late_checkout.len(), 1, "the Checkout's subscription was cancelled, not activated");
    assert_eq!(late_checkout[0].idempotency_key.as_deref(), Some("org-deleted-cancel:sub_checkout_late"));
    assert_eq!(late_checkout[0].form.get("prorate").map(String::as_str), Some("false"));
    assert_eq!(late_created.len(), 1);
    assert!(no_session_refetch.is_empty(), "nothing was credited or activated from the session");
    assert_eq!(r_status, axum::http::StatusCode::OK);
    assert_eq!(rig.stripe.calls("DELETE", "/v1/subscriptions/sub_revoked_late").len(), 1);
    assert_eq!(n_status, axum::http::StatusCode::OK);
    assert_eq!(n_body["ignored"], true);
    assert!(rig.stripe.calls("DELETE", "/v1/subscriptions/sub_ordinary").is_empty());
    let _ = d;
}

/// A webhook state wired to the rig's app pool and mock Stripe.
fn webhook_state(rig: &Rig) -> Arc<crate::AppState> {
    std::env::set_var("STRIPE_WEBHOOK_SECRET", "whsec_org_deletion_test");
    let mut state = crate::test_app_state_owned_with_db_and_store(rig.app.clone(), None);
    state.stripe = Some(rig.stripe.client.clone());
    Arc::new(state)
}

/// A revoked org's late subscription whose cancel FAILS answers 500 (Stripe
/// redelivers; with the deletion flag off there is no `stripe_sweep`), and the
/// redelivery that succeeds answers 200 under the one `org-deleted-cancel`
/// key. A Stripe "already canceled" error is success: 200.
#[tokio::test]
async fn a_failed_cancel_in_the_refusal_answers_500_and_the_redelivery_succeeds() {
    let Some(owner) = pg_test_pool("org_deletion::refusal_cancel_retry").await else { return };
    let rig = Rig::new(&owner).await;
    let state = webhook_state(&rig);

    let r = selfserve_org(&owner, false).await;
    let cus = format!("cus_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, r.account, &cus, None, "canceled").await;
    crate::clerk_provision::revoke_org_access(&rig.app, &r.org).await.unwrap();

    // Stripe refuses the cancel and the re-read says it is still live.
    let path = "/v1/subscriptions/sub_retry";
    let live = Arc::new(AtomicBool::new(true));
    let (g, d) = (live.clone(), live.clone());
    rig.stripe.respond_with("GET", path, move |_| (200, sub_json("sub_retry", if g.load(Ordering::SeqCst) { "active" } else { "canceled" })));
    rig.stripe.respond("DELETE", path, 400, json!({"error": {"type": "invalid_request_error", "message": "boom"}}));
    let (b, s) = webhook_event("evt_retry_1", "customer.subscription.created", &cus, json!({"id": "sub_retry"}));
    let (first, _) = post_webhook(&state, b, s).await;

    // Redelivery: Stripe now accepts the cancel.
    rig.stripe.respond_with("DELETE", path, move |_| {
        d.store(false, Ordering::SeqCst);
        (200, sub_json("sub_retry", "canceled"))
    });
    let (b, s) = webhook_event("evt_retry_1", "customer.subscription.created", &cus, json!({"id": "sub_retry"}));
    let (second, body) = post_webhook(&state, b, s).await;
    let keys: std::collections::BTreeSet<_> =
        rig.stripe.calls("DELETE", path).into_iter().map(|c| c.idempotency_key.unwrap_or_default()).collect();

    // Already canceled at Stripe: the DELETE errors, the re-read says canceled.
    let a = selfserve_org(&owner, false).await;
    let a_cus = format!("cus_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, a.account, &a_cus, None, "canceled").await;
    crate::clerk_provision::revoke_org_access(&rig.app, &a.org).await.unwrap();
    rig.stripe.respond("DELETE", "/v1/subscriptions/sub_gone", 400, json!({"error": {"type": "invalid_request_error", "message": "already canceled"}}));
    rig.stripe.respond("GET", "/v1/subscriptions/sub_gone", 200, sub_json("sub_gone", "canceled"));
    let (b, s) = webhook_event("evt_gone_1", "customer.subscription.created", &a_cus, json!({"id": "sub_gone"}));
    let (gone, _) = post_webhook(&state, b, s).await;
    std::env::remove_var("STRIPE_WEBHOOK_SECRET");

    assert_eq!(first, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!((second, body["refused"].as_str()), (axum::http::StatusCode::OK, Some("org_deleting")));
    assert_eq!(keys, ["org-deleted-cancel:sub_retry".to_string()].into(), "one cancel key across both deliveries");
    assert_eq!(gone, axum::http::StatusCode::OK);
}

/// A personal account that carries an ADOPTED personal Clerk org (a
/// `personal_orgs` row) is never blocked, even once that org is revoked: the
/// person's Pro Checkout reaches the normal handler. A team org with a
/// revocation is still refused.
#[tokio::test]
async fn a_personal_account_with_a_revoked_adopted_org_is_not_blocked() {
    let Some(owner) = pg_test_pool("org_deletion::personal_not_blocked").await else { return };
    let rig = Rig::new(&owner).await;
    let state = webhook_state(&rig);

    let (p_account, _, p_org) = seed_personal_org_account(&owner).await;
    let p_cus = format!("cus_{}", Uuid::new_v4().simple());
    seed_subscription(&owner, p_account, &p_cus, None, "canceled").await;
    crate::clerk_provision::revoke_org_access(&rig.app, &p_org).await.unwrap();
    let personal = billing_block_reason(&rig.app, p_account).await.unwrap();
    let (b, s) = webhook_event(
        "evt_personal_co",
        "checkout.session.completed",
        &p_cus,
        json!({"id": "cs_personal", "mode": "subscription", "subscription": "sub_personal", "payment_intent": null}),
    );
    let (_, body) = post_webhook(&state, b, s).await;
    let refetched = rig.stripe.calls("GET", "/v1/checkout/sessions/cs_personal");
    let cancelled = rig.stripe.calls("DELETE", "/v1/subscriptions/sub_personal");

    let t = selfserve_org(&owner, false).await;
    crate::clerk_provision::revoke_org_access(&rig.app, &t.org).await.unwrap();
    let team = billing_block_reason(&rig.app, t.account).await.unwrap();
    std::env::remove_var("STRIPE_WEBHOOK_SECRET");

    assert_eq!(personal, None);
    assert!(body.get("refused").is_none(), "not refused: {body}");
    assert_eq!(refetched.len(), 1, "the Checkout went on to the activation path");
    assert!(cancelled.is_empty(), "the personal subscription was not cancelled");
    assert_eq!(team.as_deref(), Some("org_revoked"));
}

/// Once Clerk has deleted the org (an `org_revocations` row) a deletion in
/// grace can no longer be canceled: not by the owner's undo, not by the
/// operator, and not by a direct UPDATE (the 0389 trigger, app role AND owner).
#[tokio::test]
async fn nothing_cancels_a_deletion_once_clerk_deleted_the_org() {
    let Some(owner) = pg_test_pool("org_deletion::no_undo_after_clerk").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    assert_eq!(by_id(&owner, d.id).await.state, "grace");
    sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)").bind(&o.org).execute(&owner).await.unwrap();

    let by_owner = cancel_by_owner(&rig.app, o.creator, &o.org).await.unwrap();
    let by_operator = cancel_by_operator(&owner, d.id, "Alex").await.unwrap();
    let direct = |pool: PgPool| async move {
        let mut tx = scoped(&pool, o.account).await.unwrap();
        let r = sqlx::query("UPDATE org_deletions SET state = 'canceled', canceled_at = now(), canceled_by = 'x' WHERE id = $1")
            .bind(d.id)
            .execute(&mut *tx)
            .await;
        r.err().map(|e| e.to_string())
    };
    let direct_app = direct(rig.app.clone()).await;
    let direct_owner = direct(owner.clone()).await;
    let after = by_id(&owner, d.id).await;

    assert_eq!(by_owner, CancelResult::RevokedInClerk);
    assert_eq!(by_operator, CancelResult::RevokedInClerk);
    for e in [direct_app, direct_owner] {
        let e = e.expect("a direct cancel must be refused");
        assert!(e.contains("org already deleted in Clerk; deletion can't be undone"), "{e}");
    }
    assert_eq!(after.state, "grace");
    assert!(after.canceled_at.is_none());
}

/// (e) the unpaid sweep skips an org with a live comped grant, and confirms
/// with Stripe before inserting: a subscription Stripe reports live drops the
/// unpaid clock and nothing is inserted; without a Stripe client nobody enters.
#[tokio::test]
async fn the_unpaid_sweep_skips_comped_orgs_and_confirms_with_stripe() {
    let Some(owner) = pg_test_pool("org_deletion::unpaid_confirm").await else { return };
    let rig = Rig::new(&owner).await;
    let comped = selfserve_org(&owner, false).await;
    let lagging = selfserve_org(&owner, false).await;
    let truly = selfserve_org(&owner, false).await;
    for (o, status) in [(&comped, "past_due"), (&lagging, "past_due"), (&truly, "past_due")] {
        seed_subscription(&owner, o.account, &format!("cus_{}", o.account.simple()), Some(&format!("sub_{}", o.account.simple())), status).await;
    }
    // A live comped grant (operator-granted).
    let plan: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'founder_comp'").fetch_one(&owner).await.unwrap();
    exec_fixture(
        &owner,
        comped.account,
        sqlx::query("INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) VALUES ($1, $2, NULL, false)")
            .bind(comped.account)
            .bind(plan),
    )
    .await
    .unwrap();
    // Stripe: `lagging` actually has an active subscription; `truly` has none live.
    let lag_cus = format!("cus_{}", lagging.account.simple());
    let truly_cus = format!("cus_{}", truly.account.simple());
    rig.stripe.respond_with("GET", "/v1/subscriptions", move |r| {
        let cus = r.query.get("customer").cloned().unwrap_or_default();
        if cus == lag_cus {
            (200, json!({"data": [{"id": "sub_new_active", "status": "active"}], "has_more": false}))
        } else if cus == truly_cus {
            (200, json!({"data": [{"id": "sub_old", "status": "canceled"}], "has_more": false}))
        } else {
            (200, json!({"data": [], "has_more": false}))
        }
    });
    let _ = unpaid_entry(&rig.app, Some(&*rig.stripe.client)).await.unwrap();
    for o in [&comped, &lagging, &truly] {
        sqlx::query("UPDATE org_unpaid_watch SET unpaid_since = now() - interval '31 days' WHERE account_id = $1")
            .bind(o.account)
            .execute(&owner)
            .await
            .unwrap();
    }
    let watched_comped: i64 = sqlx::query_scalar("SELECT count(*) FROM org_unpaid_watch WHERE account_id = $1")
        .bind(comped.account)
        .fetch_one(&owner)
        .await
        .unwrap();
    // Without Stripe nobody can be confirmed: nobody enters.
    let _ = unpaid_entry(&rig.app, None).await.unwrap();
    let none_without_stripe = live_for_account(&owner, truly.account).await.unwrap();
    let _ = unpaid_entry(&rig.app, Some(&*rig.stripe.client)).await.unwrap();
    let comped_live = live_for_account(&owner, comped.account).await.unwrap();
    let lagging_live = live_for_account(&owner, lagging.account).await.unwrap();
    let truly_live = live_for_account(&owner, truly.account).await.unwrap();
    let lagging_clock: i64 = sqlx::query_scalar("SELECT count(*) FROM org_unpaid_watch WHERE account_id = $1")
        .bind(lagging.account)
        .fetch_one(&owner)
        .await
        .unwrap();

    assert_eq!(watched_comped, 0, "a comped org is never watched");
    assert!(none_without_stripe.is_none());
    assert!(comped_live.is_none());
    assert!(lagging_live.is_none(), "Stripe says the subscription is live");
    assert_eq!(lagging_clock, 0, "and the unpaid clock was dropped");
    assert_eq!(truly_live.expect("a truly unpaid org enters").initiated_by, "unpaid");
}

// ── HIGH 3: the shared access predicate ──────────────────────────────────

/// `live_org_membership` (the predicate behind the Microsoft add-in sessions,
/// the Teams bot, Slack interactivity and MCP selections)
/// answers "no member" for everyone while a deletion is requested, in grace,
/// awaiting approval or destroying, and "member" again after an undo.
#[tokio::test]
async fn live_org_membership_refuses_everyone_while_a_deletion_is_live() {
    use crate::middleware::auth::live_org_membership;
    let Some(owner) = pg_test_pool("org_deletion::live_membership").await else { return };
    let mut rig = Rig::new(&owner).await;
    rig.approval_first_n = i64::MAX;
    let o = selfserve_org(&owner, false).await;
    let who = [o.creator, o.admin, o.member];
    let all = |rig: &Rig, org: String, who: [Uuid; 3]| {
        let pool = rig.app.clone();
        async move {
            let mut v = Vec::new();
            for u in who {
                v.push(live_org_membership(&pool, &org, u).await.unwrap().is_some());
            }
            v
        }
    };
    let before = all(&rig, o.org.clone(), who).await;
    let d = request(&rig, &o).await;
    let requested = all(&rig, o.org.clone(), who).await;
    rig.drive(d.id).await;
    let grace = all(&rig, o.org.clone(), who).await;
    end_grace(&owner, d.id).await;
    rig.drive(d.id).await;
    assert_eq!(by_id(&owner, d.id).await.state, "awaiting_approval");
    let awaiting = all(&rig, o.org.clone(), who).await;
    assert_eq!(approve(&owner, d.id, "Alex").await.unwrap(), "approved");
    sqlx::query("UPDATE org_deletions SET state = 'destroying', destroying_since = now() WHERE id = $1")
        .bind(d.id)
        .execute(&owner)
        .await
        .unwrap();
    let destroying = all(&rig, o.org.clone(), who).await;

    // An undone deletion gives access back.
    let o2 = selfserve_org(&owner, false).await;
    let d2 = request(&rig, &o2).await;
    rig.drive(d2.id).await;
    let during = all(&rig, o2.org.clone(), [o2.creator, o2.admin, o2.member]).await;
    assert_eq!(cancel_by_owner(&rig.app, o2.creator, &o2.org).await.unwrap(), CancelResult::Canceled);
    let undone = all(&rig, o2.org.clone(), [o2.creator, o2.admin, o2.member]).await;

    assert_eq!(before, vec![true, true, true]);
    for (name, v) in [("requested", requested), ("grace", grace), ("awaiting_approval", awaiting), ("destroying", destroying), ("undo window", during)] {
        assert_eq!(v, vec![false, false, false], "{name}");
    }
    assert_eq!(undone, vec![true, true, true]);
}

// ── member notices, approval alert, cancel note, closing-down read ───────

async fn email_of(pool: &PgPool, user: Uuid) -> String {
    sqlx::query_scalar("SELECT email FROM users WHERE id = $1").bind(user).fetch_one(pool).await.unwrap()
}

fn mails_to(rig: &Rig, to: &str) -> Vec<Mail> {
    rig.mailer.sent.lock().unwrap().iter().filter(|m| m.to == to).cloned().collect()
}

/// Founder decision: every member other than the owner is told, at request
/// time, that the org is being deleted, that access has ended, and that their
/// private notes in it are deleted on the grace end date with no copy for them
/// unless the owner undoes it. One email per member per deletion, across a
/// crash between the step's side effect and its checkpoint.
#[tokio::test]
async fn every_other_member_is_told_once_that_their_private_notes_will_be_deleted() {
    let Some(owner) = pg_test_pool("org_deletion::member_notice").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let d = request(&rig, &o).await;

    let crash: CrashHook = Arc::new(|s: &str| s == "notify_members");
    let crashed = rig.drive_with(d.id, Some(crash)).await;
    let mid = by_id(&owner, d.id).await;
    let resumed = rig.drive(d.id).await;
    let after = by_id(&owner, d.id).await;

    assert_eq!(crashed, RunEnd::Crashed);
    assert!(!mid.has("notify_members"));
    assert_eq!(mid.members_notified.len(), 2, "the ledger was written before the crash");
    assert_eq!(resumed, RunEnd::Waiting);
    assert!(after.has("notify_members"));
    let (admin, member, creator) = (email_of(&owner, o.admin).await, email_of(&owner, o.member).await, email_of(&owner, o.creator).await);
    for to in [&admin, &member] {
        let got = mails_to(&rig, to);
        assert_eq!(got.len(), 1, "exactly one notice for {to}");
        let t = &got[0].text;
        assert!(t.contains("no longer have access"), "{t}");
        assert!(t.contains("Your private notes in this organization will be permanently deleted on"), "{t}");
        assert!(t.contains(&after.grace_ends_at.format("%B %-d, %Y").to_string()), "grace end date: {t}");
        assert!(t.contains("unless the organization's owner undoes the deletion"), "{t}");
        assert!(t.contains("You will not get a copy of them"), "{t}");
        assert_eq!(got[0].idempotency_key.as_deref(), Some(format!("org-deletion-member:{}:{}", d.id, if to == &admin { o.admin } else { o.member }).as_str()));
    }
    let to_owner = mails_to(&rig, &creator);
    assert_eq!(to_owner.len(), 1, "the owner gets only the request email");
    assert!(!to_owner[0].text.contains("private notes in this organization"));
}

/// A send that fails is alerted and retried without re-sending to who
/// already got theirs.
#[tokio::test]
async fn a_failed_member_notice_is_retried_without_resending_the_others() {
    let Some(owner) = pg_test_pool("org_deletion::member_notice_retry").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let (admin, member) = (email_of(&owner, o.admin).await, email_of(&owner, o.member).await);
    rig.mailer.failing.lock().unwrap().push(member.clone());
    let d = request(&rig, &o).await;

    let first = rig.drive(d.id).await;
    let mid = by_id(&owner, d.id).await;
    rig.mailer.failing.lock().unwrap().clear();
    let second = rig.drive(d.id).await;
    let after = by_id(&owner, d.id).await;

    assert_eq!(first, RunEnd::Failed, "the step is retried, not skipped");
    assert!(!mid.has("notify_members"));
    assert_eq!(mid.members_notified.len(), 1, "only the admin was recorded");
    assert_eq!(second, RunEnd::Waiting);
    assert!(after.has("notify_members"));
    assert_eq!(mails_to(&rig, &admin).len(), 1, "the admin is not emailed again");
    assert_eq!(mails_to(&rig, &member).len(), 1);
}

/// The deletion that waits more than 7 days for an operator pages once, then
/// again no sooner than 24 hours later, and stops when approved.
#[tokio::test]
async fn a_deletion_waiting_over_seven_days_for_approval_alerts_once_a_day() {
    let Some(owner) = pg_test_pool("org_deletion::approval_overdue").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    let d = request(&rig, &o).await;
    rig.drive(d.id).await;
    end_grace(&owner, d.id).await;
    // Needs approval: the first-N rule.
    let mut rig = rig;
    rig.approval_first_n = 1_000_000;
    rig.drive(d.id).await;
    let waiting = by_id(&owner, d.id).await;
    assert_eq!(waiting.state, "awaiting_approval");
    assert!(waiting.awaiting_approval_since.is_some(), "the wait has a start");

    // Other tests' deletions may be waiting too: look for THIS one.
    async fn fired_for(pool: &PgPool, id: Uuid) -> bool {
        let rows: Vec<(Uuid,)> = sqlx::query_as("SELECT id FROM org_deletion_approval_overdue(7, 24)")
            .fetch_all(pool)
            .await
            .unwrap();
        rows.iter().any(|r| r.0 == id)
    }
    let fired = |id: Uuid| fired_for(&owner, id);
    // Waiting only 6 days: quiet.
    sqlx::query("UPDATE org_deletions SET awaiting_approval_since = now() - interval '6 days' WHERE id = $1").bind(d.id).execute(&owner).await.unwrap();
    assert!(!fired(d.id).await, "6 days is not overdue");
    // 8 days: fires once.
    sqlx::query("UPDATE org_deletions SET awaiting_approval_since = now() - interval '8 days' WHERE id = $1").bind(d.id).execute(&owner).await.unwrap();
    assert!(fired(d.id).await, "8 days fires");
    assert!(!fired(d.id).await, "and only once inside 24 hours");
    // A day later it fires again.
    sqlx::query("UPDATE org_deletions SET approval_alerted_at = now() - interval '25 hours' WHERE id = $1").bind(d.id).execute(&owner).await.unwrap();
    assert!(fired(d.id).await, "repeats after the interval");
    // The service wrapper the sweep calls: returns how many fired, never errs.
    sqlx::query("UPDATE org_deletions SET approval_alerted_at = now() - interval '25 hours' WHERE id = $1").bind(d.id).execute(&owner).await.unwrap();
    assert!(alert_approval_overdue(&rig.app, APPROVAL_OVERDUE_DAYS, APPROVAL_ALERT_REPEAT_HOURS).await.unwrap() >= 1);
    // Approved: no more pages.
    sqlx::query("UPDATE org_deletions SET approval_alerted_at = now() - interval '25 hours' WHERE id = $1").bind(d.id).execute(&owner).await.unwrap();
    assert_eq!(approve(&owner, d.id, "Alex").await.unwrap(), "approved");
    assert!(!fired(d.id).await, "an approved deletion is not overdue");
}

/// `org_deletion_admin cancel` on an unpaid-origin deletion says the org goes
/// back into deletion after 7 days if it is still unpaid, and when.
#[tokio::test]
async fn operator_cancel_of_an_unpaid_deletion_says_when_it_re_enters() {
    let Some(owner) = pg_test_pool("org_deletion::operator_cancel_note").await else { return };
    let rig = Rig::new(&owner).await;
    let unpaid = selfserve_org(&owner, false).await;
    let d = request(&rig, &unpaid).await;
    rig.drive(d.id).await;
    sqlx::query("UPDATE org_deletions SET initiated_by = 'unpaid' WHERE id = $1").bind(d.id).execute(&owner).await.unwrap();
    sqlx::query("INSERT INTO org_unpaid_watch (account_id, clerk_org_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(unpaid.account)
        .bind(&unpaid.org)
        .execute(&owner)
        .await
        .unwrap();
    let result = cancel_by_operator(&owner, d.id, "Alex").await.unwrap();
    let msg = operator_cancel_message(&owner, d.id, result).await.unwrap();
    let not_before: DateTime<Utc> = sqlx::query_scalar("SELECT not_before FROM org_unpaid_watch WHERE account_id = $1")
        .bind(unpaid.account)
        .fetch_one(&owner)
        .await
        .unwrap();

    assert_eq!(result, CancelResult::Canceled);
    assert!(msg.starts_with("Canceled"), "{msg}");
    assert!(msg.contains("re-enters deletion after"), "{msg}");
    assert!(msg.contains("if it is still unpaid"), "{msg}");
    assert!(msg.contains(&not_before.format("%Y-%m-%d %H:%M UTC").to_string()), "names the date: {msg}");

    // An owner-origin deletion gets no such note.
    let paid = selfserve_org(&owner, false).await;
    let d2 = request(&rig, &paid).await;
    rig.drive(d2.id).await;
    let r2 = cancel_by_operator(&owner, d2.id, "Alex").await.unwrap();
    assert_eq!(operator_cancel_message(&owner, d2.id, r2).await.unwrap(), "Canceled");
}

/// The read the Slack seat and unmatched-email gates use.
#[tokio::test]
async fn an_org_is_closing_down_while_a_deletion_locks_members_out_or_it_is_revoked() {
    let Some(owner) = pg_test_pool("org_deletion::closing_down").await else { return };
    let rig = Rig::new(&owner).await;
    let o = selfserve_org(&owner, false).await;
    assert!(!org_closing_down(&rig.app, &o.org).await.unwrap());
    assert!(!account_org_closing_down(&rig.app, o.account).await);
    let d = request(&rig, &o).await;
    assert!(org_closing_down(&rig.app, &o.org).await.unwrap(), "requested");
    assert!(account_org_closing_down(&rig.app, o.account).await);
    rig.drive(d.id).await;
    assert!(org_closing_down(&rig.app, &o.org).await.unwrap(), "grace");
    assert_eq!(cancel_by_owner(&rig.app, o.creator, &o.org).await.unwrap(), CancelResult::Canceled);
    assert!(!org_closing_down(&rig.app, &o.org).await.unwrap(), "undone");
    sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)").bind(&o.org).execute(&owner).await.unwrap();
    assert!(org_closing_down(&rig.app, &o.org).await.unwrap(), "revoked");
}

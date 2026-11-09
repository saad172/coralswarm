//! Live-Postgres proof of `GET /v1/journal/gist` (`services::journal_gist`),
//! driven through the REAL production handler over the `coralswarm_app` login
//! (RLS applies as in production) with a hand-built `UserContext`, because
//! "two members of one account" is a shape a plain personal Clerk token cannot
//! produce (see `journal_endpoint_pg.rs`'s item 5).
//!
//! Fixture: one account with two members (A the owner, A2), one shared ocean
//! and one private (`member`) ocean per member, and stored gists for one day.
//!
//!   1. Mine merges the caller's own gists across the oceans it can read and
//!      never another member's (A2's private gist and A2's own member gist in
//!      the shared ocean appear nowhere in A's answer).
//!   2. Team needs `graph_id`; it returns the ocean's team gist; naming
//!      another member's private ocean is a 404 even though a (planted) team
//!      row exists there.
//!   3. The period resolves from `anchor` + `tz`, a stored row compiled in
//!      another zone is still found by containment, `open` is false for a
//!      past day, and an empty stored gist reads as `null`.
//!   4. `span` is the caller's own first/last activity (Mine) or everyone's
//!      in the ocean (Team).
//!   5. Deleting a rollup retracts the overlapping gists (the 0218/0404
//!      trigger): the member's own and the ocean's team gist, not the
//!      member's gist in another ocean.
//!   6. The flag: 404 while `TODAY_GIST_ENABLED` is unset; 400 on bad params.
//!
//! Env-gated on `ORGT_TEST_BASE_URL`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test journal_gist_pg

mod support;

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::{journal_gist, UserContext};
use support::rls_probe::RlsProbe;

/// Serializes the shared-process `TODAY_GIST_ENABLED` mutations.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());
const FLAG: &str = "TODAY_GIST_ENABLED";

fn at(d: u32, h: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, d, h, 0, 0).unwrap()
}

struct World {
    probe: RlsProbe,
    account: Uuid,
    a: Uuid,
    a2: Uuid,
    shared: Uuid,
    a_private: Uuid,
    a2_private: Uuid,
}

async fn user(owner: &PgPool, tag: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users(email, name) VALUES ($1, $2) RETURNING id")
        .bind(format!("{tag}_{}@gist.example", Uuid::new_v4().simple()))
        .bind(tag)
        .fetch_one(owner)
        .await
        .expect("insert user")
}

async fn world(db: &str) -> Option<World> {
    let probe = RlsProbe::setup(db).await?;
    let o = &probe.owner;
    let a = user(o, "a").await;
    let a2 = user(o, "a2").await;
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(a)
            .fetch_one(o)
            .await
            .unwrap();
    for (u, role) in [(a, "owner"), (a2, "member")] {
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)")
            .bind(account)
            .bind(u)
            .bind(role)
            .execute(o)
            .await
            .unwrap();
    }
    let shared: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name) VALUES ($1, 'Platform') RETURNING id",
    )
    .bind(account)
    .fetch_one(o)
    .await
    .unwrap();
    let mut private = Vec::new();
    for (u, name) in [(a, "Private A"), (a2, "Private A2")] {
        private.push(
            sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO graphs (account_id, name, kind, owner_user_id) VALUES ($1, $2, 'member', $3) RETURNING id",
            )
            .bind(account)
            .bind(name)
            .bind(u)
            .fetch_one(o)
            .await
            .unwrap(),
        );
    }
    Some(World {
        account,
        a,
        a2,
        shared,
        a_private: private[0],
        a2_private: private[1],
        probe,
    })
}

impl World {
    fn tenant(&self) -> Tenant {
        Tenant {
            store: Some(Arc::new(PostgresStore::new(self.probe.app_pool.clone()))),
            pool: Some(self.probe.app_pool.clone()),
            decision: None,
            class: coralswarm_backend::route_class::RouteClass::Other,
        }
    }

    fn ctx(&self, user_id: Uuid) -> UserContext {
        UserContext {
            user_id,
            account_id: self.account,
            role: "member".into(),
            device_id: "test".into(),
        }
    }

    async fn get(&self, user_id: Uuid, query: &[(&str, &str)]) -> (StatusCode, Value) {
        let query:std::collections::BTreeMap<String,String>=query.iter().map(|(k,v)|(k.to_string(),v.to_string())).collect();
        let _ = support::read_models::warm::<coralswarm_backend::read_models::today::JournalGist>(&self.probe.app_pool,self.account,user_id,query.clone()).await;
        let mut state=(*coralswarm_backend::test_app_state()).clone();
        state.db=Some(self.probe.app_pool.clone());
        state.store=Some(Arc::new(PostgresStore::new(self.probe.app_pool.clone())));
        let resp=journal_gist(axum::extract::State(Arc::new(state)),self.tenant(),axum::Extension(self.ctx(user_id)),None,Query(query)).await;
        let status=resp.status();
        let bytes=axum::body::to_bytes(resp.into_body(),usize::MAX).await.unwrap();
        (status,support::read_models::payload(serde_json::from_slice(&bytes).unwrap_or(Value::Null)))
    }

    #[allow(clippy::too_many_arguments)]
    async fn gist(
        &self,
        graph: Uuid,
        user: Option<Uuid>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        tz: &str,
        gist: Value,
        child_count: i64,
    ) {
        sqlx::query(
            "INSERT INTO journal_gists (account_id, graph_id, scope, user_id, granularity, period_start, period_end, tz, \
                                        gist, stats, input_fingerprint, model) \
             VALUES ($1, $2, $3, $4, 'day', $5, $6, $7, $8, $9, 'fp', 'test-model')",
        )
        .bind(self.account)
        .bind(graph)
        .bind(if user.is_some() { "member" } else { "team" })
        .bind(user)
        .bind(start)
        .bind(end)
        .bind(tz)
        .bind(gist)
        .bind(json!({"child_count": child_count}))
        .execute(&self.probe.owner)
        .await
        .expect("insert gist");
    }

    async fn rollup(&self, graph: Uuid, user: Uuid, hour: DateTime<Utc>) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO journal_rollups (account_id, user_id, graph_id, hour_start, period_start, summary, input_fingerprint, model) \
             VALUES ($1, $2, $3, $4, $4, 'Billing — shipped', 'fp', 'm') RETURNING id",
        )
        .bind(self.account)
        .bind(user)
        .bind(graph)
        .bind(hour)
        .fetch_one(&self.probe.owner)
        .await
        .expect("insert rollup")
    }

    async fn atom(&self, graph: Uuid, by: Uuid, when: DateTime<Utc>) {
        sqlx::query(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, ingested_by, \"timestamp\") \
             VALUES ($1, 'note', $2, $3, $4, $5)",
        )
        .bind(graph)
        .bind(format!("note-{}", Uuid::new_v4()))
        .bind(format!("blob-{}", Uuid::new_v4()))
        .bind(by)
        .bind(when)
        .execute(&self.probe.owner)
        .await
        .expect("insert atom");
    }

    async fn gist_count(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM journal_gists")
            .fetch_one(&self.probe.owner)
            .await
            .unwrap()
    }
}

/// The 2026-10-28 UTC day, and its `anchor`/`tz` query.
const DAY: &[(&str, &str)] = &[("anchor", "2026-10-28"), ("tz", "UTC")];

fn with<'a>(extra: &'a [(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    DAY.iter().copied().chain(extra.iter().copied()).collect()
}

async fn seed(w: &World) {
    let (s, e) = (at(28, 0), at(29, 0));
    w.gist(
        w.shared,
        Some(w.a),
        s,
        e,
        "UTC",
        json!({"done": ["Shared done by A"], "decided": ["Shared decision"], "open": []}),
        6,
    )
    .await;
    w.gist(
        w.a_private,
        Some(w.a),
        s,
        e,
        "UTC",
        json!({"done": ["Private done by A"], "decided": [], "open": ["Private open"]}),
        2,
    )
    .await;
    w.gist(
        w.shared,
        Some(w.a2),
        s,
        e,
        "UTC",
        json!({"done": ["SECRET A2 member gist"], "decided": [], "open": []}),
        9,
    )
    .await;
    w.gist(
        w.a2_private,
        Some(w.a2),
        s,
        e,
        "UTC",
        json!({"done": ["SECRET A2 private"], "decided": [], "open": []}),
        9,
    )
    .await;
    w.gist(w.shared, None, s, e, "UTC", json!({"done": ["Team shipped v2"], "decided": ["Team picks Stripe"], "open": ["Team open"]}), 12)
        .await;
    // A planted team row in A2's private ocean: never served to A.
    w.gist(
        w.a2_private,
        None,
        s,
        e,
        "UTC",
        json!({"done": ["SECRET planted team row"], "decided": [], "open": []}),
        9,
    )
    .await;
}

#[tokio::test]
async fn mine_merges_only_the_callers_own_gists_and_team_never_reaches_a_private_ocean() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_gist_isolation").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::set_var(FLAG, "1");
    seed(&w).await;
    w.atom(w.shared, w.a, at(28, 9)).await;
    w.atom(w.a_private, w.a, at(28, 15)).await;
    w.atom(w.shared, w.a2, at(28, 18)).await;
    w.atom(w.a2_private, w.a2, at(28, 20)).await;

    // 1. Mine, as A: both of A's oceans, busiest (shared, 6 children) first.
    let (status, mine) = w.get(w.a, &with(&[("who", "mine")])).await;
    assert_eq!(status, StatusCode::OK, "{mine}");
    assert_eq!(
        mine["gist"]["done"],
        json!(["Shared done by A", "Private done by A"]),
        "{mine}"
    );
    assert_eq!(mine["gist"]["decided"], json!(["Shared decision"]));
    assert_eq!(mine["gist"]["open"], json!(["Private open"]));
    assert_eq!(mine["period_start"], json!("2026-10-28T00:00:00Z"));
    assert_eq!(mine["period_end"], json!("2026-10-29T00:00:00Z"));
    assert_eq!(mine["open"], json!(false));
    assert_eq!(mine["who"], json!("mine"));
    assert!(mine["compiled_at"].is_string());
    assert!(
        !mine.to_string().contains("SECRET"),
        "another member's gist leaked: {mine}"
    );
    // 4. Span: A's own atoms only (09:00 shared .. 15:00 private).
    assert_eq!(
        mine["span"],
        json!({"first_activity": "2026-10-28T09:00:00Z", "last_activity": "2026-10-28T15:00:00Z"})
    );

    // Mine narrowed to one ocean.
    let (_, narrowed) = w
        .get(
            w.a,
            &with(&[("who", "mine"), ("graph_id", &w.a_private.to_string())]),
        )
        .await;
    assert_eq!(
        narrowed["gist"]["done"],
        json!(["Private done by A"]),
        "{narrowed}"
    );

    // A2 symmetric (non-vacuous): sees its own member gists.
    let (_, mine_a2) = w.get(w.a2, &with(&[("who", "mine")])).await;
    assert!(
        mine_a2.to_string().contains("SECRET A2 private"),
        "{mine_a2}"
    );
    assert!(
        !mine_a2.to_string().contains("Private done by A"),
        "{mine_a2}"
    );

    // 2. Team needs graph_id.
    let (status, _) = w.get(w.a, &with(&[("who", "team")])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let shared = w.shared.to_string();
    let (status, team) = w
        .get(w.a, &with(&[("who", "team"), ("graph_id", &shared)]))
        .await;
    assert_eq!(status, StatusCode::OK, "{team}");
    assert_eq!(team["gist"]["done"], json!(["Team shipped v2"]), "{team}");
    assert_eq!(team["who"], json!("team"));
    // Team span: everyone's activity in the ocean (A at 09:00, A2 at 18:00).
    assert_eq!(
        team["span"],
        json!({"first_activity": "2026-10-28T09:00:00Z", "last_activity": "2026-10-28T18:00:00Z"})
    );
    let (status, leak) = w
        .get(
            w.a,
            &with(&[("who", "team"), ("graph_id", &w.a2_private.to_string())]),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{leak}");
    assert!(!leak.to_string().contains("SECRET"));
    let (status, _) = w
        .get(
            w.a,
            &with(&[("who", "mine"), ("graph_id", &w.a2_private.to_string())]),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // 3. Another zone: Chicago's 2026-10-28 contains the UTC row's midpoint?
    // Its midpoint is 2026-10-28T17:00Z, inside the UTC day: still found.
    let (_, chicago) = w
        .get(
            w.a,
            &[
                ("anchor", "2026-10-28"),
                ("tz", "America/Chicago"),
                ("who", "mine"),
            ],
        )
        .await;
    assert_eq!(chicago["period_start"], json!("2026-10-28T05:00:00Z"));
    assert_eq!(chicago["tz"], json!("America/Chicago"));
    assert_eq!(
        chicago["gist"]["done"][0],
        json!("Shared done by A"),
        "{chicago}"
    );
    // No stored row for that day: null gist, null compiled_at.
    let (_, empty) = w.get(w.a, &[("anchor", "2026-10-20"), ("tz", "UTC")]).await;
    assert_eq!(empty["gist"], Value::Null, "{empty}");
    assert_eq!(empty["compiled_at"], Value::Null);
    assert_eq!(empty["span"], Value::Null);
    // Week view of the same anchor: no week rows, null.
    let (_, week) = w.get(w.a, &with(&[("granularity", "week")])).await;
    assert_eq!(week["period_start"], json!("2026-10-26T00:00:00Z"));
    assert_eq!(week["gist"], Value::Null);

    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

#[tokio::test]
async fn an_all_empty_gist_reads_as_null() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_gist_empty").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::set_var(FLAG, "1");
    w.gist(
        w.a_private,
        Some(w.a),
        at(28, 0),
        at(29, 0),
        "UTC",
        json!({"done": [], "decided": [], "open": []}),
        1,
    )
    .await;
    let (status, body) = w.get(w.a, &with(&[])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["gist"], Value::Null, "{body}");
    assert_eq!(body["compiled_at"], Value::Null);
    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

#[tokio::test]
async fn deleting_a_rollup_retracts_the_overlapping_member_and_team_gists() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_gist_retract").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    seed(&w).await;
    let hour = w.rollup(w.shared, w.a, at(28, 10)).await;
    // Another day's rollup, deleted too: overlaps nothing stored.
    let other_day = w.rollup(w.shared, w.a, at(25, 10)).await;
    assert_eq!(w.gist_count().await, 6);

    sqlx::query("DELETE FROM journal_rollups WHERE id = ANY($1)")
        .bind(vec![hour, other_day])
        .execute(&w.probe.owner)
        .await
        .unwrap();

    let left: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT graph_id, user_id FROM journal_gists ORDER BY graph_id, user_id NULLS FIRST",
    )
    .fetch_all(&w.probe.owner)
    .await
    .unwrap();
    assert!(
        !left.contains(&(w.shared, Some(w.a))),
        "A's member gist in the ocean was retracted: {left:?}"
    );
    assert!(
        !left.contains(&(w.shared, None)),
        "the ocean's team gist was retracted: {left:?}"
    );
    assert!(
        left.contains(&(w.shared, Some(w.a2))),
        "A2's own gist read none of A's rollups: {left:?}"
    );
    assert!(
        left.contains(&(w.a_private, Some(w.a))),
        "A's gist in another ocean stays: {left:?}"
    );
    assert_eq!(left.len(), 4);
    w.probe.teardown().await;
}

#[tokio::test]
async fn flag_off_is_404_and_bad_params_are_400() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_gist_flag").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::remove_var(FLAG);
    let (status, _) = w.get(w.a, &with(&[])).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    std::env::set_var(FLAG, "1");
    for bad in [
        vec![("granularity", "hour")],
        vec![("anchor", "2026-02-30")],
        vec![("who", "everyone")],
        vec![("tz", "Mars/Base")],
        vec![("who", "team")],
    ] {
        let (status, body) = w.get(w.a, &bad).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?} -> {body}");
    }
    let (status, body) = w.get(w.a, &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["open"], json!(true), "the default anchor is today");
    assert_eq!(body["granularity"], json!("day"));
    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

async fn call(tenant: Tenant, ctx: UserContext, query: &[(&str, &str)]) -> (StatusCode, Value) {
    let query:std::collections::BTreeMap<String,String>=query.iter().map(|(k,v)|(k.to_string(),v.to_string())).collect();
    let pool=tenant.pool.as_ref().unwrap().clone();
    let _=support::read_models::warm::<coralswarm_backend::read_models::today::JournalGist>(&pool,ctx.account_id,ctx.user_id,query.clone()).await;
    let mut state=(*coralswarm_backend::test_app_state()).clone();state.db=Some(pool.clone());state.store=Some(Arc::new(PostgresStore::new(pool)));
    let resp=journal_gist(axum::extract::State(Arc::new(state)),tenant,axum::Extension(ctx),None,Query(query)).await;
    let status=resp.status();let bytes=axum::body::to_bytes(resp.into_body(),usize::MAX).await.unwrap();
    (status,support::read_models::payload(serde_json::from_slice(&bytes).unwrap_or(Value::Null)))
}

/// Review item 2: on the consumer plane reachability spans every account the
/// user belongs to. Mine reads only the ACTIVE account's oceans. Run on the
/// OWNER pool (RLS bypassed, as production requests run), so the account
/// predicate in the handler is what is under test, not RLS.
#[tokio::test]
async fn mine_reads_only_the_active_accounts_oceans() {
    let _env = ENV_LOCK.lock().await;
    let Some(w) = world("orgt_gist_accounts").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    std::env::set_var(FLAG, "1");
    let o = &w.probe.owner;
    // A second account, owned by someone else, where A is also a member.
    let x = user(o, "x").await;
    let other: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(x)
            .fetch_one(o)
            .await
            .unwrap();
    for (u, role) in [(x, "owner"), (w.a, "member")] {
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)")
            .bind(other)
            .bind(u)
            .bind(role)
            .execute(o)
            .await
            .unwrap();
    }
    let other_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name) VALUES ($1, 'Other company') RETURNING id",
    )
    .bind(other)
    .fetch_one(o)
    .await
    .unwrap();
    w.gist(
        w.a_private,
        Some(w.a),
        at(28, 0),
        at(29, 0),
        "UTC",
        json!({"done": ["Home account work"], "decided": [], "open": []}),
        3,
    )
    .await;
    sqlx::query(
        "INSERT INTO journal_gists (account_id, graph_id, scope, user_id, granularity, period_start, period_end, tz, \
                                    gist, stats, input_fingerprint, model) \
         VALUES ($1, $2, 'member', $3, 'day', $4, $5, 'UTC', $6, $7, 'fp', 'm')",
    )
    .bind(other)
    .bind(other_graph)
    .bind(w.a)
    .bind(at(28, 0))
    .bind(at(29, 0))
    .bind(json!({"done": ["OTHER ACCOUNT work"], "decided": [], "open": []}))
    .bind(json!({"child_count": 9}))
    .execute(o)
    .await
    .unwrap();

    let owner_tenant = || Tenant {
        store: Some(Arc::new(PostgresStore::new(w.probe.owner.clone()))),
        pool: Some(w.probe.owner.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let ctx = |account: Uuid| UserContext {
        user_id: w.a,
        account_id: account,
        role: "member".into(),
        device_id: "test".into(),
    };
    let (status, home) = call(owner_tenant(), ctx(w.account), DAY).await;
    assert_eq!(status, StatusCode::OK, "{home}");
    assert_eq!(home["gist"]["done"], json!(["Home account work"]), "{home}");
    assert!(!home.to_string().contains("OTHER ACCOUNT"), "{home}");
    // Naming the other account's ocean from the home account is a 404.
    let og = other_graph.to_string();
    let (status, _) = call(owner_tenant(), ctx(w.account), &with(&[("graph_id", &og)])).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // Non-vacuous: with the other account active, its ocean is read.
    let (_, away) = call(owner_tenant(), ctx(other), DAY).await;
    assert_eq!(
        away["gist"]["done"],
        json!(["OTHER ACCOUNT work"]),
        "{away}"
    );

    std::env::remove_var(FLAG);
    w.probe.teardown().await;
}

/// Review item 1: a team gist is built only from ocean content. A member's
/// ask (`query_history`) and their rollup prose (which narrates their asks)
/// never reach the team prompt.
#[tokio::test]
async fn a_members_ask_never_reaches_the_team_gist_prompt() {
    use coralswarm_backend::services::journal_gist as jg;
    let Some(w) = world("orgt_gist_team_inputs").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let o = &w.probe.owner;
    for (by, title, h) in [
        (w.a, "Halcyon rollout checklist drafted", 9),
        (w.a2, "Pen-test findings triaged", 11),
    ] {
        sqlx::query(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, ingested_by, \"timestamp\", title) \
             VALUES ($1, 'note', $2, $3, $4, $5, $6)",
        )
        .bind(w.shared)
        .bind(format!("note-{}", Uuid::new_v4()))
        .bind(format!("blob-{}", Uuid::new_v4()))
        .bind(by)
        .bind(at(28, h))
        .bind(title)
        .execute(o)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO query_history (account_id, graph_id, user_id, query_text, created_at) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(w.account)
    .bind(w.shared)
    .bind(w.a)
    .bind("SECRET ASK: is Tom being let go?")
    .bind(at(28, 10))
    .execute(o)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO journal_rollups (account_id, user_id, graph_id, hour_start, period_start, summary, input_fingerprint, model) \
         VALUES ($1, $2, $3, $4, $4, 'asked: SECRET ASK: is Tom being let go?', 'fp', 'm')",
    )
    .bind(w.account)
    .bind(w.a)
    .bind(w.shared)
    .bind(at(28, 10))
    .execute(o)
    .await
    .unwrap();

    let mut tx = w.probe.scoped_tx(w.account).await;
    let items = jg::load_ocean_items(&mut tx, w.shared, at(27, 0), at(30, 0))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let material = jg::team_material(
        &items,
        jg::Granularity::Day,
        at(28, 0),
        at(29, 0),
        chrono_tz::UTC,
    )
    .expect("two members contributed ocean content");
    let prompt = jg::build_prompt(&jg::PromptInput {
        granularity: jg::Granularity::Day,
        period_start: at(28, 0),
        period_end: at(29, 0),
        tz: chrono_tz::UTC,
        open: false,
        team_members: Some(material.member_count),
        children: &material.children,
        decisions: &material.decisions,
    });
    assert!(
        prompt.contains("Halcyon rollout checklist drafted"),
        "{prompt}"
    );
    assert!(prompt.contains("Pen-test findings triaged"), "{prompt}");
    assert!(
        !prompt.contains("SECRET"),
        "a member's ask reached the team prompt: {prompt}"
    );
    assert!(!prompt.contains("asked:"), "{prompt}");
    assert_eq!(material.member_count, 2);
    w.probe.teardown().await;
}

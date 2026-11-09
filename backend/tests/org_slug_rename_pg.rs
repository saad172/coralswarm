//! Clerk `organization.updated` slug reconciliation (org-tenancy).
//!
//! `tenant_databases.slug` MUST equal the Clerk organization slug: the web app
//! derives the tenant host from `useOrganization().organization.slug` and has
//! no alternative source (`/v1/me` sits behind the binding gate itself), while
//! the gate resolves the routing row from that `Host`. Onboarding asserts
//! parity once (`orgt_verify_clerk_slug`); a RENAME in Clerk afterwards
//! silently re-breaks the identical path on a LIVE tenant — every org user
//! 404s and nothing reconciles it.
//!
//! This suite proves the reconciliation end to end, covering the four ways it
//! could be subtly wrong:
//!
//!   1. a rename actually updates the routing row, and **both** the old AND the
//!      new slug (plus the org-id key) leave the slug-keyed routing cache —
//!      each test first POISONS the cache so a missing invalidation fails
//!      loudly instead of being masked by a cold cache;
//!   2. an org with no `tenant_databases` row is a no-op, never an error and
//!      never an insert;
//!   3. a rename into a RESERVED or already-TAKEN slug is refused with the
//!      existing row byte-identical (and its watermark unadvanced, so a later
//!      legal rename still applies);
//!   4. an out-of-order or replayed event never regresses a newer slug —
//!      including the rename-and-rename-back case, which only works because a
//!      no-change event still advances the watermark.
//!
//! Plus a real signed HTTP delivery through the actual router, so the event
//! type is proven WIRED, not just implemented.
//!
//! Env-gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights), the same convention as every other `*_pg.rs` suite here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test org_slug_rename_pg -- --test-threads=1

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::str::FromStr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, TimeZone, Utc};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::routing::{reconcile_slug, SlugReconcile};
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use hmac::{Hmac, Mac};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";

/// Serializes every test in this file — same rationale as
/// `binding_gate_pg.rs`'s lock: every test applies the control tree to its own
/// scratch DB, and migration 0083's `coralswarm_app` is a CLUSTER-WIDE role, so
/// two concurrent applications race `CREATE ROLE` into a 23505 that escapes
/// 0083's XX000-only retry guard. The last test additionally mutates
/// process-global env (`CLERK_WEBHOOK_SECRET`).
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}
async fn recreate(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}
async fn drop_db(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// A scratch control DB with the control tree applied and one account.
async fn control_db(base: &str, db: &str) -> (PgPool, Uuid) {
    let pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(base, db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'owner@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, 'org_a' FROM u RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    (pool, account_id)
}

async fn insert_tenant(pool: &PgPool, account_id: Uuid, org: &str, slug: &str, secret: &str) {
    financial_fixture::execute(
        pool,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
         VALUES($1,$2,$3,$4,'active')",
        )
        .bind(org)
        .bind(account_id)
        .bind(slug)
        .bind(secret),
    )
    .await
    .unwrap();
}

/// The routing row's slug + Clerk watermark, read raw (not through the cache).
async fn row(pool: &PgPool, org: &str) -> Option<(String, Option<DateTime<Utc>>)> {
    sqlx::query_as("SELECT slug, clerk_slug_updated_at FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org)
        .fetch_optional(pool)
        .await
        .unwrap()
}

fn runtime() -> Arc<TenantRuntime> {
    TenantRuntime::new(
        true,
        APEX,
        // No secrets registered: this suite only exercises the routing-row
        // lookup/cache, never `finish_binding`'s pool build, so any attempt to
        // build a tenant pool would fail loudly rather than pass silently.
        Arc::new(TenantRegistry::new(Arc::new(
            StaticMapResolver::from_pairs(Vec::<(String, String)>::new()),
        ))),
    )
}

fn ms(millis: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(millis).single().expect("valid ms")
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. A rename reconciles routing AND clears both slugs from the cache
// ═══════════════════════════════════════════════════════════════════════════

/// The load-bearing test. The routing cache is **slug-keyed**, so a rename has
/// to drop two entries that are populated for different reasons:
///
///   * the OLD slug, cached as an active tenant by ordinary traffic; and
///   * the NEW slug, cached as a NEGATIVE (`None`) entry — the web app starts
///     redirecting org users to the new host the instant Clerk renames the org,
///     so those requests reach the gate BEFORE the webhook does and cache a
///     "no such tenant" answer for the very slug that is about to become
///     correct.
///
/// Both are poisoned here on purpose, and the test asserts the stale state
/// still holds immediately after the DB write — proving the cache really is the
/// hazard — before invalidating and asserting both flipped.
#[tokio::test]
async fn slug_rename_reconciles_routing_and_invalidates_old_and_new_slug() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping slug_rename_reconciles_routing_and_invalidates_old_and_new_slug: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _guard = ENV_LOCK.lock().await;
    let db = "orgt_slug_rename_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let (control, account_id) = control_db(&base, db).await;
    insert_tenant(&control, account_id, "org_a", "acme", "SECRET_A").await;

    let rt = runtime();

    // ── Poison the cache exactly the way production would. ─────────────────
    let hit = rt.resolve_routing(&control, "acme").await.unwrap();
    assert_eq!(hit.expect("acme is an active tenant").clerk_org_id, "org_a");
    // The new host is already receiving traffic (the browser followed Clerk's
    // renamed slug) and caches a NEGATIVE answer.
    assert!(
        rt.resolve_routing(&control, "acme-corp").await.unwrap().is_none(),
        "acme-corp is not a tenant yet — this caches None, the stale entry that matters"
    );
    // ...and the org-id keyed entry, which carries the whole row (old slug and all).
    assert_eq!(
        rt.resolve_routing_by_org(&control, "org_a")
            .await
            .unwrap()
            .expect("row by org")
            .slug,
        "acme"
    );

    // ── Reconcile. ─────────────────────────────────────────────────────────
    let outcome = reconcile_slug(&control, "org_a", "acme-corp", ms(2_000)).await.unwrap();
    assert_eq!(
        outcome,
        SlugReconcile::Renamed {
            old: "acme".into(),
            new: "acme-corp".into()
        }
    );
    let (slug, watermark) = row(&control, "org_a").await.expect("row still exists");
    assert_eq!(slug, "acme-corp", "the routing row must follow Clerk");
    assert_eq!(watermark, Some(ms(2_000)), "the ordering watermark must be stamped");

    // ── The DB is right but the CACHE is still wrong — this is the bug an
    //    incomplete invalidation ships. ──────────────────────────────────────
    assert!(
        rt.resolve_routing(&control, "acme").await.unwrap().is_some(),
        "pre-invalidation: the abandoned slug is still cached as a live tenant"
    );
    assert!(
        rt.resolve_routing(&control, "acme-corp").await.unwrap().is_none(),
        "pre-invalidation: the correct slug is still cached as a 404"
    );

    // ── Invalidate all three keys and re-resolve. ──────────────────────────
    rt.invalidate_routing_rename("org_a", "acme", "acme-corp");

    assert!(
        rt.resolve_routing(&control, "acme").await.unwrap().is_none(),
        "OLD slug must be invalidated: the abandoned host must stop routing into the tenant"
    );
    let now = rt
        .resolve_routing(&control, "acme-corp")
        .await
        .unwrap()
        .expect("NEW slug must be invalidated too — otherwise the correct host 404s for the TTL");
    assert_eq!(now.clerk_org_id, "org_a");
    assert_eq!(now.slug, "acme-corp");
    assert_eq!(
        rt.resolve_routing_by_org(&control, "org_a")
            .await
            .unwrap()
            .expect("row by org")
            .slug,
        "acme-corp",
        "the org-id keyed cache entry carries the whole row and must not keep the old slug"
    );

    control.close().await;
    drop_db(&mut conn, db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. An org with no routing row is a no-op, not an error
// ═══════════════════════════════════════════════════════════════════════════

/// Most Clerk orgs never get a tenant database (consumer orgs, orgs created
/// before onboarding runs). Their `organization.updated` events must be a quiet
/// no-op — never an error the webhook 500s on and Svix retries forever, and
/// never an INSERT that invents a routing row with no tenant DB behind it (that
/// would make the gate try to build a pool for a nonexistent secret).
#[tokio::test]
async fn rename_for_a_non_onboarded_org_is_a_noop() {
    let Some(base) = base_url() else {
        eprintln!("skipping rename_for_a_non_onboarded_org_is_a_noop: set ORGT_TEST_BASE_URL");
        return;
    };
    let _guard = ENV_LOCK.lock().await;
    let db = "orgt_slug_rename_noop_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let (control, account_id) = control_db(&base, db).await;
    insert_tenant(&control, account_id, "org_a", "acme", "SECRET_A").await;

    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM tenant_databases")
        .fetch_one(&control)
        .await
        .unwrap();

    // An org that was never onboarded. Note the slug it "renamed to" is a
    // perfectly legal one — the point is the missing ROW, not a bad slug.
    assert_eq!(
        reconcile_slug(&control, "org_never_onboarded", "brand-new", ms(9_000))
            .await
            .unwrap(),
        SlugReconcile::NotOnboarded
    );
    assert!(
        row(&control, "org_never_onboarded").await.is_none(),
        "reconciliation must never CREATE a routing row"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tenant_databases")
            .fetch_one(&control)
            .await
            .unwrap(),
        before,
        "no rows may be added or removed"
    );
    assert_eq!(
        row(&control, "org_a").await.unwrap().0,
        "acme",
        "an unrelated tenant must be untouched"
    );

    // A non-onboarded org whose slug is ALSO unroutable stays a no-op rather
    // than a loud rejection about a tenant that does not exist.
    assert_eq!(
        reconcile_slug(&control, "org_never_onboarded", "www", ms(9_000))
            .await
            .unwrap(),
        SlugReconcile::NotOnboarded
    );

    control.close().await;
    drop_db(&mut conn, db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Reserved / colliding slugs are refused without corrupting routing
// ═══════════════════════════════════════════════════════════════════════════

/// A rename we cannot honor must leave the routing row **byte-identical** —
/// including its watermark, so the next legal rename is still applied — rather
/// than half-applying and stranding the tenant. Three refusals are checked:
/// a reserved apex label (`www`, migration 0079 + `gate::RESERVED_SLUGS`), a
/// malformed slug (the 0078 charset CHECK), and a slug another tenant already
/// owns (the `slug` UNIQUE constraint).
#[tokio::test]
async fn reserved_and_colliding_slugs_are_refused_without_corrupting_routing() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping reserved_and_colliding_slugs_are_refused_without_corrupting_routing: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _guard = ENV_LOCK.lock().await;
    let db = "orgt_slug_rename_conflict_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let (control, account_id) = control_db(&base, db).await;
    insert_tenant(&control, account_id, "org_a", "acme", "SECRET_A").await;
    let account_b: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(email) VALUES('owner-b@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, 'org_b' FROM u RETURNING id",
    )
    .fetch_one(&control).await.unwrap();
    insert_tenant(&control, account_b, "org_b", "beta", "SECRET_B").await;

    // ── Reserved apex label. `www.<apex>` classifies as the APEX plane, so a
    //    tenant slugged `www` would be unreachable on its own subdomain. ────
    for reserved in ["www", "app"] {
        assert_eq!(
            reconcile_slug(&control, "org_a", reserved, ms(5_000)).await.unwrap(),
            SlugReconcile::Invalid {
                slug: reserved.into()
            },
            "a reserved apex label must be refused"
        );
    }
    // ── Malformed (charset/length) — the 0078 CHECK's territory. ───────────
    for bad in ["-nope", "Acme", "ab", "a.b"] {
        assert_eq!(
            reconcile_slug(&control, "org_a", bad, ms(5_000)).await.unwrap(),
            SlugReconcile::Invalid { slug: bad.into() }
        );
    }
    // ── Collision with a DIFFERENT tenant's slug. ──────────────────────────
    assert_eq!(
        reconcile_slug(&control, "org_a", "beta", ms(5_000)).await.unwrap(),
        SlugReconcile::Conflict {
            current: "acme".into(),
            wanted: "beta".into()
        }
    );

    // ── Nothing moved: not org_a, not org_b, and not org_a's watermark. ────
    assert_eq!(
        row(&control, "org_a").await.unwrap(),
        ("acme".to_string(), None),
        "a refused rename must leave the row byte-identical, watermark included"
    );
    assert_eq!(
        row(&control, "org_b").await.unwrap(),
        ("beta".to_string(), None),
        "the tenant that OWNS the contested slug must be untouched"
    );
    // Exactly one tenant per slug — the isolation invariant a corrupted write
    // would break.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(DISTINCT slug) FROM tenant_databases")
            .fetch_one(&control)
            .await
            .unwrap(),
        2
    );

    // ── The refusals did NOT burn the watermark: a legal rename still applies. ──
    assert_eq!(
        reconcile_slug(&control, "org_a", "acme-two", ms(5_000)).await.unwrap(),
        SlugReconcile::Renamed {
            old: "acme".into(),
            new: "acme-two".into()
        },
        "refusing an illegal rename must not block the next legal one"
    );

    // ── Belt and braces: the DB itself would have refused these anyway, so
    //    the app-level check and the constraints cannot silently diverge. ───
    for bad in ["www", "app", "-nope"] {
        let e = sqlx::query("UPDATE tenant_databases SET slug = $1 WHERE clerk_org_id = 'org_b'")
            .bind(bad)
            .execute(&control)
            .await
            .expect_err("the tenant_databases CHECKs must reject this slug");
        assert_eq!(
            e.as_database_error().and_then(|d| d.code()).as_deref(),
            Some("23514"),
            "expected a check_violation for slug {bad:?}"
        );
    }

    control.close().await;
    drop_db(&mut conn, db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Out-of-order / replayed delivery never regresses a newer slug
// ═══════════════════════════════════════════════════════════════════════════

/// Clerk does not guarantee webhook ordering and Svix redelivers, so arrival
/// order is not authority order. Ordering is decided by CLERK's own
/// `data.updated_at`, recorded as the `clerk_slug_updated_at` watermark.
///
/// The rename-and-rename-back case at the end is the subtle one: it only comes
/// out right because a NO-CHANGE event still advances the watermark. If it did
/// not, the delayed older event would "win" and leave routing on a slug Clerk
/// abandoned.
/// SF-4 (self-serve org signup A1): the self-serve routing key
/// `o-<16 hex>` is derived from the account id and never follows the Clerk
/// slug. A Clerk rename must not move a self-serve row (one with a
/// provisioning job, or one already on the derived shape), and a concierge
/// org must not take the derived shape (it could claim a pending self-serve
/// org's key and make that org's `insert_routing` refuse).
#[tokio::test]
async fn selfserve_rows_and_the_derived_slug_shape_are_never_reconciled() {
    let Some(base) = base_url() else {
        eprintln!("skipping selfserve_rows_and_the_derived_slug_shape_are_never_reconciled: set ORGT_TEST_BASE_URL");
        return;
    };
    let _guard = ENV_LOCK.lock().await;
    let db = "orgt_slug_rename_selfserve_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let (control, account_id) = control_db(&base, db).await;
    insert_tenant(&control, account_id, "org_a", "acme", "SECRET_A").await;

    // ── A concierge org may not take the derived shape. ────────────────────
    for shaped in ["o-0123456789abcdef", "o-ffffffffffffffff"] {
        assert_eq!(
            reconcile_slug(&control, "org_a", shaped, ms(5_000)).await.unwrap(),
            SlugReconcile::ReservedSelfServeShape { slug: shaped.into() },
        );
    }
    assert_eq!(row(&control, "org_a").await.unwrap(), ("acme".to_string(), None), "row and watermark untouched");
    // Near misses are ordinary slugs.
    assert!(matches!(
        reconcile_slug(&control, "org_a", "o-0123456789abcde", ms(5_000)).await.unwrap(),
        SlugReconcile::Renamed { .. }
    ));

    let new_account = |org: &'static str| {
        let control = control.clone();
        async move {
            sqlx::query_scalar::<_, Uuid>(
                "WITH u AS (INSERT INTO users(email) VALUES($1) RETURNING id) \
                 INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, $2 FROM u RETURNING id",
            )
            .bind(format!("{org}@x.co"))
            .bind(org)
            .fetch_one(&control)
            .await
            .unwrap()
        }
    };
    let derived = |id: Uuid| format!("o-{}", &id.simple().to_string()[..16]);
    let derived_ref = |slug: &str| format!("TENANT_DB_{}", slug.to_uppercase().replace('-', "_"));

    // ── A row already on the derived shape (no job row needed). ────────────
    let account_b = new_account("org_b").await;
    let slug_b = derived(account_b);
    insert_tenant(&control, account_b, "org_b", &slug_b, &derived_ref(&slug_b)).await;
    assert_eq!(
        reconcile_slug(&control, "org_b", "beta-renamed", ms(6_000)).await.unwrap(),
        SlugReconcile::SelfServe { current: slug_b.clone() },
    );
    assert_eq!(row(&control, "org_b").await.unwrap(), (slug_b.clone(), None));

    // ── A row whose org has a provisioning job, whatever its slug. ─────────
    let account_c = new_account("org_c").await;
    let slug_c = derived(account_c);
    sqlx::query(
        "INSERT INTO tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref) VALUES ($1, $2, $3, $4)",
    )
    .bind("org_c")
    .bind(account_c)
    .bind(&slug_c)
    .bind(derived_ref(&slug_c))
    .execute(&control)
    .await
    .unwrap();
    insert_tenant(&control, account_c, "org_c", "gamma", "SECRET_C").await;
    assert_eq!(
        reconcile_slug(&control, "org_c", "gamma-two", ms(7_000)).await.unwrap(),
        SlugReconcile::SelfServe { current: "gamma".into() },
    );
    assert_eq!(row(&control, "org_c").await.unwrap(), ("gamma".to_string(), None), "no write, no watermark");

    control.close().await;
    drop_db(&mut conn, db).await;
}

#[tokio::test]
async fn out_of_order_and_replayed_events_never_regress_a_newer_slug() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping out_of_order_and_replayed_events_never_regress_a_newer_slug: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _guard = ENV_LOCK.lock().await;
    let db = "orgt_slug_rename_order_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let (control, account_id) = control_db(&base, db).await;
    insert_tenant(&control, account_id, "org_a", "acme", "SECRET_A").await;

    // The NEWER event arrives first.
    assert_eq!(
        reconcile_slug(&control, "org_a", "gamma", ms(2_000)).await.unwrap(),
        SlugReconcile::Renamed {
            old: "acme".into(),
            new: "gamma".into()
        }
    );

    // A delayed OLDER event must not clobber it.
    assert_eq!(
        reconcile_slug(&control, "org_a", "delta", ms(1_000)).await.unwrap(),
        SlugReconcile::Stale {
            current: "gamma".into(),
            applied_at: ms(2_000)
        }
    );
    assert_eq!(row(&control, "org_a").await.unwrap().0, "gamma");

    // An exact REPLAY of the applied event is a no-op (idempotence): equal is
    // not newer, so it neither re-writes nor advances anything.
    assert_eq!(
        reconcile_slug(&control, "org_a", "gamma", ms(2_000)).await.unwrap(),
        SlugReconcile::Stale {
            current: "gamma".into(),
            applied_at: ms(2_000)
        }
    );
    // ...and a replay of the OLD event carrying the OLD slug, the literal
    // regression this guard exists to stop.
    assert_eq!(
        reconcile_slug(&control, "org_a", "acme", ms(500)).await.unwrap(),
        SlugReconcile::Stale {
            current: "gamma".into(),
            applied_at: ms(2_000)
        }
    );
    assert_eq!(row(&control, "org_a").await.unwrap(), ("gamma".to_string(), Some(ms(2_000))));

    // A genuinely newer event still applies — the guard is a watermark, not a freeze.
    assert_eq!(
        reconcile_slug(&control, "org_a", "omega", ms(3_000)).await.unwrap(),
        SlugReconcile::Renamed {
            old: "gamma".into(),
            new: "omega".into()
        }
    );

    // ── Rename-and-rename-back, delivered in reverse. ──────────────────────
    // Clerk truth: t=4000 omega→zeta, then t=5000 zeta→omega (renamed back).
    // The t=5000 event arrives FIRST and is a no-change event (the row still
    // says `omega`) — it must still stamp the watermark, or the delayed t=4000
    // event would strand routing on `zeta`.
    assert_eq!(
        reconcile_slug(&control, "org_a", "omega", ms(5_000)).await.unwrap(),
        SlugReconcile::Unchanged {
            slug: "omega".into()
        }
    );
    assert_eq!(
        row(&control, "org_a").await.unwrap().1,
        Some(ms(5_000)),
        "a no-change event MUST advance the watermark — see this test's doc comment"
    );
    assert_eq!(
        reconcile_slug(&control, "org_a", "zeta", ms(4_000)).await.unwrap(),
        SlugReconcile::Stale {
            current: "omega".into(),
            applied_at: ms(5_000)
        }
    );
    assert_eq!(
        row(&control, "org_a").await.unwrap().0,
        "omega",
        "the reverse-delivered rename-back must leave routing on Clerk's CURRENT slug"
    );

    control.close().await;
    drop_db(&mut conn, db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. It works as the LEAST-PRIVILEGE app role, and grants nothing more
// ═══════════════════════════════════════════════════════════════════════════

/// Migration 0083 revoked `INSERT, UPDATE, DELETE ON tenant_databases` from
/// `coralswarm_app` on the stated premise that "the live app-request path only
/// ever READS it". This handler is a live request-path WRITER, so that premise
/// no longer holds and 0088 grants back a COLUMN-scoped `UPDATE (slug,
/// clerk_slug_updated_at, updated_at)`.
///
/// This runs the real reconciliation over a real login AS `coralswarm_app` —
/// the exact connection path OPS-4 cuts the app over to — so the grant cannot
/// be missing or mis-scoped without failing here rather than in production,
/// where the only symptom would be the broken-routing bug this whole PR fixes,
/// silently reappearing the day the role switches.
///
/// It then asserts the grant did NOT widen: the attack 0083 named ("repoint an
/// org's database") needs `db_secret_ref`, and that column — along with
/// `status`, `account_id`, `clerk_org_id`, and INSERT/DELETE on the table — is
/// still refused with 42501.
#[tokio::test]
async fn reconciliation_runs_as_the_least_privilege_app_role_and_grants_nothing_more() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping reconciliation_runs_as_the_least_privilege_app_role_and_grants_nothing_more: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _guard = ENV_LOCK.lock().await;
    let db = "orgt_slug_rename_role_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let (owner, account_id) = control_db(&base, db).await;
    insert_tenant(&owner, account_id, "org_a", "acme", "SECRET_A").await;

    // A real separate login AS `coralswarm_app`, not `SET ROLE` — same idiom as
    // `support/rls_probe.rs` (see its `APP_ROLE_TEST_PASSWORD` doc: a test-only
    // password on a role migration 0083 creates with none).
    const APP_PW: &str = "orgt_slug_rename_test_only_pw";
    sqlx::query(&format!("ALTER ROLE coralswarm_app WITH PASSWORD '{APP_PW}'"))
        .execute(&owner)
        .await
        .unwrap();
    let app_pool: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            sqlx::postgres::PgConnectOptions::from_str(&with_db(&base, db))
                .unwrap()
                .username("coralswarm_app")
                .password(APP_PW),
        )
        .await
        .expect("connect AS coralswarm_app");

    // ── The reconciliation itself, over the least-privilege role. ──────────
    assert_eq!(
        reconcile_slug(&app_pool, "org_a", "acme-corp", ms(1_500)).await.unwrap(),
        SlugReconcile::Renamed {
            old: "acme".into(),
            new: "acme-corp".into()
        },
        "the app role must be able to reconcile a Clerk slug rename after OPS-4"
    );
    assert_eq!(
        row(&owner, "org_a").await.unwrap(),
        ("acme-corp".to_string(), Some(ms(1_500)))
    );

    // ── ...and nothing beyond it. `42501` is insufficient_privilege. ───────
    let denied = |sql: &'static str| {
        let pool = app_pool.clone();
        async move {
            let e = sqlx::query(sql)
                .execute(&pool)
                .await
                .expect_err("this statement must be refused for coralswarm_app");
            assert_eq!(
                e.as_database_error().and_then(|d| d.code()).as_deref(),
                Some("42501"),
                "expected insufficient_privilege for: {sql}"
            );
        }
    };
    // The exact attack migration 0083 names: repointing an org at another
    // database. Still impossible.
    denied("UPDATE tenant_databases SET db_secret_ref = 'SECRET_EVIL' WHERE clerk_org_id = 'org_a'")
        .await;
    denied("UPDATE tenant_databases SET status = 'offboarded' WHERE clerk_org_id = 'org_a'").await;
    denied("UPDATE tenant_databases SET account_id = gen_random_uuid() WHERE clerk_org_id = 'org_a'")
        .await;
    denied("UPDATE tenant_databases SET clerk_org_id = 'org_evil' WHERE clerk_org_id = 'org_a'")
        .await;
    denied("DELETE FROM tenant_databases WHERE clerk_org_id = 'org_a'").await;
    denied(
        "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
         VALUES('org_evil',gen_random_uuid(),'evil','SECRET_EVIL','active')",
    )
    .await;
    // A mixed UPDATE touching one granted and one ungranted column is refused
    // WHOLESALE — the granted column cannot be used as a foot in the door.
    denied(
        "UPDATE tenant_databases SET slug = 'sneaky', db_secret_ref = 'SECRET_EVIL' \
         WHERE clerk_org_id = 'org_a'",
    )
    .await;

    // Nothing above landed.
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT db_secret_ref FROM tenant_databases WHERE clerk_org_id = 'org_a'"
        )
        .fetch_one(&owner)
        .await
        .unwrap(),
        "SECRET_A"
    );
    assert_eq!(row(&owner, "org_a").await.unwrap().0, "acme-corp");

    app_pool.close().await;
    owner.close().await;
    drop_db(&mut conn, db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Real signed delivery through the real router
// ═══════════════════════════════════════════════════════════════════════════

/// Everything above drives `reconcile_slug` directly, which proves the logic
/// but says nothing about whether `organization.updated` is actually DISPATCHED
/// (a missing match arm would leave every test above green while the handler
/// never runs in production). This posts a genuinely Svix-signed event to the
/// real `/v1/webhooks/clerk` route on the real router and asserts the routing
/// row moved and the cache was invalidated by the HANDLER, not by the test.
#[tokio::test]
async fn signed_organization_updated_reconciles_through_the_real_router() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping signed_organization_updated_reconciles_through_the_real_router: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _guard = ENV_LOCK.lock().await;
    let db = "orgt_slug_rename_http_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let (control, account_id) = control_db(&base, db).await;
    insert_tenant(&control, account_id, "org_a", "acme", "SECRET_A").await;

    let secret_b64 = STANDARD.encode(b"org-slug-rename-webhook-secret");
    // SAFETY: serialized by `_guard` for this test's whole body.
    unsafe {
        std::env::set_var("CLERK_WEBHOOK_SECRET", format!("whsec_{secret_b64}"));
    }

    let rt = runtime();
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    state.tenant_runtime = Some(rt.clone());
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // Poison both cache keys, exactly as the direct test does.
    assert!(rt.resolve_routing(&control, "acme").await.unwrap().is_some());
    assert!(rt.resolve_routing(&control, "acme-corp").await.unwrap().is_none());

    let post = |body: String| {
        let ts = chrono::Utc::now().timestamp().to_string();
        let id = format!("msg_{}", Uuid::new_v4());
        let key = STANDARD.decode(&secret_b64).unwrap();
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
        mac.update(format!("{id}.{ts}.{body}").as_bytes());
        let sig = format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()));
        HttpRequest::builder()
            .method("POST")
            .uri("/v1/webhooks/clerk")
            .header("host", APEX)
            .header("content-type", "application/json")
            .header("svix-id", id)
            .header("svix-timestamp", ts)
            .header("svix-signature", sig)
            .body(Body::from(body))
            .unwrap()
    };
    let event = |slug: &str, updated_at: i64| {
        serde_json::json!({
            "type": "organization.updated",
            "object": "event",
            "data": {
                "object": "organization",
                "id": "org_a",
                "name": "Acme",
                "slug": slug,
                "created_at": 1_000,
                "updated_at": updated_at,
            }
        })
        .to_string()
    };

    // ── The rename. ────────────────────────────────────────────────────────
    let resp = app.clone().oneshot(post(event("acme-corp", 7_000))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "a valid signed event must be acked");
    assert_eq!(
        row(&control, "org_a").await.unwrap(),
        ("acme-corp".to_string(), Some(ms(7_000))),
        "the handler must have reconciled the routing row"
    );
    // The HANDLER invalidated the cache — the test never called
    // `invalidate_routing_rename`.
    assert!(
        rt.resolve_routing(&control, "acme").await.unwrap().is_none(),
        "the handler must invalidate the OLD slug"
    );
    assert_eq!(
        rt.resolve_routing(&control, "acme-corp")
            .await
            .unwrap()
            .expect("new slug must resolve")
            .clerk_org_id,
        "org_a",
        "the handler must invalidate the NEW slug's negative cache entry too"
    );

    // ── A replay of the same delivery changes nothing. ─────────────────────
    let resp = app.clone().oneshot(post(event("acme-corp", 7_000))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(row(&control, "org_a").await.unwrap().0, "acme-corp");

    // ── An out-of-order older event is acked but ignored (never 5xx: retrying
    //    it would decide identically forever). ──────────────────────────────
    let resp = app.clone().oneshot(post(event("acme", 6_000))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        row(&control, "org_a").await.unwrap().0,
        "acme-corp",
        "a stale event must not regress routing over HTTP either"
    );

    // ── An event for an org with no routing row is acked, writes nothing. ──
    let body = serde_json::json!({
        "type": "organization.updated",
        "data": {"id": "org_unknown", "slug": "somewhere", "updated_at": 8_000}
    })
    .to_string();
    let resp = app.clone().oneshot(post(body)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "a non-onboarded org must not 5xx");
    assert!(row(&control, "org_unknown").await.is_none());

    // ── A tampered signature is still refused (the guarantee this handler
    //    inherits and must not have widened). ───────────────────────────────
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/v1/webhooks/clerk")
                .header("host", APEX)
                .header("content-type", "application/json")
                .header("svix-id", "msg_forged")
                .header("svix-timestamp", chrono::Utc::now().timestamp().to_string())
                .header("svix-signature", "v1,deadbeef")
                .body(Body::from(event("hijacked", 9_999)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        row(&control, "org_a").await.unwrap().0,
        "acme-corp",
        "an unsigned/forged event must never move a tenant's routing"
    );

    // SAFETY: serialized by `_guard`.
    unsafe {
        std::env::remove_var("CLERK_WEBHOOK_SECRET");
    }
    control.close().await;
    drop_db(&mut conn, db).await;
}

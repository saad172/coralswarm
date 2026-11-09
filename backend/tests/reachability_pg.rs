//! Integration test for the reachability decomposition (org-tenancy PR 1.7):
//! `ReachabilityResolver`'s consumer-plane delegation vs. its org-tenant-plane
//! two-step must agree on the org-scoped subset (the parity guarantee), the
//! tenant-plane path must gate team oceans correctly (the org tenant probe),
//! and a `user_id` that does not match the resolved `TenantContext` must be
//! refused rather than silently served (the P1 security fix — see
//! `reachability.rs`'s `require_matching_user`).
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights). Follows the same scratch-DB pattern as
//! `binding_gate_pg.rs` / `dek_relocation_pg.rs`.
//!
//! Unlike `binding_gate_pg.rs`/`dek_relocation_pg.rs`, this test only needs
//! ONE scratch database, not two: `migrations_control` is the pure `git mv`
//! of today's single production schema (org-tenancy PR 0.1), so it already
//! co-locates `account_members` (control-side) with `graphs`/`teams`/
//! `team_members` (tenant-tree tables) — exactly today's "consumer plane"
//! reality. The org-tenant-plane SQL this test also exercises only ever
//! touches `graphs`/`teams`/`team_members` (never `account_members`), so it
//! runs correctly against that SAME database by construction — which is
//! exactly the fixture needed to compare the two code paths against
//! identical underlying rows.

use coralswarm_backend::store::{PostgresStore, Store, StoreError};
use coralswarm_backend::tenancy::reachability::ReachabilityResolver;
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
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

async fn new_user(pool: &PgPool, email: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn new_account(pool: &PgPool, owner_user_id: Uuid) -> Uuid {
    sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn add_member(pool: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(role)
    .execute(pool)
    .await
    .unwrap();
}

async fn new_graph(pool: &PgPool, account_id: Uuid, name: &str, kind: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .bind(kind)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn new_team(pool: &PgPool, account_id: Uuid, graph_id: Uuid, created_by: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) \
         VALUES ($1, $2, 'team', $3) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(created_by)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn add_team_member(pool: &PgPool, team_id: Uuid, user_id: Uuid, departed: bool) {
    sqlx::query(
        "INSERT INTO team_members (team_id, user_id, role, departing_at) \
         VALUES ($1, $2, 'member', CASE WHEN $3 THEN now() ELSE NULL END)",
    )
    .bind(team_id)
    .bind(user_id)
    .bind(departed)
    .execute(pool)
    .await
    .unwrap();
}

/// A hand-seeded `kind = 'member'` graph (member-private-oceans PR 2, plan
/// §2.1/§2.2). Nothing in this repo creates one of these yet — no writer
/// exists before PR 4/`MEMBER_OCEANS_ENABLED` — so every fixture that needs
/// one for this PR's predicate coverage must INSERT it directly, exactly the
/// way this test already hand-seeds `graphs`/`teams`/`team_members` rows.
/// `graphs_member_requires_owner` (migration 0090/0005) enforces
/// `owner_user_id IS NOT NULL` for this kind, so it is always bound here.
async fn new_member_graph(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid, name: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id) \
         VALUES ($1, $2, 'member', $3) RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .bind(owner_user_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Stamp `archived_at` on an existing graph — the lifecycle write PR 7 will
/// eventually perform for real (plan §2.6), hand-done here since nothing in
/// this repo writes it yet.
async fn archive_graph(pool: &PgPool, graph_id: Uuid) {
    sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
        .bind(graph_id)
        .execute(pool)
        .await
        .unwrap();
}

/// Ids only, sorted. NOTE: this deliberately does not cover `ORDER BY`
/// (`is_primary DESC, created_at DESC`) — the consumer and tenant queries
/// share the identical `ORDER BY` clause by construction (see
/// `reachability.rs`), but ordering itself is not asserted here, only set
/// membership.
fn sorted_ids(mut ids: Vec<Uuid>) -> Vec<Uuid> {
    ids.sort();
    ids
}

fn tenant_ctx(account_id: Uuid, user_id: Uuid, pool: &PgPool) -> TenantContext {
    TenantContext {
        org_id: "acme".to_string(),
        account_id,
        user_id,
        pool: pool.clone(),
        page_pool: None,
    }
}

/// The parity + org-tenant-probe + P1-regression test.
///
/// Fixture (deliberately NON-degenerate — `member` belongs to TWO accounts,
/// not one, so the consumer and tenant paths are never trivially identical):
/// `member` has their own personal account (with one personal ocean) AND is
/// a member of `org_account` (org ocean + `team1_ocean`, NOT `team2_ocean`).
/// An unrelated `other_account` + ocean proves the tenant path's account
/// scope never leaks. `outsider` is not a member of `org_account` at all.
#[tokio::test]
async fn consumer_and_tenant_paths_agree_on_the_org_subset() {
    let Some(base) = base_url() else {
        eprintln!("skipping consumer_and_tenant_paths_agree_on_the_org_subset: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_reachability_parity";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;

    let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

    // ── Fixture ────────────────────────────────────────────────────────────
    let owner = new_user(&pool, "person@example.com").await;
    let member = new_user(&pool, "person@example.com").await; // in team1, not team2
    let outsider = new_user(&pool, "person@example.com").await; // not an org_account member at all

    // `member` ALSO has their own personal account + ocean — the
    // non-degenerate fixture: the consumer path's D9 union spans BOTH this
    // account and org_account for `member`, while the org tenant path only
    // ever sees org_account's slice. Without this, `member` belonging to
    // exactly one account would make consumer == tenant trivially true and
    // wouldn't distinguish "identical" from "coincidentally identical
    // because there was only one account to begin with".
    let member_personal_account = new_account(&pool, member).await;
    add_member(&pool, member_personal_account, member, "owner").await;
    let member_personal_ocean =
        new_graph(&pool, member_personal_account, "member's personal ocean", "personal").await;

    let org_account = new_account(&pool, owner).await;
    add_member(&pool, org_account, owner, "owner").await;
    add_member(&pool, org_account, member, "member").await;

    let org_ocean = new_graph(&pool, org_account, "Acme org ocean", "org").await;
    let team1_ocean = new_graph(&pool, org_account, "Team One ocean", "team").await;
    let team2_ocean = new_graph(&pool, org_account, "Team Two ocean", "team").await;

    let team1 = new_team(&pool, org_account, team1_ocean, owner).await;
    let team2 = new_team(&pool, org_account, team2_ocean, owner).await;
    add_team_member(&pool, team1, member, false).await;
    // member is explicitly NOT in team2.

    // A departed team membership must not grant reachability either (active
    // `departing_at IS NULL` gate) — add owner to team2 then have them leave.
    add_team_member(&pool, team2, owner, true).await;

    // An unrelated second account + ocean, to prove the tenant-path's
    // `account_id = $1` scope never leaks across accounts even though both
    // live in the same physical database in this test.
    let other_owner = new_user(&pool, "owner@other.example").await;
    let other_account = new_account(&pool, other_owner).await;
    add_member(&pool, other_account, other_owner, "owner").await;
    let other_ocean = new_graph(&pool, other_account, "Other org ocean", "org").await;

    // ── Consumer path (existing joined query, unchanged) ────────────────────
    let consumer_store = PostgresStore::new(pool.clone());

    let consumer_member_graphs = consumer_store.list_reachable_graphs(member).await.unwrap();
    let consumer_member_ids = sorted_ids(consumer_member_graphs.iter().map(|g| g.id).collect());
    assert_eq!(
        consumer_member_ids,
        sorted_ids(vec![org_ocean, team1_ocean, member_personal_ocean]),
        "consumer path (D9 union): member's personal ocean + org ocean + their own team ocean"
    );
    assert!(consumer_store.graph_reachable_by_user(team1_ocean, member).await.unwrap());
    assert!(!consumer_store.graph_reachable_by_user(team2_ocean, member).await.unwrap());

    // ── Tenant path (new two-step, via ReachabilityResolver) ────────────────
    // Same physical pool stands in for "the org's tenant DB" here — the
    // tenant-path SQL only ever touches `graphs`/`teams`/`team_members`, so
    // it is indistinguishable from running against a real separate tenant
    // database for the purposes of this comparison. `ctx_member.user_id` is
    // `member` — exactly the user this context is legitimately scoped to,
    // mirroring what the real binding gate would build.
    let ctx_member = tenant_ctx(org_account, member, &pool);
    let decision_member = TenancyDecision::Org(ctx_member);

    let tenant_member_graphs =
        ReachabilityResolver::list_reachable_graphs(Some(&decision_member), &consumer_store, member)
            .await
            .unwrap();
    let tenant_member_ids = sorted_ids(tenant_member_graphs.iter().map(|g| g.id).collect());

    // ── Parity assertion: the tenant path returns EXACTLY the org_account
    //    subset of what the consumer path returns — a strict subset, not
    //    equality, since `member` spans two accounts and the tenant plane
    //    only ever sees one of them. This is the intended per-plane subset
    //    semantics (D9's full union is realized across separate calls to
    //    this resolver, one per relevant plane — see `reachability.rs`'s
    //    module doc). ─────────────────────────────────────────────────────
    assert_eq!(
        tenant_member_ids,
        sorted_ids(vec![org_ocean, team1_ocean]),
        "tenant path: exactly org_account's reachable subset for member"
    );
    assert!(
        tenant_member_ids
            .iter()
            .all(|id| consumer_member_ids.contains(id)),
        "every tenant-path id must also be consumer-reachable (subset, never a superset)"
    );
    assert!(
        !tenant_member_ids.contains(&member_personal_ocean),
        "the org tenant plane must never surface the user's PERSONAL ocean \
         (a different account, a different — in reality separate — database)"
    );
    assert!(
        consumer_member_ids.contains(&member_personal_ocean) && tenant_member_ids.len() < consumer_member_ids.len(),
        "consumer path must be a strict superset here (proves the fixture is non-degenerate)"
    );

    // ── Org tenant probe: team-ocean gating on the tenant pool ──────────────
    assert!(
        ReachabilityResolver::graph_reachable_by_user(
            Some(&decision_member),
            &consumer_store,
            team1_ocean,
            member
        )
        .await
        .unwrap(),
        "member is in team1 -> must reach team1's ocean via the tenant path"
    );
    assert!(
        !ReachabilityResolver::graph_reachable_by_user(
            Some(&decision_member),
            &consumer_store,
            team2_ocean,
            member
        )
        .await
        .unwrap(),
        "member is NOT in team2 -> must NOT reach team2's ocean via the tenant path"
    );
    assert!(
        ReachabilityResolver::graph_reachable_by_user(
            Some(&decision_member),
            &consumer_store,
            org_ocean,
            member
        )
        .await
        .unwrap(),
        "member reaches the non-team org ocean unconditionally (mirrors consumer semantics)"
    );

    // ── Departed team membership must not grant access, on either path ──────
    // Uses a context legitimately scoped to `owner` (distinct from
    // `ctx_member`/`decision_member` above) — exercising this through
    // `owner`'s OWN, properly-matched context, not by passing `owner` as a
    // mismatched user_id against `decision_member` (that's a DIFFERENT case,
    // covered below).
    let ctx_owner = tenant_ctx(org_account, owner, &pool);
    let decision_owner = TenancyDecision::Org(ctx_owner);
    assert!(!consumer_store.graph_reachable_by_user(team2_ocean, owner).await.unwrap());
    assert!(
        !ReachabilityResolver::graph_reachable_by_user(Some(&decision_owner), &consumer_store, team2_ocean, owner)
            .await
            .unwrap(),
        "a departed team_members row must not grant reachability on the tenant path either"
    );

    // ── No cross-ACCOUNT leak (tenant path's account_id scope) ─────────────
    assert!(
        !tenant_member_ids.contains(&other_ocean),
        "tenant path must never surface another account's ocean"
    );
    // Point-lookup variant of the same probe: the list query and the singular
    // `graph_reachable_by_user` query carry separate copies of the account_id
    // filter, so assert BOTH — a regression in either must fail here.
    assert!(
        !ReachabilityResolver::graph_reachable_by_user(
            Some(&decision_member),
            &consumer_store,
            other_ocean,
            member
        )
        .await
        .unwrap(),
        "tenant point-lookup must never reach another account's ocean"
    );

    // ── P1 security fix: a user_id that does not match the TenantContext's
    //    own user_id must be REFUSED (a hard Err), never silently served.
    //    Pre-fix, `decision_member` (built for `member`) would still have
    //    returned `org_ocean` for `outsider` or `owner` here — a real
    //    authorization widening, since the tenant plane has no
    //    `account_members` to check membership against (see
    //    `reachability.rs`'s `require_matching_user` doc comment). This is
    //    exactly the P2 gap: the OLD version of this test only asserted
    //    `id != other_ocean` for the outsider case, which passed even
    //    though `org_ocean` (a real, wrongly-widened result) came back. ────
    let outsider_mismatch =
        ReachabilityResolver::list_reachable_graphs(Some(&decision_member), &consumer_store, outsider)
            .await;
    assert!(
        matches!(outsider_mismatch, Err(StoreError::TenantAuthzMismatch(_))),
        "a user_id not matching the TenantContext must error closed, got {outsider_mismatch:?}"
    );
    let owner_mismatch =
        ReachabilityResolver::list_reachable_graphs(Some(&decision_member), &consumer_store, owner).await;
    assert!(
        matches!(owner_mismatch, Err(StoreError::TenantAuthzMismatch(_))),
        "even a REAL member of the same account must be refused if they aren't THIS context's own \
         user — the context is scoped to exactly one user, not to the account, got {owner_mismatch:?}"
    );
    let graph_check_mismatch = ReachabilityResolver::graph_reachable_by_user(
        Some(&decision_member),
        &consumer_store,
        org_ocean,
        outsider,
    )
    .await;
    assert!(
        matches!(graph_check_mismatch, Err(StoreError::TenantAuthzMismatch(_))),
        "graph_reachable_by_user must apply the same user_id-match fail-closed rule, got {graph_check_mismatch:?}"
    );

    // ── `None` / `Personal` decisions delegate identically to the consumer
    //    path (unaffected by the tenant-side user_id check, since they never
    //    reach it) ─────────────────────────────────────────────────────────
    let none_ids = sorted_ids(
        ReachabilityResolver::list_reachable_graphs(None, &consumer_store, member)
            .await
            .unwrap()
            .iter()
            .map(|g| g.id)
            .collect(),
    );
    let personal_ids = sorted_ids(
        ReachabilityResolver::list_reachable_graphs(
            Some(&TenancyDecision::Personal),
            &consumer_store,
            member,
        )
        .await
        .unwrap()
        .iter()
        .map(|g| g.id)
        .collect(),
    );
    assert_eq!(none_ids, consumer_member_ids);
    assert_eq!(personal_ids, consumer_member_ids);

    // Cleanup.
    pool.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// `my_team_role` (workspace-manage design doc's "Making role cheap"
/// section, folded into the SAME migration pair as `team_id` —
/// `migrations_control/0132_reachable_graphs_team_id.sql` — precisely so this
/// runs against the REAL `resolve_reachable_graphs` SQL function body, not a
/// Rust-side stand-in): the creator of a team ocean must read `my_team_role
/// == Some("admin")` immediately (`create_team_tx` seeds the creator as the
/// team's first admin in the SAME transaction that inserts the `graphs` row),
/// and a personal ocean — no `teams` row at all — must read `None`, never a
/// stale or wrong role.
#[tokio::test]
async fn my_team_role_is_admin_for_the_creator_and_null_for_a_personal_ocean() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping my_team_role_is_admin_for_the_creator_and_null_for_a_personal_ocean: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_reachability_my_team_role";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;

    let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

    let owner = new_user(&pool, "owner@my-team-role.example").await;
    let account_id = new_account(&pool, owner).await;
    add_member(&pool, account_id, owner, "owner").await;

    let personal_ocean = new_graph(&pool, account_id, "Personal", "personal").await;

    let store = PostgresStore::new(pool.clone());
    let created = store
        .create_team_tx(account_id, owner, "Engineering", b"fake-wrapped-key")
        .await
        .expect("create_team_tx");

    let graphs = store.list_reachable_graphs(owner).await.unwrap();

    let team_graph = graphs
        .iter()
        .find(|g| g.id == created.graph_id)
        .expect("list_reachable_graphs includes the team ocean just created");
    assert_eq!(
        team_graph.my_team_role,
        Some("admin".to_string()),
        "the creator of a team ocean is seeded as its first admin (create_team_tx) -> \
         my_team_role must reflect that immediately, not just team_id"
    );

    let personal_graph = graphs
        .iter()
        .find(|g| g.id == personal_ocean)
        .expect("list_reachable_graphs includes the personal ocean");
    assert_eq!(
        personal_graph.my_team_role, None,
        "a personal ocean has no teams row at all -> my_team_role must be NULL, \
         never a stale/wrong role"
    );

    // Cleanup.
    pool.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Member-private-oceans PR 2 (plan §2.2, §6 probe #1/#9): a hand-seeded
/// `kind = 'member'` graph must be visible to its owner and invisible to
/// EVERY other org member on BOTH arms — including an `org:admin`, since
/// neither predicate (consumer's `resolve_reachable_graphs` nor the tenant
/// arm's `resolve_tenant_reachable_graphs`) consults `account_members.role`
/// at all. This is the "the two arms must agree on the member ... subset"
/// contract this file's header describes, extended to cover the new kind.
#[tokio::test]
async fn member_ocean_is_owner_only_on_both_planes_including_org_admin() {
    let Some(base) = base_url() else {
        eprintln!("skipping member_ocean_is_owner_only_on_both_planes_including_org_admin: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_reachability_member_ocean";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;

    let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

    let owner = new_user(&pool, "owner@memberocean.example").await;
    let member = new_user(&pool, "member@memberocean.example").await;
    let admin_user = new_user(&pool, "admin@memberocean.example").await;

    let org_account = new_account(&pool, owner).await;
    add_member(&pool, org_account, owner, "owner").await;
    add_member(&pool, org_account, member, "member").await;
    // Deliberately role = 'admin', not 'member' or 'owner' — the P0 this
    // predicate must not regress: nothing in either arm's SQL references
    // `account_members.role`, so an `org:admin` gets EXACTLY the same
    // denial as any other non-owner (plan §6 probe #9).
    add_member(&pool, org_account, admin_user, "admin").await;

    let member_ocean = new_member_graph(&pool, org_account, member, "member's private ocean").await;
    // A second, ordinary org ocean so the fixture proves the member arm
    // gates ONLY the member ocean, not the whole account.
    let org_ocean = new_graph(&pool, org_account, "org commons", "org").await;

    let consumer_store = PostgresStore::new(pool.clone());

    // ── Consumer arm ─────────────────────────────────────────────────────
    for (label, user, should_see) in [
        ("owner", owner, false),
        ("member (the owner)", member, true),
        ("org:admin", admin_user, false),
    ] {
        let ids: Vec<Uuid> = consumer_store
            .list_reachable_graphs(user)
            .await
            .unwrap()
            .into_iter()
            .map(|g| g.id)
            .collect();
        assert_eq!(
            ids.contains(&member_ocean),
            should_see,
            "{label}: consumer-arm list_reachable_graphs disagrees on member_ocean visibility"
        );
        assert!(
            ids.contains(&org_ocean),
            "{label}: the ordinary org ocean must remain visible to every account member \
             regardless of the member-ocean gate"
        );
        assert_eq!(
            consumer_store.graph_reachable_by_user(member_ocean, user).await.unwrap(),
            should_see,
            "{label}: consumer-arm graph_reachable_by_user disagrees on member_ocean visibility"
        );
    }

    // ── Tenant arm — same three users, via ReachabilityResolver ─────────────
    // `require_matching_user` demands ctx.user_id == the queried user, so
    // each user gets their OWN context, mirroring what the real binding gate
    // would build per-request.
    for (label, user, should_see) in [
        ("owner", owner, false),
        ("member (the owner)", member, true),
        ("org:admin", admin_user, false),
    ] {
        let ctx = tenant_ctx(org_account, user, &pool);
        let decision = TenancyDecision::Org(ctx);

        let ids: Vec<Uuid> =
            ReachabilityResolver::list_reachable_graphs(Some(&decision), &consumer_store, user)
                .await
                .unwrap()
                .into_iter()
                .map(|g| g.id)
                .collect();
        assert_eq!(
            ids.contains(&member_ocean),
            should_see,
            "{label}: tenant-arm list_reachable_graphs disagrees on member_ocean visibility"
        );
        assert!(
            ids.contains(&org_ocean),
            "{label}: tenant arm must still surface the ordinary org ocean"
        );
        assert_eq!(
            ReachabilityResolver::graph_reachable_by_user(Some(&decision), &consumer_store, member_ocean, user)
                .await
                .unwrap(),
            should_see,
            "{label}: tenant-arm graph_reachable_by_user disagrees on member_ocean visibility"
        );

        // ── The parity contract this file exists to prove, extended to the
        //    member subset: both arms must agree, user by user. ────────────
        let consumer_ids: Vec<Uuid> = consumer_store
            .list_reachable_graphs(user)
            .await
            .unwrap()
            .into_iter()
            .map(|g| g.id)
            .collect();
        assert_eq!(
            ids.contains(&member_ocean),
            consumer_ids.contains(&member_ocean),
            "{label}: consumer and tenant arms must agree on member_ocean visibility"
        );
    }

    pool.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Member-private-oceans PR 2 (plan §2.6, §6 probe #12): once `archived_at`
/// is set, a graph is unreachable EVERYWHERE, on both arms, including for
/// its own owner (a `kind = 'member'` ocean) and including for the account
/// owner (an ordinary `kind = 'org'` ocean) — `archived_at IS NULL` is an
/// unconditional AND in the predicate, not scoped to any particular kind or
/// user. `graphs_archived_at_idx`/the archival write itself are PR 7+; this
/// hand-stamps the column the same way `new_member_graph` hand-seeds a kind
/// nothing yet creates, so the PREDICATE can be proven now instead of only
/// once a writer exists.
#[tokio::test]
async fn archived_graph_is_unreachable_everywhere_including_its_own_owner() {
    let Some(base) = base_url() else {
        eprintln!("skipping archived_graph_is_unreachable_everywhere_including_its_own_owner: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_reachability_archived";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;

    let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

    let owner = new_user(&pool, "owner@archived.example").await;
    let member = new_user(&pool, "member@archived.example").await;

    let org_account = new_account(&pool, owner).await;
    add_member(&pool, org_account, owner, "owner").await;
    add_member(&pool, org_account, member, "member").await;

    let org_ocean = new_graph(&pool, org_account, "org commons", "org").await;
    let member_ocean = new_member_graph(&pool, org_account, member, "member's private ocean").await;

    // Both reachable BEFORE archiving — the control that makes the negative
    // below meaningful (a permanently-404 id would trivially pass).
    let consumer_store = PostgresStore::new(pool.clone());
    assert!(consumer_store.graph_reachable_by_user(org_ocean, owner).await.unwrap());
    assert!(consumer_store.graph_reachable_by_user(member_ocean, member).await.unwrap());

    archive_graph(&pool, org_ocean).await;
    archive_graph(&pool, member_ocean).await;

    // ── Consumer arm ─────────────────────────────────────────────────────
    assert!(
        !consumer_store.graph_reachable_by_user(org_ocean, owner).await.unwrap(),
        "an archived org ocean must be unreachable even for the account owner"
    );
    assert!(
        !consumer_store.graph_reachable_by_user(member_ocean, member).await.unwrap(),
        "an archived member ocean must be unreachable even for its own owner"
    );
    let owner_ids: Vec<Uuid> = consumer_store
        .list_reachable_graphs(owner)
        .await
        .unwrap()
        .into_iter()
        .map(|g| g.id)
        .collect();
    assert!(!owner_ids.contains(&org_ocean));
    let member_ids: Vec<Uuid> = consumer_store
        .list_reachable_graphs(member)
        .await
        .unwrap()
        .into_iter()
        .map(|g| g.id)
        .collect();
    assert!(!member_ids.contains(&member_ocean));

    // ── Tenant arm ────────────────────────────────────────────────────────
    let ctx_owner = tenant_ctx(org_account, owner, &pool);
    let decision_owner = TenancyDecision::Org(ctx_owner);
    assert!(
        !ReachabilityResolver::graph_reachable_by_user(Some(&decision_owner), &consumer_store, org_ocean, owner)
            .await
            .unwrap(),
        "tenant arm: an archived org ocean must be unreachable even for the account owner"
    );
    let tenant_owner_ids: Vec<Uuid> =
        ReachabilityResolver::list_reachable_graphs(Some(&decision_owner), &consumer_store, owner)
            .await
            .unwrap()
            .into_iter()
            .map(|g| g.id)
            .collect();
    assert!(!tenant_owner_ids.contains(&org_ocean));

    let ctx_member = tenant_ctx(org_account, member, &pool);
    let decision_member = TenancyDecision::Org(ctx_member);
    assert!(
        !ReachabilityResolver::graph_reachable_by_user(Some(&decision_member), &consumer_store, member_ocean, member)
            .await
            .unwrap(),
        "tenant arm: an archived member ocean must be unreachable even for its own owner"
    );
    let tenant_member_ids: Vec<Uuid> =
        ReachabilityResolver::list_reachable_graphs(Some(&decision_member), &consumer_store, member)
            .await
            .unwrap()
            .into_iter()
            .map(|g| g.id)
            .collect();
    assert!(!tenant_member_ids.contains(&member_ocean));

    pool.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

//! Parity proof for the ONE permanent RLS carve-out (org-tenancy, OPS-4
//! pre-flight).
//!
//! # Why this file exists
//!
//! Every other consumer-plane query path is being wrapped in
//! `tenancy::scope::begin_account_scoped` so that, after OPS-4 flips the
//! backend's DB role to the non-BYPASSRLS `coralswarm_app`, migration
//! `0083_consumer_rls`'s policies act as a second, database-enforced backstop
//! behind the application's own predicates.
//!
//! `Store::list_reachable_graphs` / `Store::graph_reachable_by_user` — the
//! authorization backbone called from 13+ route files — cannot be one of them.
//! Its predicate is deliberately MULTI-account (ADR-0007 "Model B", documented
//! in `middleware/auth.rs`): a user belongs to several `accounts` rows and sees
//! across all of them in ONE query, so there is no single `account_id` to
//! `SET LOCAL`. PR #284 therefore moved it behind the `SECURITY DEFINER`
//! functions `resolve_reachable_graphs` / `resolve_graph_reachable_by_user`
//! (migration `0084_bootstrap_scope_functions`), which re-implement the same
//! predicate and run with the function OWNER's privileges.
//!
//! **That is precisely why this test is mandatory.** `SECURITY DEFINER` means
//! RLS is NOT evaluated inside the function body. If its predicate ever drifts
//! from the original — someone weakens or drops the `account_members` join in a
//! refactor, or the `team_members`/`departing_at` gate — the database provides
//! NO backstop whatsoever and every graph leaks to every caller. Unlike the
//! other 36 tables, this one query's correctness rests entirely on review and
//! tests. This file converts "trust the predicate" into "CI proves the
//! predicate".
//!
//! # What is actually asserted
//!
//! For every user in the fixture, the function's result must be **identical**
//! (same rows, same columns, same ORDER) to [`ORIGINAL_LIST_SQL`] — a verbatim
//! copy of the inline query `store/mod.rs` ran before PR #284 (recovered from
//! `git show 4df8126^:backend/src/store/mod.rs`, NOT paraphrased). The inline
//! side runs on the harness's owner pool, which is exactly how it executed in
//! production pre-OPS-4 (the app connects as the owner/BYPASSRLS role today);
//! the function side runs on a real `coralswarm_app` login, which is exactly
//! how it will execute post-OPS-4. Equality across that pair is the migration's
//! whole safety claim.
//!
//! Parity alone is not enough, though: a drift that hit BOTH sides identically
//! would still pass. So every user's reachable set is ALSO asserted against a
//! hand-derived expectation (`expected_ids`) written from the ADR, and there
//! are explicit negative assertions that no test user ever receives the third
//! account's graphs — the assertion that fails loudly if the `user_id` filter
//! were dropped entirely.
//!
//! Env-gated on `ORGT_TEST_BASE_URL`, like every `*_pg.rs` here — and WIRED
//! INTO CI (`.github/workflows/ci.yml`, job `rls-probe-harness`), because an
//! env-gated suite that never runs would be worth nothing at all here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test reachable_graphs_parity_pg

mod support;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// The ORIGINAL inline predicate, byte-for-byte as `Store::list_reachable_graphs`
/// ran it immediately before PR #284 replaced it with the SECURITY DEFINER
/// function (`git show 4df8126^:backend/src/store/mod.rs`). This constant is the
/// "expected" side of the parity check and must NEVER be edited to match the
/// function: if the two ever disagree, the function has drifted and that is a
/// live authorization defect, not a test-authoring problem.
const ORIGINAL_LIST_SQL: &str = "SELECT g.id, g.name, g.kind, g.is_primary, g.created_at \
     FROM graphs g \
     WHERE g.account_id IN \
         (SELECT account_id FROM account_members WHERE user_id = $1) \
       AND (g.kind <> 'team' OR g.id IN \
         (SELECT t.graph_id FROM teams t \
          JOIN team_members tm ON tm.team_id = t.id \
          WHERE tm.user_id = $1 AND tm.departing_at IS NULL)) \
     ORDER BY g.is_primary DESC, g.created_at DESC \
     LIMIT 500";

/// The post-#284 call shape, copied from today's `Store::list_reachable_graphs`.
/// `ORDER BY`/`LIMIT` live inside the function body, so the caller is a
/// passthrough — which is exactly what makes an ordered comparison here also a
/// proof that the function kept the original's ordering and 500-row backstop.
const FUNCTION_LIST_SQL: &str =
    "SELECT id, name, kind, is_primary, created_at FROM resolve_reachable_graphs($1)";

/// The ORIGINAL inline single-graph IDOR guard, byte-for-byte as
/// `Store::graph_reachable_by_user` ran it before PR #284 (same provenance as
/// [`ORIGINAL_LIST_SQL`]).
const ORIGINAL_EXISTS_SQL: &str = "SELECT EXISTS ( \
               SELECT 1 FROM graphs g \
               WHERE g.id = $1 \
                 AND g.account_id IN \
                     (SELECT account_id FROM account_members WHERE user_id = $2) \
                 AND (g.kind <> 'team' OR g.id IN \
                     (SELECT t.graph_id FROM teams t \
                      JOIN team_members tm ON tm.team_id = t.id \
                      WHERE tm.user_id = $2 AND tm.departing_at IS NULL)) \
             )";

/// The post-#284 call shape, copied from today's `Store::graph_reachable_by_user`.
const FUNCTION_EXISTS_SQL: &str = "SELECT resolve_graph_reachable_by_user($1, $2)";

/// The five columns both sides return, compared as whole tuples (not just ids)
/// so a drift that swapped/dropped a projected column is caught too.
type GraphRow = (Uuid, String, String, bool, DateTime<Utc>);

// ── Fixture construction ────────────────────────────────────────────────────

async fn new_user(owner: &PgPool, email: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(owner)
        .await
        .expect("insert user")
}

async fn new_account(owner: &PgPool, owner_user_id: Uuid) -> Uuid {
    sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
        .bind(owner_user_id)
        .fetch_one(owner)
        .await
        .expect("insert account")
}

async fn add_member(owner: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, $3)")
        .bind(account_id)
        .bind(user_id)
        .bind(role)
        .execute(owner)
        .await
        .expect("insert account_members");
}

/// A graph with an EXPLICIT `created_at` (`age_secs` ago). Explicit ages make
/// the `ORDER BY g.is_primary DESC, g.created_at DESC` comparison fully
/// deterministic — two rows inserted microseconds apart could otherwise tie and
/// let the two query plans order them differently, which would be a flake, not
/// a finding.
async fn new_graph(
    owner: &PgPool,
    account_id: Uuid,
    name: &str,
    kind: &str,
    is_primary: bool,
    age_secs: i32,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary, created_at) \
         VALUES ($1, $2, $3, $4, now() - ($5::int * interval '1 second')) RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .bind(kind)
    .bind(is_primary)
    .bind(age_secs)
    .fetch_one(owner)
    .await
    .expect("insert graph")
}

/// Re-stamp a graph the harness seeded for us (`probe.a`/`probe.b`) so it takes
/// part in the same deterministic ordering as the graphs this test seeds.
async fn restamp_graph(
    owner: &PgPool,
    graph_id: Uuid,
    kind: &str,
    is_primary: bool,
    age_secs: i32,
) {
    sqlx::query(
        "UPDATE graphs SET kind = $2, is_primary = $3, \
         created_at = now() - ($4::int * interval '1 second') WHERE id = $1",
    )
    .bind(graph_id)
    .bind(kind)
    .bind(is_primary)
    .bind(age_secs)
    .execute(owner)
    .await
    .expect("restamp harness graph");
}

async fn new_team(owner: &PgPool, account_id: Uuid, graph_id: Uuid, name: &str, by: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO teams(account_id, graph_id, name, created_by) \
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(name)
    .bind(by)
    .fetch_one(owner)
    .await
    .expect("insert team")
}

/// `departing` mirrors the predicate's `tm.departing_at IS NULL` gate: a
/// departed member row still EXISTS, it just must not grant reachability.
async fn add_team_member(owner: &PgPool, team_id: Uuid, user_id: Uuid, departing: bool) {
    let sql = if departing {
        "INSERT INTO team_members(team_id, user_id, role, departing_at) VALUES ($1, $2, 'member', now())"
    } else {
        "INSERT INTO team_members(team_id, user_id, role) VALUES ($1, $2, 'member')"
    };
    sqlx::query(sql)
        .bind(team_id)
        .bind(user_id)
        .execute(owner)
        .await
        .expect("insert team_members");
}

// ── The two sides of the parity check ───────────────────────────────────────

/// The ORIGINAL inline query, on the owner pool — i.e. exactly how this
/// authorization read executes in production TODAY (the app connects as the
/// owner/BYPASSRLS role until OPS-4, and 0083 does not `FORCE ROW LEVEL
/// SECURITY`, so no policy applies to it).
async fn list_via_original(owner: &PgPool, user_id: Uuid) -> Vec<GraphRow> {
    sqlx::query_as(ORIGINAL_LIST_SQL)
        .bind(user_id)
        .fetch_all(owner)
        .await
        .expect("original inline list_reachable_graphs predicate")
}

/// The SECURITY DEFINER function, on a real `coralswarm_app` login with
/// `app.account_id` never set — i.e. exactly how it will execute AFTER OPS-4.
async fn list_via_function(app_pool: &PgPool, user_id: Uuid) -> Vec<GraphRow> {
    sqlx::query_as(FUNCTION_LIST_SQL)
        .bind(user_id)
        .fetch_all(app_pool)
        .await
        .expect("resolve_reachable_graphs as coralswarm_app")
}

async fn exists_via_original(owner: &PgPool, graph_id: Uuid, user_id: Uuid) -> bool {
    sqlx::query_scalar(ORIGINAL_EXISTS_SQL)
        .bind(graph_id)
        .bind(user_id)
        .fetch_one(owner)
        .await
        .expect("original inline graph_reachable_by_user predicate")
}

async fn exists_via_function(app_pool: &PgPool, graph_id: Uuid, user_id: Uuid) -> bool {
    sqlx::query_scalar(FUNCTION_EXISTS_SQL)
        .bind(graph_id)
        .bind(user_id)
        .fetch_one(app_pool)
        .await
        .expect("resolve_graph_reachable_by_user as coralswarm_app")
}

fn ids(rows: &[GraphRow]) -> Vec<Uuid> {
    rows.iter().map(|r| r.0).collect()
}

// ── The fixture ─────────────────────────────────────────────────────────────

/// Named handles for every row the parity test reasons about. The shape is
/// chosen to catch the drift that would actually matter:
///
/// * `u_multi` belongs to TWO accounts (A and B) — the Model B case that makes
///   RLS scoping impossible in the first place, and the case a naive
///   "one account per user" rewrite would silently truncate;
/// * `u_single` belongs to ONE of them (B) — so A's rows must NOT appear for
///   them, proving the `account_members` join filters per-user and not merely
///   per-"has any membership";
/// * `u_none` belongs to none — the empty-set floor;
/// * account C belongs to a user none of the above is: its graphs are the
///   "must never appear" control, and `u_outsider` proves those rows are
///   genuinely reachable-by-someone (so their absence elsewhere is real
///   filtering, not an artifact of a broken fixture);
/// * team oceans cover all three states: active member (`a_team`), departed
///   member (`b_team` for `u_multi`, via `departing_at`), and non-member
///   (`a_team_out`) — the `teams`/`team_members` path IS part of the original
///   predicate, so it is part of the parity surface.
struct Fixture {
    u_multi: Uuid,
    u_single: Uuid,
    u_none: Uuid,
    u_outsider: Uuid,

    a_home: Uuid,
    a_team: Uuid,
    a_team_out: Uuid,
    b_home: Uuid,
    b_org: Uuid,
    b_team: Uuid,
    c_home: Uuid,
    c_team: Uuid,

    account_a: Uuid,
    account_b: Uuid,
}

impl Fixture {
    fn all_graphs(&self) -> Vec<(&'static str, Uuid)> {
        vec![
            ("a_home", self.a_home),
            ("a_team", self.a_team),
            ("a_team_out", self.a_team_out),
            ("b_home", self.b_home),
            ("b_org", self.b_org),
            ("b_team", self.b_team),
            ("c_home", self.c_home),
            ("c_team", self.c_team),
        ]
    }

    fn all_users(&self) -> Vec<(&'static str, Uuid)> {
        vec![
            ("u_multi", self.u_multi),
            ("u_single", self.u_single),
            ("u_none", self.u_none),
            ("u_outsider", self.u_outsider),
        ]
    }

    /// The hand-derived D9 read union (ADR-0007) per user, IN THE ORDER the
    /// predicate must return it (`is_primary DESC, created_at DESC`). Written
    /// from the ADR rather than from either SQL text, so it fails even if both
    /// sides drift together.
    fn expected_ids(&self, user: Uuid) -> Vec<Uuid> {
        if user == self.u_multi {
            // A + B, minus the team oceans they are not an ACTIVE member of
            // (`a_team_out`: never a member; `b_team`: departed).
            vec![self.a_home, self.b_org, self.b_home, self.a_team]
        } else if user == self.u_single {
            // B only — none of A's graphs, however reachable they are for the
            // co-member `u_multi`.
            vec![self.b_team, self.b_org, self.b_home]
        } else if user == self.u_none {
            vec![]
        } else if user == self.u_outsider {
            // `c_home` is C's is_primary ocean, so it sorts ahead of the newer
            // `c_team` under `is_primary DESC` before `created_at DESC` applies.
            vec![self.c_home, self.c_team]
        } else {
            panic!("unknown fixture user {user}");
        }
    }
}

/// Ages (seconds ago) are hand-assigned so the expected ORDER above is a
/// property of the fixture, not of insertion timing.
async fn seed(probe: &RlsProbe) -> Fixture {
    let owner = &probe.owner;

    // ── Users ────────────────────────────────────────────────────────────────
    // Reuse the harness's two seeded accounts (A, B) and their owner users;
    // the harness does NOT create any `account_members` rows, so every
    // membership below is explicit and this test owns the whole predicate
    // input.
    let u_multi = probe.a.user_id;
    let u_single = probe.b.user_id;
    let u_none = new_user(owner, "parity-none@x.co").await;
    let u_outsider = new_user(owner, "parity-outsider@x.co").await;

    let account_a = probe.a.account_id;
    let account_b = probe.b.account_id;
    let account_c = new_account(owner, u_outsider).await;

    // ── Memberships (Model B: u_multi spans TWO accounts) ────────────────────
    add_member(owner, account_a, u_multi, "owner").await;
    add_member(owner, account_b, u_multi, "member").await;
    add_member(owner, account_b, u_single, "owner").await;
    add_member(owner, account_c, u_outsider, "owner").await;
    // u_none deliberately gets no membership row at all.

    // ── Account A ────────────────────────────────────────────────────────────
    let a_home = probe.a.graph_id;
    restamp_graph(owner, a_home, "personal", true, 500).await;
    let a_team = new_graph(owner, account_a, "a-team", "team", false, 400).await;
    let a_team_out = new_graph(owner, account_a, "a-team-out", "team", false, 300).await;
    let t_a = new_team(owner, account_a, a_team, "eng", u_multi).await;
    add_team_member(owner, t_a, u_multi, false).await;
    // `a_team_out`'s team exists and has a member — just not u_multi. Without
    // this row the "not reachable" result could come from an empty team rather
    // than from the membership gate.
    let t_a_out = new_team(owner, account_a, a_team_out, "ops", u_multi).await;
    add_team_member(owner, t_a_out, u_outsider, false).await;

    // ── Account B ────────────────────────────────────────────────────────────
    let b_home = probe.b.graph_id;
    restamp_graph(owner, b_home, "personal", false, 250).await;
    let b_org = new_graph(owner, account_b, "b-org", "org", false, 150).await;
    let b_team = new_graph(owner, account_b, "b-team", "team", false, 100).await;
    let t_b = new_team(owner, account_b, b_team, "design", u_single).await;
    add_team_member(owner, t_b, u_single, false).await;
    // Departed: the row exists, `departing_at` is set — the predicate must NOT
    // count it, for u_multi, even though u_multi IS a member of account B.
    add_team_member(owner, t_b, u_multi, true).await;

    // ── Account C: reachable by nobody in the parity loop ────────────────────
    let c_home = new_graph(owner, account_c, "c-home", "personal", true, 90).await;
    let c_team = new_graph(owner, account_c, "c-team", "team", false, 80).await;
    let t_c = new_team(owner, account_c, c_team, "outsiders", u_outsider).await;
    add_team_member(owner, t_c, u_outsider, false).await;

    Fixture {
        u_multi,
        u_single,
        u_none,
        u_outsider,
        a_home,
        a_team,
        a_team_out,
        b_home,
        b_org,
        b_team,
        c_home,
        c_team,
        account_a,
        account_b,
    }
}

// ── The proofs ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn resolve_reachable_graphs_matches_the_original_inline_predicate() {
    let Some(probe) = RlsProbe::setup("orgt_reachable_parity").await else {
        eprintln!(
            "skipping resolve_reachable_graphs_matches_the_original_inline_predicate: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let f = seed(&probe).await;

    for (label, user) in f.all_users() {
        let expected = list_via_original(&probe.owner, user).await;
        let actual = list_via_function(&probe.app_pool, user).await;

        // 1. PARITY: identical rows, identical columns, identical ORDER. This
        //    is the whole claim migration 0084 makes about this carve-out.
        assert_eq!(
            actual, expected,
            "{label}: resolve_reachable_graphs() must return EXACTLY what the original \
             inline `account_members`-joined predicate returns (same rows, same columns, \
             same order). A difference here is a live authorization defect in the \
             SECURITY DEFINER function, NOT a test to adjust — see this file's header."
        );

        // 2. Neither side may have drifted from the ADR-0007 D9 read union.
        //    (Parity alone would still pass if BOTH sides broke identically.)
        assert_eq!(
            ids(&actual),
            f.expected_ids(user),
            "{label}: the reachable set itself (and its ORDER BY is_primary DESC, \
             created_at DESC ordering) must match the hand-derived D9 read union"
        );
    }

    // 3. NEGATIVE — the assertion that fails loudly if the `user_id` filter (or
    //    the whole `account_members` subquery) were ever dropped: account C's
    //    graphs must never reach anyone outside account C.
    for (label, user) in [("u_multi", f.u_multi), ("u_single", f.u_single), ("u_none", f.u_none)] {
        let got = ids(&list_via_function(&probe.app_pool, user).await);
        assert!(
            !got.contains(&f.c_home) && !got.contains(&f.c_team),
            "{label} is not a member of account C — resolve_reachable_graphs() leaked C's \
             graphs ({:?} / {:?}) into {got:?}. Either the account_members filter is gone \
             or it no longer filters by user_id.",
            f.c_home,
            f.c_team
        );
    }
    // ...and the control that makes the negative meaningful: C's graphs ARE
    // returned to C's own member, so their absence above is real filtering
    // rather than rows that were never visible to anyone.
    let outsider = ids(&list_via_function(&probe.app_pool, f.u_outsider).await);
    assert!(
        outsider.contains(&f.c_home) && outsider.contains(&f.c_team),
        "account C's own member must reach C's graphs — otherwise the negative assertions \
         above prove nothing. Got {outsider:?}"
    );

    // 4. The carve-out must keep working from a connection that IS already
    //    scoped to one account (routes reach this authorization check from
    //    inside `begin_account_scoped` transactions elsewhere in the request).
    //    SECURITY DEFINER means the caller's `app.account_id` is irrelevant —
    //    the multi-account result must not be silently narrowed to the scoped
    //    account, which is the failure mode a "just scope it like everything
    //    else" refactor would introduce.
    for scope in [f.account_a, f.account_b] {
        let mut tx = probe.scoped_tx(scope).await;
        let scoped: Vec<GraphRow> = sqlx::query_as(FUNCTION_LIST_SQL)
            .bind(f.u_multi)
            .fetch_all(&mut *tx)
            .await
            .expect("resolve_reachable_graphs inside a scoped transaction");
        tx.rollback().await.ok();
        assert_eq!(
            ids(&scoped),
            f.expected_ids(f.u_multi),
            "scoped to {scope}, the multi-account carve-out must STILL return the full \
             cross-account reachable set (that is the entire reason it is a carve-out)"
        );
    }

    probe.teardown().await;
}

#[tokio::test]
async fn resolve_graph_reachable_by_user_matches_the_original_inline_predicate() {
    let Some(probe) = RlsProbe::setup("orgt_reachable_parity_bool").await else {
        eprintln!(
            "skipping resolve_graph_reachable_by_user_matches_the_original_inline_predicate: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let f = seed(&probe).await;

    // The boolean variant is on the SAME authorization path as the list variant
    // (it is the IDOR guard the retrieval route calls), so it gets the same
    // treatment: every user x every graph, function vs original vs the
    // hand-derived expectation.
    for (ulabel, user) in f.all_users() {
        let expected_set = f.expected_ids(user);
        for (glabel, graph) in f.all_graphs() {
            let expected = exists_via_original(&probe.owner, graph, user).await;
            let actual = exists_via_function(&probe.app_pool, graph, user).await;
            assert_eq!(
                actual, expected,
                "{ulabel} x {glabel}: resolve_graph_reachable_by_user() disagrees with the \
                 original inline EXISTS predicate — a live authorization defect, not a test \
                 to adjust (see this file's header)"
            );
            assert_eq!(
                actual,
                expected_set.contains(&graph),
                "{ulabel} x {glabel}: reachability must match the hand-derived D9 read union"
            );
        }
    }

    // NEGATIVE, single-graph form: the unreachable third account's graphs are
    // false for everyone outside C, and true for C's own member (so the false
    // is filtering, not a missing row).
    for (label, user) in [("u_multi", f.u_multi), ("u_single", f.u_single), ("u_none", f.u_none)] {
        for (glabel, graph) in [("c_home", f.c_home), ("c_team", f.c_team)] {
            assert!(
                !exists_via_function(&probe.app_pool, graph, user).await,
                "{label} is not a member of account C — resolve_graph_reachable_by_user() \
                 must be false for {glabel}"
            );
        }
    }
    assert!(
        exists_via_function(&probe.app_pool, f.c_home, f.u_outsider).await,
        "account C's own member must reach c_home — otherwise the negatives prove nothing"
    );

    // A graph id that does not exist at all is false, never an error.
    assert!(
        !exists_via_function(&probe.app_pool, Uuid::new_v4(), f.u_multi).await,
        "a nonexistent graph_id must be unreachable (false), not an error"
    );

    // Same scoped-connection property as the list variant: a caller already
    // inside `begin_account_scoped(account_a)` must still be able to authorize
    // a graph in account B (Model B cross-account read).
    let mut tx = probe.scoped_tx(f.account_a).await;
    let cross: bool = sqlx::query_scalar(FUNCTION_EXISTS_SQL)
        .bind(f.b_org)
        .bind(f.u_multi)
        .fetch_one(&mut *tx)
        .await
        .expect("resolve_graph_reachable_by_user inside a scoped transaction");
    tx.rollback().await.ok();
    assert!(
        cross,
        "scoped to account A, the carve-out must still authorize u_multi against account B's \
         org ocean — narrowing it to the scoped account would break Model B"
    );

    probe.teardown().await;
}

/// The `ORDER BY ... LIMIT 500` backstop is part of the predicate that moved
/// into the function body, so it is part of the parity surface: a refactor that
/// dropped (or changed) the LIMIT would make the function return a different
/// result set than the original for a pathological account, and the two smaller
/// fixtures above — well under 500 rows — could never notice.
#[tokio::test]
async fn resolve_reachable_graphs_matches_the_original_at_the_limit_backstop() {
    let Some(probe) = RlsProbe::setup("orgt_reachable_parity_limit").await else {
        eprintln!(
            "skipping resolve_reachable_graphs_matches_the_original_at_the_limit_backstop: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let user = probe.a.user_id;
    add_member(&probe.owner, probe.a.account_id, user, "owner").await;

    // 520 personal oceans with strictly distinct, strictly decreasing
    // created_at values, so `ORDER BY is_primary DESC, created_at DESC` is a
    // total order and "which 500 survive the LIMIT" is deterministic.
    sqlx::query(
        "INSERT INTO graphs(account_id, name, kind, is_primary, created_at) \
         SELECT $1, 'bulk-' || i, 'personal', false, now() - (i * interval '1 second') \
         FROM generate_series(1, 520) AS i",
    )
    .bind(probe.a.account_id)
    .execute(&probe.owner)
    .await
    .expect("bulk-insert 520 graphs");

    let expected = list_via_original(&probe.owner, user).await;
    let actual = list_via_function(&probe.app_pool, user).await;

    assert_eq!(
        expected.len(),
        500,
        "sanity: the original predicate's LIMIT 500 must actually bind here \
         (521 reachable graphs seeded)"
    );
    assert_eq!(
        actual, expected,
        "at the LIMIT boundary the function must return the SAME 500 rows in the SAME order \
         as the original inline predicate — a dropped or changed LIMIT/ORDER BY is drift"
    );

    probe.teardown().await;
}

/// Member-private-oceans PR 2 (plan §2.1/§2.2/§7): `resolve_reachable_graphs`'s
/// `RETURNS TABLE` grew from 5 to 7 columns (migration
/// `0091_member_ocean_reachability_functions`, adding `owner_user_id` /
/// `archived_at` — the two columns migration 0090 added to `graphs`). That
/// growth is exactly the hazard `store::mod`'s `GraphRow`/
/// `tenancy::reachability`'s `GraphRow` warn about: a caller that lists
/// columns by name still compiles and runs even if the function's declared
/// output order silently drifted from the SELECT list, because `sqlx`'s
/// tuple `FromRow` binds positionally — it does not check names. This test
/// selects all 7 columns EXPLICITLY BY NAME (never `SELECT *`) and asserts
/// `owner_user_id`/`archived_at` round-trip to the RIGHT row, which is the
/// only way a transposition (e.g. the two new columns swapped, or bound
/// against the wrong row) would actually be caught. It also proves the new
/// predicate clauses this same migration adds — `archived_at IS NULL` and
/// the member-ocean pure-equality gate — hold on the SAME carve-out function
/// this file's other tests already prove parity for.
#[tokio::test]
async fn resolve_reachable_graphs_seven_column_shape_and_member_archived_gating() {
    let Some(probe) = RlsProbe::setup("orgt_reachable_parity_7col").await else {
        eprintln!(
            "skipping resolve_reachable_graphs_seven_column_shape_and_member_archived_gating: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let owner = probe.a.user_id;
    let member = new_user(&probe.owner, "sevencol-member@x.co").await;
    add_member(&probe.owner, probe.a.account_id, owner, "owner").await;
    add_member(&probe.owner, probe.a.account_id, member, "member").await;

    // A hand-seeded `kind = 'member'` ocean owned by `member` — nothing in
    // this repo creates one yet (PR 4/`MEMBER_OCEANS_ENABLED` does), so this
    // test seeds it directly, same as `graphs_member_requires_owner`
    // (migration 0090/0005) requires: `owner_user_id` must be set iff
    // `kind = 'member'`.
    let member_ocean: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
         VALUES ($1, 'member ocean', 'member', $2) RETURNING id",
    )
    .bind(probe.a.account_id)
    .bind(member)
    .fetch_one(&probe.owner)
    .await
    .expect("insert member graph");

    // A hand-stamped archived ordinary ocean — same rationale: PR 7 is the
    // real writer, this test hand-seeds the column to prove the predicate.
    let archived_ocean: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, archived_at) \
         VALUES ($1, 'archived org', 'org', now()) RETURNING id",
    )
    .bind(probe.a.account_id)
    .fetch_one(&probe.owner)
    .await
    .expect("insert archived graph");

    // ── The 7-column tuple bind, selected by name ───────────────────────────
    type GraphRow7 = (
        Uuid,
        String,
        String,
        bool,
        DateTime<Utc>,
        Option<Uuid>,
        Option<DateTime<Utc>>,
    );
    let rows: Vec<GraphRow7> = sqlx::query_as(
        "SELECT id, name, kind, is_primary, created_at, owner_user_id, archived_at \
         FROM resolve_reachable_graphs($1)",
    )
    .bind(member)
    .fetch_all(&probe.app_pool)
    .await
    .expect("7-column resolve_reachable_graphs select as coralswarm_app");

    let member_row = rows
        .iter()
        .find(|r| r.0 == member_ocean)
        .expect("member must reach their own member ocean, 7-column shape");
    assert_eq!(
        member_row.5,
        Some(member),
        "owner_user_id must round-trip to the SAME row it belongs to, not \
         transposed against archived_at or any other column"
    );
    assert_eq!(
        member_row.6, None,
        "archived_at must be NULL for a non-archived row"
    );

    // ── Predicate proof, on the SAME carve-out function: archived is gone
    //    for EVERYONE (including the account owner), and the member ocean is
    //    gone for everyone except its owner. ─────────────────────────────────
    let owner_ids = ids(&list_via_function(&probe.app_pool, owner).await);
    assert!(
        !owner_ids.contains(&archived_ocean),
        "an archived graph must never be reachable, even for the account owner"
    );
    assert!(
        !owner_ids.contains(&member_ocean),
        "the account owner must never reach another member's private ocean"
    );

    let member_ids = ids(&list_via_function(&probe.app_pool, member).await);
    assert!(
        member_ids.contains(&member_ocean),
        "the owning member must reach their own member ocean"
    );
    assert!(
        !member_ids.contains(&archived_ocean),
        "archived_at IS NULL is unconditional — it must exclude the archived ocean here too"
    );

    probe.teardown().await;
}

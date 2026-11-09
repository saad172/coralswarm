//! Live behavioral proof for `store::identity_class` (identity-resolution-
//! at-the-door design doc, §10.1 item 1 / §10.5) against a REAL Postgres:
//! union-find class folding over the three same-identity relations
//! (title/alias, `reef_identifiers`, active POSITIVE `reef_judgement`),
//! representative selection, and the three §10.5 convergence-gauge queries.
//!
//! Uses the shared [`RlsProbe`] harness purely for its scratch DB + migrated
//! control tree (`probe.owner`, the BYPASSRLS pool) — these functions run
//! plain SQL with no RLS session variable of their own (the caller's scope
//! is what enforces tenancy in production; see the module doc on
//! `store::identity_class`), so the `coralswarm_app` role dance is not
//! needed here.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same convention as every other `*_pg.rs`
//! suite here; a plain `cargo test` with no DB configured skips every test
//! in this file (each prints its own skip line — confirm a real run by
//! checking wall-clock and pass count, per the "0 passed in 0.00s means the
//! DSN was unset" lesson).
//!
//! Run with:
//!   ORGT_TEST_BASE_URL="postgres://alex@localhost:5432/postgres" \
//!     cargo test -p coralswarm-backend --test identity_class_pg

mod support;

use chrono::{Duration, Utc};
use coralswarm_backend::store::identity_class;
use serde_json::json;
use sqlx::PgPool;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

// ── Fixture helpers ──────────────────────────────────────────────────────────

async fn insert_node(
    pool: &PgPool,
    graph_id: Uuid,
    title: &str,
    body: serde_json::Value,
    coral_count: i64,
    created_at: chrono::DateTime<Utc>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title, body, coral_count, created_at) \
         VALUES ($1, 'entity', $2, $3, $4, $5) RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .bind(body)
    .bind(coral_count)
    .bind(created_at)
    .fetch_one(pool)
    .await
    .expect("insert node")
}

async fn set_aliases(pool: &PgPool, node_id: Uuid, aliases: &[&str]) {
    sqlx::query("UPDATE nodes SET body = jsonb_set(body, '{aliases}', $2::jsonb) WHERE node_id = $1")
        .bind(node_id)
        .bind(serde_json::to_value(aliases).unwrap())
        .execute(pool)
        .await
        .expect("set aliases");
}

async fn clear_aliases(pool: &PgPool, node_id: Uuid) {
    sqlx::query("UPDATE nodes SET body = body - 'aliases' WHERE node_id = $1")
        .bind(node_id)
        .execute(pool)
        .await
        .expect("clear aliases");
}

async fn insert_identifier(pool: &PgPool, graph_id: Uuid, node_id: Uuid, kind: &str, value_norm: &str) {
    sqlx::query(
        "INSERT INTO reef_identifiers (graph_id, node_id, kind, value_norm, source) \
         VALUES ($1, $2, $3, $4, 'test')",
    )
    .bind(graph_id)
    .bind(node_id)
    .bind(kind)
    .bind(value_norm)
    .execute(pool)
    .await
    .expect("insert reef_identifiers row");
}

async fn insert_positive_judgement(pool: &PgPool, graph_id: Uuid, a: Uuid, b: Uuid) {
    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
    sqlx::query(
        "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source) \
         VALUES ($1, $2, $3, 'positive', 'test')",
    )
    .bind(graph_id)
    .bind(lo)
    .bind(hi)
    .execute(pool)
    .await
    .expect("insert positive reef_judgement row");
}

/// `reef_identifiers` carries a live `UNIQUE (graph_id, kind, value_norm)`
/// constraint (migration 0138: "this email/slack id/etc. names exactly one
/// reef per ocean") — under normal writes, two DIFFERENT nodes can never
/// simultaneously hold the same identifier, so the "two rows, same value"
/// shape `classes_for`'s union-find defends against cannot arise through the
/// application's own write paths on a schema-correct database. It remains a
/// real, worth-testing case: legacy rows written before 0138 existed, or a
/// narrow write race that slips past the constraint via a future schema
/// change, both leave the SAME defensive fold doing real work. This helper
/// drops the constraint on the test's own throwaway scratch DB ONLY, so the
/// fixture below can legitimately construct that state and prove the query
/// logic folds it correctly.
async fn drop_unique_identifier_constraint(pool: &PgPool) {
    let name: String = sqlx::query_scalar(
        "SELECT conname FROM pg_constraint WHERE conrelid = 'reef_identifiers'::regclass AND contype = 'u'",
    )
    .fetch_one(pool)
    .await
    .expect("find reef_identifiers' UNIQUE constraint name");
    sqlx::query(&format!("ALTER TABLE reef_identifiers DROP CONSTRAINT {name}"))
        .execute(pool)
        .await
        .expect("drop the UNIQUE constraint for this test's legacy-duplicate fixture");
}

async fn set_self_node(pool: &PgPool, graph_id: Uuid, node_id: Uuid) {
    sqlx::query("UPDATE graphs SET self_node_id = $1 WHERE id = $2")
        .bind(node_id)
        .bind(graph_id)
        .execute(pool)
        .await
        .expect("set self_node_id");
}

fn sorted(mut v: Vec<Uuid>) -> Vec<Uuid> {
    v.sort();
    v
}

fn find_class(classes: &[Vec<Uuid>], member: Uuid) -> Vec<Uuid> {
    classes
        .iter()
        .find(|c| c.contains(&member))
        .unwrap_or_else(|| panic!("{member} not found in any class: {classes:?}"))
        .clone()
}

// ── classes_for ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn classes_for_never_unions_over_name_alias_alone() {
    let Some(probe) = RlsProbe::setup("orgt_identity_class_alias").await else {
        eprintln!("skipping classes_for_never_unions_over_name_alias_alone: set ORGT_TEST_BASE_URL");
        return;
    };
    let g = probe.a.graph_id;
    let now = Utc::now();

    // "Alex Example" carries "Alex" as a learned alias; "Alex" is a
    // separate reef whose OWN lowercased title equals that alias.
    let hub = insert_node(&probe.owner, g, "Alex Example", json!({}), 100, now).await;
    set_aliases(&probe.owner, hub, &["Alex", "Alex Alex"]).await;
    let twin = insert_node(&probe.owner, g, "Alex", json!({}), 3, now).await;
    // An unrelated third reef must stay its own singleton.
    let stranger = insert_node(&probe.owner, g, "CoralSwarm", json!({}), 50, now).await;

    // Each `PoolConnection` is scoped to its own block and dropped at the
    // block's end — NOT left held via a `let`-shadowed binding, which would
    // still be alive (Rust shadowing does not drop early) when
    // `probe.teardown()` calls `owner.close().await`, deadlocking it against
    // its own still-checked-out connection.
    let classes = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::classes_for(&mut conn, g, &[hub, twin, stranger]).await.expect("classes_for")
    };

    assert_eq!(find_class(&classes, hub), vec![hub], "a shared name/alias is not established identity");
    assert_eq!(find_class(&classes, twin),vec![twin]);
    assert_eq!(find_class(&classes, stranger), vec![stranger], "unrelated reef stays a singleton");

    // ── Negative control: remove the alias, the fold must not survive it ──────
    clear_aliases(&probe.owner, hub).await;
    let classes_after = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::classes_for(&mut conn, g, &[hub, twin, stranger])
            .await
            .expect("classes_for after removing the alias")
    };
    assert_eq!(find_class(&classes_after, hub), vec![hub], "class must split once the alias is gone");
    assert_eq!(find_class(&classes_after, twin), vec![twin]);

    probe.teardown().await;
}

#[tokio::test]
async fn classes_for_unions_over_shared_reef_identifier() {
    let Some(probe) = RlsProbe::setup("orgt_identity_class_ident").await else {
        eprintln!("skipping classes_for_unions_over_shared_reef_identifier: set ORGT_TEST_BASE_URL");
        return;
    };
    let g = probe.a.graph_id;
    let now = Utc::now();

    // See `drop_unique_identifier_constraint`'s doc: this simulates a
    // legacy/pre-migration state the live schema no longer permits new
    // writes to create, so `classes_for`'s identifier fold can be proven
    // against real rows instead of only asserted by code inspection.
    drop_unique_identifier_constraint(&probe.owner).await;

    // No title/alias overlap at all — the ONLY signal is the shared email
    // identifier, exactly the "Alex" / email-titled-reef shape from the
    // design doc's evidence.
    let a = insert_node(&probe.owner, g, "Alex Example", json!({}), 100, now).await;
    let b = insert_node(&probe.owner, g, "person@example.com", json!({}), 0, now).await;
    insert_identifier(&probe.owner, g, a, "email", "person@example.com").await;
    insert_identifier(&probe.owner, g, b, "email", "person@example.com").await;

    let classes = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::classes_for(&mut conn, g, &[a, b]).await.expect("classes_for")
    };
    assert_eq!(sorted(find_class(&classes, a)), sorted(vec![a, b]), "shared identifier folds the pair");

    probe.teardown().await;
}

#[tokio::test]
async fn classes_for_unions_over_active_positive_judgement() {
    let Some(probe) = RlsProbe::setup("orgt_identity_class_judg").await else {
        eprintln!("skipping classes_for_unions_over_active_positive_judgement: set ORGT_TEST_BASE_URL");
        return;
    };
    let g = probe.a.graph_id;
    let now = Utc::now();

    let a = insert_node(&probe.owner, g, "Mahomet SAD", json!({}), 1, now).await;
    let b = insert_node(&probe.owner, g, "alex", json!({}), 0, now).await;
    insert_positive_judgement(&probe.owner, g, a, b).await;

    let classes = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::classes_for(&mut conn, g, &[a, b]).await.expect("classes_for")
    };
    assert_eq!(sorted(find_class(&classes, a)), sorted(vec![a, b]), "an active POSITIVE judgement folds the pair");

    probe.teardown().await;
}

#[tokio::test]
async fn classes_for_returns_singletons_with_no_relation() {
    let Some(probe) = RlsProbe::setup("orgt_identity_class_none").await else {
        eprintln!("skipping classes_for_returns_singletons_with_no_relation: set ORGT_TEST_BASE_URL");
        return;
    };
    let g = probe.a.graph_id;
    let now = Utc::now();

    let a = insert_node(&probe.owner, g, "Apollo", json!({}), 5, now).await;
    let b = insert_node(&probe.owner, g, "Zephyr", json!({}), 5, now).await;

    let classes = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::classes_for(&mut conn, g, &[a, b]).await.expect("classes_for")
    };
    assert_eq!(classes.len(), 2, "no shared signal ⇒ two singleton classes, got {classes:?}");
    assert_eq!(find_class(&classes, a), vec![a]);
    assert_eq!(find_class(&classes, b), vec![b]);

    probe.teardown().await;
}

// ── representative ───────────────────────────────────────────────────────────

#[tokio::test]
async fn representative_prefers_the_self_reef_over_higher_mass() {
    let Some(probe) = RlsProbe::setup("orgt_identity_class_self").await else {
        eprintln!("skipping representative_prefers_the_self_reef_over_higher_mass: set ORGT_TEST_BASE_URL");
        return;
    };
    let g = probe.a.graph_id;
    let now = Utc::now();

    // The self reef is the SMALLEST by mass — a naive max-coral-count pick
    // would choose `hub` instead. Representative selection must not.
    let self_reef = insert_node(&probe.owner, g, "Alex Example", json!({}), 5, now).await;
    let hub = insert_node(&probe.owner, g, "Alex Alex", json!({}), 3198, now).await;
    let tiny = insert_node(&probe.owner, g, "alex", json!({}), 0, now).await;
    set_self_node(&probe.owner, g, self_reef).await;

    let rep = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::representative(&mut conn, g, &[self_reef, hub, tiny]).await.expect("representative")
    };
    assert_eq!(rep, self_reef, "the self reef must win regardless of mass");

    probe.teardown().await;
}

// ── §10.5 convergence gauges ──────────────────────────────────────────────────

#[tokio::test]
async fn gauges_report_expected_counts_on_a_small_fixture() {
    let Some(probe) = RlsProbe::setup("orgt_identity_class_gauges").await else {
        eprintln!("skipping gauges_report_expected_counts_on_a_small_fixture: set ORGT_TEST_BASE_URL");
        return;
    };
    let g = probe.a.graph_id;
    let now = Utc::now();

    // ── rebirths_24h: a reef minted in the last 24h whose lower(title) is
    // ALREADY an alias of another same-type reef. ──────────────────────────
    let owner_reef = insert_node(&probe.owner, g, "Alex Example", json!({}), 500, now - Duration::days(30)).await;
    set_aliases(&probe.owner, owner_reef, &["Alex"]).await;
    let rebirth = insert_node(&probe.owner, g, "Alex", json!({}), 0, now - Duration::hours(2)).await;
    // A control reef minted just as recently, but with NO alias/identifier
    // collision — must NOT count as a rebirth.
    let _fresh_unrelated = insert_node(&probe.owner, g, "Fresh Topic", json!({}), 0, now - Duration::hours(1)).await;

    // ── twins_24h: a merge in the last 24h whose dup lived < 7 days. ───────
    let short_lived_dup_created = now - Duration::hours(6);
    sqlx::query(
        "INSERT INTO node_merges (graph_id, keep_id, dup_id, dup_snapshot, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(g)
    .bind(owner_reef)
    .bind(Uuid::new_v4())
    .bind(json!({"type": "entity", "title": "dup", "created_at": short_lived_dup_created.to_rfc3339()}))
    .bind(now - Duration::hours(1))
    .execute(&probe.owner)
    .await
    .expect("insert a twin merge (dup lived 6h)");
    // A control merge whose dup lived > 7 days — must NOT count as a twin.
    let long_lived_dup_created = now - Duration::days(30);
    sqlx::query(
        "INSERT INTO node_merges (graph_id, keep_id, dup_id, dup_snapshot, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(g)
    .bind(owner_reef)
    .bind(Uuid::new_v4())
    .bind(json!({"type": "entity", "title": "old dup", "created_at": long_lived_dup_created.to_rfc3339()}))
    .bind(now - Duration::hours(1))
    .execute(&probe.owner)
    .await
    .expect("insert a non-twin merge (dup lived 30d)");

    // ── stale_provisional: provisional > 7 days old with no judgement. ────
    let _stale = insert_node(&probe.owner, g, "Stale Provisional", json!({"provisional": true}), 0, now - Duration::days(10)).await;
    // A control: provisional but young — must NOT count as stale.
    let _young_provisional =
        insert_node(&probe.owner, g, "Young Provisional", json!({"provisional": true}), 0, now - Duration::days(1)).await;
    // A control: provisional and old, but a judgement touches it — must NOT count.
    let judged_provisional =
        insert_node(&probe.owner, g, "Judged Provisional", json!({"provisional": true}), 0, now - Duration::days(10)).await;
    let other_side = insert_node(&probe.owner, g, "Other Side", json!({}), 0, now).await;
    insert_positive_judgement(&probe.owner, g, judged_provisional, other_side).await;

    // Each gauge's connection is scoped to its own block — see the alias
    // test's comment on why an un-dropped, `let`-shadowed `PoolConnection`
    // would deadlock `probe.teardown()`'s `owner.close().await` below.
    let rebirths = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::rebirths_24h(&mut conn, g).await.expect("rebirths_24h")
    };
    assert_eq!(rebirths, 1, "exactly the 'Alex' rebirth, not the unrelated fresh reef");
    assert!(
        rebirths == 1 && {
            // Sanity: the counted row really is `rebirth`, not some other id.
            let count_is_rebirth: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM nodes WHERE node_id = $1 AND created_at >= now() - interval '24 hours'",
            )
            .bind(rebirth)
            .fetch_one(&probe.owner)
            .await
            .unwrap();
            count_is_rebirth == 1
        },
        "the counted rebirth must be the aliased reef"
    );

    let twins = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::twins_24h(&mut conn, g).await.expect("twins_24h")
    };
    assert_eq!(twins, 1, "exactly the short-lived-dup merge, not the 30-day-old one");

    let stale_count = {
        let mut conn = probe.owner.acquire().await.expect("acquire conn");
        identity_class::stale_provisional(&mut conn, g).await.expect("stale_provisional")
    };
    assert_eq!(stale_count, 1, "exactly the un-judged 10-day-old provisional reef");

    probe.teardown().await;
}

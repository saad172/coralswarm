//! `Store::source_groups[_scoped]` / `Store::source_group_detail_meta[_scoped]`
//! / `Store::list_atoms_page_for_channel[_scoped]` — the perf-fix pair behind
//! `GET /v1/graphs/{graph_id}/source-groups` and
//! `GET /v1/graphs/{graph_id}/source-groups/{channel_key}`
//! (`routes::graphs::list_source_groups`/`get_source_group`).
//!
//! Built on the shared [`support::rls_probe::RlsProbe`] harness (Phase P.1),
//! same convention as `atom_titles_and_excerpts_scoped_pg.rs`: a scratch DB
//! with the full control tree applied (so `coralswarm_app` + every 0083
//! policy are real), seeding via the owner/BYPASSRLS pool, and every
//! `_scoped` call run through a REAL `begin_account_scoped` transaction on the
//! `coralswarm_app` login — the exact non-BYPASSRLS role OPS-4 put in front
//! of production. This file proves the semantics of the three new `Store`
//! methods directly (grouping key, `:chunk:N` collapse, reef membership,
//! pagination) plus the RLS discipline `routes::graphs.rs`'s own comment on
//! these two routes requires (own-account visibility, cross-account
//! isolation, ambient-call hard-error).
//!
//! Route-level concerns this file deliberately does NOT cover (`page`/
//! `page_size`/`has_more` JSON shape, the 404 status codes for an unknown
//! `channel_key` and a cross-account graph): `routes`/`middleware` are
//! PRIVATE modules (`mod routes;`/`mod middleware;` in `lib.rs`, never `pub
//! mod`), so an external integration-test crate cannot call
//! `list_source_groups`/`get_source_group` directly, and reaching them via
//! `coralswarm_backend::test_support_app` would require standing up the full
//! Clerk JWKS/JWT stack (`route_rls_b1_pg.rs`'s much heavier shape) just to
//! observe a status code that is a thin, already-reviewed wrapper around
//! `Option`/`Err` — a real cost for no additional RLS proof. Those route-
//! level behaviors (404 unknown key, 404 cross-tenant via
//! `ReachabilityResolver`, `has_more`/pagination math) are instead covered
//! in-crate, in `routes::graphs`'s own `#[cfg(test)]` module, which (unlike
//! this file) can see the private handler functions directly.
//!
//! Env-gated like every other `*_pg.rs` test here:
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5442/postgres \
//!     cargo test -p coralswarm-backend --test source_groups_pg -- --nocapture

mod support;

use chrono::{DateTime, Duration, Utc};
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::PgPool;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// Seeds one `atoms` row with exactly the fields these methods' grouping/
/// ordering logic reads — everything else takes the fixture's/column's
/// default (mirrors `support::rls_probe::seed_account`'s own atom insert,
/// just with the extra provenance/timing columns this suite needs to
/// control).
#[allow(clippy::too_many_arguments)]
async fn seed_atom(
    owner: &PgPool,
    graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    channel_id: Option<&str>,
    channel_name: Option<&str>,
    timestamp: DateTime<Utc>,
    created_at: DateTime<Utc>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \"timestamp\", \
                            created_at, channel_id, channel_name) \
         VALUES ($1, $2, $3, 'blob', $4, $5, $6, $7) \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_type)
    .bind(source_id)
    .bind(timestamp)
    .bind(created_at)
    .bind(channel_id)
    .bind(channel_name)
    .fetch_one(owner)
    .await
    .expect("seed atom")
}

async fn seed_node(owner: &PgPool, graph_id: Uuid, node_type: &str, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, body) VALUES ($1, $2, $3, '{}') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(node_type)
    .bind(title)
    .fetch_one(owner)
    .await
    .expect("seed node")
}

async fn link_node_atom(owner: &PgPool, node_id: Uuid, atom_id: Uuid) {
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(owner)
        .await
        .expect("link node_atoms");
}

#[tokio::test]
async fn channel_grouping_prefers_channel_id_over_slash_split_fallback() {
    let Some(probe) = RlsProbe::setup("orgt_source_groups_channel_key").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let now = Utc::now();
    // No channel_id on either — both fall back to the `/`-split heuristic
    // over their (un-chunked) source_id, and land in the SAME group.
    seed_atom(&probe.owner, probe.a.graph_id, "slack", "chanA/111", None, None, now, now).await;
    seed_atom(&probe.owner, probe.a.graph_id, "slack", "chanA/222", None, None, now, now).await;
    // Structured channel_id wins even when the source_id's own `/`-prefix
    // would have implied a DIFFERENT key ("other", not "C0123").
    seed_atom(
        &probe.owner,
        probe.a.graph_id,
        "slack",
        "other/333",
        Some("C0123"),
        Some("eng-platform"),
        now,
        now,
    )
    .await;

    let store = PostgresStore::new(probe.app_pool.clone());
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let groups = store
        .source_groups_scoped(&mut tx, probe.a.graph_id)
        .await
        .expect("source_groups_scoped");
    tx.rollback().await.ok();

    let fallback = groups
        .iter()
        .find(|g| g.channel_key == "chanA")
        .unwrap_or_else(|| panic!("no 'chanA' fallback group in {groups:?}"));
    assert_eq!(fallback.coral_count, 2);
    assert_eq!(fallback.instance_count, 2, "chanA/111 and chanA/222 are two distinct instances");
    assert_eq!(fallback.channel_label, None, "neither atom carries a channel_name");

    let structured = groups
        .iter()
        .find(|g| g.channel_key == "C0123")
        .unwrap_or_else(|| panic!("no 'C0123' structured group in {groups:?}"));
    assert_eq!(structured.coral_count, 1);
    assert_eq!(structured.channel_label.as_deref(), Some("#eng-platform"));
    assert!(
        groups.iter().all(|g| g.channel_key != "other"),
        "channel_id must win over the source_id's own `/`-split — 'other/333' must NOT form \
         an 'other' group: {groups:?}"
    );

    probe.teardown().await;
}

#[tokio::test]
async fn chunk_suffix_collapses_to_one_instance_and_meta_spans_every_chunk() {
    let Some(probe) = RlsProbe::setup("orgt_source_groups_chunk").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let now = Utc::now();
    // Three chunks of the SAME source instance — one base source_id, three
    // distinct full source_ids, newest (created_at) last inserted first here
    // to prove ordering isn't insertion-order-dependent.
    seed_atom(
        &probe.owner,
        probe.a.graph_id,
        "note",
        "meeting-1:chunk:2",
        None,
        None,
        now,
        now,
    )
    .await;
    seed_atom(
        &probe.owner,
        probe.a.graph_id,
        "note",
        "meeting-1:chunk:0",
        None,
        None,
        now - Duration::minutes(10),
        now - Duration::minutes(10),
    )
    .await;
    seed_atom(
        &probe.owner,
        probe.a.graph_id,
        "note",
        "meeting-1:chunk:1",
        None,
        None,
        now - Duration::minutes(5),
        now - Duration::minutes(5),
    )
    .await;

    let store = PostgresStore::new(probe.app_pool.clone());

    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let groups = store
        .source_groups_scoped(&mut tx, probe.a.graph_id)
        .await
        .expect("source_groups_scoped");
    let group = groups
        .iter()
        .find(|g| g.channel_key == "meeting-1")
        .unwrap_or_else(|| panic!("no 'meeting-1' group in {groups:?}"));
    assert_eq!(group.coral_count, 3, "three chunks");
    assert_eq!(group.instance_count, 1, "one base source_id — the chunks collapse");

    let meta = store
        .source_group_detail_meta_scoped(&mut tx, probe.a.graph_id, "meeting-1")
        .await
        .expect("source_group_detail_meta_scoped")
        .expect("group must exist");
    tx.rollback().await.ok();

    assert_eq!(meta.summary.coral_count, 3);
    assert_eq!(meta.summary.instance_count, 1);
    let mut ids = meta.source_ids.clone();
    ids.sort();
    let mut want = vec![
        "meeting-1:chunk:0".to_string(),
        "meeting-1:chunk:1".to_string(),
        "meeting-1:chunk:2".to_string(),
    ];
    want.sort();
    assert_eq!(ids, want, "source_ids must span EVERY chunk, not just one page");
    // Newest created_at first.
    assert_eq!(
        meta.source_ids[0], "meeting-1:chunk:2",
        "source_ids must be newest-created_at-first: got {:?}",
        meta.source_ids
    );

    probe.teardown().await;
}

#[tokio::test]
async fn reef_count_and_reefs_come_from_real_node_atom_edges() {
    let Some(probe) = RlsProbe::setup("orgt_source_groups_reefs").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let now = Utc::now();
    let atom1 =
        seed_atom(&probe.owner, probe.a.graph_id, "slack", "chanR/1", None, None, now, now).await;
    let atom2 =
        seed_atom(&probe.owner, probe.a.graph_id, "slack", "chanR/2", None, None, now, now).await;

    let node1 = seed_node(&probe.owner, probe.a.graph_id, "entity", "Reef One").await;
    let node2 = seed_node(&probe.owner, probe.a.graph_id, "entity", "Reef Two").await;
    // atom1 belongs to BOTH nodes, atom2 only to node2 — three edges, two
    // DISTINCT reefs; a naive join-then-count would report 3.
    link_node_atom(&probe.owner, node1, atom1).await;
    link_node_atom(&probe.owner, node2, atom1).await;
    link_node_atom(&probe.owner, node2, atom2).await;

    let store = PostgresStore::new(probe.app_pool.clone());
    let mut tx = probe.scoped_tx(probe.a.account_id).await;

    let groups = store
        .source_groups_scoped(&mut tx, probe.a.graph_id)
        .await
        .expect("source_groups_scoped");
    let group = groups
        .iter()
        .find(|g| g.channel_key == "chanR")
        .unwrap_or_else(|| panic!("no 'chanR' group in {groups:?}"));
    assert_eq!(group.reef_count, 2, "two DISTINCT reefs, not three edges");

    let meta = store
        .source_group_detail_meta_scoped(&mut tx, probe.a.graph_id, "chanR")
        .await
        .expect("source_group_detail_meta_scoped")
        .expect("group must exist");
    tx.rollback().await.ok();

    assert_eq!(meta.reefs.len(), 2);
    let mut node_ids: Vec<Uuid> = meta.reefs.iter().map(|r| r.node_id).collect();
    node_ids.sort();
    let mut want = vec![node1, node2];
    want.sort();
    assert_eq!(node_ids, want);
    let two = meta.reefs.iter().find(|r| r.node_id == node2).expect("node2 present");
    assert_eq!(two.node_type, "entity");
    assert_eq!(two.title, "", "an unsupported node label must remain hidden");

    // Membership alone does not justify exposing a label. Give node2 real,
    // permitted literal support through the atom already linked to it.
    sqlx::query("UPDATE atoms SET content_inline=$1 WHERE atom_id=$2")
        .bind("Reef Two is referenced by this source.")
        .bind(atom2)
        .execute(&probe.owner)
        .await
        .expect("seed permitted title support");
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let supported = store
        .source_group_detail_meta_scoped(&mut tx, probe.a.graph_id, "chanR")
        .await
        .expect("source_group_detail_meta_scoped with title support")
        .expect("supported group must exist");
    tx.rollback().await.ok();
    assert_eq!(supported.reefs.len(), 2, "support must not change reef membership");
    let two = supported.reefs.iter().find(|r| r.node_id == node2).expect("node2 present");
    assert_eq!(two.node_type, "entity");
    assert_eq!(two.title, "Reef Two");

    probe.teardown().await;
}

#[tokio::test]
async fn list_atoms_page_for_channel_paginates_newest_first_and_meta_is_page_independent() {
    let Some(probe) = RlsProbe::setup("orgt_source_groups_page").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let now = Utc::now();
    // Five atoms in one channel, strictly descending created_at as inserted
    // (atom index 0 is newest): p0..p4.
    let mut ids = Vec::new();
    for i in 0..5 {
        let ts = now - Duration::minutes(i);
        let id = seed_atom(
            &probe.owner,
            probe.a.graph_id,
            "slack",
            &format!("chanP/{i}"),
            None,
            None,
            ts,
            ts,
        )
        .await;
        ids.push(id);
    }

    let store = PostgresStore::new(probe.app_pool.clone());
    let mut tx = probe.scoped_tx(probe.a.account_id).await;

    let page1 = store
        .list_atoms_page_for_channel_scoped(&mut tx, probe.a.graph_id, "chanP", 2, 0)
        .await
        .expect("page 1");
    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0].atom_id, ids[0], "newest first");
    assert_eq!(page1[1].atom_id, ids[1]);

    let page2 = store
        .list_atoms_page_for_channel_scoped(&mut tx, probe.a.graph_id, "chanP", 2, 2)
        .await
        .expect("page 2");
    assert_eq!(page2.len(), 2);
    assert_eq!(page2[0].atom_id, ids[2]);
    assert_eq!(page2[1].atom_id, ids[3]);

    let page3 = store
        .list_atoms_page_for_channel_scoped(&mut tx, probe.a.graph_id, "chanP", 2, 4)
        .await
        .expect("page 3 (last, partial)");
    assert_eq!(page3.len(), 1);
    assert_eq!(page3[0].atom_id, ids[4]);

    // Whole-group meta is independent of any single page — `coral_count`
    // covers all 5 regardless of the `page_size` a detail-route caller used.
    let meta = store
        .source_group_detail_meta_scoped(&mut tx, probe.a.graph_id, "chanP")
        .await
        .expect("meta")
        .expect("group exists");
    tx.rollback().await.ok();
    assert_eq!(meta.summary.coral_count, 5);
    assert_eq!(meta.source_ids.len(), 5);

    probe.teardown().await;
}

#[tokio::test]
async fn list_atoms_page_for_channel_is_stable_across_pages_when_created_at_ties() {
    // Runtime check on the PR B+C stack: a 452-coral meeting group paged
    // 10× came back 451 distinct + 1 duplicate. Every coral of a group
    // written by one ingest shares ONE `created_at`, so `ORDER BY
    // created_at DESC` alone leaves the LIMIT/OFFSET window undefined —
    // Postgres is free to return a different permutation per page. The
    // `atom_id DESC` tiebreak is what makes the pages a partition. This
    // seeds 23 atoms at a single timestamp (more than any page) and asserts
    // the union of pages is exactly the set, with no repeats.
    let Some(probe) = RlsProbe::setup("orgt_source_groups_ties").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let ts = Utc::now();
    let mut seeded = std::collections::HashSet::new();
    for i in 0..23 {
        let id = seed_atom(
            &probe.owner,
            probe.a.graph_id,
            "transcript",
            &format!("meeting-T:chunk:{i}"),
            None,
            None,
            ts,
            ts,
        )
        .await;
        seeded.insert(id);
    }

    let store = PostgresStore::new(probe.app_pool.clone());
    let mut tx = probe.scoped_tx(probe.a.account_id).await;

    let mut seen = Vec::new();
    let mut offset = 0;
    loop {
        let page = store
            .list_atoms_page_for_channel_scoped(&mut tx, probe.a.graph_id, "meeting-T", 5, offset)
            .await
            .expect("page");
        if page.is_empty() {
            break;
        }
        seen.extend(page.iter().map(|a| a.atom_id));
        offset += 5;
    }
    tx.rollback().await.ok();

    let distinct: std::collections::HashSet<Uuid> = seen.iter().copied().collect();
    assert_eq!(seen.len(), 23, "pages must cover the group exactly once: {seen:?}");
    assert_eq!(distinct, seeded, "union of pages must equal the seeded set (no dup, no drop)");
    // The set assertions above can pass by luck on a tiny heap (a seq scan
    // hands back insertion order consistently); the ORDER the pages arrive
    // in is the contract that cannot: with the tiebreak it is `atom_id
    // DESC` across page boundaries, without it, heap order of random UUIDs.
    let mut expected: Vec<Uuid> = seeded.iter().copied().collect();
    expected.sort_by(|a, b| b.cmp(a));
    assert_eq!(seen, expected, "pages must be a single atom_id DESC sequence within one created_at");

    probe.teardown().await;
}

#[tokio::test]
async fn unknown_channel_key_returns_none() {
    let Some(probe) = RlsProbe::setup("orgt_source_groups_unknown").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let now = Utc::now();
    seed_atom(&probe.owner, probe.a.graph_id, "slack", "chanK/1", None, None, now, now).await;

    let store = PostgresStore::new(probe.app_pool.clone());
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let meta = store
        .source_group_detail_meta_scoped(&mut tx, probe.a.graph_id, "does-not-exist")
        .await
        .expect("query must succeed");
    let page = store
        .list_atoms_page_for_channel_scoped(&mut tx, probe.a.graph_id, "does-not-exist", 50, 0)
        .await
        .expect("query must succeed");
    tx.rollback().await.ok();

    assert!(meta.is_none(), "an unknown channel_key must yield Ok(None), not a row of zeros");
    assert!(page.is_empty());

    probe.teardown().await;
}

#[tokio::test]
async fn cross_account_scope_sees_nothing_and_ambient_read_hard_errors() {
    let Some(probe) = RlsProbe::setup("orgt_source_groups_rls").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let now = Utc::now();
    let atom = seed_atom(&probe.owner, probe.a.graph_id, "slack", "chanX/1", None, None, now, now)
        .await;
    let node = seed_node(&probe.owner, probe.a.graph_id, "entity", "A's Reef").await;
    link_node_atom(&probe.owner, node, atom).await;

    let store = PostgresStore::new(probe.app_pool.clone());

    // ── 1. Scoped to its OWN account, A sees its own group ─────────────────
    let mut tx_a = probe.scoped_tx(probe.a.account_id).await;
    let groups_a = store
        .source_groups_scoped(&mut tx_a, probe.a.graph_id)
        .await
        .expect("scoped read must succeed");
    tx_a.rollback().await.ok();
    assert!(
        groups_a.iter().any(|g| g.channel_key == "chanX"),
        "account A must see its own 'chanX' group: {groups_a:?}"
    );

    // ── 2. Scoped to B, querying A's graph_id explicitly returns NOTHING —
    // never an error, never A's data (RLS filters the rows, not the query).
    let mut tx_b = probe.scoped_tx(probe.b.account_id).await;
    let groups_b = store
        .source_groups_scoped(&mut tx_b, probe.a.graph_id)
        .await
        .expect("cross-account read must succeed, just empty");
    assert!(
        groups_b.iter().all(|g| g.channel_key != "chanX"),
        "account B must NEVER see account A's 'chanX' group: {groups_b:?}"
    );
    let meta_b = store
        .source_group_detail_meta_scoped(&mut tx_b, probe.a.graph_id, "chanX")
        .await
        .expect("cross-account read must succeed");
    assert!(
        meta_b.is_none(),
        "account B must see NO group under A's graph_id — got {meta_b:?}"
    );
    let page_b = store
        .list_atoms_page_for_channel_scoped(&mut tx_b, probe.a.graph_id, "chanX", 50, 0)
        .await
        .expect("cross-account read must succeed");
    assert!(page_b.is_empty(), "account B must see none of A's atoms: {page_b:?}");
    tx_b.rollback().await.ok();

    // ── 3. Ambient (unscoped) reads against the REAL coralswarm_app role
    // must hard-error, never quietly return data or an empty success — the
    // same "no app.account_id set" strict-predicate proof every other
    // `_scoped` twin in this codebase carries.
    let unscoped_groups = store.source_groups(probe.a.graph_id).await;
    assert!(
        unscoped_groups.is_err(),
        "an ambient source_groups call against coralswarm_app must hard-error: {unscoped_groups:?}"
    );
    let unscoped_meta = store.source_group_detail_meta(probe.a.graph_id, "chanX").await;
    assert!(
        unscoped_meta.is_err(),
        "an ambient source_group_detail_meta call must hard-error: {unscoped_meta:?}"
    );
    let unscoped_page = store.list_atoms_page_for_channel(probe.a.graph_id, "chanX", 50, 0).await;
    assert!(
        unscoped_page.is_err(),
        "an ambient list_atoms_page_for_channel call must hard-error: {unscoped_page:?}"
    );

    probe.teardown().await;
}

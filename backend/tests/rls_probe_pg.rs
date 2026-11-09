//! Example consumer of the shared RLS probe harness
//! (`tests/support/rls_probe.rs`) — and the end-to-end proof that the
//! harness itself works against real Postgres. Route/worker conversion PRs
//! (wiring `begin_account_scoped` into each query path) copy this file's
//! shape and swap in their own converted query text.
//!
//! Probes the two policy shapes migration `0083_consumer_rls` uses:
//!   * `graphs` / `queries` — direct `account_id` column;
//!   * `atoms` — graph-mediated (scoped through the owning graph), including
//!     the direct-by-`graph_id` / direct-by-`atom_id` reads that never join
//!     `graphs` at all (the classic IDOR pivot).
//!
//! Env-gated, like every `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test rls_probe_pg

mod support;

use support::rls_probe::RlsProbe;

#[tokio::test]
async fn harness_probes_direct_and_graph_mediated_tables() {
    let Some(probe) = RlsProbe::setup("orgt_rls_probe_example").await else {
        eprintln!("skipping harness_probes_direct_and_graph_mediated_tables: set ORGT_TEST_BASE_URL");
        return;
    };

    // ── graphs: direct account_id column ───────────────────────────────────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM graphs",
            &[],
            &[probe.a.graph_id],
        )
        .await;
    // Asking for B's graph BY ID from A's scope: filtered to nothing.
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM graphs WHERE id = $1",
            &[probe.b.graph_id],
        )
        .await;
    probe.assert_unscoped_hard_errors("SELECT id FROM graphs", &[]).await;

    // ── queries: second direct-account_id table, with a bind in the OWN-rows
    // direction too (proves the helpers handle positional binds both ways) ──
    probe
        .assert_scoped_returns_own_rows(
            probe.b.account_id,
            "SELECT id FROM queries WHERE id = $1",
            &[probe.b.query_id],
            &[probe.b.query_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.b.account_id,
            "SELECT id FROM queries WHERE id = $1",
            &[probe.a.query_id],
        )
        .await;

    // ── atoms: graph-mediated policy, probed via the query shapes the
    // codebase actually uses (direct by graph_id / by atom_id, no join) ─────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE graph_id = $1",
            &[probe.a.graph_id],
            &[probe.a.atom_id],
        )
        .await;
    // The IDOR pivot: A queries B's graph_id directly — zero rows, not B's atom.
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    // And by atom_id directly (both accounts' ids in one query via two binds):
    // only A's own atom comes back.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE atom_id IN ($1, $2)",
            &[probe.a.atom_id, probe.b.atom_id],
            &[probe.a.atom_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT atom_id FROM atoms WHERE graph_id = $1",
            &[probe.a.graph_id],
        )
        .await;

    // ── symmetry: B sees only B (the fixture is genuinely two-sided) ───────
    probe
        .assert_scoped_returns_own_rows(
            probe.b.account_id,
            "SELECT id FROM graphs",
            &[],
            &[probe.b.graph_id],
        )
        .await;

    probe.teardown().await;
}

/// The escape hatch for shapes the three assert helpers don't cover:
/// `scoped_tx` hands back a transaction scoped via the PRODUCTION
/// `begin_account_scoped` primitive, on which arbitrary reads/writes run
/// under the policies. Shown here with a scoped INSERT into a
/// graph-mediated table (`nodes`) — own graph succeeds, B's graph is
/// rejected by the policy's WITH CHECK.
#[tokio::test]
async fn scoped_tx_supports_arbitrary_shapes_including_writes() {
    let Some(probe) = RlsProbe::setup("orgt_rls_probe_example_tx").await else {
        eprintln!("skipping scoped_tx_supports_arbitrary_shapes_including_writes: set ORGT_TEST_BASE_URL");
        return;
    };

    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let own = sqlx::query(
        "INSERT INTO nodes(graph_id, type, title, body) VALUES ($1, 'entity', 'ok', '{}')",
    )
    .bind(probe.a.graph_id)
    .execute(&mut *tx)
    .await;
    assert!(own.is_ok(), "scoped to A, inserting a node into A's own graph must succeed: {own:?}");

    let cross = sqlx::query(
        "INSERT INTO nodes(graph_id, type, title, body) VALUES ($1, 'entity', 'nope', '{}')",
    )
    .bind(probe.b.graph_id)
    .execute(&mut *tx)
    .await;
    assert!(
        cross.is_err(),
        "scoped to A, inserting a node into B's graph must be rejected by WITH CHECK: {cross:?}"
    );
    tx.rollback().await.ok();

    probe.teardown().await;
}

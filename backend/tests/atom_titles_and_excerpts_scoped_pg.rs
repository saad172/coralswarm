//! `Store::atom_titles_and_excerpts_scoped` against a real `coralswarm_app`
//! login (CodeRabbit round 1 on #837, filed against the title-always-present
//! PR).
//!
//! `atom_titles_and_excerpts` (added by that PR, no `_scoped` twin) was
//! called by `routes::graphs::{list_atoms,list_sources}`/
//! `routes::nodes::get_node_candidates` AFTER their scoped read transaction
//! already committed — ambient on `self.pool`, same shape `atom_authors`/
//! `atom_channels`/`atom_provenance` deliberately avoid via their own
//! `_scoped` twins. Under the real, non-`BYPASSRLS` `coralswarm_app` role
//! `0083_consumer_rls` puts in front of production, an ambient read either
//! hard-errors (no `app.account_id` set at all) or silently sees nothing —
//! either way `unwrap_or_default()` degrades every title/excerpt on the page,
//! the opposite of what "titles always present" promises.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same convention as every other `*_pg.rs`
//! here that uses `support::rls_probe::RlsProbe`.

mod support;

use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

async fn seed_titled_atom(owner: &sqlx::PgPool, graph_id: Uuid, source_id: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \"timestamp\", \
                           title, excerpt) \
         VALUES ($1, 'note', $2, 'blob', now(), 'A real headline', 'A real headline, in full.') \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(owner)
    .await
    .expect("seed titled atom")
}

#[tokio::test]
async fn scoped_read_sees_its_own_account_and_never_another_accounts() {
    let Some(probe) = RlsProbe::setup("orgt_titles_excerpts_scoped").await else {
        eprintln!("skipping scoped_read_sees_its_own_account…: set ORGT_TEST_BASE_URL");
        return;
    };

    let atom_a = seed_titled_atom(&probe.owner, probe.a.graph_id, "titles-scoped-a").await;
    let atom_b = seed_titled_atom(&probe.owner, probe.b.graph_id, "titles-scoped-b").await;

    let store = PostgresStore::new(probe.app_pool.clone());

    // Scoped to account A: sees A's atom, never B's — even when B's id is
    // explicitly in the requested batch (the same "ask for both, get only
    // yours" shape `atom_authors_scoped`'s own tests pin).
    let mut tx = begin_account_scoped(&probe.app_pool, probe.a.account_id)
        .await
        .expect("begin_account_scoped");
    let rows = store
        .atom_titles_and_excerpts_scoped(&mut tx, &[atom_a, atom_b])
        .await
        .expect("scoped read must succeed, not error");
    tx.rollback().await.ok();

    let ids: Vec<Uuid> = rows.iter().map(|(id, ..)| *id).collect();
    assert!(ids.contains(&atom_a), "account A's own atom must be visible: got {ids:?}");
    assert!(
        !ids.contains(&atom_b),
        "account B's atom must NEVER leak into account A's scoped read: got {ids:?}"
    );
    let (_, title, excerpt) = rows.iter().find(|(id, ..)| *id == atom_a).expect("row for atom_a");
    assert_eq!(title.as_deref(), Some("A real headline"));
    assert_eq!(excerpt.as_deref(), Some("A real headline, in full."));

    // ── The bug this closes: the OLD call site's shape (ambient, no scope
    // at all) against the SAME non-BYPASSRLS role and the SAME atom_ids —
    // `0083_consumer_rls`'s strict `current_setting` predicate means this
    // is not a quiet "sees nothing", it is a hard error, which is exactly
    // what `unwrap_or_default()` at the old call sites was silently
    // swallowing into empty titles for the whole page.
    let unscoped = store.atom_titles_and_excerpts(&[atom_a]).await;
    assert!(
        unscoped.is_err(),
        "an ambient (unscoped) read against the real coralswarm_app role must hard-error, not \
         quietly succeed — proving why the old call sites' unwrap_or_default() was a real \
         titles-always-present regression, not a theoretical one: {unscoped:?}"
    );

    probe.teardown().await;
}

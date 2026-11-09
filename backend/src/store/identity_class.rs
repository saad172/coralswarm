//! Identity-class folding + convergence gauges (identity-resolution-at-the-
//! door design doc, §10.1/§10.5).
//!
//! `infer_expertise`'s candidate list degrades to a four-way tie whenever the
//! same person exists as several un-merged reefs ("Alex" / "Alex Alex" /
//! "person@example.com" / …). Waiting for `entity_resolution` to
//! merge them all before expertise can say anything useful is exactly the
//! coupling §3 of the design doc rejects — readers should fold by identity
//! CLASS (a relation), not wait for every merge to materialize (a row).
//!
//! This module is the shared primitive for that fold: a union-find over three
//! established same-identity signals — shared `reef_identifiers` and an
//! active POSITIVE `reef_judgement` — plus a
//! deterministic representative pick per class. It also carries the §10.5
//! convergence-gauge queries (`rebirths_24h`, `twins_24h`, `stale_provisional`)
//! since they read the same tables this module already touches.
//!
//! ## Why free functions, not `Store` trait methods
//!
//! §10.1's file-ownership table reserves `store/mod.rs` for builder B1; B3
//! (this module) may add exactly one line there (`pub mod identity_class;`).
//! These are plain `pub async fn(conn: &mut PgConnection, …)` functions —
//! callers pass either a `sqlx::Transaction<'_, Postgres>` (which derefs to
//! `PgConnection`, the same coercion `tenancy::migrate::run_migrate` already
//! relies on) or a pooled connection, so every caller — the org-tenancy
//! `_scoped`/plain split in `workers::entity_resolution`, the pg-integration
//! test, `workers::infer_expertise` — decides its own transaction boundary.
//! No RLS session variable is set here; the caller's own transaction is what
//! enforces tenancy, exactly like every other raw-SQL helper in the worker
//! files. On the consumer plane that is `begin_account_scoped`. On an org
//! tenant it is a `tenancy::scope` TENANT primitive — the org tenant's
//! whole-database isolation is not a substitute. That isolation is real, and
//! it covers the CROSS-ORG boundary only. Every table these functions read
//! carries a member-ocean policy inside a tenant DB:
//! `graphs`/`nodes`/`node_merges` from `migrations_tenant/0009_tenant_rls`,
//! `reef_identifiers` from `0045`, `reef_judgement` from `0046`. Once OPS-4-T
//! moves the tenant DSN to the NOBYPASSRLS role, a caller that hands in a
//! pooled connection with no GUC set sees the org and personal oceans and no
//! member or team ocean — so the fold silently runs on a partial candidate
//! set instead of failing.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::store::StoreError;

// ── Union-find over three same-identity relations ────────────────────────────

/// Minimal union-find keyed by `Uuid`, seeded with exactly the ids the caller
/// asked about — [`UnionFind::union`] is a no-op for any id outside that seed
/// set, so a query that (by a future bug) returns a foreign id can never grow
/// the partition beyond what the caller requested.
struct UnionFind {
    parent: HashMap<Uuid, Uuid>,
}

impl UnionFind {
    fn new(ids: &[Uuid]) -> Self {
        UnionFind { parent: ids.iter().map(|id| (*id, *id)).collect() }
    }

    fn find(&mut self, x: Uuid) -> Uuid {
        let Some(&p) = self.parent.get(&x) else { return x };
        if p == x {
            return x;
        }
        let root = self.find(p);
        self.parent.insert(x, root);
        root
    }

    fn union(&mut self, a: Uuid, b: Uuid) {
        if !self.parent.contains_key(&a) || !self.parent.contains_key(&b) {
            return;
        }
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            self.parent.insert(ra, rb);
        }
    }

    /// Partition every seeded id into its class. Each class and the outer
    /// list are sorted for determinism — `HashMap` iteration order is
    /// otherwise arbitrary, and this result is compared/rendered by callers
    /// (provenance, tests) that need a stable order.
    fn classes(mut self) -> Vec<Vec<Uuid>> {
        let ids: Vec<Uuid> = self.parent.keys().copied().collect();
        let mut groups: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        for id in ids {
            let root = self.find(id);
            groups.entry(root).or_default().push(id);
        }
        let mut out: Vec<Vec<Uuid>> = groups.into_values().collect();
        for class in &mut out {
            class.sort_unstable();
        }
        out.sort_by(|a, b| a.first().cmp(&b.first()));
        out
    }
}

/// Partition requested nodes using stable identifiers and active positive
/// judgments. Shared titles/aliases are candidates, never identity proof.
/// The caller owns the authorized graph/member transaction.
pub async fn classes_for(
    conn: &mut PgConnection,
    graph_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<Vec<Uuid>>, StoreError> {
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut ids: Vec<Uuid> = node_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();

    let mut uf = UnionFind::new(&ids);

    // Names and aliases are retrieval candidates, never sufficient identity
    // evidence. Homonyms stay in separate classes until a stable identifier or
    // a reviewed positive judgement establishes equivalence.

    // (b) shared reef_identifiers (kind, value_norm).
    let ident_rows: Vec<(Uuid, String, String)> = sqlx::query_as(
        "SELECT node_id, kind, value_norm FROM reef_identifiers \
         WHERE graph_id = $1 AND node_id = ANY($2)",
    )
    .bind(graph_id)
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await?;
    let mut by_identifier: HashMap<(String, String), Vec<Uuid>> = HashMap::new();
    for (node_id, kind, value_norm) in ident_rows {
        by_identifier.entry((kind, value_norm)).or_default().push(node_id);
    }
    for members in by_identifier.values() {
        fold_group(&mut uf, members);
    }

    // (c) active POSITIVE reef_judgement rows between members.
    let judgement_rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT node_a_id, node_b_id FROM reef_judgement \
         WHERE graph_id = $1 AND verdict = 'positive' AND annulled_at IS NULL \
           AND node_a_id = ANY($2) AND node_b_id = ANY($2)",
    )
    .bind(graph_id)
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await?;
    for (a, b) in judgement_rows {
        uf.union(a, b);
    }

    Ok(uf.classes())
}

/// Union every member of one "these ids share a same-identity signal" group
/// pairwise-consecutively — sufficient for [`UnionFind`], since `find()`
/// transitively folds the whole chain (a~b, b~c ⇒ a~c) regardless of which
/// consecutive pairs were unioned directly.
fn fold_group(uf: &mut UnionFind, members: &[Uuid]) {
    for pair in members.windows(2) {
        uf.union(pair[0], pair[1]);
    }
}

/// The representative of `class` (§10.1 item 1): the self reef if present,
/// else the member with the highest `coral_count`, tie → older `created_at`,
/// tie → smaller `node_id` (a total, deterministic order regardless of query
/// result ordering).
///
/// `class` must be non-empty (every caller derives it from [`classes_for`],
/// which never emits an empty class). A single-member class returns that
/// member without a query. If every member has vanished by the time this
/// reads `nodes` (a race with a concurrent delete/merge), falls back to
/// `class[0]` rather than erroring — a representative pick is a ranking
/// heuristic, not a correctness-load-bearing lookup.
pub async fn representative(
    conn: &mut PgConnection,
    graph_id: Uuid,
    class: &[Uuid],
) -> Result<Uuid, StoreError> {
    let Some(&first) = class.first() else {
        return Err(StoreError::NotFound);
    };
    if class.len() == 1 {
        return Ok(first);
    }

    let self_node_id: Option<Uuid> =
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT self_node_id FROM graphs WHERE id = $1")
            .bind(graph_id)
            .fetch_optional(&mut *conn)
            .await?
            .flatten();

    // `nodes.coral_count` is `INT` (migration 0109) — i32, not i64.
    let rows: Vec<(Uuid, i32, DateTime<Utc>)> = sqlx::query_as(
        "SELECT node_id, coral_count, created_at FROM nodes WHERE node_id = ANY($1)",
    )
    .bind(class)
    .fetch_all(&mut *conn)
    .await?;
    if rows.is_empty() {
        return Ok(first);
    }

    Ok(choose_representative(self_node_id, &rows))
}

/// Pure representative-selection rule, split out of [`representative`] so it
/// is unit-testable without a database (the DB round trips are the only
/// non-pure part of the pick).
fn choose_representative(self_node_id: Option<Uuid>, members: &[(Uuid, i32, DateTime<Utc>)]) -> Uuid {
    if let Some(self_id) = self_node_id {
        if members.iter().any(|(id, _, _)| *id == self_id) {
            return self_id;
        }
    }
    members
        .iter()
        .max_by(|a, b| {
            a.1.cmp(&b.1) // higher coral_count wins
                .then_with(|| b.2.cmp(&a.2)) // tie → older created_at wins
                .then_with(|| b.0.cmp(&a.0)) // tie → smaller node_id wins
        })
        .map(|(id, _, _)| *id)
        .unwrap_or(members[0].0)
}

// ── §10.5 convergence gauges ──────────────────────────────────────────────────

/// Entity reefs minted in the last 24h whose `lower(title)` is ALREADY an
/// alias or identifier of another same-type reef in the graph — i.e. a mint
/// the door's alias/identifier ladder (steps 2-3) should have folded instead
/// of creating. Target after the door ships: 0. This is a point-in-time
/// re-check against CURRENT graph state (not a mint-time audit trail — no new
/// column exists to record that), so a title later taught as an alias by a
/// same-day merge still counts here until that merge lands; that lag is
/// accepted (the gauge is a trend signal, not a per-mint audit).
pub async fn rebirths_24h(conn: &mut PgConnection, graph_id: Uuid) -> Result<i64, StoreError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM nodes n \
         WHERE n.graph_id = $1 \
           AND n.type = 'entity' \
           AND n.created_at >= now() - interval '24 hours' \
           AND ( \
             EXISTS ( \
               SELECT 1 FROM nodes other \
               WHERE other.graph_id = n.graph_id \
                 AND other.type = n.type \
                 AND other.node_id <> n.node_id \
                 AND jsonb_exists(COALESCE(other.body->'aliases', '[]'::jsonb), lower(n.title)) \
             ) \
             OR EXISTS ( \
               SELECT 1 FROM reef_identifiers ri \
               WHERE ri.graph_id = n.graph_id \
                 AND ri.node_id <> n.node_id \
                 AND ri.value_norm = lower(n.title) \
             ) \
           )",
    )
    .bind(graph_id)
    .fetch_one(&mut *conn)
    .await?;
    Ok(count)
}

/// Merges in the last 24h whose duplicate lived under 7 days
/// (`dup_snapshot->>'created_at'` to the merge's own `created_at`) — a
/// "twin", a duplicate created and merged away in the same short window.
/// Should fall toward the genuine-new-duplicate rate as the door closes the
/// re-birth loop, not to zero (a brand-new person/topic can still
/// legitimately arrive as two reefs once, before any signal links them).
pub async fn twins_24h(conn: &mut PgConnection, graph_id: Uuid) -> Result<i64, StoreError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM node_merges \
         WHERE graph_id = $1 \
           AND created_at >= now() - interval '24 hours' \
           AND (dup_snapshot->>'created_at') IS NOT NULL \
           AND created_at - (dup_snapshot->>'created_at')::timestamptz < interval '7 days'",
    )
    .bind(graph_id)
    .fetch_one(&mut *conn)
    .await?;
    Ok(count)
}

/// Provisional reefs (`body.provisional = true`) older than 7 days with no
/// `reef_judgement` row touching them on either side — the residue the door
/// design's own §4.7 calls "the honest measure of remaining autonomy debt":
/// a mint nothing has yet confirmed, denied, or even flagged UNSURE about.
pub async fn stale_provisional(conn: &mut PgConnection, graph_id: Uuid) -> Result<i64, StoreError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM nodes n \
         WHERE n.graph_id = $1 \
           AND n.body @> '{\"provisional\": true}'::jsonb \
           AND n.created_at < now() - interval '7 days' \
           AND NOT EXISTS ( \
             SELECT 1 FROM reef_judgement rj \
             WHERE rj.graph_id = n.graph_id \
               AND (rj.node_a_id = n.node_id OR rj.node_b_id = n.node_id) \
           )",
    )
    .bind(graph_id)
    .fetch_one(&mut *conn)
    .await?;
    Ok(count)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn mid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn choose_representative_prefers_the_self_reef() {
        let self_id = mid(2);
        let members = vec![
            (mid(1), 500, Utc::now()),
            (self_id, 0, Utc::now()), // self reef, but tiny — must still win
            (mid(3), 10, Utc::now()),
        ];
        assert_eq!(choose_representative(Some(self_id), &members), self_id);
    }

    #[test]
    fn choose_representative_falls_back_to_max_coral_count() {
        let members = vec![(mid(1), 5, Utc::now()), (mid(2), 3198, Utc::now()), (mid(3), 0, Utc::now())];
        assert_eq!(choose_representative(None, &members), mid(2));
    }

    #[test]
    fn choose_representative_ties_on_coral_count_prefer_older() {
        let now = Utc::now();
        let older = now - chrono::Duration::days(30);
        let members = vec![(mid(1), 10, now), (mid(2), 10, older)];
        assert_eq!(choose_representative(None, &members), mid(2), "the older member must win the tie");
    }

    #[test]
    fn choose_representative_ties_on_coral_count_and_age_prefer_smaller_id() {
        let now = Utc::now();
        let members = vec![(mid(9), 10, now), (mid(2), 10, now)];
        assert_eq!(choose_representative(None, &members), mid(2));
    }

    #[test]
    fn choose_representative_ignores_a_self_id_absent_from_the_class() {
        // Self reef is bound elsewhere in the graph but is NOT a member of
        // this particular class — must not be returned as this class's rep.
        let members = vec![(mid(1), 500, Utc::now()), (mid(3), 10, Utc::now())];
        assert_eq!(choose_representative(Some(mid(99)), &members), mid(1));
    }

    #[test]
    fn union_find_folds_a_transitive_chain_from_pairwise_unions() {
        let ids = [mid(1), mid(2), mid(3), mid(4)];
        let mut uf = UnionFind::new(&ids);
        uf.union(mid(1), mid(2));
        uf.union(mid(2), mid(3));
        // mid(4) untouched — stays its own singleton class.
        let mut classes = uf.classes();
        classes.sort_by(|a, b| a.len().cmp(&b.len()));
        assert_eq!(classes.len(), 2, "one 3-member class + one singleton");
        assert_eq!(classes[0], vec![mid(4)]);
        assert_eq!(classes[1], vec![mid(1), mid(2), mid(3)]);
    }

    #[test]
    fn union_find_ignores_a_union_call_for_an_id_outside_the_seed_set() {
        let ids = [mid(1), mid(2)];
        let mut uf = UnionFind::new(&ids);
        // mid(99) was never seeded — this must be a no-op, not a panic or a
        // silent grow of the partition.
        uf.union(mid(1), mid(99));
        let classes = uf.classes();
        assert_eq!(classes, vec![vec![mid(1)], vec![mid(2)]]);
    }
}

//! Resumable, bounded Library evidence refresh on every content plane.
//!
//! The graph transaction lock serializes refreshers, never capture writers.
//! Both stored grains come from ONE canonical statement/snapshot. A change
//! committing during that statement keeps its own log id for a later pass.
//! High-fanout keys retain their original log row while its durable node cursor
//! advances; reads MUST ignore that cursor when resolving their exact overlay.
//!
//! Capture is gated by the listing switch (tenant 0201 / control 0380): Off
//! logs nothing, so the node grain records the switch generation it was
//! certified under and a flip rebuilds it. While Off (or On but before the
//! generation's flip barrier) nothing reads the model and anything computed
//! would be rebuilt anyway, so a refresh computes no node evidence and only
//! deletes the log in bounded batches. A completed backfill acknowledges the
//! rows its start snapshot covers, in bounded batches over later refreshes.

use std::collections::{BTreeSet, HashMap};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::library_read::evidence::{self, READ_MODEL_VERSION};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

mod listing_facts;
pub use listing_facts::{
    flip_switch as flip_listing_switch, legacy_setting_warning as listing_legacy_setting_warning,
    stamp_barrier as stamp_listing_barrier, switch_settled as listing_switch_settled,
    warn_on_legacy_setting as warn_on_legacy_listing_setting, Limits as ListingLimits,
    LEGACY_SETTING as LISTING_LEGACY_SETTING,
};

pub const INTERVAL: Duration = Duration::from_secs(2);
const TICK_TIMEOUT: Duration = Duration::from_secs(35);

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Distinct nodes recomputed per refresh: a node several keys of one
    /// turn resolve to is recomputed (and charged) once.
    pub nodes: usize,
    /// Distinct log keys resolved per refresh; also bounds empty/stale keys,
    /// which consume no node budget. The refresh reads `changes *
    /// DEDUPE_WINDOW` log rows to find them, and acknowledges every row it
    /// read for a key it resolved, so one turn may consume up to that many.
    pub changes: usize,
}

/// Log rows read per change slot. A burst logs one key many times (an item
/// re-extracted logs its item key once per statement; production held 126k
/// rows naming 635 items), and resolving the key once covers them all.
pub const DEDUPE_WINDOW: usize = 8;

/// Change rows one refresh deletes without resolving them: those a completed
/// backfill's start snapshot covers (see `purge_covered`), or, while the
/// switch is Off, any (see `purge_while_off`). A backlog of any size clears
/// in bounded transactions, without an operator DELETE.
pub const PURGE_BATCH: usize = 10_000;

/// Wall-clock bound on one drain turn's key resolution. A turn makes one
/// statement for every key's first share, then at most one
/// `library_evidence_change_nodes` call per still-unfinished key; each is an
/// indexed, bounded probe, but measured locally against a 7,000-node hub
/// with mid-walk cursors 64 calls took 10-14 s (debug build, PG16), and the
/// cost grows with the hub. No further key is resolved once this has
/// elapsed; the turn stores what it resolved, untouched keys keep their rows
/// and partly walked keys keep their cursors, so the work resumes next turn
/// well inside TICK_TIMEOUT (35 s) and the 30 s statement timeout.
const RESOLVE_DEADLINE: Duration = Duration::from_secs(10);

/// Production budgets (#1524 §3.6). At 128 nodes and 32 keys a busy graph
/// drained ~3.4 rows/s against ~16 produced: a captured item is ~35 node-log
/// keys (one per semantic atom, plus its item and node links), so 32 keys
/// was under one item per turn. A turn now resolves 256 keys in one
/// statement, and the node budget counts DISTINCT nodes (the canonical write
/// runs once over the set), doubled to 256: a 2,000-node batch measured
/// 0.65-2.0 s warm on the org tenant, so a full turn stays well inside the
/// 30 s statement timeout and the 35 s tick.
impl Default for Limits {
    fn default() -> Self {
        Self {
            nodes: 256,
            changes: 256,
        }
    }
}

impl Limits {
    fn checked(self) -> Result<Self, sqlx::Error> {
        if self.nodes == 0 || self.nodes > 2000 || self.changes == 0 || self.changes > 2000 {
            return Err(sqlx::Error::Protocol(
                "Library refresh limits must be in 1..=2000".into(),
            ));
        }
        Ok(self)
    }
}

/// Compatible wrapper: existing callers retain their node/event limits while
/// listing work has independently observed caps. There is one worker entry.
#[derive(Clone, Copy, Debug)]
pub struct RefreshLimits {
    pub evidence: Limits,
    pub listing: ListingLimits,
}
impl From<Limits> for RefreshLimits {
    fn from(evidence: Limits) -> Self { Self { evidence, listing: ListingLimits::default() } }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Batch {
    /// Graph owner derived inside the scoped transaction, or the trusted control
    /// account roster for an empty account; never a request id.
    pub account: Option<Uuid>,
    pub nodes: usize,
    pub changes_consumed: usize,
    /// Change rows a completed backfill covered and dropped this refresh.
    pub changes_purged: usize,
    pub backfill_completed: bool,
    pub locked: bool,
    pub listing_outcomes: usize,
    pub listing_changes_consumed: usize,
    pub listing_items_published: usize,
    pub listing_restarted: bool,
    pub listing_backfill_completed: bool,
    /// A listing step failed in this refresh: rolled back to its savepoint,
    /// recorded on the graph's listing state, and the graph's listing work
    /// quarantined. The node evidence of the same refresh still committed.
    pub listing_failed: bool,
}

/// Listing work runs inside the refresh transaction, but none of its
/// failures may fail the node grain. Each listing call (prepare, refresh)
/// sits between these two: a failure rolls the transaction back to the
/// savepoint (node evidence written before it survives, the listing's own
/// partial writes do not), records and quarantines the failure, and reports
/// `None`.
async fn listing_guard_begin(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("SAVEPOINT library_listing").execute(conn).await?;
    Ok(())
}
async fn listing_guard_end<T>(
    conn: &mut PgConnection,
    graph: Uuid,
    phase: &'static str,
    outcome: Result<T, sqlx::Error>,
) -> Result<Option<T>, sqlx::Error> {
    match outcome {
        Ok(value) => {
            sqlx::query("RELEASE SAVEPOINT library_listing").execute(conn).await?;
            Ok(Some(value))
        }
        Err(error) => {
            // ROLLBACK TO is what makes an aborted transaction usable again;
            // if the session itself is gone this fails and the refresh fails.
            sqlx::query("ROLLBACK TO SAVEPOINT library_listing").execute(&mut *conn).await?;
            sqlx::query("RELEASE SAVEPOINT library_listing").execute(&mut *conn).await?;
            listing_facts::quarantine(conn, graph, phase, &error).await?;
            Ok(None)
        }
    }
}

/// The reusable refresh resolver, bounded by actual resolved nodes. An extra
/// row is lookahead, not work: the original log id is retained if it exists.
pub async fn change_nodes(
    conn: &mut PgConnection,
    graph: Uuid,
    id: i64,
    after: Option<Uuid>,
    limit: usize,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT node_id FROM library_evidence_change_nodes($1,$2,$3,$4) ORDER BY node_id",
    )
    .bind(graph)
    .bind(id)
    .bind(after)
    .bind(limit as i32)
    .fetch_all(conn)
    .await
}

/// One graph lock, with no waiting snapshot. These helpers require READ
/// COMMITTED: a caller must not retain a pre-lock repeatable-read snapshot and
/// overwrite evidence committed by the preceding refresher.
pub async fn lock_graph(conn: &mut PgConnection, graph: Uuid) -> Result<bool, sqlx::Error> {
    let isolation: String = sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
        .fetch_one(&mut *conn)
        .await?;
    if isolation != "read committed" {
        return Err(sqlx::Error::Protocol(
            "Library refresh requires READ COMMITTED".into(),
        ));
    }
    sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(hashtextextended('library_evidence'||$1::uuid::text,0))",
    )
    .bind(graph)
    .fetch_one(conn)
    .await
}

static WRITE_SQL: LazyLock<String> = LazyLock::new(|| {
    let columns = evidence::NODE_EVIDENCE_COLUMNS;
    let update = columns
        .split(',')
        .skip(2)
        .map(|c| format!("{c}=EXCLUDED.{c}"))
        .collect::<Vec<_>>()
        .join(",");
    let item_columns = evidence::NODE_ITEM_EVIDENCE_COLUMNS;
    let item_update = item_columns
        .split(',')
        .skip(3)
        .map(|c| format!("{c}=EXCLUDED.{c}"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{}, pruned AS (
 DELETE FROM library_node_item_evidence old WHERE old.graph_id=$1 AND old.node_id=ANY($2)
 AND NOT EXISTS(SELECT 1 FROM node_item_evidence fresh WHERE fresh.graph_id=old.graph_id
  AND fresh.node_id=old.node_id AND fresh.source_item_id=old.source_item_id) RETURNING 1
), written_items AS (
 INSERT INTO library_node_item_evidence({item_columns})
 SELECT {item_columns} FROM node_item_evidence
 ON CONFLICT(graph_id,node_id,source_item_id) DO UPDATE SET {item_update} RETURNING 1
), written_nodes AS (
 INSERT INTO library_node_evidence({columns},computed_version,computed_at)
 SELECT {columns},$3,now() FROM node_evidence
 ON CONFLICT(graph_id,node_id) DO UPDATE SET {update},computed_version=EXCLUDED.computed_version,computed_at=EXCLUDED.computed_at
 RETURNING 1
) SELECT count(*)::bigint FROM written_nodes"#,
        evidence::node_evidence_ctes(evidence::NODE_BATCH_SQL)
    )
});

#[cfg(feature = "test-support")]
pub fn write_sql_for_explain() -> &'static str {
    &WRITE_SQL
}

/// Materialized evidence sets have deliberately bounded inputs but PostgreSQL
/// estimates their cardinality as one. Hash joins retain every key instead of
/// comparing every pair within a graph. Prefer hash aggregation over sorting
/// every expanded link; work_mem remains the database's actual runtime value.
/// Invoker wrappers isolate the canonical indexed/generation lookup plans.
/// Call only inside the caller's scoped transaction; settings restore at end.
pub async fn configure_node_query(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('enable_nestloop','off',true),set_config('enable_mergejoin','off',true),set_config('enable_sort','off',true),set_config('jit','off',true)")
        .execute(conn).await?;
    Ok(())
}

/// Writes both grains atomically from one invocation of the canonical evidence
/// definition. `conn` must be the caller's scoped, graph-locked transaction.
/// The listing grain's node count follows this write by trigger delta
/// (`library_listing_node_count()`), switch-gated like capture.
pub async fn store_nodes(
    conn: &mut PgConnection,
    graph: Uuid,
    nodes: &[Uuid],
) -> Result<usize, sqlx::Error> {
    if nodes.is_empty() {
        return Ok(0);
    }
    configure_node_query(conn).await?;
    let count: i64 = sqlx::query_scalar(&WRITE_SQL)
        .bind(graph)
        .bind(nodes)
        .bind(READ_MODEL_VERSION)
        .fetch_one(&mut *conn)
        .await?;
    Ok(count as usize)
}

#[cfg(feature = "test-support")]
pub fn listing_build_sql_for_explain() -> &'static str { listing_facts::BUILD_SQL }

/// One-use barrier at the actual first source-header snapshot, for concurrency
/// tests that still call the public refresh entry and real PostgreSQL writers.
#[cfg(feature="test-support")]
pub fn pause_listing_input_for_test(graph:Uuid) -> Result<(tokio::sync::oneshot::Receiver<()>,tokio::sync::oneshot::Sender<()>),&'static str> {listing_facts::pause_input(graph)}

/// At most one bounded batch, with transaction lifetime owned by the caller.
/// A rollback retries the same durable cursor/log ids and the same backfill
/// page. A crash cannot publish only one grain or acknowledge unwritten work.
/// A listing failure, however, never fails the node grain: each listing call
/// runs under a savepoint and a failure quarantines only the graph's listing
/// work (see `listing_guard_end`).
pub async fn refresh_graph_tx(
    conn: &mut PgConnection,
    graph: Uuid,
    limits: impl Into<RefreshLimits>,
) -> Result<Batch, sqlx::Error> {
    let budgets = limits.into();
    let limits = budgets.evidence.checked()?;
    let listing_limits = budgets.listing.checked()?;
    if !lock_graph(conn, graph).await? {
        return Ok(Batch {
            locked: true,
            ..Batch::default()
        });
    }
    let Some(account) = sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM graphs WHERE id=$1")
        .bind(graph)
        .fetch_optional(&mut *conn)
        .await?
    else {
        return Ok(Batch::default());
    };
    // Tenant systems can see every member/team ocean but correction-title RLS
    // also needs its owning account. Derive it from the scoped graph, never a
    // request value; settings are transaction-local and pooler-safe.
    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id','',true),set_config('statement_timeout','30s',true),set_config('jit','off',true),set_config('plan_cache_mode','force_custom_plan',true)")
        .bind(account.to_string()).execute(&mut *conn).await?;
    sqlx::query("INSERT INTO library_evidence_state(graph_id) VALUES($1) ON CONFLICT DO NOTHING")
        .bind(graph)
        .execute(&mut *conn)
        .await?;
    // The access class (tenant 0206 / control 0391) both grains certify
    // under and the reader compares: account, history floor, correction
    // visibility and the RLS contract digest, never the role.
    let context: Value = crate::read_access::certify(&mut *conn, graph).await?;
    // One Off setting: with the switch off (or on but before its flip
    // barrier) this refresh runs no listing work and no node work, and the
    // capture triggers write nothing. A quarantined graph (an earlier listing
    // failure, still backing off) closes the listing gate alone; node
    // evidence proceeds.
    let mut gate = listing_facts::gate(conn, graph).await?;
    let quarantined_before = gate.quarantined;
    if let Some(since) = gate.active_since() {
        listing_guard_begin(conn).await?;
        let outcome = listing_facts::prepare(conn, graph, account, &context, since).await;
        if listing_guard_end(conn, graph, "prepare", outcome).await?.is_none() {
            gate.quarantined = true;
        }
    }
    let mut report = Batch {
        account: Some(account),
        ..Batch::default()
    };
    // The switch generation this refresh maintains under. None: the switch
    // is Off, or On but its generation has not passed the flip barrier.
    let Some(since) = gate.enabled_since else {
        // Nothing reads the model now (the reader requires the switch's
        // enabled_since), and the next generation rebuilds every grain from
        // a fresh start snapshot, so node work here is wasted: a grain
        // certified now would be certified under the wrong generation. The
        // state row is left as it is (a running backfill is restarted by the
        // next generation). Every row this can see committed before that
        // future start snapshot, so the rebuild covers its write; the log is
        // simply dropped, a bounded batch per refresh.
        report.changes_purged = purge_while_off(conn, graph).await?;
        report.listing_failed = gate.quarantined && !quarantined_before;
        sqlx::query("UPDATE library_evidence_state SET last_refreshed_at=clock_timestamp() WHERE graph_id=$1")
            .bind(graph)
            .execute(conn)
            .await?;
        return Ok(report);
    };
    // Capture missed while Off never reaches the log, so a grain certified
    // under a different generation is a rebuild, exactly as the listing
    // grain's capture_since: the first refresh after a flip to On restarts
    // the backfill.
    let generation_ok = |certified: Option<DateTime<Utc>>| certified == Some(since);
    let (ready, computed_account, version, backfill_account, after, computed_context, backfill_context, capture_since, backfill_capture_since, snapshot_present): (Option<i32>,Option<Uuid>,Option<i32>,Option<Uuid>,Option<Uuid>,Option<Value>,Option<Value>,Option<DateTime<Utc>>,Option<DateTime<Utc>>,bool) =
        sqlx::query_as("SELECT ready_version,computed_account_id,backfill_version,backfill_account_id,backfill_after_node_id,computed_context,backfill_context,capture_since,backfill_capture_since,backfill_snapshot IS NOT NULL FROM library_evidence_state WHERE graph_id=$1")
            .bind(graph).fetch_one(&mut *conn).await?;
    if ready != Some(READ_MODEL_VERSION)
        || computed_account != Some(account)
        || computed_context.as_ref() != Some(&context)
        || !generation_ok(capture_since)
    {
        // A backfill without a start snapshot (begun before 0201/0380) cannot
        // acknowledge anything safely, so it restarts rather than resumes.
        let after = if version == Some(READ_MODEL_VERSION)
            && backfill_account == Some(account)
            && backfill_context.as_ref() == Some(&context)
            && generation_ok(backfill_capture_since)
            && snapshot_present
        {
            after
        } else {
            // The snapshot is taken here, before this transaction's first
            // node page, and only after the generation's flip barrier (the
            // gate): every transaction that wrote without logging has ended,
            // so this snapshot sees its data. A change visible in it
            // committed before every page's snapshot, so the completed
            // backfill covers it; a change whose transaction was still open,
            // or began later, keeps its row.
            sqlx::query("UPDATE library_evidence_state SET ready_version=NULL,computed_account_id=NULL,backfilled_at=NULL,computed_context=NULL,capture_since=NULL,backfill_version=$2,backfill_account_id=$3,backfill_context=$4,backfill_capture_since=$5,backfill_after_node_id=NULL,backfill_started_at=now(),backfill_snapshot=pg_current_snapshot() WHERE graph_id=$1")
                .bind(graph).bind(READ_MODEL_VERSION).bind(account).bind(&context).bind(since).execute(&mut *conn).await?;
            None
        };
        let mut nodes: Vec<Uuid> = sqlx::query_scalar("SELECT node_id FROM nodes WHERE graph_id=$1 AND ($2::uuid IS NULL OR node_id>$2) ORDER BY node_id LIMIT $3")
            .bind(graph).bind(after).bind(limits.nodes as i64 + 1).fetch_all(&mut *conn).await?;
        let more = nodes.len() > limits.nodes;
        nodes.truncate(limits.nodes);
        report.nodes = store_nodes(conn, graph, &nodes).await?;
        if more {
            sqlx::query(
                "UPDATE library_evidence_state SET backfill_after_node_id=$2 WHERE graph_id=$1",
            )
            .bind(graph)
            .bind(nodes.last().copied())
            .execute(&mut *conn)
            .await?;
        } else {
            sqlx::query("UPDATE library_evidence_state SET ready_version=$2,computed_account_id=$3,backfilled_at=now(),backfill_after_node_id=NULL,computed_context=$4,capture_since=$5 WHERE graph_id=$1")
                .bind(graph).bind(READ_MODEL_VERSION).bind(account).bind(&context).bind(since).execute(&mut *conn).await?;
            report.backfill_completed = true;
            // Every change the start snapshot covers was recomputed by a later
            // page; a write behind the UUID cursor that committed after the
            // snapshot keeps its row. Acknowledge the first bounded batch now;
            // the snapshot stays on the state row until nothing is left.
            report.changes_purged = purge_covered(conn, graph).await?.0;
        }
    } else {
        let mut drain = true;
        if snapshot_present {
            let (purged, finished) = purge_covered(conn, graph).await?;
            report.changes_purged = purged;
            // A full batch means more covered rows wait at the head of the
            // log; draining now would only resolve rows the next refresh drops.
            drain = finished;
        }
        if drain {
            drain_changes(conn, graph, limits, &mut report).await?;
        }
    }
    if gate.open() {
        listing_guard_begin(conn).await?;
        let outcome = listing_facts::refresh(conn, graph, account, &context, listing_limits).await;
        match listing_guard_end(conn, graph, "refresh", outcome).await? {
            Some(listing) => {
                report.listing_outcomes = listing.outcomes;
                report.listing_changes_consumed = listing.changes;
                report.listing_items_published = listing.published;
                report.listing_restarted = listing.restarted;
                report.listing_backfill_completed = listing.completed;
                listing_facts::recover(conn, graph).await?;
            }
            None => gate.quarantined = true,
        }
    }
    report.listing_failed = gate.quarantined && !quarantined_before;
    sqlx::query(
        "UPDATE library_evidence_state SET last_refreshed_at=clock_timestamp() WHERE graph_id=$1",
    )
    .bind(graph)
    .execute(conn)
    .await?;
    Ok(report)
}

/// One bounded batch of the change rows a completed backfill covers: logged
/// by a transaction visible in the backfill's start snapshot (so every page
/// saw its data), or before 0201/0380 added `capture_xid` (so before any
/// snapshot). Returns the rows dropped and whether the purge is finished,
/// which clears the snapshot. Late commits (an xid in progress at, or after,
/// the snapshot) are never visible in it and keep their rows for the drain.
async fn purge_covered(conn: &mut PgConnection, graph: Uuid) -> Result<(usize, bool), sqlx::Error> {
    let purged: i64 = sqlx::query_scalar(
        "WITH covered AS (SELECT c.id FROM library_evidence_changes c WHERE c.graph_id=$1 AND (c.capture_xid IS NULL OR pg_visible_in_snapshot(c.capture_xid,(SELECT s.backfill_snapshot FROM library_evidence_state s WHERE s.graph_id=$1))) ORDER BY c.id LIMIT $2), gone AS (DELETE FROM library_evidence_changes d USING covered WHERE d.graph_id=$1 AND d.id=covered.id RETURNING 1) SELECT count(*) FROM gone",
    )
    .bind(graph)
    .bind(PURGE_BATCH as i64)
    .fetch_one(&mut *conn)
    .await?;
    let finished = (purged as usize) < PURGE_BATCH;
    if finished {
        sqlx::query("UPDATE library_evidence_state SET backfill_snapshot=NULL WHERE graph_id=$1")
            .bind(graph)
            .execute(conn)
            .await?;
    }
    Ok((purged as usize, finished))
}

/// One bounded batch of the log, oldest first, deleted without resolving
/// it: the refresh runs Off (or before the flip barrier), when nothing reads
/// the model and the next generation's rebuild covers every committed write.
async fn purge_while_off(conn: &mut PgConnection, graph: Uuid) -> Result<usize, sqlx::Error> {
    let purged: i64 = sqlx::query_scalar(
        "WITH doomed AS (SELECT c.id FROM library_evidence_changes c WHERE c.graph_id=$1 ORDER BY c.id LIMIT $2), gone AS (DELETE FROM library_evidence_changes d USING doomed WHERE d.graph_id=$1 AND d.id=doomed.id RETURNING 1) SELECT count(*) FROM gone",
    )
    .bind(graph)
    .bind(PURGE_BATCH as i64)
    .fetch_one(conn)
    .await?;
    Ok(purged as usize)
}

/// One distinct log key of a drain turn: every row read for it, the lowest
/// id as the row that carries a cursor, and the node cursor the walk resumes
/// from. Any row without a cursor restarts the walk from the first node:
/// that row's write may have reached a node an earlier cursor already passed.
struct Pending {
    carrier: i64,
    ids: Vec<i64>,
    after: Option<Uuid>,
    restart: bool,
    touched: bool,
    more: bool,
}
impl Pending {
    fn new(id: i64, cursor: Option<Uuid>) -> Self {
        Self { carrier: id, ids: vec![id], after: cursor, restart: cursor.is_none(), touched: false, more: false }
    }
    fn merge(&mut self, id: i64, cursor: Option<Uuid>) {
        self.ids.push(id);
        match cursor {
            None => self.restart = true,
            Some(c) if !self.restart => self.after = Some(self.after.map_or(c, |a| a.min(c))),
            Some(_) => {}
        }
    }
    /// A hub expansion already under way: an earlier turn walked part of
    /// this key's fan-out and left its cursor, and no row asks for a restart.
    fn hub(&self) -> bool {
        !self.restart && self.after.is_some()
    }
}

/// Take the longest prefix of one key's ordered page (at most `cap` nodes;
/// the page may carry one more as lookahead) that the turn's node budget
/// allows. The budget bounds the canonical recomputation, which runs once
/// over the turn's DISTINCT nodes, so a node another key of this turn already
/// took is free; every new node is charged, a concurrently deleted one too
/// (the budget never grows because an input disappeared). A prefix shorter
/// than the page leaves the key with its cursor at the last node taken, so
/// its carrier row is kept rather than acknowledged; an empty prefix leaves
/// the key untouched.
fn take(key: &mut Pending, page: &[Uuid], cap: usize, budget: &mut usize, nodes: &mut BTreeSet<Uuid>, report: &mut Batch) {
    let mut taken = 0;
    for node in page.iter().take(cap) {
        if !nodes.contains(node) {
            if *budget == 0 {
                break;
            }
            *budget -= 1;
            report.nodes += 1;
            nodes.insert(*node);
        }
        taken += 1;
    }
    // A key with no node left to walk (a deleted object, an unlinked item) is
    // fully resolved by its empty page; only a non-empty page the budget
    // could not start leaves the key untouched.
    if taken == 0 && !page.is_empty() {
        return;
    }
    key.touched = true;
    key.more = taken < page.len();
    if key.more {
        key.after = Some(page[taken - 1]);
    }
}

/// Resolve up to `cap` more nodes of one key from its cursor, within the
/// turn's node budget (see `take`).
async fn resolve(
    conn: &mut PgConnection,
    graph: Uuid,
    key: &mut Pending,
    cap: usize,
    budget: &mut usize,
    nodes: &mut BTreeSet<Uuid>,
    report: &mut Batch,
) -> Result<(), sqlx::Error> {
    let cap = cap.min(*budget);
    if cap == 0 {
        return Ok(());
    }
    let page = change_nodes(conn, graph, key.carrier, key.after, cap + 1).await?;
    take(key, &page, cap, budget, nodes, report);
    Ok(())
}

/// Every key's next page in ONE statement: the same bounded
/// `library_evidence_change_nodes` walk per key, from each key's own cursor,
/// `limit` nodes each (a share plus one lookahead). One round trip for the
/// whole turn instead of one per key.
async fn change_node_pages(
    conn: &mut PgConnection,
    graph: Uuid,
    keys: &[Pending],
    limit: usize,
) -> Result<Vec<Vec<Uuid>>, sqlx::Error> {
    let ids: Vec<i64> = keys.iter().map(|k| k.carrier).collect();
    let afters: Vec<Option<Uuid>> = keys.iter().map(|k| k.after).collect();
    let rows: Vec<(i64, Uuid)> = sqlx::query_as(
        "SELECT k.ord,r.node_id FROM unnest($2::bigint[],$3::uuid[]) WITH ORDINALITY k(id,after_node,ord) CROSS JOIN LATERAL library_evidence_change_nodes($1,k.id,k.after_node,$4) r ORDER BY k.ord,r.node_id",
    )
    .bind(graph)
    .bind(&ids)
    .bind(&afters)
    .bind(limit as i32)
    .fetch_all(conn)
    .await?;
    let mut pages = vec![Vec::new(); keys.len()];
    for (ord, node) in rows {
        pages[(ord - 1) as usize].push(node);
    }
    Ok(pages)
}

/// One bounded drain turn over the head of the graph's log. Each distinct key
/// among the rows read is resolved once, so a burst that logged one key many
/// times costs one walk, and every row read for a resolved key is
/// acknowledged together (a partially walked key keeps only its carrier row,
/// whose cursor the next turn resumes from; a walk that restarted from the
/// first node covers what every merged row needed). The node budget is
/// shared in two passes so one large fan-out cannot take the whole turn and
/// stall the keys behind it: an equal share per key, every key's page read
/// in one statement, then the remainder to the keys still unfinished, one
/// walk each. Hub expansions already under way (a key carrying a cursor from
/// an earlier turn) go first in both passes, in log order, then every other
/// key in log order: a started hub finishes in as few turns as the budget
/// allows, which bounds how long its many dirty nodes keep reads off the
/// overlay (`dirty_hub`, crate::library_changes).
async fn drain_changes(
    conn: &mut PgConnection,
    graph: Uuid,
    limits: Limits,
    report: &mut Batch,
) -> Result<(), sqlx::Error> {
    let rows: Vec<(i64, Option<Uuid>, Option<Uuid>, Option<Uuid>, Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
        "SELECT id,resolved_after_node_id,node_id,source_item_id,atom_id,semantic_atom_id FROM library_evidence_changes WHERE graph_id=$1 ORDER BY id LIMIT $2",
    )
    .bind(graph)
    .bind((limits.changes * DEDUPE_WINDOW) as i64)
    .fetch_all(&mut *conn)
    .await?;
    let mut keys: Vec<Pending> = Vec::new();
    let mut index: HashMap<(u8, Uuid), usize> = HashMap::new();
    for (id, cursor, node, item, atom, outcome) in rows {
        let key = match (node, item, atom, outcome) {
            (Some(k), _, _, _) => (0, k),
            (_, Some(k), _, _) => (1, k),
            (_, _, Some(k), _) => (2, k),
            (_, _, _, Some(k)) => (3, k),
            _ => continue, // the queue CHECK forbids a keyless row
        };
        match index.get(&key) {
            Some(&at) => keys[at].merge(id, cursor),
            None if keys.len() < limits.changes => {
                index.insert(key, keys.len());
                keys.push(Pending::new(id, cursor));
            }
            // Beyond this turn's key budget: left untouched for a later turn.
            None => {}
        }
    }
    for key in &mut keys {
        if key.restart {
            key.after = None;
        }
    }
    // Hub expansions under way first, then log order; a stable sort keeps
    // log order within each class.
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by_key(|&i| !keys[i].hub());
    let mut budget = limits.nodes;
    let mut nodes = BTreeSet::new();
    let started = Instant::now();
    if !keys.is_empty() && budget > 0 {
        let share = budget.div_ceil(keys.len());
        let pages = change_node_pages(conn, graph, &keys, share + 1).await?;
        for &i in &order {
            take(&mut keys[i], &pages[i], share, &mut budget, &mut nodes, report);
        }
    }
    for &i in &order {
        if budget == 0 || started.elapsed() >= RESOLVE_DEADLINE {
            break;
        }
        if keys[i].more {
            resolve(conn, graph, &mut keys[i], budget, &mut budget, &mut nodes, report).await?;
        }
    }
    // One canonical statement for the turn's node set, as a backfill page.
    let batch: Vec<Uuid> = nodes.into_iter().collect();
    store_nodes(conn, graph, &batch).await?;
    // A partially walked key keeps its carrier with the advanced cursor;
    // every other row read for a touched key is acknowledged, all in one
    // statement. Untouched keys keep every row for a later turn.
    let mut acked: Vec<i64> = Vec::new();
    for key in &keys {
        if !key.touched {
            continue;
        }
        if key.more {
            sqlx::query("UPDATE library_evidence_changes SET resolved_after_node_id=$3 WHERE graph_id=$1 AND id=$2")
                .bind(graph).bind(key.carrier).bind(key.after).execute(&mut *conn).await?;
            acked.extend(key.ids.iter().copied().filter(|id| *id != key.carrier));
        } else {
            acked.extend(key.ids.iter().copied());
        }
    }
    if !acked.is_empty() {
        let deleted = sqlx::query("DELETE FROM library_evidence_changes WHERE graph_id=$1 AND id=ANY($2)")
            .bind(graph)
            .bind(&acked)
            .execute(&mut *conn)
            .await?
            .rows_affected();
        report.changes_consumed += deleted as usize;
    }
    Ok(())
}

// Advance before attempting work, including failures and lock contention.
// Scheduling must not depend on committing the graph's refresh transaction:
// a malformed/expensive graph would otherwise monopolize every future tick.
static GRAPH_ATTEMPTS: LazyLock<Mutex<HashMap<(TenantId, Option<Uuid>), Uuid>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

async fn next_graph(
    conn: &mut PgConnection,
    tenant: &TenantId,
    account: Option<Uuid>,
) -> Result<Option<Uuid>, sqlx::Error> {
    let key = (tenant.clone(), account);
    let previous = GRAPH_ATTEMPTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .copied();
    let graph = sqlx::query_scalar(
        "SELECT id FROM graphs ORDER BY ($1::uuid IS NULL OR id>$1) DESC,id LIMIT 1",
    )
    .bind(previous)
    .fetch_optional(conn)
    .await?;
    if let Some(graph) = graph {
        GRAPH_ATTEMPTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, graph);
    }
    Ok(graph)
}

static CONSUMER_ACCOUNT: Mutex<Option<Uuid>> = Mutex::new(None);

async fn run_tick(t: &WorkerTenant) -> Result<Batch, String> {
    let err = |e: sqlx::Error| e.to_string();
    match &t.tenant {
        TenantId::Org(_) => {
            let mut tx = begin_tenant_system_scoped_without_account(&t.pool)
                .await
                .map_err(|e| e.to_string())?;
            let Some(graph) = next_graph(&mut tx, &t.tenant, None).await.map_err(err)? else {
                return Ok(Batch::default());
            };
            let batch = refresh_graph_tx(&mut tx, graph, Limits::default())
                .await
                .map_err(err)?;
            tx.commit().await.map_err(err)?;
            Ok(batch)
        }
        TenantId::Consumer => consumer_tick(&t.pool).await,
    }
}

async fn consumer_tick(pool: &PgPool) -> Result<Batch, String> {
    // Only the control identity roster is read unscoped. Content discovery and
    // recomputation use a separate account transaction. Rotate the starting
    // account between ticks so one continuously busy account cannot starve the
    // next; each tick still recomputes at most one graph's bounded node batch.
    let previous = *CONSUMER_ACCOUNT.lock().unwrap_or_else(|e| e.into_inner());
    let account: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM accounts ORDER BY ($1::uuid IS NULL OR id>$1) DESC,id LIMIT 1",
    )
    .bind(previous)
    .fetch_optional(pool)
    .await
    .map_err(|e| e.to_string())?;
    let Some(account) = account else {
        return Ok(Batch::default());
    };
    *CONSUMER_ACCOUNT.lock().unwrap_or_else(|e| e.into_inner()) = Some(account);
    let mut tx = begin_account_scoped(pool, account)
        .await
        .map_err(|e| e.to_string())?;
    sqlx::query("SELECT set_config('app.user_id','',true),set_config('app.tenant_actor','',true)")
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    let Some(graph) = next_graph(&mut tx, &TenantId::Consumer, Some(account))
        .await
        .map_err(|e| e.to_string())?
    else {
        // The account is known even without a graph. Finish the discovery
        // transaction before the independent, bounded progress observation.
        tx.rollback().await.map_err(|e| e.to_string())?;
        return Ok(Batch {
            account: Some(account),
            ..Batch::default()
        });
    };
    let batch = refresh_graph_tx(&mut tx, graph, Limits::default())
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(batch)
}

pub async fn tick(t: &WorkerTenant) -> Result<Tick, String> {
    // The emergency stop opens no database connection at all (its contract,
    // proved by the success-events suite); it therefore mirrors nothing.
    if crate::admission::env_flag("LIBRARY_EVIDENCE_REFRESH_DISABLED") {
        return Ok(Tick::idle());
    }
    // The switch row is flipped only by the operator command, never here
    // (tenant 0202 / control 0383); a tick only stamps a flip barrier the
    // flip left unstamped (a crash between the operator's flip and its
    // stamp), which writes nothing once the generation has one.
    if listing_facts::stamp_barrier(&t.pool)
        .await
        .map_err(|e| format!("Library listing switch: {e}"))?
    {
        tracing::info!(
            worker = "library_evidence",
            tenant = ?t.tenant,
            "Library listing flip barrier stamped"
        );
    }
    let started = Instant::now();
    let batch = tokio::time::timeout(TICK_TIMEOUT, run_tick(t))
        .await
        .map_err(|_| "Library evidence refresh timed out; transaction rolls back".to_string())??;
    let processed = batch.nodes + batch.changes_consumed + batch.changes_purged + usize::from(batch.backfill_completed)
        + batch.listing_outcomes + batch.listing_changes_consumed + batch.listing_items_published
        + usize::from(batch.listing_backfill_completed);
    // Both content-plane paths have awaited COMMIT before returning a positive
    // Batch. Report before the optional observer await so its cancellation does
    // not hide committed work. These are per-tick mixed units, not a durable
    // counter or node throughput. Idle/error/cancelled ticks emit no success.
    if processed > 0 {
        // The JSON formatter falls back to the current span even for a root
        // event. Enter a field-free root to exclude private ambient context.
        let root = tracing::info_span!(parent: None, "library_evidence_commit");
        // An event-only filter may disable the root span. Skip only this log
        // rather than allowing that fallback to serialize an ambient span.
        if !root.is_disabled() {
            root.in_scope(|| tracing::info!(
                parent: &root,
                worker = "library_evidence",
                event = "library_evidence_batch_committed",
                tenant = ?t.tenant,
                plane = match &t.tenant { TenantId::Consumer => "consumer", TenantId::Org(_) => "tenant" },
                nodes = batch.nodes,
                changes_consumed = batch.changes_consumed,
                changes_purged = batch.changes_purged,
                backfill_completed = usize::from(batch.backfill_completed),
                listing_outcomes = batch.listing_outcomes,
                listing_changes_consumed = batch.listing_changes_consumed,
                listing_items_published = batch.listing_items_published,
                listing_backfill_completed = usize::from(batch.listing_backfill_completed),
                processed,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Library evidence batch committed"
            ));
        }
    }
    Ok(Tick {
        processed,
        defer: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn an_empty_page_resolves_its_key_and_a_full_budget_leaves_it_untouched() {
        let mut nodes = BTreeSet::new();
        let mut report = Batch::default();
        let mut empty = Pending::new(1, None);
        let mut budget = 0;
        take(&mut empty, &[], 4, &mut budget, &mut nodes, &mut report);
        assert!(empty.touched && !empty.more, "nothing left to walk: acknowledged");
        let mut starved = Pending::new(2, None);
        take(&mut starved, &[node(1)], 4, &mut budget, &mut nodes, &mut report);
        assert!(!starved.touched, "no budget for its first node: kept for a later turn");
    }

    #[test]
    fn shared_nodes_are_charged_once_and_a_cut_page_keeps_its_cursor() {
        let mut nodes = BTreeSet::new();
        let mut report = Batch::default();
        let mut budget = 3;
        let mut first = Pending::new(1, None);
        take(&mut first, &[node(1), node(2)], 4, &mut budget, &mut nodes, &mut report);
        assert!(first.touched && !first.more);
        // Two shared nodes are free; the budget's last unit takes node 3 and
        // node 4 is the cut: the key keeps its cursor at node 3.
        let mut second = Pending::new(2, None);
        take(&mut second, &[node(1), node(2), node(3), node(4)], 4, &mut budget, &mut nodes, &mut report);
        assert_eq!((report.nodes, budget), (3, 0));
        assert!(second.touched && second.more);
        assert_eq!(second.after, Some(node(3)));
        // The lookahead row alone marks more work without being taken.
        let mut third = Pending::new(3, None);
        take(&mut third, &[node(1), node(2)], 1, &mut budget, &mut nodes, &mut report);
        assert!(third.more && third.after == Some(node(1)));
    }

    #[test]
    fn hub_expansions_under_way_are_recognized() {
        let mut hub = Pending::new(1, Some(node(5)));
        assert!(hub.hub());
        hub.merge(2, None);
        assert!(!hub.hub(), "a row without a cursor restarts the walk");
        assert!(!Pending::new(3, None).hub());
    }
}

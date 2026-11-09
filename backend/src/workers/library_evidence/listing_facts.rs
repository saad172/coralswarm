//! One graph-locked maintenance interface. Generation state, bounded raw walks,
//! active reconciliation, the node count and ACK are private to this file.
//! No caller can publish a token or acknowledge a lifecycle step independently.

use crate::library_read::listing_facts::{self as facts, ItemInput, SourceHeader};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgConnection, Row};
use uuid::Uuid;

/// `LIBRARY_LISTING_FACTS` no longer sets anything (tenant 0202 / control
/// 0383). Each plane's `library_listing_switch` row is the only source of
/// truth, flipped only by an explicit operator action (`flip_switch`, run by
/// the `library_listing_switch` operator binary). Before this, every backend
/// mirrored its own env value into the row every tick, so instances that
/// disagreed during a rolling deploy flapped `enabled_since` and forced a
/// full rebuild of every graph on each flip.
///
/// The variable is ignored with a warning rather than rejected: a machine
/// that still carries it must boot (a hard error would fail a rolling deploy
/// on configuration that is now harmless), and an operator who sets it
/// expecting it to work must be told where the setting moved. Returns the
/// warning for a set, non-empty value.
pub const LEGACY_SETTING: &str = "LIBRARY_LISTING_FACTS";
pub fn legacy_setting_warning(value: Option<&str>) -> Option<String> {
    let value = value.map(str::trim).filter(|v| !v.is_empty())?;
    Some(format!(
        "{LEGACY_SETTING}={value:?} is ignored: the Library listing switch is the library_listing_switch row of each plane, flipped only by the library_listing_switch operator binary"
    ))
}
pub fn warn_on_legacy_setting() {
    if let Some(warning) = legacy_setting_warning(std::env::var(LEGACY_SETTING).ok().as_deref()) {
        tracing::warn!(worker = "library_evidence", event = "library_listing_setting_ignored", "{warning}");
    }
}

/// The session setting the switch's guard trigger (tenant 0202 / control
/// 0383) requires on any UPDATE that changes `enabled` or `enabled_since`.
/// It is not a security boundary (any session may set it); it makes the
/// flip an explicit act no other code path performs, and it refuses the
/// per-tick env mirror of a binary that predates this change, so an old
/// instance still running during the deploy cannot revert an operator flip.
const FLIP_SETTING: &str = "coralswarm.library_listing_switch_flip";

/// The explicit operator flip of one plane's switch. Returns the new value
/// when the row changed, `None` when it already had that value (the call is
/// idempotent and a no-op flip starts no new generation).
///
/// The flip commits in its own transaction; the flip barrier is stamped
/// afterwards by `stamp_barrier`, in a separate transaction whose xid is
/// assigned after the flip committed. That ordering is what the barrier
/// needs (see `SETTLED`): the flip's own xid would not do, since a writer
/// can take a larger xid than the flip's while still reading Off. A crash
/// between the two leaves the barrier unstamped; the worker's next tick on
/// the plane stamps it (and so does re-running the operator command).
pub async fn flip_switch<'a, A>(conn: A, on: bool) -> Result<Option<bool>, sqlx::Error>
where
    A: sqlx::Acquire<'a, Database = sqlx::Postgres>,
{
    let mut conn = conn.acquire().await?;
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    sqlx::query("SELECT set_config($1,'operator',true)")
        .bind(FLIP_SETTING)
        .execute(&mut *tx)
        .await?;
    let flipped = sqlx::query_scalar("UPDATE library_listing_switch SET enabled=$1,enabled_since=CASE WHEN $1 THEN now() END,enabled_snapshot=NULL,snapshot_since=NULL,barrier_xid=NULL,barrier_since=NULL,changed_at=now() WHERE enabled IS DISTINCT FROM $1 RETURNING enabled")
        .bind(on)
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    stamp_barrier(&mut *conn).await?;
    Ok(flipped)
}

/// Stamp the current On generation's flip barrier if it has none yet: the
/// xid of this statement's own transaction, which `pg_current_xact_id()`
/// assigns here, after a snapshot that already sees the flip committed.
/// Writes nothing (and assigns no xid) when the switch is Off or the
/// generation is already stamped, so the worker runs it every tick. It never
/// flips the switch. The InitPlan pins the generation this statement's
/// snapshot saw, so a concurrent re-flip is not stamped with this xid.
pub async fn stamp_barrier<'e, E: sqlx::PgExecutor<'e>>(executor: E) -> Result<bool, sqlx::Error> {
    let stamped = sqlx::query("UPDATE library_listing_switch SET barrier_xid=pg_current_xact_id(),barrier_since=enabled_since WHERE enabled AND barrier_since IS DISTINCT FROM enabled_since AND enabled_since=(SELECT w.enabled_since FROM library_listing_switch w)")
        .execute(executor)
        .await?
        .rows_affected();
    Ok(stamped > 0)
}

/// Whether the current On generation has passed its flip barrier: every
/// transaction that could have read the switch as Off has ended. `None`
/// when the switch is Off.
pub async fn switch_settled<'e, E: sqlx::PgExecutor<'e>>(
    executor: E,
) -> Result<Option<bool>, sqlx::Error> {
    sqlx::query_scalar(&format!("SELECT {SETTLED} FROM library_listing_switch w WHERE w.enabled"))
        .fetch_optional(executor)
        .await
}

/// The flip barrier, over the switch row `w`. A missing stamp, or one from
/// another generation, is unsettled.
///
/// Why the stamp is an xid and the test is strict. A capture trigger reads
/// the switch after its statement's writes, so its transaction W already has
/// an xid; in READ COMMITTED each statement of the plpgsql trigger takes a
/// fresh snapshot. If W read Off, that snapshot did not see the flip F
/// committed. The stamp S runs after F committed and gets its xid after a
/// snapshot that sees F, so xid(W) < xid(S): xids are assigned in order, and
/// W's was assigned before its read, which preceded F's commit. Once
/// `pg_snapshot_xmin(pg_current_snapshot())` (the lowest xid still running)
/// exceeds xid(S), every such W has ended.
///
/// The previous barrier (0201/0380) stamped `pg_current_snapshot()` and
/// settled at xmin >= its xmax. A snapshot's xmax is one past the highest
/// COMPLETED xid, not the next xid to be assigned: a W whose xid is above
/// every completed xid at the stamp (W took its xid after F's, read Off
/// before F committed) is neither below xmax nor in the snapshot's xip list,
/// so xmin could reach xmax with W still open and its unlogged write missed
/// by the rebuild.
const SETTLED: &str = "(w.barrier_since IS NOT DISTINCT FROM w.enabled_since AND w.barrier_xid IS NOT NULL AND pg_snapshot_xmin(pg_current_snapshot())>w.barrier_xid)";

/// What one refresh may do for the listing grain, read once under the
/// graph lock. `enabled_since` is the switch generation the facts must be
/// certified under: `None` when the switch is off, and also while it is on
/// but the generation has not yet passed its flip barrier (`awaiting_flip`),
/// since no rebuild for it may take its snapshot before then.
/// `quarantined` is a graph still waiting out the backoff from an earlier
/// listing failure. Any of these closes the gate: no listing work at all.
#[derive(Clone, Copy, Debug)]
pub(super) struct Gate {
    pub enabled_since: Option<DateTime<Utc>>,
    pub awaiting_flip: bool,
    pub quarantined: bool,
}
impl Gate {
    pub fn open(&self) -> bool {
        self.enabled_since.is_some() && !self.quarantined
    }
    /// The switch generation to maintain under, when the gate is open.
    pub fn active_since(&self) -> Option<DateTime<Utc>> {
        self.enabled_since.filter(|_| !self.quarantined)
    }
}
pub(super) async fn gate(conn: &mut PgConnection, graph: Uuid) -> Result<Gate, sqlx::Error> {
    let (enabled_since, settled, quarantined): (Option<DateTime<Utc>>, Option<bool>, bool) = sqlx::query_as(&format!(
        "SELECT w.enabled_since,{SETTLED},EXISTS(SELECT 1 FROM library_listing_state WHERE graph_id=$1 AND retry_after>now()) FROM (SELECT 1) anchor LEFT JOIN library_listing_switch w ON w.enabled",
    ))
    .bind(graph)
    .fetch_one(conn)
    .await?;
    let awaiting_flip = enabled_since.is_some() && settled != Some(true);
    Ok(Gate {
        enabled_since: enabled_since.filter(|_| !awaiting_flip),
        awaiting_flip,
        quarantined,
    })
}

/// Record a failed listing step after its savepoint rolled back, and hold
/// the graph's listing work for 30s doubling per consecutive failure, capped
/// at an hour. The row is written with the refresh's own scope; the caller
/// commits it with the node evidence that succeeded. Observable in the
/// state row (`failure_count`, `last_failure`, `last_failed_at`,
/// `retry_after`) and in the warning this emits.
pub(super) async fn quarantine(
    conn: &mut PgConnection,
    graph: Uuid,
    phase: &'static str,
    error: &sqlx::Error,
) -> Result<(), sqlx::Error> {
    let message = format!("{phase}: {error}");
    let (failures, retry_after): (i32, DateTime<Utc>) = sqlx::query_as(
        "INSERT INTO library_listing_state(graph_id,failure_count,last_failure,last_failed_at,retry_after) VALUES($1,1,substring($2 FOR 1000),now(),now()+interval '30 seconds') ON CONFLICT(graph_id) DO UPDATE SET failure_count=library_listing_state.failure_count+1,last_failure=EXCLUDED.last_failure,last_failed_at=now(),retry_after=now()+LEAST(interval '1 hour',interval '30 seconds'*power(2,LEAST(library_listing_state.failure_count,7))) RETURNING failure_count,retry_after",
    )
    .bind(graph)
    .bind(&message)
    .fetch_one(conn)
    .await?;
    tracing::warn!(
        worker = "library_evidence",
        event = "library_listing_quarantined",
        graph = %graph,
        phase,
        failures,
        retry_after = %retry_after,
        error = %error,
        "Library listing work failed and is quarantined for this graph; node evidence is unaffected"
    );
    Ok(())
}

/// A completed listing pass lifts the quarantine; the last failure stays.
pub(super) async fn recover(conn: &mut PgConnection, graph: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE library_listing_state SET failure_count=0,retry_after=NULL WHERE graph_id=$1 AND (failure_count<>0 OR retry_after IS NOT NULL)")
        .bind(graph).execute(conn).await?;
    Ok(())
}

/// Row budgets for one refresh. The node count is one statement per
/// generation, not a walk, so it has no budget of its own.
///
/// One generation already covers every pending row of its item: `ack`
/// consumes every row of the item that committed before the generation's
/// input snapshot, whatever `start` captured, in one bounded DELETE. What
/// held the drain to about one item per tick (#1524 §3.6) was the budget: a
/// captured item logs ~35 rows (one per semantic atom) and costs ~4 outcome
/// units per atom (build, upsert, prune, staging retirement), so 32 changes
/// and 128 outcomes ran out inside the first item. Each unit is a row inside
/// a bounded set statement, not a statement, so the budgets are now the
/// largest `checked` allows and the turn is bounded by `REFRESH_DEADLINE`.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub outcomes: usize,
    pub changes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            outcomes: 2000,
            changes: 2000,
        }
    }
}

/// Wall-clock bound on one listing refresh: no new step starts after it.
/// Each step is bounded by the row budgets, so the turn ends well inside the
/// tick's 35 s timeout even after a slow node-evidence turn in the same
/// transaction. Unfinished work resumes from durable state next tick.
const REFRESH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(8);
impl Limits {
    pub(super) fn checked(self) -> Result<Self, sqlx::Error> {
        if [self.outcomes, self.changes]
            .iter()
            .any(|n| !(1..=2000).contains(n))
        {
            return Err(protocol("listing work limits must be in 1..=2000"));
        }
        Ok(self)
    }
}
#[derive(Default, Debug)]
pub(super) struct Report {
    pub outcomes: usize,
    pub changes: usize,
    pub published: usize,
    pub restarted: bool,
    pub completed: bool,
}
fn protocol(s: &str) -> sqlx::Error {
    sqlx::Error::Protocol(s.into())
}

#[derive(sqlx::FromRow)]
struct State {
    epoch: Uuid,
    computed_account_id: Option<Uuid>,
    computed_context: Option<Value>,
    sources_complete: bool,
    nodes_complete: bool,
    sources_old_complete: bool,
    backfill_after_source_item_id: Option<Uuid>,
    backfill_after_source_updated_at: Option<DateTime<Utc>>,
}
#[derive(sqlx::FromRow)]
struct Generation {
    source_item_id: Uuid,
    generation: Uuid,
    phase: String,
    epoch: Uuid,
    backfill_inventory_updated_at: Option<DateTime<Utc>>,
    guard_change_id: Option<i64>,
    input_account_id: Option<Uuid>,
    input_context: Option<Value>,
    source_present: bool,
    retained_count: i64,
    served_count: i64,
    build_after_version_captured_at: Option<DateTime<Utc>>,
    build_after_version_id: Option<Uuid>,
    build_version_id: Option<Uuid>,
    build_version_captured_at: Option<DateTime<Utc>>,
    build_after_extraction_version: Option<String>,
    build_after_ordinal: Option<i32>,
    build_version_count: i64,
    current_outcome_count: i64,
    winner_version_id: Option<Uuid>,
    winner_captured_at: Option<DateTime<Utc>>,
    winner_done: Option<bool>,
    winner_outcome_count: i64,
    upsert_after_outcome_id: Option<Uuid>,
    prune_after_outcome_id: Option<Uuid>,
}

pub(super) async fn prepare(
    conn: &mut PgConnection,
    graph: Uuid,
    account: Uuid,
    context: &Value,
    since: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO library_listing_state(graph_id) VALUES($1) ON CONFLICT DO NOTHING")
        .bind(graph)
        .execute(&mut *conn)
        .await?;
    // Keep the epoch; old heads remain until bounded replacement.
    // Each unfinished generation also carries its own immutable input context.
    // The switch generation is part of that context: capture missed while
    // the switch was off never reaches the log, so a new generation re-walks
    // every item and node (old heads first, then the raw inventory).
    sqlx::query("UPDATE library_listing_state SET ready_version=NULL,computed_account_id=$2,computed_context=$3,capture_since=$4,sources_complete=false,nodes_complete=false,aggregates_complete=false,sources_old_complete=false,backfill_after_source_item_id=NULL,backfill_after_source_updated_at=NULL WHERE graph_id=$1 AND (computed_account_id IS DISTINCT FROM $2 OR computed_context IS DISTINCT FROM $3 OR capture_since IS DISTINCT FROM $4)")
        .bind(graph).bind(account).bind(context).bind(since).execute(&mut *conn).await?;
    sqlx::query("UPDATE library_listing_state SET ready_version=NULL,nodes_complete=false WHERE graph_id=$1 AND NOT EXISTS(SELECT 1 FROM library_evidence_state e WHERE e.graph_id=$1 AND e.ready_version=$2 AND e.computed_account_id=$3 AND e.computed_context=$4)")
        .bind(graph).bind(super::READ_MODEL_VERSION).bind(account).bind(context).execute(conn).await?;
    Ok(())
}

async fn state(conn: &mut PgConnection, graph: Uuid) -> Result<State, sqlx::Error> {
    sqlx::query_as("SELECT * FROM library_listing_state WHERE graph_id=$1")
        .bind(graph)
        .fetch_one(conn)
        .await
}
async fn generation(
    conn: &mut PgConnection,
    graph: Uuid,
) -> Result<Option<Generation>, sqlx::Error> {
    sqlx::query_as("SELECT * FROM library_listing_generations WHERE graph_id=$1 AND phase NOT IN('complete','abandoned') LIMIT 1").bind(graph).fetch_optional(conn).await
}

/// Exactly one unfinished generation per graph is enforced by a partial UNIQUE
/// index, not a lock held across transactions. Other items' events remain queued.
///
/// One call carries the open generation through every phase its row budgets
/// allow and starts the next item in the same transaction, so an item costs
/// rows rather than a fixed number of polls. A detected input change still
/// ends the call with the staged input intact.
pub(super) async fn refresh(
    conn: &mut PgConnection,
    graph: Uuid,
    account: Uuid,
    context: &Value,
    limits: Limits,
) -> Result<Report, sqlx::Error> {
    sqlx::query("SELECT set_config('enable_nestloop','on',true),set_config('enable_mergejoin','on',true),set_config('enable_sort','on',true)").execute(&mut *conn).await?;
    let mut report = Report::default();
    let started = std::time::Instant::now();
    for _ in 0..MAX_STEPS {
        if started.elapsed() >= REFRESH_DEADLINE {
            break;
        }
        let remaining = Limits {
            outcomes: limits.outcomes.saturating_sub(report.outcomes),
            changes: limits.changes.saturating_sub(report.changes),
        };
        if remaining.outcomes == 0 || remaining.changes == 0 {
            break;
        }
        if !step(conn, graph, account, context, remaining, &mut report).await? {
            break;
        }
    }
    Ok(report)
}

/// Metadata-only phase transitions consume no row budget; this bounds them.
const MAX_STEPS: usize = 512;
/// Log rows one between-generation step reads to find the next item (the
/// head of the log, oldest first). The step stops at the first row naming
/// an item, so a larger change budget is no reason to read more.
const STEP_SCAN: usize = 64;

/// One phase of the open generation, or the between-generation work. Returns
/// whether durable progress was made, so the caller knows another step can.
async fn step(
    conn: &mut PgConnection,
    graph: Uuid,
    account: Uuid,
    context: &Value,
    limits: Limits,
    report: &mut Report,
) -> Result<bool, sqlx::Error> {
    if let Some(g) = generation(conn, graph).await? {
        if g.phase != "discard"
            && (g.input_account_id != Some(account)
                || g.input_context.as_ref() != Some(context)
                || changed_input(conn, graph, &g).await?)
        {
            sqlx::query("UPDATE library_listing_generations SET phase='discard' WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3").bind(graph).bind(g.source_item_id).bind(g.generation).execute(&mut *conn).await?;
            report.restarted = true;
            return Ok(false);
        }
        advance(conn, graph, &g, limits, report).await?;
        return Ok(true);
    }
    let s = state(conn, graph).await?;
    let mut worked = false;
    if !s.nodes_complete {
        worked |= count_nodes(conn, graph).await?;
    }
    if let Some(item) = next_item(conn, graph, &s).await? {
        start(conn, graph, item, &s, account, context, limits.changes).await?;
        return Ok(true);
    }
    if !s.sources_complete {
        return Ok(true);
    }
    let changes: Vec<(i64,Option<Uuid>,Option<Uuid>,Option<Uuid>)>=sqlx::query_as("SELECT id,source_item_id,source_version_id,semantic_atom_id FROM library_listing_changes WHERE graph_id=$1 ORDER BY id LIMIT $2").bind(graph).bind(limits.changes.min(STEP_SCAN) as i64).fetch_all(&mut *conn).await?;
    for (id, item, version, outcome) in changes {
        if let Some(item) = item {
            start(conn, graph, item, &s, account, context, limits.changes).await?;
            return Ok(true);
        }
        // Resolve/delete/move using retained OLD identities, not only the raw row.
        let parents:Vec<Uuid>=sqlx::query_scalar("SELECT source_item_id FROM (SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2 UNION SELECT source_item_id FROM library_listing_outcomes WHERE graph_id=$1 AND semantic_atom_id=$3 UNION SELECT source_item_id FROM library_listing_staged_outcomes WHERE graph_id=$1 AND semantic_atom_id=$3) p LIMIT 4")
            .bind(graph).bind(version).bind(outcome).fetch_all(&mut *conn).await?;
        if parents.len() > 3 {
            return Err(protocol(
                "listing event has excessive retained parent identities",
            ));
        }
        for parent in parents {
            sqlx::query("INSERT INTO library_listing_changes(graph_id,source_item_id,source_version_id,semantic_atom_id) VALUES($1,$2,$3,$4)").bind(graph).bind(parent).bind(version).bind(outcome).execute(&mut *conn).await?;
        }
        // The replacement guards and this exact obsolete-key ACK commit together.
        sqlx::query("DELETE FROM library_listing_changes WHERE graph_id=$1 AND id=$2")
            .bind(graph)
            .bind(id)
            .execute(&mut *conn)
            .await?;
        report.changes += 1;
        worked = true;
    }
    // This certifies only the listing grain. Node evidence is certified by its
    // own backfill at its own READ_MODEL_VERSION; the reader requires both
    // before it serves a Model page.
    if changes_empty(conn, graph).await? {
        let updated=sqlx::query("UPDATE library_listing_state SET ready_version=3,aggregates_complete=true WHERE graph_id=$1 AND sources_complete AND nodes_complete AND sources_old_complete AND ready_version IS NULL")
            .bind(graph).execute(&mut *conn).await?;
        if updated.rows_affected() > 0 {
            report.completed = true;
            return Ok(false);
        }
    }
    Ok(worked)
}
async fn changes_empty(conn: &mut PgConnection, graph: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM library_listing_changes WHERE graph_id=$1)")
        .bind(graph)
        .fetch_one(conn)
        .await
}

/// The baseline for the maintained node count: taken once per generation
/// from the certified node grain, in one statement under the graph lock.
/// `library_listing_node_count()` then follows every node-evidence write by
/// delta. The grain must be certified at the node definition this worker
/// writes; a row stamped with another version is a protocol breach, not
/// something to count.
async fn count_nodes(conn: &mut PgConnection, graph: Uuid) -> Result<bool, sqlx::Error> {
    let certified:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_evidence_state e JOIN library_listing_state s ON s.graph_id=e.graph_id WHERE e.graph_id=$1 AND e.ready_version=$2 AND e.computed_account_id=s.computed_account_id AND e.computed_context=s.computed_context)")
        .bind(graph).bind(super::READ_MODEL_VERSION).fetch_one(&mut *conn).await?;
    if !certified {
        return Ok(false);
    }
    let wrong:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_node_evidence WHERE graph_id=$1 AND computed_version<>$2)")
        .bind(graph).bind(super::READ_MODEL_VERSION).fetch_one(&mut *conn).await?;
    if wrong {
        return Err(protocol("node publication contains wrong-version evidence"));
    }
    sqlx::query("UPDATE library_listing_state SET node_count=(SELECT count(*) FROM library_node_evidence n WHERE n.graph_id=$1 AND n.admitted_unfiltered),nodes_complete=true WHERE graph_id=$1")
        .bind(graph).execute(conn).await?;
    Ok(true)
}
async fn next_item(
    conn: &mut PgConnection,
    graph: Uuid,
    s: &State,
) -> Result<Option<Uuid>, sqlx::Error> {
    if s.sources_complete {
        return Ok(None);
    }
    let next: Option<(Uuid, Option<DateTime<Utc>>)> = if !s.sources_old_complete {
        sqlx::query_as("SELECT source_item_id,NULL::timestamptz FROM library_listing_source_heads WHERE graph_id=$1 AND ($2::uuid IS NULL OR source_item_id>$2) ORDER BY source_item_id LIMIT 1").bind(graph).bind(s.backfill_after_source_item_id).fetch_optional(&mut *conn).await?
    } else {
        sqlx::query_as("SELECT source_item_id,updated_at FROM source_items WHERE graph_id=$1 AND ($2::timestamptz IS NULL OR (updated_at,source_item_id)<($2,$3::uuid)) ORDER BY updated_at DESC,source_item_id DESC LIMIT 1").bind(graph).bind(s.backfill_after_source_updated_at).bind(s.backfill_after_source_item_id).fetch_optional(&mut *conn).await?
    };
    if let Some((item, time)) = next {
        // Advance only after the item's publication, never at generation start.
        // Its inventory key is retained in the generation's staged header inputs.
        let _ = time;
        return Ok(Some(item));
    }
    sqlx::query(if s.sources_old_complete {"UPDATE library_listing_state SET sources_complete=true,backfill_after_source_item_id=NULL,backfill_after_source_updated_at=NULL WHERE graph_id=$1"} else {"UPDATE library_listing_state SET sources_old_complete=true,backfill_after_source_item_id=NULL WHERE graph_id=$1"}).bind(graph).execute(conn).await?;
    Ok(None)
}

async fn changed_input(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
) -> Result<bool, sqlx::Error> {
    // Both arms are indexable transaction identities, including an older xid
    // that was still in-progress at the generation's input snapshot.
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_listing_changes c JOIN library_listing_generations g ON g.graph_id=c.graph_id AND g.source_item_id=c.source_item_id AND g.generation=$3 WHERE c.graph_id=$1 AND c.source_item_id=$2 AND c.id<>g.guard_change_id AND (c.capture_xid>=pg_snapshot_xmax(g.input_snapshot) OR c.capture_xid IN(SELECT pg_snapshot_xip(g.input_snapshot))))")
        .bind(graph).bind(g.source_item_id).bind(g.generation).fetch_one(conn).await
}

async fn start(
    conn: &mut PgConnection,
    graph: Uuid,
    item: Uuid,
    s: &State,
    account: Uuid,
    context: &Value,
    change_limit: usize,
) -> Result<(), sqlx::Error> {
    let mut ids:Vec<i64>=sqlx::query_scalar("SELECT id FROM library_listing_changes WHERE graph_id=$1 AND source_item_id=$2 ORDER BY id LIMIT $3").bind(graph).bind(item).bind(change_limit as i64).fetch_all(&mut *conn).await?;
    if ids.is_empty() {
        ids.push(sqlx::query_scalar("INSERT INTO library_listing_changes(graph_id,source_item_id) VALUES($1,$2) RETURNING id").bind(graph).bind(item).fetch_one(&mut *conn).await?);
    }
    let token = Uuid::new_v4();
    // READ COMMITTED changes its snapshot between statements. Freeze the input
    // snapshot WITH the first source-header read, never in the later INSERT.
    let sql = format!(
        "{} SELECT pg_current_snapshot()::text input_snapshot,inventory.updated_at inventory_updated_at,s.*,NULL::uuid served_version_id FROM (SELECT 1) anchor LEFT JOIN listing_sources s ON true LEFT JOIN LATERAL(SELECT updated_at FROM source_items WHERE graph_id=$1 AND source_item_id=ANY($2::uuid[]) LIMIT 1) inventory ON true",
        facts::canonical_ctes(ItemInput::Worker)
    );
    let captured = sqlx::query(&sql)
        .bind(graph)
        .bind(&[item])
        .fetch_one(&mut *conn)
        .await?;
    let snapshot: String = captured.try_get("input_snapshot")?;
    let inventory: Option<DateTime<Utc>> = captured.try_get("inventory_updated_at")?;
    let header = if captured
        .try_get::<Option<Uuid>, _>("source_item_id")?
        .is_some()
    {
        Some(SourceHeader::from_row(&captured)?)
    } else {
        None
    };
    #[cfg(feature = "test-support")]
    input_pause(graph).await?;
    sqlx::query("INSERT INTO library_listing_generations(graph_id,source_item_id,generation,epoch,guard_change_id,captured_change_ids,input_account_id,input_context,source_present,backfill_inventory_updated_at,input_snapshot) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::pg_snapshot)")
        .bind(graph).bind(item).bind(token).bind(s.epoch).bind(ids[0]).bind(&ids)
        .bind(account).bind(context).bind(header.is_some()).bind(inventory).bind(snapshot)
        .execute(&mut *conn).await?;
    if let Some(h) = header {
        // Typed JSONB binding preserves SQL NULL versus Some(JSON null).
        // jsonb_populate_record would collapse both into SQL NULL.
        sqlx::query("INSERT INTO library_listing_staged_sources(graph_id,source_item_id,epoch,generation,source_type,provider_namespace,source_id,origin,discovery_content_kind,current_version_id,selected_version_id,served_version_id,captured_at,title256,unknown_independence,raw_extraction_status,raw_next_start_char,raw_extraction_reason,retained_count,served_count) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,0,0)")
            .bind(graph).bind(item).bind(s.epoch).bind(token)
            .bind(h.source_type).bind(h.provider_namespace).bind(h.source_id).bind(h.origin)
            .bind(h.discovery_content_kind).bind(h.current_version_id).bind(h.selected_version_id)
            .bind(h.served_version_id).bind(h.captured_at).bind(h.title256).bind(h.unknown_independence)
            .bind(h.raw_extraction_status).bind(h.raw_next_start_char).bind(h.raw_extraction_reason)
            .execute(&mut *conn).await?;
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct Version {
    version_id: Uuid,
    captured_at: DateTime<Utc>,
    occurred_at: Option<DateTime<Utc>>,
    current: bool,
    candidate: bool,
    done: bool,
}
async fn version(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
    next: bool,
) -> Result<Option<Version>, sqlx::Error> {
    // The historic walk uses the shipped item_history(graph,item,captured,id)
    // order. The current pointer comes from this generation's frozen header.
    sqlx::query_as("SELECT v.version_id,v.captured_at,v.occurred_at,COALESCE(v.version_id=h.current_version_id,false) current,COALESCE(v.version_id<>c.version_id AND v.content_sha256=c.content_sha256 AND v.representation=c.representation,false) candidate,v.extraction_status='done' done FROM source_versions v JOIN library_listing_staged_sources h ON h.graph_id=v.graph_id AND h.source_item_id=v.source_item_id AND h.generation=$3 LEFT JOIN source_versions c ON c.graph_id=h.graph_id AND c.source_item_id=h.source_item_id AND c.version_id=h.current_version_id WHERE v.graph_id=$1 AND v.source_item_id=$2 AND (CASE WHEN $4 THEN $5::timestamptz IS NULL OR (v.captured_at,v.version_id)<($5,$6::uuid) ELSE v.version_id=$7::uuid END) ORDER BY v.captured_at DESC,v.version_id DESC LIMIT 1")
        .bind(graph).bind(g.source_item_id).bind(g.generation).bind(next)
        .bind(g.build_after_version_captured_at).bind(g.build_after_version_id).bind(g.build_version_id).fetch_optional(conn).await
}

/// Actual indexed semantic build probe. Exposed only for physical proof; this
/// does not expose a staging/publication/ACK lifecycle to callers.
pub(super) const BUILD_SQL:&str="SELECT semantic_atom_id,extraction_version,ordinal FROM semantic_atoms WHERE graph_id=$1 AND source_version_id=$2 AND ($3::text IS NULL OR (extraction_version,ordinal)>($3,$4::integer)) ORDER BY extraction_version,ordinal LIMIT $5";

async fn build(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
    limit: usize,
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    if !g.source_present {
        return finish_build(conn, graph, g).await;
    }
    let active = g.build_version_id.is_some();
    let Some(v) = version(conn, graph, g, !active).await? else {
        if active {
            return discard(conn, graph, g, limit, report).await;
        }
        return finish_build(conn, graph, g).await;
    };
    let mut rows: Vec<(Uuid, String, i32)> = sqlx::query_as(BUILD_SQL)
        .bind(graph)
        .bind(v.version_id)
        .bind(if active {
            g.build_after_extraction_version.as_deref()
        } else {
            None
        })
        .bind(if active { g.build_after_ordinal } else { None })
        .bind(limit as i64 + 1)
        .fetch_all(&mut *conn)
        .await?;
    let more = rows.len() > limit;
    rows.truncate(limit);
    let ids = rows.iter().map(|r| r.0).collect::<Vec<_>>();
    if !ids.is_empty() {
        // Membership/order only. The final served winner is applied during the
        // bounded active upsert, not an all-item retag of the staging relation.
        sqlx::query("INSERT INTO library_listing_staged_outcomes(graph_id,source_item_id,generation,semantic_atom_id,source_version_id,occurred_at,captured_at,source_version_is_current) SELECT $1,$2,$3,id,$4,$5,$6,false FROM unnest($7::uuid[]) id")
            .bind(graph).bind(g.source_item_id).bind(g.generation).bind(v.version_id).bind(v.occurred_at).bind(v.captured_at).bind(&ids).execute(&mut *conn).await?;
    }
    report.outcomes += ids.len();
    let count = (if active { g.build_version_count } else { 0 }) + ids.len() as i64;
    if more {
        let last = rows.last().unwrap();
        sqlx::query("UPDATE library_listing_generations SET build_version_id=$4,build_version_captured_at=$5,build_after_extraction_version=$6,build_after_ordinal=$7,build_version_count=$8,retained_count=retained_count+$9 WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3")
            .bind(graph).bind(g.source_item_id).bind(g.generation).bind(v.version_id).bind(v.captured_at).bind(&last.1).bind(last.2).bind(count).bind(ids.len() as i64).execute(conn).await?;
    } else {
        let better = v.candidate
            && count > 0
            && match (g.winner_done, g.winner_captured_at, g.winner_version_id) {
                (Some(done), Some(capture), Some(id)) => {
                    (v.done, v.captured_at, v.version_id) > (done, capture, id)
                }
                _ => true,
            };
        sqlx::query("UPDATE library_listing_generations SET build_after_version_id=$4,build_after_version_captured_at=$5,build_version_id=NULL,build_version_captured_at=NULL,build_after_extraction_version=NULL,build_after_ordinal=NULL,build_version_count=0,retained_count=retained_count+$6,current_outcome_count=CASE WHEN $7 THEN $8 ELSE current_outcome_count END,winner_version_id=CASE WHEN $9 THEN $4 ELSE winner_version_id END,winner_captured_at=CASE WHEN $9 THEN $5 ELSE winner_captured_at END,winner_done=CASE WHEN $9 THEN $10 ELSE winner_done END,winner_outcome_count=CASE WHEN $9 THEN $8 ELSE winner_outcome_count END WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3")
            .bind(graph).bind(g.source_item_id).bind(g.generation).bind(v.version_id).bind(v.captured_at).bind(ids.len() as i64).bind(v.current).bind(count).bind(better).bind(v.done).execute(conn).await?;
    }
    Ok(())
}
async fn finish_build(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
) -> Result<(), sqlx::Error> {
    let header:Option<SourceHeader>=sqlx::query_as("SELECT * FROM library_listing_staged_sources WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3")
        .bind(graph).bind(g.source_item_id).bind(g.generation).fetch_optional(&mut *conn).await?;
    let (served, count) = match header {
        None => (None, 0),
        Some(h) => {
            let carry =
                h.raw_extraction_status.as_deref() != Some("done") || g.current_outcome_count == 0;
            if carry && g.winner_version_id.is_some() {
                (g.winner_version_id, g.winner_outcome_count)
            } else {
                (h.current_version_id, g.current_outcome_count)
            }
        }
    };
    sqlx::query("UPDATE library_listing_staged_sources SET served_version_id=$4,retained_count=$5,served_count=$6 WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3")
        .bind(graph).bind(g.source_item_id).bind(g.generation).bind(served).bind(g.retained_count).bind(count).execute(&mut *conn).await?;
    sqlx::query("UPDATE library_listing_generations SET phase='upsert',build_complete=true,served_count=$4 WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3")
        .bind(graph).bind(g.source_item_id).bind(g.generation).bind(count).execute(conn).await?;
    Ok(())
}
async fn advance(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
    limits: Limits,
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    match g.phase.as_str() {
        "build" => build(conn, graph, g, limits.outcomes, report).await?,
        "upsert" | "prune" => {
            let is_upsert = g.phase == "upsert";
            let sql = if is_upsert {
                facts::RECONCILE_UPSERT_SQL
            } else {
                facts::RECONCILE_PRUNE_SQL
            };
            let row: (i64, i64, Option<Uuid>, bool) = sqlx::query_as(sql)
                .bind(graph)
                .bind(g.source_item_id)
                .bind(g.generation)
                .bind(if is_upsert {
                    g.upsert_after_outcome_id
                } else {
                    g.prune_after_outcome_id
                })
                .bind(limits.outcomes as i64)
                .fetch_one(&mut *conn)
                .await?;
            report.outcomes += row.0 as usize;
            if row.3 {
                sqlx::query(if is_upsert {"UPDATE library_listing_generations SET upsert_after_outcome_id=$4 WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3"} else {"UPDATE library_listing_generations SET prune_after_outcome_id=$4 WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3"})
                    .bind(graph).bind(g.source_item_id).bind(g.generation).bind(row.2).execute(conn).await?;
            } else {
                sqlx::query(if is_upsert {"UPDATE library_listing_generations SET phase='prune',upsert_complete=true WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3"} else {"UPDATE library_listing_generations SET phase='ack',prune_complete=true WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3"})
                    .bind(graph).bind(g.source_item_id).bind(g.generation).execute(conn).await?;
            }
        }
        "ack" => ack(conn, graph, g, limits, report).await?,
        "publish" => publish(conn, graph, g, report).await?,
        "discard" => discard(conn, graph, g, limits.outcomes, report).await?,
        _ => return Err(protocol("unknown listing generation phase")),
    }
    Ok(())
}
async fn discard(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
    limit: usize,
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    if g.phase != "discard" {
        sqlx::query("UPDATE library_listing_generations SET phase='discard' WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3").bind(graph).bind(g.source_item_id).bind(g.generation).execute(&mut *conn).await?;
    }
    let removed:i64=sqlx::query_scalar("WITH keys AS MATERIALIZED(SELECT semantic_atom_id FROM library_listing_staged_outcomes WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3 ORDER BY semantic_atom_id LIMIT $4), gone AS(DELETE FROM library_listing_staged_outcomes s USING keys k WHERE s.graph_id=$1 AND s.source_item_id=$2 AND s.generation=$3 AND s.semantic_atom_id=k.semantic_atom_id RETURNING 1) SELECT count(*) FROM gone")
        .bind(graph).bind(g.source_item_id).bind(g.generation).bind(limit as i64).fetch_one(&mut *conn).await?;
    report.outcomes += removed as usize;
    report.restarted = true;
    if removed == 0 {
        sqlx::query("DELETE FROM library_listing_staged_sources WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3").bind(graph).bind(g.source_item_id).bind(g.generation).execute(&mut *conn).await?;
        // No selected or subsequent source events are ACKed on restart.
        sqlx::query("UPDATE library_listing_generations SET phase='abandoned',guard_change_id=NULL WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3").bind(graph).bind(g.source_item_id).bind(g.generation).execute(conn).await?;
    }
    Ok(())
}

async fn ack(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
    limits: Limits,
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    // Retire staging in capped chunks before the final metadata transaction.
    // Active rows already hold the complete replacement; the item guard remains.
    let removed:i64=sqlx::query_scalar("WITH keys AS MATERIALIZED(SELECT semantic_atom_id FROM library_listing_staged_outcomes WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3 ORDER BY semantic_atom_id LIMIT $4), gone AS(DELETE FROM library_listing_staged_outcomes s USING keys k WHERE s.graph_id=$1 AND s.source_item_id=$2 AND s.generation=$3 AND s.semantic_atom_id=k.semantic_atom_id RETURNING 1) SELECT count(*) FROM gone")
        .bind(graph).bind(g.source_item_id).bind(g.generation).bind(limits.outcomes as i64).fetch_one(&mut *conn).await?;
    report.outcomes += removed as usize;
    if removed > 0 {
        return Ok(());
    }
    let ids:Vec<i64>=sqlx::query_scalar("SELECT c.id FROM library_listing_changes c JOIN library_listing_generations g ON g.graph_id=c.graph_id AND g.source_item_id=c.source_item_id AND g.generation=$3 WHERE c.graph_id=$1 AND c.source_item_id=$2 AND c.id<>g.guard_change_id AND NOT(c.capture_xid>=pg_snapshot_xmax(g.input_snapshot) OR c.capture_xid IN(SELECT pg_snapshot_xip(g.input_snapshot))) ORDER BY c.id LIMIT $4")
        .bind(graph).bind(g.source_item_id).bind(g.generation).bind(limits.changes as i64).fetch_all(&mut *conn).await?;
    if !ids.is_empty() {
        // These exact initial-snapshot IDs were incorporated. New/late events
        // cannot enter this set even if they commit between our statements.
        sqlx::query("DELETE FROM library_listing_changes WHERE graph_id=$1 AND source_item_id=$2 AND id=ANY($3)").bind(graph).bind(g.source_item_id).bind(&ids).execute(conn).await?;
        report.changes += ids.len();
    } else {
        sqlx::query("UPDATE library_listing_generations SET phase='publish' WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3").bind(graph).bind(g.source_item_id).bind(g.generation).execute(conn).await?;
    }
    Ok(())
}
async fn publish(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    // The published head is the item's whole listing contribution; totals and
    // facets are read-time aggregates over heads. No item-wide row operation
    // is hidden here: active reconciliation/stage retirement ended.
    sqlx::query("DELETE FROM library_listing_source_heads WHERE graph_id=$1 AND source_item_id=$2")
        .bind(graph)
        .bind(g.source_item_id)
        .execute(&mut *conn)
        .await?;
    sqlx::query("INSERT INTO library_listing_source_heads SELECT * FROM library_listing_staged_sources WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3").bind(graph).bind(g.source_item_id).bind(g.generation).execute(&mut *conn).await?;
    sqlx::query("DELETE FROM library_listing_staged_sources WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3").bind(graph).bind(g.source_item_id).bind(g.generation).execute(&mut *conn).await?;
    // Only the retained guard remains from the initial snapshot. All later or
    // lower-ID late commits remain queued and independently protect dirty reads.
    sqlx::query("UPDATE library_listing_generations SET phase='complete',guard_change_id=NULL WHERE graph_id=$1 AND source_item_id=$2 AND generation=$3").bind(graph).bind(g.source_item_id).bind(g.generation).execute(&mut *conn).await?;
    sqlx::query(
        "DELETE FROM library_listing_changes WHERE graph_id=$1 AND source_item_id=$2 AND id=$3",
    )
    .bind(graph)
    .bind(g.source_item_id)
    .bind(
        g.guard_change_id
            .ok_or_else(|| protocol("publication lost guard"))?,
    )
    .execute(&mut *conn)
    .await?;
    report.changes += 1;
    report.published += 1;
    advance_inventory(conn, graph, g).await?;
    Ok(())
}

async fn advance_inventory(
    conn: &mut PgConnection,
    graph: Uuid,
    g: &Generation,
) -> Result<(), sqlx::Error> {
    let s = state(conn, graph).await?;
    if !s.sources_complete {
        if !s.sources_old_complete {
            sqlx::query("UPDATE library_listing_state SET backfill_after_source_item_id=$2 WHERE graph_id=$1").bind(graph).bind(g.source_item_id).execute(conn).await?;
        } else if let Some(time) = g.backfill_inventory_updated_at {
            sqlx::query("UPDATE library_listing_state SET backfill_after_source_item_id=$2,backfill_after_source_updated_at=$3 WHERE graph_id=$1").bind(graph).bind(g.source_item_id).bind(time).execute(conn).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::legacy_setting_warning;
    #[test]
    fn the_legacy_env_setting_is_ignored_with_a_warning_when_set() {
        assert_eq!(legacy_setting_warning(None), None);
        assert_eq!(legacy_setting_warning(Some("")), None);
        assert_eq!(legacy_setting_warning(Some("  ")), None);
        for value in ["on", "off", "shadow"] {
            let warning = legacy_setting_warning(Some(value)).expect("a set value warns");
            assert!(warning.contains("ignored") && warning.contains("library_listing_switch"), "{warning}");
        }
    }
}

// A keyed, one-use synchronization point for real concurrency proofs. It never
// fabricates a result or exposes any publish/ACK lifecycle to a caller.
#[cfg(feature = "test-support")]
type InputPause = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);
#[cfg(feature = "test-support")]
static INPUT_PAUSES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<Uuid, InputPause>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));
#[cfg(feature = "test-support")]
pub(super) fn pause_input(
    graph: Uuid,
) -> Result<
    (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ),
    &'static str,
> {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let mut pauses = INPUT_PAUSES
        .lock()
        .map_err(|_| "listing input pause poisoned")?;
    if pauses.contains_key(&graph) {
        return Err("listing input pause already registered");
    }
    pauses.insert(graph, (reached_tx, resume_rx));
    Ok((reached_rx, resume_tx))
}
#[cfg(feature = "test-support")]
async fn input_pause(graph: Uuid) -> Result<(), sqlx::Error> {
    let pause = INPUT_PAUSES
        .lock()
        .map_err(|_| protocol("listing input pause poisoned"))?
        .remove(&graph);
    if let Some((reached, resume)) = pause {
        reached
            .send(())
            .map_err(|_| protocol("listing input observer disappeared"))?;
        resume
            .await
            .map_err(|_| protocol("listing input pause cancelled"))?;
    }
    Ok(())
}

//! Core of the one-shot `purge_desktop_data` (ADR 0015: the Tauri desktop
//! app is retired and its "work session" captures are purged).
//!
//! A work session is a `coding_sessions` row with `session_kind='work'`,
//! its `source_items`/`atoms` with `source_type='work_session'`, and the
//! sync history (`op_log`, `entity='coding_session'`) of those sessions.
//! Coding and meeting sessions are NOT touched.
//!
//! The purge reuses the platform's own source deletion instead of deleting
//! leaf tables: [`deletion::delete_source_item_in_graph`] (the cascade
//! behind `DELETE /v1/sources/{id}`) per work source item, then
//! [`deletion::delete_source`] for work atoms that no longer have a source
//! item. Both delete the atoms, the source item (whose triggers queue the
//! purge dependencies the worker drains), every reef left with no evidence,
//! queue `DeleteSourceBlob` jobs, and write the retraction audit as the
//! `System` actor. This module adds only what those paths do not cover:
//!
//! * `atoms.superseded_blob_keys` — R2 objects a publication refresh
//!   replaced. Deleting the atom loses the record, so they are queued first.
//! * `coding_sessions` rows with no atom left (the cascade only deletes a
//!   session when it removes the session's LAST atom), and the sessions'
//!   `op_log` history, which no deletion path removes. `session_kind` is
//!   fixed at a session's first upsert, so a 'work' session can also hold
//!   coding/agent atoms under its `cc:{session_id}:` prefix. Such a session
//!   is kept, with its history, and reported as `sessions_kept`.
//!
//! **Accepted collateral (founder decision, PR #1594 review).** Deleting a
//! source item fires `source_purge_delete` -> `library_capture_item_purge`
//! -> `library_capture_purge`, which runs `invalidate_derived_read_surfaces`
//! on the item's whole graph (control 0294/0319, tenant 0129/0151; the
//! drain repeats it per atom dependency). That blanks conversation titles,
//! overwrites assistant message text, nulls every `queries.answer` of the
//! graph's ACCOUNT, deletes `query_history` and `journal_rollups`, fails
//! `mcp_tasks`, clears the tenant-wide `extraction_cache`, and deletes
//! query/conversation `op_log` rows. The purge keeps that path and makes the
//! collateral visible: [`survey`] counts it ([`Collateral`]) and
//! [`affected_graphs`] lists the graphs and accounts, so the dry run the
//! founder approves states what will be invalidated.
//!
//! Every function runs PLAIN (unscoped) SQL across every ocean in one
//! database, so the connection must bypass RLS — [`assert_policy_bypass`]
//! proves that before any work. Re-running is safe: every step selects only
//! what is still present, so a run after a clean one finds nothing.

use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::{queue::enqueue_tx, JobKind};
use crate::services::connector_audit::Actor;
use crate::services::deletion;
use crate::store::CLOUD_DEVICE_ID;

pub const WORK_SOURCE_TYPE: &str = "work_session";
pub const WORK_SESSION_KIND: &str = "work";

/// `LIKE` pattern for the atoms of coding session `s`: the SQL form of
/// `store::session_source_id_prefix` (`cc:{escaped session_id}:%`), as used by
/// `deletion::delete_orphaned_coding_session` to decide a session is empty.
const SESSION_ATOMS: &str =
    r"('cc:'||replace(replace(replace(s.session_id,'\','\\'),'%','\%'),'_','\_')||':%')";

#[derive(Debug, thiserror::Error)]
pub enum PurgeError {
    #[error("{0}")]
    Preflight(String),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

#[derive(Clone, Copy, Debug)]
pub struct PurgeOpts {
    pub execute: bool,
    /// Narrow to one ocean. A rehearsal/test aid, never a complete run.
    pub only_graph: Option<Uuid>,
    /// Source items (or leftover sources, or sessions) per batch. Each
    /// source is still its own transaction; this only bounds each read.
    pub batch: i64,
}

impl PurgeOpts {
    pub fn mode(&self) -> &'static str {
        if self.execute { "execute" } else { "dry-run" }
    }
}

/// What is present in one database. A dry run reports this; an execute run
/// reports it before and after (the after survey must have nothing
/// [`Survey::remaining`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Survey {
    pub source_items: i64,
    pub atoms: i64,
    /// Work sessions the purge deletes: every atom under their
    /// `cc:{session_id}:` prefix is a work-session atom (or there is none).
    pub coding_sessions: i64,
    /// Work sessions that also hold non-work atoms. Kept, not deleted.
    pub sessions_kept: i64,
    /// `op_log` history of the sessions the purge deletes.
    pub session_op_log: i64,
    /// Reefs whose every piece of evidence is work-session evidence. The
    /// cascade deletes them; reefs with any other evidence stay.
    pub reefs_to_delete: i64,
    /// Distinct R2 keys the purge queues for deletion.
    pub blob_keys: i64,
    /// Distinct keys that fail [`queueable_blob_key`]: malformed, or another
    /// graph's object. Never queued, so their objects stay in R2.
    pub blob_keys_skipped: i64,
    pub collateral: Collateral,
}

/// Derived read surfaces the platform deletion invalidates in the graphs
/// (and accounts) that hold work-session data. See the module doc.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Collateral {
    /// Graphs holding work-session source items or atoms. An upper bound: a
    /// graph whose only work data is source-less atoms gets the narrower
    /// receipt-scoped invalidation, not the whole-graph one.
    pub graphs: i64,
    /// Accounts owning those graphs.
    pub accounts: i64,
    /// Non-blank `conversations.title` set to '' (graph-scoped).
    pub conversations: i64,
    /// Assistant `conversation_messages` whose text is overwritten.
    pub assistant_messages: i64,
    /// `queries.answer` set to NULL. ACCOUNT-scoped: every answered query of
    /// an owning account, not only the graph's.
    pub query_answers: i64,
    pub query_history: i64,
    pub journal_rollups: i64,
    /// `mcp_tasks` marked failed.
    pub mcp_tasks: i64,
    /// `extraction_cache` rows deleted. TENANT-wide: the owning accounts'
    /// rows plus every org row (the nil-account sentinel).
    pub extraction_cache: i64,
    /// `op_log` rows of queries, conversations and messages deleted.
    pub op_log: i64,
}

impl Collateral {
    fn add(&mut self, o: &Collateral) {
        self.graphs += o.graphs;
        self.accounts += o.accounts;
        self.conversations += o.conversations;
        self.assistant_messages += o.assistant_messages;
        self.query_answers += o.query_answers;
        self.query_history += o.query_history;
        self.journal_rollups += o.journal_rollups;
        self.mcp_tasks += o.mcp_tasks;
        self.extraction_cache += o.extraction_cache;
        self.op_log += o.op_log;
    }
}

impl Survey {
    /// Work-session data still present. Kept sessions, skipped keys and the
    /// invalidation counts are reported, not purged, so they do not count.
    pub fn remaining(&self) -> i64 {
        self.source_items
            + self.atoms
            + self.coding_sessions
            + self.session_op_log
            + self.reefs_to_delete
            + self.blob_keys
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn add(&mut self, o: &Survey) {
        self.source_items += o.source_items;
        self.atoms += o.atoms;
        self.coding_sessions += o.coding_sessions;
        self.sessions_kept += o.sessions_kept;
        self.session_op_log += o.session_op_log;
        self.reefs_to_delete += o.reefs_to_delete;
        self.blob_keys += o.blob_keys;
        self.blob_keys_skipped += o.blob_keys_skipped;
        self.collateral.add(&o.collateral);
    }
}

/// What an execute run did.
#[derive(Clone, Copy, Debug, Default)]
pub struct Purged {
    pub source_items: i64,
    pub leftover_sources: i64,
    pub atoms: i64,
    pub reefs: i64,
    pub coding_sessions: i64,
    pub session_op_log: i64,
    pub superseded_blobs_queued: i64,
    pub superseded_blobs_skipped: i64,
    /// Per-source failures. The run continues past each one.
    pub errors: i64,
}

impl Purged {
    fn add(&mut self, o: &Purged) {
        self.source_items += o.source_items;
        self.leftover_sources += o.leftover_sources;
        self.atoms += o.atoms;
        self.reefs += o.reefs;
        self.coding_sessions += o.coding_sessions;
        self.session_op_log += o.session_op_log;
        self.superseded_blobs_queued += o.superseded_blobs_queued;
        self.superseded_blobs_skipped += o.superseded_blobs_skipped;
        self.errors += o.errors;
    }
}

/// Asynchronous work the source-blob-cleanup worker still has to do in this
/// database. Database-wide, not only this purge's share.
#[derive(Clone, Copy, Debug, Default)]
pub struct Pending {
    pub purge_dependencies: i64,
    pub blob_jobs: i64,
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub before: Survey,
    /// `(graph_id, account_id)` of the graphs [`Collateral`] counts, before
    /// any purge. Per database: [`Report::add`] does not merge it.
    pub affected: Vec<(Uuid, Uuid)>,
    pub purged: Purged,
    pub after: Survey,
    pub pending: Pending,
}

impl Report {
    /// Clean means no per-source error and, after an execute run, nothing left.
    pub fn clean(&self, opts: &PurgeOpts) -> bool {
        self.purged.errors == 0 && (!opts.execute || self.after.is_empty())
    }

    pub fn add(&mut self, o: &Report) {
        self.before.add(&o.before);
        self.purged.add(&o.purged);
        self.after.add(&o.after);
        self.pending.purge_dependencies += o.pending.purge_dependencies;
        self.pending.blob_jobs += o.pending.blob_jobs;
    }
}

/// Refuse unless this connection bypasses RLS on `atoms`: the purge reads
/// and deletes across every account in the database.
pub async fn assert_policy_bypass(pool: &PgPool) -> Result<(), PurgeError> {
    let (bypasses, is_owner, forced): (bool, bool, bool) = sqlx::query_as(
        "SELECT \
            (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
            c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user), \
            c.relforcerowsecurity \
         FROM pg_class c WHERE c.oid = 'atoms'::regclass",
    )
    .fetch_one(pool)
    .await?;
    if bypasses || (is_owner && !forced) {
        return Ok(());
    }
    Err(PurgeError::Preflight(format!(
        "REFUSING: this role does not bypass row-level security on `atoms` (superuser/BYPASSRLS \
         = {bypasses}, owns the table = {is_owner}, FORCE ROW LEVEL SECURITY = {forced}). Under \
         RLS the purge would see no rows and report a false zero. Use MIGRATOR_DATABASE_URL \
         (control plane) or the tenant owner DSN."
    )))
}

/// The org segment (`None` for a legacy personal key) of a blob key this
/// purge may queue for deletion in `graph_id`, or `None` when it must skip
/// the key. The rule of `deletion::queue_blob_cleanup_tx`: the key passes
/// `validate_blob_key` and its graph segment is `graph_id`. A legacy atom
/// copy may point at another graph's object, which is not this purge's to
/// delete. The survey and [`queue_superseded_blobs`] both use this.
pub fn queueable_blob_key(graph_id: Uuid, key: &str) -> Option<Option<String>> {
    coralswarm_core::r2::validate_blob_key(key).ok()?;
    let graph = graph_id.to_string();
    match key.split('/').collect::<Vec<_>>().as_slice() {
        [g, _] if *g == graph => Some(None),
        [org, g, _] if *g == graph => Some(Some((*org).to_string())),
        _ => None,
    }
}

/// Count what a purge of this database would remove, and the read surfaces
/// it would invalidate. Read-only.
pub async fn survey(pool: &PgPool, only_graph: Option<Uuid>) -> Result<Survey, PurgeError> {
    let row: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(&survey_sql())
        .bind(only_graph)
        .bind(WORK_SOURCE_TYPE)
        .bind(WORK_SESSION_KIND)
        .fetch_one(pool)
        .await?;
    let keys: Vec<(Uuid, String)> = sqlx::query_as(BLOB_KEYS_SQL)
        .bind(only_graph)
        .bind(WORK_SOURCE_TYPE)
        .fetch_all(pool)
        .await?;
    let queueable = keys.iter().filter(|(g, k)| queueable_blob_key(*g, k).is_some()).count() as i64;
    let c: (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(COLLATERAL_SQL)
        .bind(only_graph)
        .bind(WORK_SOURCE_TYPE)
        .fetch_one(pool)
        .await?;
    Ok(Survey {
        source_items: row.0,
        atoms: row.1,
        coding_sessions: row.2,
        sessions_kept: row.3,
        session_op_log: row.4,
        reefs_to_delete: row.5,
        blob_keys: queueable,
        blob_keys_skipped: keys.len() as i64 - queueable,
        collateral: Collateral {
            graphs: c.0,
            accounts: c.1,
            conversations: c.2,
            assistant_messages: c.3,
            query_answers: c.4,
            query_history: c.5,
            journal_rollups: c.6,
            mcp_tasks: c.7,
            extraction_cache: c.8,
            op_log: c.9,
        },
    })
}

/// The graphs [`Collateral`] counts, with their accounts. Read-only.
pub async fn affected_graphs(pool: &PgPool, only_graph: Option<Uuid>) -> Result<Vec<(Uuid, Uuid)>, PurgeError> {
    Ok(sqlx::query_as(
        "SELECT g.id, g.account_id FROM graphs g WHERE g.id IN ( \
           SELECT graph_id FROM source_items WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1) \
           UNION SELECT graph_id FROM atoms WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1)) \
         ORDER BY g.account_id, g.id",
    )
    .bind(only_graph)
    .bind(WORK_SOURCE_TYPE)
    .fetch_all(pool)
    .await?)
}

fn survey_sql() -> String {
    format!(
        r"
WITH work_atoms AS (
  SELECT atom_id, graph_id FROM atoms
  WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1)
), work_items AS (
  SELECT source_item_id, graph_id FROM source_items
  WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1)
), work_semantic AS (
  SELECT sa.semantic_atom_id FROM semantic_atoms sa
  JOIN source_versions v ON v.version_id = sa.source_version_id AND v.graph_id = sa.graph_id
  JOIN work_items i ON i.source_item_id = v.source_item_id AND i.graph_id = v.graph_id
), work_sessions AS (
  SELECT s.id, s.graph_id, s.session_id,
    EXISTS(SELECT 1 FROM atoms a WHERE a.graph_id = s.graph_id AND a.source_type <> $2
           AND a.source_id LIKE {SESSION_ATOMS}) AS kept
  FROM coding_sessions s
  WHERE s.session_kind = $3 AND ($1::uuid IS NULL OR s.graph_id = $1)
), candidate_reefs AS (
  SELECT l.node_id FROM node_atoms l JOIN work_atoms a ON a.atom_id = l.atom_id
  UNION
  SELECT l.node_id FROM node_semantic_atoms l JOIN work_semantic s ON s.semantic_atom_id = l.semantic_atom_id
)
SELECT
  (SELECT count(*) FROM work_items),
  (SELECT count(*) FROM work_atoms),
  (SELECT count(*) FROM work_sessions WHERE NOT kept),
  (SELECT count(*) FROM work_sessions WHERE kept),
  (SELECT count(*) FROM op_log o JOIN work_sessions s ON o.graph_id = s.graph_id AND NOT s.kept
     AND o.entity = 'coding_session' AND o.entity_id IN (s.id::text, s.session_id)),
  (SELECT count(*) FROM candidate_reefs c
     WHERE NOT EXISTS (SELECT 1 FROM node_atoms l JOIN atoms a ON a.atom_id = l.atom_id
                       WHERE l.node_id = c.node_id AND a.source_type <> $2)
       AND NOT EXISTS (SELECT 1 FROM node_semantic_atoms l
                       JOIN semantic_atoms sa ON sa.semantic_atom_id = l.semantic_atom_id AND sa.graph_id = l.graph_id
                       JOIN source_versions v ON v.version_id = sa.source_version_id AND v.graph_id = sa.graph_id
                       JOIN source_items i ON i.source_item_id = v.source_item_id AND i.graph_id = v.graph_id
                       WHERE l.node_id = c.node_id AND i.source_type <> $2))
"
    )
}

/// Every distinct `(graph, key)` the purge would delete: work atoms' content
/// and superseded keys, and work source versions' raw keys. The survey
/// classifies them with [`queueable_blob_key`].
const BLOB_KEYS_SQL: &str = r"
WITH work_atoms AS (
  SELECT graph_id, content_blob_key, superseded_blob_keys FROM atoms
  WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1)
), keys AS (
  SELECT graph_id, content_blob_key AS key FROM work_atoms
  UNION SELECT graph_id, unnest(superseded_blob_keys) FROM work_atoms
  UNION SELECT v.graph_id, v.raw_blob_key FROM source_versions v
    JOIN source_items i ON i.source_item_id = v.source_item_id AND i.graph_id = v.graph_id
    WHERE i.source_type = $2 AND ($1::uuid IS NULL OR i.graph_id = $1)
)
SELECT DISTINCT graph_id, key FROM keys WHERE key IS NOT NULL AND key <> '' ORDER BY graph_id, key
";

/// What `invalidate_derived_read_surfaces` and `library_capture_purge`
/// (control 0294/0319, tenant 0129/0151) change for the graphs holding work
/// data, plus what answer receipts citing work items or atoms change
/// anywhere. Counts only rows the purge would actually change.
const COLLATERAL_SQL: &str = r"
WITH work_atoms AS (
  SELECT atom_id, graph_id FROM atoms WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1)
), work_items AS (
  SELECT source_item_id, graph_id FROM source_items WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1)
), tg AS (
  SELECT graph_id FROM work_items UNION SELECT graph_id FROM work_atoms
), ta AS (
  SELECT DISTINCT g.account_id FROM graphs g JOIN tg ON tg.graph_id = g.id
), tg_text AS (
  SELECT COALESCE(array_agg(graph_id::text), '{}') AS ids FROM tg
), receipts AS (
  SELECT r.receipt_id FROM answer_support_receipts r
  WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(r.support) e WHERE
    (e->>'kind' = 'source_passage' AND EXISTS (SELECT 1 FROM work_items i
       WHERE i.graph_id::text = e->>'graph_id' AND i.source_item_id::text = e->>'source_item_id'))
    OR (e->>'kind' = 'legacy_chunk' AND EXISTS (SELECT 1 FROM work_atoms a
       WHERE a.graph_id::text = e->>'graph_id' AND a.atom_id::text = e->>'atom_id')))
)
SELECT
  (SELECT count(*) FROM tg),
  (SELECT count(*) FROM ta),
  (SELECT count(*) FROM conversations c WHERE c.title <> '' AND (
     c.graph_id IN (SELECT graph_id FROM tg)
     OR EXISTS (SELECT 1 FROM conversation_oceans o JOIN tg ON tg.graph_id = o.graph_id WHERE o.conversation_id = c.id)
     OR c.id IN (SELECT receipt_id FROM receipts)
     OR c.id IN (SELECT m.conversation_id FROM conversation_messages m JOIN receipts r ON r.receipt_id = m.id))),
  (SELECT count(*) FROM conversation_messages m
     WHERE m.role = 'assistant' AND m.text <> '[Evidence no longer available]' AND (
       m.scope_graph_ids ?| (SELECT ids FROM tg_text)
       OR EXISTS (SELECT 1 FROM conversations c JOIN tg ON tg.graph_id = c.graph_id WHERE c.id = m.conversation_id)
       OR m.id IN (SELECT receipt_id FROM receipts))),
  (SELECT count(*) FROM queries q WHERE q.answer IS NOT NULL AND q.account_id IN (SELECT account_id FROM ta)),
  (SELECT count(*) FROM query_history h
     WHERE h.graph_id IN (SELECT graph_id FROM tg) OR h.id IN (SELECT receipt_id FROM receipts)),
  (SELECT count(*) FROM journal_rollups j WHERE j.graph_id IN (SELECT graph_id FROM tg)),
  (SELECT count(*) FROM mcp_tasks t WHERE t.status <> 'failed' AND (
     t.graph_id IN (SELECT graph_id FROM tg)
     OR (t.answer_receipt_id IS NULL AND t.status = 'completed' AND t.account_id IN (SELECT account_id FROM ta))
     OR t.answer_receipt_id IN (SELECT receipt_id FROM receipts))),
  (SELECT count(*) FROM extraction_cache x WHERE EXISTS (SELECT 1 FROM tg)
     AND (x.account_id IN (SELECT account_id FROM ta) OR x.account_id = '00000000-0000-0000-0000-000000000000')),
  (SELECT count(*) FROM op_log o WHERE o.graph_id IN (SELECT graph_id FROM tg)
     AND o.entity IN ('query', 'conversation', 'conversation_message'))
";

/// Survey, and with `opts.execute` purge, one database. `control` receives
/// the retraction audit (`retraction_audit_log` lives on the control plane).
pub async fn purge_database(
    pool: &PgPool,
    control: Option<&PgPool>,
    opts: &PurgeOpts,
) -> Result<Report, PurgeError> {
    assert_policy_bypass(pool).await?;
    let before = survey(pool, opts.only_graph).await?;
    let affected = affected_graphs(pool, opts.only_graph).await?;
    let mut report = Report { before, affected, after: before, ..Report::default() };
    if opts.execute {
        // Sync history first: the cascade deletes a session row with its last
        // atom, after which nothing links its op_log rows to a work session.
        report.purged.session_op_log = delete_session_op_log(pool, opts).await?;
        let (queued, skipped) = queue_superseded_blobs(pool, opts).await?;
        report.purged.superseded_blobs_queued = queued;
        report.purged.superseded_blobs_skipped = skipped;
        purge_source_items(pool, control, opts, &mut report.purged).await?;
        purge_leftover_atoms(pool, control, opts, &mut report.purged).await?;
        report.purged.coding_sessions = delete_work_sessions(pool, opts).await?;
        report.after = survey(pool, opts.only_graph).await?;
    }
    report.pending = pending(pool).await?;
    Ok(report)
}

/// History of the work sessions the purge will delete; kept sessions (with
/// non-work atoms) keep theirs.
async fn delete_session_op_log(pool: &PgPool, opts: &PurgeOpts) -> Result<i64, PurgeError> {
    let deleted = sqlx::query(&format!(
        "DELETE FROM op_log o USING coding_sessions s \
         WHERE s.session_kind = $2 AND ($1::uuid IS NULL OR s.graph_id = $1) \
           AND o.graph_id = s.graph_id AND o.entity = 'coding_session' \
           AND o.entity_id IN (s.id::text, s.session_id) \
           AND NOT EXISTS (SELECT 1 FROM atoms a WHERE a.graph_id = s.graph_id \
                           AND a.source_type <> $3 AND a.source_id LIKE {SESSION_ATOMS})"
    ))
    .bind(opts.only_graph)
    .bind(WORK_SESSION_KIND)
    .bind(WORK_SOURCE_TYPE)
    .execute(pool)
    .await?;
    Ok(deleted.rows_affected() as i64)
}

/// Queue `DeleteSourceBlob` for every superseded key a work atom still
/// records, with the ownership rule ([`queueable_blob_key`]) and idempotency
/// key of `deletion::queue_blob_cleanup_tx`. Superseded objects are already
/// unreferenced, so queuing them before the atom is deleted is safe.
/// Returns `(queued, skipped)`.
async fn queue_superseded_blobs(pool: &PgPool, opts: &PurgeOpts) -> Result<(i64, i64), PurgeError> {
    let rows: Vec<(Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT DISTINCT a.graph_id, g.account_id, k.key FROM atoms a \
         JOIN graphs g ON g.id = a.graph_id \
         CROSS JOIN LATERAL unnest(a.superseded_blob_keys) AS k(key) \
         WHERE a.source_type = $2 AND ($1::uuid IS NULL OR a.graph_id = $1) \
           AND k.key IS NOT NULL AND k.key <> '' \
         ORDER BY a.graph_id, k.key",
    )
    .bind(opts.only_graph)
    .bind(WORK_SOURCE_TYPE)
    .fetch_all(pool)
    .await?;
    let (mut queued, mut skipped) = (0, 0);
    let mut tx = pool.begin().await?;
    for (graph_id, account_id, key) in rows {
        let Some(org_id) = queueable_blob_key(graph_id, &key) else {
            tracing::warn!(%graph_id, "skipping superseded blob key: malformed or another graph's object");
            skipped += 1;
            continue;
        };
        let kind = JobKind::DeleteSourceBlob { graph_id, account_id, org_id, blob_key: key.clone() };
        enqueue_tx(&mut tx, &kind, Some(account_id), Some(&format!("delete-source-blob:{key}")), 0)
            .await?;
        queued += 1;
    }
    tx.commit().await?;
    Ok((queued, skipped))
}

async fn purge_source_items(
    pool: &PgPool,
    control: Option<&PgPool>,
    opts: &PurgeOpts,
    purged: &mut Purged,
) -> Result<(), PurgeError> {
    let mut after = (Uuid::nil(), Uuid::nil());
    loop {
        let batch: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT graph_id, source_item_id FROM source_items \
             WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1) \
               AND (graph_id, source_item_id) > ($3, $4) \
             ORDER BY graph_id, source_item_id LIMIT $5",
        )
        .bind(opts.only_graph)
        .bind(WORK_SOURCE_TYPE)
        .bind(after.0)
        .bind(after.1)
        .bind(opts.batch)
        .fetch_all(pool)
        .await?;
        let Some(last) = batch.last() else { return Ok(()) };
        after = *last;
        for (graph_id, item_id) in batch {
            match deletion::delete_source_item_in_graph(
                pool, None, CLOUD_DEVICE_ID, graph_id, item_id, Actor::System, control,
            )
            .await
            {
                Ok(outcome) => {
                    purged.source_items += 1;
                    purged.atoms += outcome.atoms_deleted.len() as i64;
                    purged.reefs += outcome.deleted_reef_ids.len() as i64;
                }
                Err(e) => {
                    purged.errors += 1;
                    tracing::warn!(%graph_id, source_item_id = %item_id, error = %e, "work source purge failed");
                }
            }
        }
    }
}

/// Work atoms with no source item left (pre-source-item captures, promoted
/// copies in shared oceans): delete each distinct source identity.
async fn purge_leftover_atoms(
    pool: &PgPool,
    control: Option<&PgPool>,
    opts: &PurgeOpts,
    purged: &mut Purged,
) -> Result<(), PurgeError> {
    let mut after = (Uuid::nil(), String::new());
    loop {
        let batch: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT DISTINCT graph_id, source_id FROM atoms \
             WHERE source_type = $2 AND ($1::uuid IS NULL OR graph_id = $1) \
               AND (graph_id, source_id) > ($3, $4) \
             ORDER BY graph_id, source_id LIMIT $5",
        )
        .bind(opts.only_graph)
        .bind(WORK_SOURCE_TYPE)
        .bind(after.0)
        .bind(&after.1)
        .bind(opts.batch)
        .fetch_all(pool)
        .await?;
        let Some(last) = batch.last() else { return Ok(()) };
        after = last.clone();
        for (graph_id, source_id) in batch {
            match deletion::delete_source(
                pool, None, CLOUD_DEVICE_ID, graph_id, WORK_SOURCE_TYPE, &source_id, Actor::System, control,
            )
            .await
            {
                Ok(outcome) => {
                    purged.leftover_sources += 1;
                    purged.atoms += outcome.atoms_deleted.len() as i64;
                    purged.reefs += outcome.deleted_reef_ids.len() as i64;
                }
                Err(e) => {
                    purged.errors += 1;
                    tracing::warn!(%graph_id, %source_id, error = %e, "leftover work atom purge failed");
                }
            }
        }
    }
}

/// Work session rows the cascade left behind with no atom under their
/// prefix (the `deletion::delete_orphaned_coding_session` test);
/// `coding_session_shares` cascades with them. A session that still holds
/// any atom is kept.
async fn delete_work_sessions(pool: &PgPool, opts: &PurgeOpts) -> Result<i64, PurgeError> {
    let sql = format!(
        "DELETE FROM coding_sessions WHERE id IN ( \
           SELECT s.id FROM coding_sessions s WHERE s.session_kind = $2 \
             AND ($1::uuid IS NULL OR s.graph_id = $1) \
             AND NOT EXISTS (SELECT 1 FROM atoms a WHERE a.graph_id = s.graph_id \
                             AND a.source_id LIKE {SESSION_ATOMS}) \
           ORDER BY s.id LIMIT $3)"
    );
    let mut deleted = 0;
    loop {
        let n = sqlx::query(&sql)
            .bind(opts.only_graph)
            .bind(WORK_SESSION_KIND)
            .bind(opts.batch)
            .execute(pool)
            .await?
            .rows_affected() as i64;
        if n == 0 {
            return Ok(deleted);
        }
        deleted += n;
    }
}

async fn pending(pool: &PgPool) -> Result<Pending, PurgeError> {
    let (purge_dependencies, blob_jobs): (i64, i64) = sqlx::query_as(
        "SELECT \
           (SELECT count(*) FROM source_purge_dependencies WHERE processed_at IS NULL AND error_category IS NULL), \
           (SELECT count(*) FROM jobs WHERE kind = 'DeleteSourceBlob' AND status IN ('pending', 'running'))",
    )
    .fetch_one(pool)
    .await?;
    Ok(Pending { purge_dependencies, blob_jobs })
}

/// Human-readable block for one database (stdout).
pub fn report_block(label: &str, opts: &PurgeOpts, r: &Report) -> String {
    let b = &r.before;
    let c = &b.collateral;
    let mut out = format!(
        "[{label}] mode={}\n  work_session source_items: {}\n  work_session atoms:        {}\n  \
         work coding_sessions:      {} (deleted)\n  \
         work sessions kept:        {} (also hold non-work atoms; NOT deleted)\n  \
         their op_log rows:         {}\n  \
         reefs losing ALL evidence: {} (deleted; reefs with other evidence stay)\n  \
         blob keys to queue:        {}\n  \
         blob keys skipped:         {} (malformed or another graph's object; NOT queued, stay in R2)\n",
        opts.mode(), b.source_items, b.atoms, b.coding_sessions, b.sessions_kept, b.session_op_log,
        b.reefs_to_delete, b.blob_keys, b.blob_keys_skipped,
    );
    out.push_str(&format!(
        "  INVALIDATED (accepted collateral; backups are the only recovery):\n    \
         graphs / accounts holding work data: {} / {}\n    \
         conversation titles blanked:         {} (graph-scoped)\n    \
         assistant messages overwritten:      {} (graph-scoped)\n    \
         query answers nulled:                {} (ACCOUNT-scoped: every answered query of those accounts)\n    \
         query_history rows deleted:          {}\n    \
         journal_rollups rows deleted:        {}\n    \
         mcp_tasks marked failed:             {}\n    \
         extraction_cache rows deleted:       {} (TENANT-wide: those accounts plus every org row)\n    \
         op_log query/conversation rows:      {}\n",
        c.graphs, c.accounts, c.conversations, c.assistant_messages, c.query_answers, c.query_history,
        c.journal_rollups, c.mcp_tasks, c.extraction_cache, c.op_log,
    ));
    for (graph, account) in &r.affected {
        out.push_str(&format!("    graph {graph} (account {account})\n"));
    }
    if opts.execute {
        let p = &r.purged;
        out.push_str(&format!(
            "  purged: {} source items + {} leftover sources, {} atoms, {} reefs, {} sessions, \
             {} op_log rows, {} superseded blobs queued ({} skipped), {} errors\n  \
             remaining after purge: {:?}\n",
            p.source_items, p.leftover_sources, p.atoms, p.reefs, p.coding_sessions, p.session_op_log,
            p.superseded_blobs_queued, p.superseded_blobs_skipped, p.errors, r.after,
        ));
    }
    out.push_str(&format!(
        "  worker queue (whole database): {} purge dependencies, {} DeleteSourceBlob jobs pending\n",
        r.pending.purge_dependencies, r.pending.blob_jobs,
    ));
    out
}

/// One machine-greppable line per database (stderr).
pub fn summary_line(label: &str, opts: &PurgeOpts, r: &Report) -> String {
    let (b, p, a) = (&r.before, &r.purged, &r.after);
    let c = &b.collateral;
    let mut line = format!(
        "summary: db={label} mode={} source_items={} atoms={} coding_sessions={} sessions_kept={} \
         op_log={} reefs_to_delete={} blob_keys={} blob_keys_skipped={} \
         inv_graphs={} inv_accounts={} inv_conversations={} inv_assistant_messages={} \
         inv_query_answers={} inv_query_history={} inv_journal_rollups={} inv_mcp_tasks={} \
         inv_extraction_cache={} inv_op_log={}",
        opts.mode(), b.source_items, b.atoms, b.coding_sessions, b.sessions_kept, b.session_op_log,
        b.reefs_to_delete, b.blob_keys, b.blob_keys_skipped, c.graphs, c.accounts, c.conversations,
        c.assistant_messages, c.query_answers, c.query_history, c.journal_rollups, c.mcp_tasks,
        c.extraction_cache, c.op_log,
    );
    if opts.execute {
        line.push_str(&format!(
            " purged_atoms={} purged_reefs={} superseded_skipped={} errors={} remaining={}",
            p.atoms, p.reefs, p.superseded_blobs_skipped, p.errors, a.remaining(),
        ));
    }
    line.push_str(&format!(
        " pending_purge_deps={} pending_blob_jobs={}",
        r.pending.purge_dependencies, r.pending.blob_jobs,
    ));
    line
}

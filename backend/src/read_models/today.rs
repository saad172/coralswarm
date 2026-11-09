//! Stored Today answers. Gist generation remains in the existing journal worker;
//! these models only SELECT its committed output in the refresher snapshot.
use super::{
    dirty::{DirtyEvent, DirtyTrigger},
    *,
};
use crate::services::{journal_activities as ja, journal_gist as jg};
use chrono::{Duration, NaiveDate};
use chrono_tz::Tz;
use serde_json::json;
use sqlx::Row;

fn query_only(
    path: BTreeMap<String, String>,
    query: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, Error> {
    if !path.is_empty() {
        return Err("unknown path parameter".into());
    }
    Ok(query)
}
fn tz_param(raw: Option<String>) -> Result<Option<String>, Error> {
    Ok(ja::parse_tz(raw.as_deref())?.map(|z| z.name().into()))
}
fn graph_param(q: &mut BTreeMap<String, String>) -> Result<Option<Uuid>, Error> {
    Ok(q.remove("graph_id").map(|s| s.parse()).transpose()?)
}
fn who_param(q: &mut BTreeMap<String, String>) -> Result<String, Error> {
    Ok(match ja::Who::parse(q.remove("who").as_deref())? {
        ja::Who::Mine => "mine",
        ja::Who::Team => "team",
    }
    .into())
}
fn narrowed(v: &Viewer, g: Option<Uuid>) -> Vec<Uuid> {
    g.map(|g| vec![g]).unwrap_or_else(|| v.visible_graphs())
}
async fn graphs(cx: &mut ComputeCx<'_>) -> Result<Vec<ja::GraphRef>, Error> {
    let rows:Vec<(Uuid,String)>=sqlx::query_as("SELECT id,name FROM graphs WHERE id=ANY($1) AND account_id=$2 AND archived_at IS NULL ORDER BY id")
        .bind(cx.graphs).bind(cx.account_id).fetch_all(&mut **cx.tx).await?;
    Ok(rows
        .into_iter()
        .map(|(id, name)| ja::GraphRef { id, name })
        .collect())
}
/// Preserve the sweep's exact timezone chain: on consumer users.timezone,
/// then JOURNAL_TZ/UTC; org tenants have no users table and use the fallback.
/// Resolve only the indexed preference before deriving the stored address.
/// This is not content computation. Persisting the resolved zone/day makes a
/// later worker refresh reproduce the same calendar window.
async fn normalize_zone(
    tx: &mut sqlx::PgConnection,
    user: Uuid,
    explicit: Option<&str>,
) -> Result<Tz, Error> {
    if let Some(z) = explicit {
        return Ok(z.parse()?);
    }
    let consumer: bool = sqlx::query_scalar("SELECT to_regclass('public.users') IS NOT NULL")
        .fetch_one(&mut *tx)
        .await
        .map_err(crate::store::StoreError::from)?;
    if consumer {
        let stored: Option<String> = sqlx::query_scalar("SELECT timezone FROM users WHERE id=$1")
            .bind(user)
            .fetch_optional(&mut *tx)
            .await
            .map_err(crate::store::StoreError::from)?
            .flatten();
        if let Some(tz) = stored.and_then(|s| s.parse().ok()) {
            return Ok(tz);
        }
    }
    Ok(crate::workers::journal_sweep::journal_tz())
}
async fn zone(cx: &mut ComputeCx<'_>, explicit: Option<&str>) -> Result<Tz, Error> {
    if let Some(z) = explicit {
        return Ok(z.parse()?);
    }
    let consumer: bool = sqlx::query_scalar("SELECT to_regclass('public.users') IS NOT NULL")
        .fetch_one(&mut **cx.tx)
        .await?;
    if consumer {
        let control = cx.control_tx.as_mut().ok_or("control facts required")?;
        let stored: Option<String> = sqlx::query_scalar("SELECT timezone FROM users WHERE id=$1")
            .bind(cx.compute_as)
            .fetch_optional(&mut ***control)
            .await?
            .flatten();
        if let Some(tz) = stored.and_then(|s| s.parse().ok()) {
            return Ok(tz);
        }
    }
    Ok(crate::workers::journal_sweep::journal_tz())
}
async fn activities(
    cx: &mut ComputeCx<'_>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    who: &str,
    tz: Tz,
) -> Result<Value, Error> {
    let gs = graphs(cx).await?;
    let req = ja::ListRequest {
        caller: cx.compute_as,
        who: ja::Who::parse(Some(who))?,
        from,
        to,
        tz,
        graphs: &gs,
    };
    let mut list = ja::list_activities(cx.tx, &req).await?;
    let mut ids: Vec<_> = list
        .activities
        .iter()
        .filter_map(|a| a.owner.user_id)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    if !ids.is_empty() {
        let control = cx.control_tx.as_mut().ok_or("control facts required")?;
        let names = ja::resolve_owner_names(&mut ***control, &ids).await?;
        for a in &mut list.activities {
            a.apply_names(&names);
        }
    }
    Ok(
        json!({"activities":list.activities,"day_activity":list.day_activity,"truncated":list.truncated,"window":{"from":from,"to":to,"tz":tz.name(),"who":who}}),
    )
}
const TODAY_TRIGGERS: &[DirtyTrigger] = &[
    DirtyTrigger::Table("atoms"),
    DirtyTrigger::Table("nodes"),
    DirtyTrigger::Table("reef_identity_corrections"),
    DirtyTrigger::Table("graphs"),
    DirtyTrigger::Table("node_atoms"),
    DirtyTrigger::Table("node_semantic_atoms"),
    DirtyTrigger::Table("semantic_atoms"),
    DirtyTrigger::Table("source_items"),
    DirtyTrigger::Table("source_versions"),
    DirtyTrigger::Table("source_version_chunks"),
    DirtyTrigger::Table("coding_sessions"),
    DirtyTrigger::Table("meeting_transcripts"),
    DirtyTrigger::Table("journal_rollups"),
    DirtyTrigger::Table("journal_gists"),
    DirtyTrigger::Table("query_history"),
    DirtyTrigger::Event(DirtyEvent::PrefsChanged),
];
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalActivitiesParams {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub who: String,
    pub graph_id: Option<Uuid>,
    pub tz: Option<String>,
}
impl Params for JournalActivitiesParams {
    fn normalize<'a>(
        mut self,
        tx: &'a mut sqlx::PgConnection,
        user_id: Uuid,
    ) -> ComputeFuture<'a, Self> {
        Box::pin(async move {
            let tz = normalize_zone(tx, user_id, self.tz.as_deref()).await?;
            self.tz = Some(tz.name().into());

            Ok(self)
        })
    }

    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let mut q = query_only(path, query)?;
        let (from, to) = ja::parse_window(q.remove("from").as_deref(), q.remove("to").as_deref())?;
        let who = who_param(&mut q)?;
        let graph_id = graph_param(&mut q)?;
        let tz = tz_param(q.remove("tz"))?;
        if !q.is_empty() {
            return Err("unknown query parameter".into());
        }
        Ok(Self {
            from,
            to,
            who,
            graph_id,
            tz,
        })
    }
}
pub struct JournalActivities;
impl ReadModel for JournalActivities {
    const HIDE_DENIED_SCOPE: bool = true;
    const NAME: &'static str = "journal_activities";
    const PATH: &'static str = "/journal/activities";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    type Params = JournalActivitiesParams;
    type Payload = Value;
    fn enabled() -> bool {
        ja::enabled()
    }
    fn graphs(v: &Viewer, p: &Self::Params) -> Vec<Uuid> {
        narrowed(v, p.graph_id)
    }
    fn compute<'a>(mut cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let tz = zone(&mut cx, p.tz.as_deref()).await?;
            activities(&mut cx, p.from, p.to, &p.who, tz).await
        })
    }
    fn empty(p: &Self::Params) -> Value {
        let tz =
            p.tz.as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(Tz::UTC);
        json!({"activities":[],"day_activity":ja::empty_strip(p.to,tz),"truncated":false,"window":{"from":p.from,"to":p.to,"tz":tz.name(),"who":p.who}})
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        TODAY_TRIGGERS
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalStripParams {
    pub tz: Option<String>,
    pub who: String,
    pub graph_id: Option<Uuid>,
    pub calendar_day: NaiveDate,
}
impl Params for JournalStripParams {
    fn normalize<'a>(
        mut self,
        tx: &'a mut sqlx::PgConnection,
        user_id: Uuid,
    ) -> ComputeFuture<'a, Self> {
        Box::pin(async move {
            let tz = normalize_zone(tx, user_id, self.tz.as_deref()).await?;
            self.tz = Some(tz.name().into());
            self.calendar_day = Utc::now().with_timezone(&tz).date_naive();
            Ok(self)
        })
    }

    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let mut q = query_only(path, query)?;
        let tz = tz_param(q.remove("tz"))?;
        let who = who_param(&mut q)?;
        let graph_id = graph_param(&mut q)?;
        if !q.is_empty() {
            return Err("unknown query parameter".into());
        }
        let zone = tz
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(Tz::UTC);
        Ok(Self {
            tz,
            who,
            graph_id,
            calendar_day: Utc::now().with_timezone(&zone).date_naive(),
        })
    }
}
pub struct JournalStrip;
impl ReadModel for JournalStrip {
    const HIDE_DENIED_SCOPE: bool = true;
    const NAME: &'static str = "journal_strip";
    const PATH: &'static str = "/journal/strip";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    type Params = JournalStripParams;
    type Payload = Value;
    fn enabled() -> bool {
        ja::enabled()
    }
    fn graphs(v: &Viewer, p: &Self::Params) -> Vec<Uuid> {
        narrowed(v, p.graph_id)
    }
    fn compute<'a>(mut cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let tz = zone(&mut cx, p.tz.as_deref()).await?;
            let day = p.calendar_day;
            let to = crate::workers::journal_sweep::resolve_local_midnight(
                tz,
                day.succ_opt()
                    .ok_or("invalid date")?
                    .and_hms_opt(0, 0, 0)
                    .ok_or("invalid calendar midnight")?,
            );
            let gs = graphs(&mut cx).await?;
            let req = ja::ListRequest {
                caller: cx.compute_as,
                who: ja::Who::parse(Some(&p.who))?,
                from: to - Duration::days(14),
                to,
                tz,
                graphs: &gs,
            };
            let day_activity = ja::list_strip(cx.tx, &req).await?;
            Ok(json!({"day_activity":day_activity,"window":{"to":to,"tz":tz.name(),"who":p.who}}))
        })
    }
    fn empty(p: &Self::Params) -> Value {
        let tz =
            p.tz.as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(Tz::UTC);
        json!({"day_activity":ja::empty_strip(Utc::now(),tz),"window":{"to":Utc::now(),"tz":tz.name(),"who":p.who}})
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        TODAY_TRIGGERS
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalGistParams {
    pub granularity: String,
    pub anchor: Option<NaiveDate>,
    pub who: String,
    pub graph_id: Option<Uuid>,
    pub tz: Option<String>,
    pub calendar_day: NaiveDate,
}
impl Params for JournalGistParams {
    fn normalize<'a>(
        mut self,
        tx: &'a mut sqlx::PgConnection,
        user_id: Uuid,
    ) -> ComputeFuture<'a, Self> {
        Box::pin(async move {
            let tz = normalize_zone(tx, user_id, self.tz.as_deref()).await?;
            self.tz = Some(tz.name().into());
            self.calendar_day = Utc::now().with_timezone(&tz).date_naive();
            if self.anchor.is_none() {
                self.anchor = Some(self.calendar_day);
            }
            Ok(self)
        })
    }

    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let mut q = query_only(path, query)?;
        let granularity = jg::Granularity::parse(q.remove("granularity").as_deref())?
            .as_str()
            .into();
        let anchor = jg::parse_anchor(q.remove("anchor").as_deref())?;
        let who = who_param(&mut q)?;
        let graph_id = graph_param(&mut q)?;
        let tz = tz_param(q.remove("tz"))?;
        if who == "team" && graph_id.is_none() {
            return Err("graph_id is required for who=team".into());
        }
        if !q.is_empty() {
            return Err("unknown query parameter".into());
        }
        let zone = tz
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(Tz::UTC);
        Ok(Self {
            granularity,
            anchor,
            who,
            graph_id,
            tz,
            calendar_day: Utc::now().with_timezone(&zone).date_naive(),
        })
    }
}
fn gist_body(
    p: &JournalGistParams,
    tz: Tz,
    now: DateTime<Utc>,
    answer: Option<jg::ReadAnswer>,
) -> Value {
    let granularity = jg::Granularity::parse(Some(&p.granularity)).expect("validated granularity");
    let (start, end) = jg::period_for(granularity, p.anchor, tz, now);
    json!({"period_start":start,"period_end":end,"open":now>=start&&now<end,"who":p.who,"granularity":p.granularity,"tz":tz.name(),"gist":answer.as_ref().and_then(|a|a.gist.as_ref()),"compiled_at":answer.as_ref().and_then(|a|a.compiled_at),"span":answer.as_ref().and_then(|a|a.span.as_ref())})
}
pub struct JournalGist;
impl ReadModel for JournalGist {
    const HIDE_DENIED_SCOPE: bool = true;
    const NAME: &'static str = "journal_gist";
    const PATH: &'static str = "/journal/gist";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    type Params = JournalGistParams;
    type Payload = Value;
    fn enabled() -> bool {
        jg::enabled()
    }
    fn graphs(v: &Viewer, p: &Self::Params) -> Vec<Uuid> {
        narrowed(v, p.graph_id)
    }
    fn compute<'a>(mut cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let tz = zone(&mut cx, p.tz.as_deref()).await?;
            let granularity = jg::Granularity::parse(Some(&p.granularity))?;
            let (period_start, period_end) = jg::period_for(granularity, p.anchor, tz, cx.now);
            let gs = graphs(&mut cx).await?;
            let ids: Vec<_> = gs.iter().map(|g| g.id).collect();
            let req = jg::ReadRequest {
                caller: cx.compute_as,
                who: jg::Who::parse(Some(&p.who))?,
                granularity,
                period_start,
                period_end,
                graph_ids: &ids,
            };
            let answer = jg::read_gist(cx.tx, &req).await?;
            Ok(gist_body(p, tz, cx.now, Some(answer)))
        })
    }
    fn empty(p: &Self::Params) -> Value {
        gist_body(
            p,
            p.tz.as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(Tz::UTC),
            Utc::now(),
            None,
        )
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        TODAY_TRIGGERS
    }
}

use crate::store::{JournalAggregateBlock, JournalHourAggregate, JournalRollup};
async fn journal_range_aggregate(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user_id: Uuid,
    graph_ids: &[Uuid],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<JournalHourAggregate>, Error> {
    // One row per (hour, graph, source_type, grouping_key) — the atoms
    // half of the aggregate. `grouping_key` mirrors the convention
    // documented on `JournalInputAtom::source_id`: a coding-session
    // atom's `source_id` is `"cc:{session_id}:{checkpoint_id}[:chunk:N]"`
    // (migration `0076_coding_sessions`'s header) — but, unlike every
    // other MCP identifier in this codebase, `session_id` and
    // `checkpoint_id` carry NO charset restriction beyond a length cap
    // (`add_context`'s validation, `mcp/tools/write.rs`), so they may
    // themselves contain colons. `split_part(source_id, ':', 2)` used to
    // extract `session_id` here, which truncates at the FIRST colon
    // inside a colon-bearing `session_id` (e.g. `"cc:team:alpha:ckpt-1"`
    // truncates to `"team"`) — splitting one session into multiple
    // blocks and breaking the `coding_sessions` title join. Fixed by
    // joining the same way every other session read in this codebase
    // does (`Store::list_coding_sessions`'s `coral_count` subquery,
    // `session_source_id_prefix`): match each atom against the actual
    // `coding_sessions.session_id` via the escaped `"cc:{session_id}:%"`
    // LIKE prefix, so the grouping key is the session ROW's own
    // `session_id`, never a heuristic re-split of `source_id`. The
    // `REPLACE` chain mirrors `session_source_id_prefix`'s escaping
    // exactly, just expressed as SQL string functions since the pattern
    // is correlated (depends on `cs.session_id`, a column from the
    // joined row, not a single caller-supplied value known before the
    // query runs) — same rationale as that method's own doc comment.
    // `UNIQUE (graph_id, session_id)` (migration `0076`'s `coding_sessions`
    // table) plus that escaping's own no-cross-session-prefix-collision
    // guarantee (`session_source_id_prefix`'s test suite) means at most
    // one `coding_sessions` row can match any one atom, so this LEFT
    // JOIN can never fan a row out. A `coding_session` atom whose
    // session upsert never landed (`upsert_coding_session_scoped` is
    // best-effort, `routes/ingest.rs` step 8d) has no match; rather than
    // guess a boundary between `session_id`/`checkpoint_id` with no
    // verified session row to check it against, that atom falls back to
    // grouping by its own full `source_id` (one block per orphaned
    // checkpoint) instead of silently mis-grouping it with another
    // orphan under a wrong heuristic split. Every other source type
    // still strips a trailing `:chunk:{n}` off its own `source_id` (the
    // `meeting_transcripts` chunking convention, migration `0056`'s
    // header) to recover its "base" container id. `MAX(fa.title)` is a
    // cheap, deterministic stand-in for "some atom in this group got
    // titled" (0097's per-CORAL headline, NOT a session/meeting title)
    // — used only when there is no better title source for this block
    // (see the mapping below).
    type BlockRow = (
        DateTime<Utc>,  // hour_start
        String,         // source_type
        i64,            // atom_count
        Option<String>, // any_title (MAX(atoms.title) in the group)
        Option<String>, // session_title (coding_sessions.title, coding_session groups only)
    );
    // `{sess}` below is `ingest::atom::SESSION_SOURCE_TYPES_SQL_ARRAY`
    // — see the inline SQL comments and that constant's doc comment.
    let sql = format!(
        r#"
            WITH candidate_ids AS MATERIALIZED (
                SELECT a.graph_id,a.atom_id FROM atoms a
                WHERE a.ingested_by=$1 AND a.graph_id=ANY($2)
                  AND a."timestamp">=$3 AND a."timestamp"<$4 AND {floor}
            ), per_graph AS (
                SELECT graph_id,array_agg(atom_id) ids FROM candidate_ids GROUP BY graph_id
            ), admitted AS MATERIALIZED (
                SELECT pg.graph_id,p.atom_id FROM per_graph pg
                CROSS JOIN LATERAL library_permitted_atoms(pg.graph_id,
                    CASE WHEN cardinality(pg.ids)>10000 THEN NULL::uuid[] ELSE pg.ids END) p
                JOIN candidate_ids c ON c.graph_id=pg.graph_id AND c.atom_id=p.atom_id
            ), filtered_atoms AS (
                SELECT
                    a.graph_id,
                    date_trunc('hour', a."timestamp" AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS hour_start,
                    a.source_type,
                    a.title,
                    {grouping_key} AS grouping_key,
                    cs.title AS session_title
                FROM atoms a
                JOIN admitted ok ON ok.graph_id=a.graph_id AND ok.atom_id=a.atom_id
                {session_join}
                WHERE a.ingested_by = $1
                  AND a.graph_id = ANY($2)
                  AND a."timestamp" >= $3 AND a."timestamp" < $4
                  -- The reader's team-seat history floor (#1545).
                  AND {floor}
            )
            SELECT
                fa.hour_start,
                fa.source_type,
                COUNT(*) AS atom_count,
                MAX(fa.title) AS any_title,
                MAX(fa.session_title) AS session_title
            FROM filtered_atoms fa
            GROUP BY fa.hour_start, fa.graph_id, fa.source_type, fa.grouping_key
            ORDER BY fa.hour_start ASC, COUNT(*) DESC, fa.source_type ASC, fa.grouping_key ASC
            "#,
        grouping_key = crate::journal_grouping::grouping_key_sql(),
        session_join = crate::journal_grouping::session_join_sql(),
        floor = crate::history_floor::admits_atom("a")
    );
    let block_rows: Vec<BlockRow> = sqlx::query_as(&sql)
        .bind(user_id)
        .bind(graph_ids)
        .bind(from)
        .bind(to)
        .fetch_all(&mut **tx)
        .await?;

    // The "asks" half — `query_history` rows in the same window, same
    // author-attribution and graph_ids predicates.
    type AskRow = (DateTime<Utc>, i64);
    let ask_rows: Vec<AskRow> = sqlx::query_as(
            "SELECT date_trunc('hour', qh.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS hour_start, \
                    COUNT(*) AS asks \
             FROM query_history qh \
             WHERE qh.user_id = $1 AND qh.graph_id = ANY($2) \
               AND qh.created_at >= $3 AND qh.created_at < $4 \
             GROUP BY 1",
        )
        .bind(user_id)
        .bind(graph_ids)
        .bind(from)
        .bind(to)
        .fetch_all(&mut **tx)
        .await?;

    // `BTreeMap` so the final `Vec` comes out hour_start-ascending for
    // free, matching `list_journal_rollups`'s own ordering contract.
    let mut by_hour: std::collections::BTreeMap<DateTime<Utc>, (i64, Vec<JournalAggregateBlock>)> =
        std::collections::BTreeMap::new();
    for (hour_start, source_type, atom_count, any_title, session_title) in block_rows {
        // Title precedence: a coding session's own (title-sweep-produced)
        // title first, then any grouped atom's per-coral headline, then a
        // generic source_type label. Every other source type skips
        // straight to "grouped atom's headline, else label" — there is no
        // cheap session/meeting-level title source for them today (a real
        // meeting title requires resolving the raw transcript, which can
        // be R2-encrypted — out of scope for this deterministic, DB-only
        // aggregate; see `routes::journal`'s module doc for the full
        // deviation note).
        // Rider A: was `source_type == "coding_session"` — every session
        // source type now gets a `session_title` from the widened
        // LATERAL join above, so the title-precedence rule must widen
        // with it or a `meeting_session` block would have
        // a real session title available and never use it.
        let title = if crate::ingest::atom::is_session_source_type(&source_type) {
            session_title
                .or(any_title)
                .unwrap_or_else(|| crate::store::journal_block_title_fallback(&source_type))
        } else {
            any_title.unwrap_or_else(|| crate::store::journal_block_title_fallback(&source_type))
        };
        let entry = by_hour.entry(hour_start).or_insert_with(|| (0, Vec::new()));
        entry.0 += atom_count;
        entry.1.push(JournalAggregateBlock {
            source_type,
            title,
            atom_count,
        });
    }

    let mut asks_by_hour: std::collections::HashMap<DateTime<Utc>, i64> =
        std::collections::HashMap::new();
    for (hour_start, asks) in ask_rows {
        asks_by_hour.insert(hour_start, asks);
        // An "asks only" hour (no atoms) must still surface — this makes
        // sure it has a `by_hour` entry to attach `asks` to below.
        by_hour.entry(hour_start).or_insert_with(|| (0, Vec::new()));
    }

    Ok(by_hour
        .into_iter()
        .map(|(hour_start, (atom_count, blocks))| JournalHourAggregate {
            hour_start,
            atom_count,
            asks: asks_by_hour.get(&hour_start).copied().unwrap_or(0),
            blocks,
        })
        .collect())
}

async fn rollups(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    user: Uuid,
    graphs: &[Uuid],
    granularity: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<JournalRollup>, Error> {
    let sql=format!("WITH admitted AS MATERIALIZED (SELECT g.id graph_id,p.atom_id FROM unnest($2::uuid[]) g(id) CROSS JOIN LATERAL library_permitted_atoms(g.id,NULL::uuid[]) p) SELECT r.* FROM journal_rollups r WHERE user_id=$1 AND graph_id=ANY($2) AND granularity=$3 AND hour_start >= $4 AND hour_start < $5 AND {} AND NOT EXISTS (SELECT 1 FROM atoms a WHERE a.graph_id=r.graph_id AND a.ingested_by=$1 AND a.\"timestamp\">=r.period_start AND a.\"timestamp\"< CASE r.granularity WHEN 'hour' THEN r.period_start+interval '1 hour' ELSE ((r.period_start AT TIME ZONE COALESCE(r.tz,'UTC')) + CASE r.granularity WHEN 'day' THEN interval '1 day' WHEN 'week' THEN interval '1 week' ELSE interval '1 month' END) AT TIME ZONE COALESCE(r.tz,'UTC') END AND NOT EXISTS(SELECT 1 FROM admitted p WHERE p.graph_id=a.graph_id AND p.atom_id=a.atom_id)) ORDER BY hour_start ASC,graph_id ASC",crate::history_floor::admits_period("r.graph_id","r.hour_start"));
    let rows = sqlx::query(&sql)
        .bind(user)
        .bind(graphs)
        .bind(granularity)
        .bind(from)
        .bind(to)
        .fetch_all(&mut **tx)
        .await?;
    rows.into_iter()
        .map(|r| {
            Ok(JournalRollup {
                id: r.try_get("id")?,
                account_id: r.try_get("account_id")?,
                user_id: r.try_get("user_id")?,
                graph_id: r.try_get("graph_id")?,
                hour_start: r.try_get("hour_start")?,
                period_start: r.try_get("period_start")?,
                granularity: r.try_get("granularity")?,
                tz: r.try_get("tz")?,
                summary: r.try_get("summary")?,
                stats: r.try_get("stats")?,
                input_fingerprint: r.try_get("input_fingerprint")?,
                recompile_count: r.try_get("recompile_count")?,
                model: r.try_get("model")?,
                compiled_at: r.try_get("compiled_at")?,
            })
        })
        .collect()
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalRollupsParams {
    pub granularity: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub refresh_day: NaiveDate,
    pub tz: Option<String>,
}
impl Params for JournalRollupsParams {
    fn normalize<'a>(
        mut self,
        tx: &'a mut sqlx::PgConnection,
        user_id: Uuid,
    ) -> ComputeFuture<'a, Self> {
        Box::pin(async move {
            let tz = normalize_zone(tx, user_id, self.tz.as_deref()).await?;
            self.tz = Some(tz.name().into());
            self.refresh_day = Utc::now().with_timezone(&tz).date_naive();
            Ok(self)
        })
    }

    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let mut q = query_only(path, query)?;
        let raw = q.remove("granularity");
        let granularity = crate::routes::journal::Granularity::parse(raw.as_deref())?;
        let params = crate::routes::journal::JournalQueryParams {
            from: q.remove("from"),
            to: q.remove("to"),
            granularity: None,
        };
        let (from, to) = crate::routes::journal::parse_journal_range(&params, granularity)?;
        if !q.is_empty() {
            return Err("unknown query parameter".into());
        }
        Ok(Self {
            granularity: granularity.as_str().into(),
            from,
            to,
            refresh_day: Utc::now().date_naive(),
            tz: None,
        })
    }
}
pub struct JournalRollups;
impl ReadModel for JournalRollups {
    const NAME: &'static str = "journal_rollups";
    const PATH: &'static str = "/journal";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    type Params = JournalRollupsParams;
    type Payload = Value;
    fn compute<'a>(mut cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let gs = graphs(&mut cx).await?;
            let ids: Vec<_> = gs.iter().map(|g| g.id).collect();
            let names: std::collections::HashMap<_, _> =
                gs.iter().map(|g| (g.id, g.name.as_str())).collect();
            let rows = rollups(cx.tx, cx.compute_as, &ids, &p.granularity, p.from, p.to).await?;
            if p.granularity == "hour" {
                let aggregates =
                    journal_range_aggregate(cx.tx, cx.compute_as, &ids, p.from, p.to).await?;
                let mut by_hour: BTreeMap<DateTime<Utc>, Vec<Value>> = BTreeMap::new();
                for r in rows {
                    by_hour.entry(r.hour_start).or_default().push(json!({"graph_id":r.graph_id,"graph_name":names.get(&r.graph_id).copied().unwrap_or(""),"summary":r.summary,"stats":r.stats,"compiled_at":r.compiled_at}));
                }
                let mut by_agg: BTreeMap<_, _> =
                    aggregates.into_iter().map(|a| (a.hour_start, a)).collect();
                for hour in by_agg.keys() {
                    by_hour.entry(*hour).or_default();
                }
                let hours:Vec<_>=by_hour.into_iter().map(|(hour,entries)|{
                let aggregate=by_agg.remove(&hour).map(|a|json!({"atom_count":a.atom_count,"asks":a.asks,"blocks":a.blocks.into_iter().map(|b|json!({"source_type":b.source_type,"title":b.title,"atom_count":b.atom_count})).collect::<Vec<_>>()})).unwrap_or_else(||json!({"atom_count":0,"asks":0,"blocks":[]}));
                json!({"hour_start":hour,"entries":entries,"aggregate":aggregate})
            }).collect();
                return Ok(json!({"hours":hours}));
            }
            let tz = zone(&mut cx, p.tz.as_deref()).await?;
            let granularity = crate::routes::journal::Granularity::parse(Some(&p.granularity))?;
            let bounds = granularity.bounds_of().ok_or("invalid tier")?;
            let child = granularity.child().ok_or("invalid tier")?;
            let periods = crate::workers::journal_sweep::periods_covering(p.from, p.to, tz, bounds);
            let open_period = periods
                .iter()
                .copied()
                .find(|(start, end)| cx.now >= *start && cx.now < *end);
            let mut entries: BTreeMap<DateTime<Utc>, Vec<Value>> = BTreeMap::new();
            for r in rows {
                entries.entry(r.period_start).or_default().push(json!({"graph_id":r.graph_id,"graph_name":names.get(&r.graph_id).copied().unwrap_or(""),"summary":r.summary,"stats":r.stats,"tz":r.tz,"compiled_at":r.compiled_at,"recompile_count":r.recompile_count,"open":false}));
            }
            if let Some((start, end)) = open_period {
                entries.entry(start).or_default();
                let live_to = cx.now.min(end);
                if live_to > start {
                    let children =
                        rollups(cx.tx, cx.compute_as, &ids, child.as_str(), start, live_to).await?;
                    let mut grouped: BTreeMap<Uuid, Vec<JournalRollup>> = BTreeMap::new();
                    for c in children {
                        grouped.entry(c.graph_id).or_default().push(c);
                    }
                    for (graph, children) in grouped {
                        entries.entry(start).or_default().push(json!({"graph_id":graph,"graph_name":names.get(&graph).copied().unwrap_or(""),"summary":children.iter().map(|c|c.summary.as_str()).collect::<Vec<_>>().join("\n\n"),"stats":crate::workers::journal_sweep::sum_child_stats(&children),"tz":tz.name(),"compiled_at":Value::Null,"recompile_count":Value::Null,"open":true}));
                    }
                }
            }
            let periods:Vec<_>=periods.into_iter().filter_map(|(start,end)|entries.remove(&start).map(|entries|json!({"period_start":start,"period_end":end,"open":Some((start,end))==open_period,"entries":entries}))).collect();
            Ok(json!({"granularity":p.granularity,"periods":periods}))
        })
    }
    fn empty(p: &Self::Params) -> Value {
        if p.granularity == "hour" {
            json!({"hours":[]})
        } else {
            json!({"granularity":p.granularity,"periods":[]})
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        TODAY_TRIGGERS
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_and_canonicalizes_params() {
        let p = JournalActivitiesParams::parse(
            BTreeMap::new(),
            [
                ("from".into(), "2026-10-30T01:00:00+01:00".into()),
                ("to".into(), "2026-10-31T00:00:00Z".into()),
            ]
            .into(),
        )
        .unwrap();
        assert_eq!(p.from.to_rfc3339(), "2026-10-30T00:00:00+00:00");
        assert_eq!(p.who, "mine");
        assert!(
            JournalGistParams::parse(BTreeMap::new(), [("who".into(), "team".into())].into())
                .is_err()
        );
        assert!(JournalStripParams::parse(
            BTreeMap::new(),
            [("compute_as".into(), Uuid::new_v4().to_string())].into()
        )
        .is_err());
    }
}

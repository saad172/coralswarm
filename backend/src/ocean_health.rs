//! Ocean health metrics and paging alarms (knowledge-graph-connectivity §12.3 / G3).
//!
//! Rolling 7-day window (or an explicit `[start, end)`). Paging bands match
//! the replay that would have fired the week coding sessions started:
//!
//! * zero-coral share of **new** entity reefs > 25% (min 20 new reefs)
//! * reefs-created / corals-created > 1.0 (min 20 new corals)
//! * dead jobs for this graph > 20
//!
//! Self-touch of new edges and new-reef title-containment are diagnostics,
//! not pages (self-touch is permanently ~99% on a single-author coding
//! ocean). A paging alarm enqueues a `HealthViolation` job (one per graph /
//! alarm / ISO week) so drift is a job row, not a seven-week surprise.

use chrono::{DateTime, Datelike, Duration, Utc};
use serde::Serialize;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::jobs::queue::{self, JobOutcome};
use crate::jobs::JobKind;
use crate::subtopic::WholeWordIndex;

pub const WINDOW: Duration = Duration::days(7);

pub const ZERO_CORAL_PAGE: f64 = 0.25;
pub const ZERO_CORAL_MIN_REEFS: i64 = 20;
pub const REEFS_PER_CORAL_PAGE: f64 = 1.0;
pub const REEFS_PER_CORAL_MIN_CORALS: i64 = 20;
pub const DEAD_JOBS_PAGE: i64 = 20;
pub const SELF_TOUCH_DIAGNOSTIC: f64 = 0.90;
pub const CONTAIN_DIAGNOSTIC: f64 = 0.30;

const UUID_RE: &str =
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlarmKind {
    ZeroCoralShare,
    ReefsPerCoral,
    DeadJobs,
}

impl AlarmKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AlarmKind::ZeroCoralShare => "zero_coral_share",
            AlarmKind::ReefsPerCoral => "reefs_per_coral",
            AlarmKind::DeadJobs => "dead_jobs",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Alarm {
    pub kind: AlarmKind,
    pub value: f64,
    pub threshold: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WindowMetrics {
    pub graph_id: Uuid,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub new_reefs: i64,
    pub new_zero_coral: i64,
    pub new_corals: i64,
    pub new_edges: i64,
    pub self_edges: i64,
    pub contain_hits: i64,
    pub dead_jobs: i64,
    pub entity_reefs: i64,
    pub zero_coral_stock: i64,
    pub single_coral_stock: i64,
    pub dead_jobs_by_kind: Vec<(String, i64)>,
}

impl WindowMetrics {
    pub fn zero_coral_share(&self) -> Option<f64> {
        (self.new_reefs > 0).then(|| self.new_zero_coral as f64 / self.new_reefs as f64)
    }

    pub fn reefs_per_coral(&self) -> Option<f64> {
        (self.new_corals > 0).then(|| self.new_reefs as f64 / self.new_corals as f64)
    }

    pub fn self_edge_share(&self) -> Option<f64> {
        (self.new_edges > 0).then(|| self.self_edges as f64 / self.new_edges as f64)
    }

    pub fn contain_share(&self) -> Option<f64> {
        (self.new_reefs > 0).then(|| self.contain_hits as f64 / self.new_reefs as f64)
    }

    pub fn single_coral_share(&self) -> Option<f64> {
        (self.entity_reefs > 0).then(|| self.single_coral_stock as f64 / self.entity_reefs as f64)
    }
}

/// Paging alarms only. Diagnostics live on [`to_json`].
pub fn evaluate(m: &WindowMetrics) -> Vec<Alarm> {
    let mut out = Vec::new();
    if let Some(share) = m.zero_coral_share() {
        if m.new_reefs >= ZERO_CORAL_MIN_REEFS && share > ZERO_CORAL_PAGE {
            out.push(Alarm {
                kind: AlarmKind::ZeroCoralShare,
                value: share,
                threshold: ZERO_CORAL_PAGE,
            });
        }
    }
    if let Some(rpc) = m.reefs_per_coral() {
        if m.new_corals >= REEFS_PER_CORAL_MIN_CORALS && rpc > REEFS_PER_CORAL_PAGE {
            out.push(Alarm {
                kind: AlarmKind::ReefsPerCoral,
                value: rpc,
                threshold: REEFS_PER_CORAL_PAGE,
            });
        }
    }
    if m.dead_jobs > DEAD_JOBS_PAGE {
        out.push(Alarm {
            kind: AlarmKind::DeadJobs,
            value: m.dead_jobs as f64,
            threshold: DEAD_JOBS_PAGE as f64,
        });
    }
    out
}

pub fn rolling_window(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    (now - WINDOW, now)
}

/// Monday 00:00 UTC of `week` through +7 days (Postgres `date_trunc('week')`).
pub fn monday_week(week: chrono::NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
    let start = week.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
    (start, start + WINDOW)
}

pub fn iso_week_key(t: DateTime<Utc>) -> String {
    format!("{:04}-W{:02}", t.iso_week().year(), t.iso_week().week())
}

pub fn to_json(m: &WindowMetrics, paging: &[Alarm]) -> serde_json::Value {
    serde_json::json!({
        "graph_id": m.graph_id,
        "window_start": m.window_start,
        "window_end": m.window_end,
        "new_reefs": m.new_reefs,
        "new_zero_coral": m.new_zero_coral,
        "new_corals": m.new_corals,
        "new_edges": m.new_edges,
        "zero_coral_share": m.zero_coral_share(),
        "reefs_per_coral": m.reefs_per_coral(),
        "self_edge_share": m.self_edge_share(),
        "contain_share": m.contain_share(),
        "single_coral_share": m.single_coral_share(),
        "dead_jobs": m.dead_jobs,
        "dead_jobs_by_kind": m.dead_jobs_by_kind.iter().map(|(k, c)| {
            serde_json::json!({"kind": k, "count": c})
        }).collect::<Vec<_>>(),
        "entity_reefs": m.entity_reefs,
        "zero_coral_stock": m.zero_coral_stock,
        "paging": paging,
        "diagnostic": {
            "self_touch": m.self_edge_share().map(|s| s > SELF_TOUCH_DIAGNOSTIC).unwrap_or(false),
            "contain": m.contain_share().map(|s| s > CONTAIN_DIAGNOSTIC).unwrap_or(false),
        },
    })
}

/// An entity reef title as `read_window` loads it.
#[derive(Debug, Clone)]
pub struct EntityTitle {
    pub title: String,
    pub is_new: bool,
    pub is_self: bool,
}

/// Everything the window metrics need from the database. Reading it is the
/// only part that holds a transaction; [`finish`] does the CPU work after the
/// caller has closed it.
#[derive(Debug, Clone)]
pub struct WindowRead {
    graph_id: Uuid,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    stock: (i64, i64, i64, i64, i64),
    new_corals: i64,
    edges: (i64, i64),
    dead_rows: Vec<(String, i64)>,
    titles: Vec<EntityTitle>,
}

/// New entity titles that whole-word contain an older, non-self entity title.
pub fn contain_hits(titles: &[EntityTitle]) -> i64 {
    let older = WholeWordIndex::new(
        titles
            .iter()
            .filter(|t| !t.is_new && !t.is_self)
            .map(|t| t.title.as_str()),
    );
    titles
        .iter()
        .filter(|t| t.is_new && older.contained_in(&t.title))
        .count() as i64
}

impl WindowRead {
    fn into_metrics(self) -> WindowMetrics {
        WindowMetrics {
            graph_id: self.graph_id,
            window_start: self.window_start,
            window_end: self.window_end,
            new_reefs: self.stock.0,
            new_zero_coral: self.stock.1,
            new_corals: self.new_corals,
            new_edges: self.edges.0,
            self_edges: self.edges.1,
            contain_hits: contain_hits(&self.titles),
            dead_jobs: self.dead_rows.iter().map(|(_, c)| *c).sum(),
            entity_reefs: self.stock.2,
            zero_coral_stock: self.stock.3,
            single_coral_stock: self.stock.4,
            dead_jobs_by_kind: self.dead_rows,
        }
    }
}

/// Turn a [`WindowRead`] into metrics on the blocking pool. The title scan
/// is CPU-bound, and the backend may run a single tokio worker thread.
pub async fn finish(read: WindowRead) -> WindowMetrics {
    tokio::task::spawn_blocking(move || read.into_metrics())
        .await
        .unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
}

pub async fn read_window(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Result<WindowRead, sqlx::Error> {
    let stock: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT \
            count(*) FILTER (WHERE n.created_at >= $2 AND n.created_at < $3)::bigint, \
            count(*) FILTER (WHERE n.created_at >= $2 AND n.created_at < $3 AND n.coral_count = 0)::bigint, \
            count(*)::bigint, \
            count(*) FILTER (WHERE n.coral_count = 0)::bigint, \
            count(*) FILTER (WHERE n.coral_count = 1)::bigint \
           FROM nodes n \
          WHERE n.graph_id = $1 AND n.type = 'entity'",
    )
    .bind(graph_id)
    .bind(window_start)
    .bind(window_end)
    .fetch_one(&mut **tx)
    .await?;

    let new_corals: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM atoms \
          WHERE graph_id = $1 AND created_at >= $2 AND created_at < $3",
    )
    .bind(graph_id)
    .bind(window_start)
    .bind(window_end)
    .fetch_one(&mut **tx)
    .await?;

    let self_id: Option<Uuid> = sqlx::query_scalar("SELECT self_node_id FROM graphs WHERE id = $1")
        .bind(graph_id)
        .fetch_one(&mut **tx)
        .await?;

    let edges: (i64, i64) = sqlx::query_as(
        "SELECT count(*)::bigint, \
                count(*) FILTER (WHERE src_node_id = $4 OR dst_node_id = $4)::bigint \
           FROM relationship_edge e \
          WHERE e.graph_id = $1 AND e.created_at >= $2 AND e.created_at < $3 \
            AND e.status = 'confirmed'",
    )
    .bind(graph_id)
    .bind(window_start)
    .bind(window_end)
    .bind(self_id)
    .fetch_one(&mut **tx)
    .await?;

    let dead_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT j.kind, count(*)::bigint \
           FROM jobs j \
          WHERE j.status = 'dead' \
            AND COALESCE(j.finished_at, j.created_at) >= $2 \
            AND COALESCE(j.finished_at, j.created_at) < $3 \
            AND ( \
                  (j.payload->>'graph_id' ~ $4 AND (j.payload->>'graph_id')::uuid = $1) \
               OR (j.payload->>'atom_id' ~ $4 AND EXISTS ( \
                      SELECT 1 FROM atoms a \
                       WHERE a.atom_id = (j.payload->>'atom_id')::uuid AND a.graph_id = $1)) \
               OR (j.payload->>'node_id' ~ $4 AND EXISTS ( \
                      SELECT 1 FROM nodes n \
                       WHERE n.node_id = (j.payload->>'node_id')::uuid AND n.graph_id = $1)) \
            ) \
          GROUP BY j.kind \
          ORDER BY 2 DESC",
    )
    .bind(graph_id)
    .bind(window_start)
    .bind(window_end)
    .bind(UUID_RE)
    .fetch_all(&mut **tx)
    .await?;

    let titles = sqlx::query_as::<_, (String, bool, bool)>(
        "SELECT n.title, \
                (n.created_at >= $2 AND n.created_at < $3) AS is_new, \
                COALESCE(n.node_id = g.self_node_id, false) \
           FROM nodes n JOIN graphs g ON g.id = n.graph_id \
          WHERE n.graph_id = $1 AND n.type = 'entity'",
    )
    .bind(graph_id)
    .bind(window_start)
    .bind(window_end)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|(title, is_new, is_self)| EntityTitle { title, is_new, is_self })
    .collect();

    Ok(WindowRead {
        graph_id,
        window_start,
        window_end,
        stock,
        new_corals,
        edges,
        dead_rows,
        titles,
    })
}

pub async fn compute_on_pool(
    pool: &PgPool,
    graph_id: Uuid,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Result<WindowMetrics, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let read = read_window(&mut tx, graph_id, window_start, window_end).await;
    let _ = tx.rollback().await;
    Ok(finish(read?).await)
}

/// Enqueue a `HealthViolation` once per graph/alarm/ISO week, then mark it
/// `done` so it is an audit row rather than a stuck pending job.
pub async fn record_violations(
    pool: &PgPool,
    account_id: Option<Uuid>,
    m: &WindowMetrics,
    paging: &[Alarm],
) -> Result<u64, String> {
    let week = iso_week_key(m.window_start);
    let mut wrote = 0u64;
    for alarm in paging {
        let key = format!(
            "health-violation:{}:{}:{}",
            m.graph_id,
            alarm.kind.as_str(),
            week
        );
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE idempotency_key = $1)")
                .bind(&key)
                .fetch_one(pool)
                .await
                .map_err(|e| format!("idempotency lookup failed: {e}"))?;
        if exists {
            continue;
        }
        let kind = JobKind::HealthViolation {
            graph_id: m.graph_id,
            alarm: alarm.kind.as_str().to_string(),
        };
        let id = queue::enqueue(pool, &kind, account_id, Some(&key), 0)
            .await
            .map_err(|e| format!("enqueue HealthViolation failed: {e}"))?;
        let Some(id) = id else {
            continue;
        };
        let payload = serde_json::json!({
            "graph_id": m.graph_id,
            "alarm": alarm.kind,
            "value": alarm.value,
            "threshold": alarm.threshold,
            "week": week,
            "metrics": to_json(m, paging),
        });
        queue::complete_with_payload(pool, id, JobOutcome::DidWork, payload)
            .await
            .map_err(|e| format!("complete HealthViolation failed: {e}"))?;
        wrote += 1;
    }
    Ok(wrote)
}

/// Refuse `--execute` against the live local backend DB and the frozen copy.
pub async fn assert_execute_db(pool: &PgPool) -> Result<(), String> {
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("current_database failed: {e}"))?;
    if name == "coralswarm_prodclone" || name == "coralswarm_kg_spike" {
        return Err(format!(
            "REFUSING --execute against `{name}`. Point MIGRATOR_DATABASE_URL at \
             coralswarm_kg_guard (scratch) or another writable copy. Dry-run is allowed."
        ));
    }
    Ok(())
}

pub async fn assert_policy_bypass(pool: &PgPool) -> Result<(), String> {
    crate::self_reef_backfill::assert_policy_bypass(pool).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subtopic::whole_word_contains;
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
    use std::time::Instant;

    /// The pre-COR-96 O(new × older) scan, kept as the oracle.
    fn contain_hits_pairwise(titles: &[EntityTitle]) -> i64 {
        let older: Vec<&str> = titles
            .iter()
            .filter(|t| !t.is_new && !t.is_self)
            .map(|t| t.title.as_str())
            .collect();
        titles
            .iter()
            .filter(|t| t.is_new && older.iter().any(|p| whole_word_contains(&t.title, p)))
            .count() as i64
    }

    fn entity(title: &str, is_new: bool, is_self: bool) -> EntityTitle {
        EntityTitle { title: title.into(), is_new, is_self }
    }

    #[test]
    fn contain_hits_counts_new_titles_against_older_non_self_titles() {
        let titles = vec![
            entity("Alex Example", false, true),
            entity("React", false, false),
            entity("Postgres", true, false),
            entity("React 19 upgrade", true, false),
            entity("React", true, false),
            entity("Alex Example coding session", true, false),
            entity("Postgres tuning", true, false),
            entity("coralswarm-connect plugin", true, false),
            entity("CoralSwarm", false, false),
        ];
        // Only "React 19 upgrade" hits: self is never a parent, a new title
        // is never a parent, an equal title never contains itself, and `-`
        // joins words.
        assert_eq!(contain_hits(&titles), 1);
        assert_eq!(contain_hits_pairwise(&titles), 1);
    }

    /// Entity-like titles: 1 to 5 words from a fixed vocabulary, mixed case,
    /// some hyphen-joined, some with punctuation. About a third of the new
    /// titles extend an older title, so the scan finds real hits.
    fn synthetic_titles(rng: &mut StdRng, older: usize, new: usize) -> Vec<EntityTitle> {
        const SYLLABLES: &[&str] = &[
            "co", "ral", "swarm", "re", "act", "post", "gres", "ka", "mi", "to", "lu", "xen",
            "dra", "vo", "nex", "qui", "bel", "tor", "ion", "zu",
        ];
        let vocab: Vec<String> = (0..4000)
            .map(|_| {
                (0..rng.gen_range(1..4))
                    .map(|_| *SYLLABLES.choose(rng).unwrap())
                    .collect()
            })
            .collect();
        let title = |rng: &mut StdRng| -> String {
            let mut out = String::new();
            for i in 0..rng.gen_range(1..6) {
                if i > 0 {
                    out.push_str(if rng.gen_bool(0.1) { "-" } else { " " });
                }
                let w = vocab.choose(rng).unwrap();
                match rng.gen_range(0..4) {
                    0 => out.push_str(&w.to_uppercase()),
                    1 => {
                        let mut c = w.chars();
                        out.extend(c.next().map(|f| f.to_ascii_uppercase()));
                        out.push_str(c.as_str());
                    }
                    _ => out.push_str(w),
                }
            }
            if rng.gen_bool(0.05) {
                out.push_str(" (v2)");
            }
            out
        };
        let mut titles: Vec<EntityTitle> = (0..older)
            .map(|_| EntityTitle { title: title(rng), is_new: false, is_self: false })
            .collect();
        titles.push(entity("Alex Example", false, true));
        for _ in 0..new {
            let t = if rng.gen_bool(0.33) {
                let parent = titles[rng.gen_range(0..older)].title.clone();
                format!("{parent} {}", title(rng))
            } else {
                title(rng)
            };
            titles.push(EntityTitle { title: t, is_new: true, is_self: false });
        }
        titles.shuffle(rng);
        titles
    }

    #[test]
    fn contain_hits_matches_pairwise_on_synthetic_graphs() {
        let mut rng = StdRng::seed_from_u64(96);
        for _ in 0..20 {
            let titles = synthetic_titles(&mut rng, 400, 150);
            let hits = contain_hits(&titles);
            assert!(hits > 0);
            assert_eq!(hits, contain_hits_pairwise(&titles));
        }
    }

    /// The backend can run one tokio worker thread, so the title scan must not
    /// run on it: other tasks on a single-threaded runtime keep being polled
    /// while `finish` works.
    #[tokio::test(flavor = "current_thread")]
    async fn finish_leaves_the_runtime_free() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let mut rng = StdRng::seed_from_u64(7);
        let read = WindowRead {
            graph_id: Uuid::nil(),
            window_start: DateTime::<Utc>::UNIX_EPOCH,
            window_end: DateTime::<Utc>::UNIX_EPOCH,
            stock: (0, 0, 0, 0, 0),
            new_corals: 0,
            edges: (0, 0),
            dead_rows: vec![("Extract".into(), 2), ("Embed".into(), 3)],
            titles: synthetic_titles(&mut rng, 4_000, 1_500),
        };
        let polls = Arc::new(AtomicU64::new(0));
        let ticker = tokio::spawn({
            let polls = polls.clone();
            async move {
                loop {
                    polls.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            }
        });
        let metrics = finish(read).await;
        let polled = polls.load(Ordering::Relaxed);
        ticker.abort();
        assert!(polled > 0, "runtime was blocked while finish ran");
        assert!(metrics.contain_hits > 0);
        assert_eq!(metrics.dead_jobs, 5);
    }

    /// Production scale from the 2026-10-14 stall: graph `ac9befa1` had 3,723
    /// new and 12,704 older entity titles, 47M pairs. Ignored by default: the
    /// pairwise oracle takes minutes in a debug build. Run with
    /// `cargo test --release -p coralswarm-backend --lib contain_hits_production_scale -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn contain_hits_production_scale() {
        let mut rng = StdRng::seed_from_u64(0xac9b_efa1);
        let titles = synthetic_titles(&mut rng, 12_704, 3_723);

        let t = Instant::now();
        let hits = contain_hits(&titles);
        let indexed = t.elapsed();

        let t = Instant::now();
        let expected = contain_hits_pairwise(&titles);
        let pairwise = t.elapsed();

        println!(
            "contain_hits 3723 new x 12704 older: indexed {indexed:?}, pairwise {pairwise:?}, hits {hits}"
        );
        assert_eq!(hits, expected);
        assert!(indexed.as_secs_f64() < 1.0, "indexed scan took {indexed:?}");
    }

    fn m(new_reefs: i64, new_zero: i64, new_corals: i64, dead: i64) -> WindowMetrics {
        WindowMetrics {
            graph_id: Uuid::nil(),
            window_start: DateTime::<Utc>::UNIX_EPOCH,
            window_end: DateTime::<Utc>::UNIX_EPOCH,
            new_reefs,
            new_zero_coral: new_zero,
            new_corals,
            new_edges: 0,
            self_edges: 0,
            contain_hits: 0,
            dead_jobs: dead,
            entity_reefs: 0,
            zero_coral_stock: 0,
            single_coral_stock: 0,
            dead_jobs_by_kind: vec![],
        }
    }

    #[test]
    fn week_07_20_zero_coral_pages() {
        // 866 new reefs, 40.1% zero — first fire, coding-session start week.
        let metrics = m(866, 347, 1268, 0);
        let paging = evaluate(&metrics);
        assert_eq!(paging.len(), 1);
        assert_eq!(paging[0].kind, AlarmKind::ZeroCoralShare);
        assert!(paging[0].value > 0.40);
    }

    #[test]
    fn week_07_13_does_not_page_at_25_percent() {
        // 799 reefs, 20.5% zero — below the 25% page.
        let paging = evaluate(&m(799, 164, 2608, 0));
        assert!(paging.is_empty());
    }

    #[test]
    fn small_week_does_not_page() {
        // 16 reefs, 12.5% zero — below the 20-reef floor.
        assert!(evaluate(&m(16, 2, 40, 0)).is_empty());
    }

    #[test]
    fn week_08_03_reefs_per_coral_and_dead_page() {
        let metrics = m(1872, 1310, 1302, 145);
        let paging = evaluate(&metrics);
        let kinds: Vec<_> = paging.iter().map(|a| a.kind).collect();
        assert!(kinds.contains(&AlarmKind::ZeroCoralShare));
        assert!(kinds.contains(&AlarmKind::ReefsPerCoral));
        assert!(kinds.contains(&AlarmKind::DeadJobs));
        assert!(metrics.reefs_per_coral().unwrap() > 1.0);
    }

    #[test]
    fn dead_jobs_threshold() {
        assert!(evaluate(&m(0, 0, 0, 20)).is_empty());
        assert_eq!(evaluate(&m(0, 0, 0, 21))[0].kind, AlarmKind::DeadJobs);
    }
}

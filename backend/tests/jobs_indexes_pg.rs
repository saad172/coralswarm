//! The `jobs` claim indexes exist, are valid, and carry every hot idle poll on
//! both planes, with and without due rows.
//!
//!   REQUIRE_PG_SUITE=1 ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:55432/postgres \
//!     cargo test -p coralswarm-backend --test jobs_indexes_pg -- --test-threads=1

use coralswarm_backend::capture_admission_health::LIVE_CAPTURE;
use coralswarm_backend::test_db::{migrated_database, Plane, TestDatabase};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use std::str::FromStr;

const ACTIVE: &str = "jobs_active_kind_status_run_after";
const DONE: &str = "jobs_done_kind_finished";

/// A worker poll as its prepared statement: the parameter list and body after
/// `PREPARE poll`, the binds the worker passes, and the `jobs` indexes its plan
/// may name, in name order. The plan under test is the custom plan: the first
/// five executions of any prepared statement, and on tenant-coralswarm every
/// later one too, because the generic plan prices `kind = ANY($1)` at
/// thousands of rows and never wins.
struct HotPoll {
    name: &'static str,
    prepare: String,
    binds: &'static str,
    allowed_indexes: &'static [&'static str],
}

fn hot_polls() -> Vec<HotPoll> {
    vec![
        HotPoll { name: "A_ocean_health", prepare: "(int8) AS SELECT DISTINCT payload->>'graph_id' FROM jobs WHERE kind = 'OceanHealth' AND (status IN ('pending', 'running') OR (status = 'done' AND finished_at > now() - ($1::bigint * interval '1 second'))) AND payload->>'graph_id' IS NOT NULL".to_string(), binds: "(21600)", allowed_indexes: &[ACTIVE, DONE] },
        HotPoll { name: "B_chain_in_flight", prepare: " AS SELECT EXISTS (SELECT 1 FROM jobs WHERE kind = 'BackfillAtomTitles' AND status IN ('pending', 'running'))".to_string(), binds: "", allowed_indexes: &[ACTIVE] },
        HotPoll { name: "C_digest_accounts", prepare: "(text[], int8) AS SELECT account_id FROM public.jobs WHERE kind=ANY($1) AND status='pending' AND run_after<=now() AND account_id IS NOT NULL GROUP BY account_id ORDER BY min(run_after) LIMIT $2".to_string(), binds: "('{RegenerateDigest}', 16)", allowed_indexes: &[ACTIVE] },
        HotPoll { name: "C_digest_lane", prepare: "(text[], uuid, int8) AS SELECT j.id, COALESCE(j.payload->>'requested','')='true' AS requested, (n.digest_coral_count <= 0 OR (n.body->'digest'->>'managed_receipt') IS NULL) AS first_digest, n.coral_count::bigint FROM public.jobs j JOIN public.nodes n ON n.node_id = CASE WHEN j.payload->>'node_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (j.payload->>'node_id')::uuid END WHERE j.kind=ANY($1) AND j.status='pending' AND j.run_after<=now() AND ($2::uuid IS NULL OR j.account_id=$2) ORDER BY 2 DESC, 3 DESC, 4 DESC, j.run_after, j.id LIMIT $3".to_string(), binds: "('{RegenerateDigest}', NULL, 4)", allowed_indexes: &[ACTIVE] },
        HotPoll { name: "D_source_accounts", prepare: "(text[], int8, float8) AS SELECT account_id FROM public.jobs WHERE status='pending' AND run_after<=now() AND kind=ANY($1) AND account_id IS NOT NULL AND created_at>=now()-make_interval(secs=>$3) GROUP BY account_id ORDER BY max(created_at) DESC LIMIT $2".to_string(), binds: "('{ExtractMeetingAtoms,ExtractSourceAtoms,ExtractStandingMeetingAtoms,ExtractStandingSourceAtoms}', 16, 86400)", allowed_indexes: &[ACTIVE] },
        HotPoll { name: "D_source_lane", prepare: format!("(text[], float8, uuid, int8) AS SELECT id,lane_rank,pair_pending,captured_at FROM (SELECT j.id,j.run_after,v.captured_at,row_number() OVER (PARTITION BY v.graph_id,v.source_type ORDER BY v.captured_at DESC,j.run_after,j.id) AS lane_rank,count(*) OVER (PARTITION BY v.graph_id,v.source_type) AS pair_pending FROM public.jobs j JOIN public.source_versions v ON v.version_id=CASE WHEN j.payload->>'source_version_id' ~* '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$' THEN (j.payload->>'source_version_id')::uuid END AND v.graph_id::text=j.payload->>'graph_id' WHERE j.status='pending' AND j.run_after<=now() AND j.kind=ANY($1) AND j.created_at>=now()-make_interval(secs=>$2) AND v.captured_at>=now()-make_interval(secs=>$2) AND {LIVE_CAPTURE} AND ($3::uuid IS NULL OR j.account_id=$3)) lane ORDER BY lane_rank,pair_pending,captured_at DESC,run_after,id LIMIT $4"), binds: "('{ExtractMeetingAtoms,ExtractSourceAtoms,ExtractStandingMeetingAtoms,ExtractStandingSourceAtoms}', 86400, NULL, 8)", allowed_indexes: &[ACTIVE] },
        HotPoll { name: "G_generic_dequeue", prepare: "(text[], int8) AS WITH claimed AS (SELECT id FROM jobs WHERE status = 'pending' AND run_after <= now() AND kind = ANY($1) ORDER BY run_after, id LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE jobs j SET status = 'running', started_at = now() FROM claimed WHERE j.id = claimed.id RETURNING j.id, j.kind, j.payload, j.attempts, j.account_id".to_string(), binds: "('{OceanHealth}', 8)", allowed_indexes: &[ACTIVE, "jobs_pkey"] },
    ]
}

const INVALID_INDEXES: &str = "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'jobs'::regclass AND NOT i.indisvalid";

const DONE_PCT: u32 = 82;
const DEAD_PCT: u32 = 14;
const PENDING_PCT: u32 = 3;
const SPREAD_MINUTES: u32 = 90 * 24 * 60;
const DUE_PER_KIND: u32 = 10;

fn seed_sql(rows: u32) -> Vec<String> {
    let done = DONE_PCT;
    let dead = DONE_PCT + DEAD_PCT;
    let pending = dead + PENDING_PCT;
    vec![
        "ALTER TABLE jobs SET (autovacuum_enabled = off)".to_string(),
        format!("INSERT INTO jobs(kind,payload,status,run_after,created_at) \
          SELECT CASE WHEN i % 1000 < 280 THEN 'RegenerateDigest' WHEN i % 1000 < 560 THEN 'ExtractEntities' WHEN i % 1000 < 700 THEN 'EmbedStatements' WHEN i % 1000 < 800 THEN 'ExtractSourceAtoms' WHEN i % 1000 < 880 THEN 'ExtractMeetingAtoms' WHEN i % 1000 < 940 THEN 'ReevaluateKind' WHEN i % 1000 < 980 THEN 'ClassifyKind' WHEN i % 1000 < 990 THEN 'DeleteSourceBlob' WHEN i % 1000 < 999 THEN 'OceanHealth' ELSE 'BackfillAtomTitles' END, \
            jsonb_build_object('graph_id', gen_random_uuid()::text, 'node_id', gen_random_uuid()::text, 'source_version_id', gen_random_uuid()::text, 'seed', i), 'pending', \
            CASE WHEN i % 100 >= {dead} AND i % 100 < {pending} THEN now() + interval '1 day' * (1 + i % 7) ELSE now() - interval '1 minute' * (i % {SPREAD_MINUTES}) END, \
            now() - interval '1 minute' * (i % {SPREAD_MINUTES}) \
          FROM generate_series(1,{rows}) i"),
        format!("UPDATE jobs SET status = CASE WHEN (payload->>'seed')::int % 100 < {done} THEN 'done' WHEN (payload->>'seed')::int % 100 < {dead} THEN 'dead' ELSE 'running' END::job_status, \
            started_at = run_after, finished_at = CASE WHEN (payload->>'seed')::int % 100 < {dead} THEN run_after END \
          WHERE NOT ((payload->>'seed')::int % 100 >= {dead} AND (payload->>'seed')::int % 100 < {pending})"),
        format!("INSERT INTO jobs(kind,payload,status,run_after,created_at,account_id) \
          SELECT k.kind, jsonb_build_object('graph_id', gen_random_uuid()::text, 'node_id', gen_random_uuid()::text, 'source_version_id', gen_random_uuid()::text), 'pending', now(), now(), gen_random_uuid() \
          FROM generate_series(1,{DUE_PER_KIND}) i, unnest(ARRAY['RegenerateDigest','ExtractSourceAtoms','OceanHealth']) AS k(kind)"),
    ]
}

/// What `CREATE INDEX CONCURRENTLY` on the live table produces: claim indexes
/// without the churn's dead entries, next to the pre-existing pending indexes
/// that keep theirs until a vacuum (autovacuum is off so none runs mid-test).
fn rebuild_claim_indexes_sql() -> [String; 3] {
    [format!("REINDEX INDEX {ACTIVE}"), format!("REINDEX INDEX {DONE}"), "ANALYZE jobs".to_string()]
}

async fn plane_pool(plane: Plane) -> Option<(PgPool, TestDatabase)> {
    let Ok(url) = std::env::var("ORGT_TEST_BASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "ORGT_TEST_BASE_URL required");
        eprintln!("skipping jobs_indexes_pg: set ORGT_TEST_BASE_URL");
        return None;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"), "test database must be local");
    let db = migrated_database(&opts, plane, "jobs_indexes").await;
    let pool = PgPoolOptions::new().max_connections(1).connect_with(opts.database(db.name())).await.unwrap();
    Some((pool, db))
}

#[tokio::test]
async fn jobs_indexes_are_valid_on_both_planes() {
    for plane in [Plane::Tenant, Plane::Control] {
        let Some((pool, _db)) = plane_pool(plane).await else { return };
        let valid: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='jobs'::regclass AND c.relname IN ($1,$2) AND i.indisvalid AND i.indisready")
            .bind(ACTIVE).bind(DONE).fetch_one(&pool).await.unwrap();
        assert_eq!(valid, 2, "{plane:?}: both concurrent builds must have run to a valid index");
        let invalid: Vec<String> = sqlx::query_scalar(INVALID_INDEXES).fetch_all(&pool).await.unwrap();
        assert!(invalid.is_empty(), "{plane:?}: INVALID indexes on jobs: {invalid:?}");
        let definitions: Vec<String> = sqlx::query_scalar("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid='jobs'::regclass AND indexrelid::regclass::text IN ($1,$2) ORDER BY 1")
            .bind(ACTIVE).bind(DONE).fetch_all(&pool).await.unwrap();
        assert_eq!(definitions, [
            format!("CREATE INDEX {ACTIVE} ON public.jobs USING btree (kind, status, run_after) WHERE (status = ANY (ARRAY['pending'::job_status, 'running'::job_status]))"),
            format!("CREATE INDEX {DONE} ON public.jobs USING btree (kind, finished_at) WHERE (status = 'done'::job_status)"),
        ], "{plane:?}");
        let options: Option<Vec<String>> = sqlx::query_scalar("SELECT reloptions FROM pg_class WHERE oid='jobs'::regclass").fetch_one(&pool).await.unwrap();
        assert!(options.unwrap_or_default().iter().any(|o| o == "parallel_workers=0"), "{plane:?}: jobs must not plan parallel scans, the claim polls fork workers otherwise");
        pool.close().await;
    }
}

#[tokio::test]
async fn hot_job_polls_use_the_partial_indexes() {
    for plane in [Plane::Tenant, Plane::Control] {
        let Some((pool, _db)) = plane_pool(plane).await else { return };
        // One statement per round trip: in one transaction REINDEX would keep the just-updated tuple versions.
        for statement in seed_sql(200_000).into_iter().chain(rebuild_claim_indexes_sql()) {
            sqlx::raw_sql(&statement).execute(&pool).await.unwrap();
        }
        let jobs_indexes: Vec<String> = sqlx::query_scalar("SELECT indexname FROM pg_indexes WHERE tablename='jobs' ORDER BY 1").fetch_all(&pool).await.unwrap();
        for HotPoll { name, prepare, binds, allowed_indexes } in hot_polls() {
            sqlx::raw_sql(&format!("PREPARE poll{prepare}")).execute(&pool).await.unwrap();
            let rows = sqlx::raw_sql(&format!("EXPLAIN (COSTS OFF) EXECUTE poll{binds}")).fetch_all(&pool).await.unwrap();
            sqlx::raw_sql("DEALLOCATE poll").execute(&pool).await.unwrap();
            let plan = rows.iter().map(|row| row.get::<String, _>(0)).collect::<Vec<_>>().join("\n");
            let named: Vec<&str> = jobs_indexes.iter().map(String::as_str).filter(|index| plan.contains(index)).collect();
            assert_eq!(named, allowed_indexes, "{plane:?} {name}: jobs must be read through the claim index alone:\n{plan}");
            assert!(!plan.contains("Gather"), "{plane:?} {name}: a poll must not fork parallel workers:\n{plan}");
        }
        pool.close().await;
    }
}

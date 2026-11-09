//! Live test for the one-shot `purge_desktop_data` (ADR 0015): the core
//! (`coralswarm_backend::desktop_purge`) on both planes, and the compiled
//! binary end to end on the control plane.
//!
//! Each test gets its own freshly migrated database (`test_db`), so the
//! whole-database counts are exact. The fixture holds one work-session
//! source (two chunk atoms, a raw blob, a superseded blob and a malformed
//! superseded key), one work atom with no source item, a work session with no
//! atoms, a work session that also holds a coding atom (kept), one
//! coding-session source, a reef shared by work and coding evidence, a reef
//! backed only by work evidence, and the derived read surfaces the platform
//! deletion invalidates (the accepted collateral): a conversation with an
//! assistant answer, an answered query, a query_history row, an MCP task,
//! an extraction_cache row and a conversation op_log row.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55491/postgres \
//!     cargo test -p coralswarm-backend --test desktop_purge_pg

use std::str::FromStr;

use coralswarm_backend::desktop_purge::{purge_database, survey, Collateral, PurgeOpts, Survey};
use coralswarm_backend::test_db::{migrated_database, Plane, TestDatabase};
use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

const DRY: PurgeOpts = PurgeOpts { execute: false, only_graph: None, batch: 1 };
const EXECUTE: PurgeOpts = PurgeOpts { execute: true, only_graph: None, batch: 1 };

fn base_options() -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "RELINFER_TEST_DATABASE_URL required");
        eprintln!("skipping desktop purge test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    Some(PgConnectOptions::from_str(&url).unwrap())
}

async fn fresh_database(base: &PgConnectOptions, plane: Plane) -> (PgPool, TestDatabase) {
    let name = migrated_database(base, plane, "desktop_purge").await;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(base.clone().database(&name))
        .await
        .unwrap();
    (pool, name)
}

fn database_url(name: &str) -> String {
    let base_url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    format!("{}/{name}", base_url.rsplit_once('/').unwrap().0)
}

struct Fixture {
    account: Uuid,
    graph: Uuid,
    work_atoms: Vec<Uuid>,
    coding_atom: Uuid,
    coding_item: Uuid,
    kept_session_atom: Uuid,
    shared_reef: Uuid,
    work_reef: Uuid,
    work_blob_keys: Vec<String>,
    conversation: Uuid,
    assistant_message: Uuid,
    query: Uuid,
    mcp_task: Uuid,
}

/// A blob key in this graph: `{graph}/{id}` on the control plane,
/// `{org}/{graph}/{id}` on an org tenant (what the purge drain expects).
fn blob_key(plane: Plane, graph: Uuid) -> String {
    match plane {
        Plane::Control => format!("{graph}/{}", Uuid::new_v4()),
        Plane::Tenant => format!("{ORG}/{graph}/{}", Uuid::new_v4()),
    }
}

const ORG: &str = "org_desktop_purge";
const MALFORMED_KEY: &str = "not-a-blob-key";

async fn atom(pool: &PgPool, key: String, graph: Uuid, source_type: &str, source_id: &str, superseded: &[String]) -> (Uuid, String) {
    let id = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp,superseded_blob_keys) \
         VALUES($1,$2,$3,$4,now(),$5) RETURNING atom_id",
    )
    .bind(graph).bind(source_type).bind(source_id).bind(&key).bind(superseded)
    .fetch_one(pool).await.unwrap();
    (id, key)
}

/// A source item with one version that owns `atoms` as its chunks.
async fn source(pool: &PgPool, graph: Uuid, user: Uuid, source_type: &str, source_id: &str, raw_blob_key: Option<&str>, atoms: &[Uuid]) -> Uuid {
    let (item, version) = (Uuid::new_v4(), Uuid::new_v4());
    let origin = json!({"service_id":"fixture","transport":"native","content_kind":source_type});
    let namespace = format!("declared:user:{user}");
    sqlx::query("INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(item).bind(graph).bind(source_type).bind(&namespace).bind(source_id).bind(&origin)
        .execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO source_versions(version_id,source_item_id,graph_id,source_id,source_type,provider_namespace,raw_inline,raw_blob_key,origin) \
         VALUES($1,$2,$3,$4,$5,$6,CASE WHEN $7::text IS NULL THEN 'checkpoint text' END,$7,$8)",
    )
    .bind(version).bind(item).bind(graph).bind(source_id).bind(source_type).bind(&namespace).bind(raw_blob_key).bind(&origin)
    .execute(pool).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$1 WHERE source_item_id=$2")
        .bind(version).bind(item).execute(pool).await.unwrap();
    for a in atoms {
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
            .bind(graph).bind(version).bind(a).execute(pool).await.unwrap();
    }
    item
}

async fn reef(pool: &PgPool, graph: Uuid, title: &str, atoms: &[Uuid]) -> Uuid {
    let id: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id")
        .bind(graph).bind(title).fetch_one(pool).await.unwrap();
    for a in atoms {
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(id).bind(a).execute(pool).await.unwrap();
    }
    id
}

async fn session(pool: &PgPool, graph: Uuid, session_id: &str, kind: &str) {
    sqlx::query("INSERT INTO coding_sessions(graph_id,session_id,session_kind) VALUES($1,$2,$3)")
        .bind(graph).bind(session_id).bind(kind).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id) \
         VALUES(gen_random_uuid(),'coding_session',$2,'upsert','{}',1,now(),'cloud',$1)",
    )
    .bind(graph).bind(session_id).execute(pool).await.unwrap();
}

/// A control-plane user and account (the tenant plane has no FK to either).
async fn control_account(pool: &PgPool) -> (Uuid, Uuid) {
    let user = Uuid::new_v4();
    sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)").bind(user)
        .bind(format!("{user}@fixture.invalid")).execute(pool).await.unwrap();
    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
        .bind(user).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
        .bind(account).bind(user).execute(pool).await.unwrap();
    (user, account)
}

async fn seed(pool: &PgPool, plane: Plane) -> Fixture {
    let (user, account) = match plane {
        Plane::Tenant => {
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1) ON CONFLICT DO NOTHING")
                .bind(ORG).execute(pool).await.unwrap();
            (Uuid::new_v4(), Uuid::new_v4())
        }
        Plane::Control => control_account(pool).await,
    };
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'desktop purge','member',$2) RETURNING id")
        .bind(account).bind(user).fetch_one(pool).await.unwrap();

    session(pool, graph, "w1", "work").await;
    // A work session whose atoms are already gone: only the explicit session
    // delete reaches it, since the cascade drops a session with its last atom.
    session(pool, graph, "w2", "work").await;
    // A 'work' session that also holds a coding atom (session_kind is fixed
    // at first upsert): kept, with its history, and reported.
    session(pool, graph, "w3", "work").await;
    session(pool, graph, "c1", "coding").await;

    let key = || blob_key(plane, graph);
    let superseded = key();
    let raw = key();
    let (w0, k0) = atom(pool, key(), graph, "work_session", "cc:w1:cp1:chunk:0", &[superseded.clone(), MALFORMED_KEY.to_string()]).await;
    let (w1, k1) = atom(pool, key(), graph, "work_session", "cc:w1:cp1:chunk:1", &[]).await;
    source(pool, graph, user, "work_session", "cc:w1:cp1", Some(&raw), &[w0, w1]).await;
    // A work atom whose source item is already gone.
    let (orphan, k2) = atom(pool, key(), graph, "work_session", "cc:w1:cp0:chunk:0", &[]).await;

    let (coding_atom, _) = atom(pool, key(), graph, "coding_session", "cc:c1:cp1:chunk:0", &[]).await;
    let coding_item = source(pool, graph, user, "coding_session", "cc:c1:cp1", None, &[coding_atom]).await;
    let (kept_session_atom, _) = atom(pool, key(), graph, "coding_session", "cc:w3:cp1:chunk:0", &[]).await;

    let shared_reef = reef(pool, graph, "Shared reef", &[w0, coding_atom]).await;
    let work_reef = reef(pool, graph, "Work-only reef", &[w1, orphan]).await;

    // Derived read surfaces in this graph and account: the accepted collateral.
    let conversation: Uuid = sqlx::query_scalar(
        "INSERT INTO conversations(account_id,graph_id,user_id,title) VALUES($1,$2,$3,'Desktop chat') RETURNING id",
    )
    .bind(account).bind(graph).bind(user).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,0,'user','what did I do?')")
        .bind(conversation).execute(pool).await.unwrap();
    let assistant_message: Uuid = sqlx::query_scalar(
        "INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES($1,1,'assistant','You worked on X.') RETURNING id",
    )
    .bind(conversation).fetch_one(pool).await.unwrap();
    let query: Uuid = sqlx::query_scalar(
        "INSERT INTO queries(account_id,user_id,text,status,answer) VALUES($1,$2,'q','complete','an answer') RETURNING id",
    )
    .bind(account).bind(user).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO query_history(account_id,graph_id,user_id,query_text) VALUES($1,$2,$3,'q')")
        .bind(account).bind(graph).bind(user).execute(pool).await.unwrap();
    let mcp_task: Uuid = sqlx::query_scalar(
        "INSERT INTO mcp_tasks(account_id,graph_id,owner_user_id,tool_name) VALUES($1,$2,$3,'ask_ocean') RETURNING id",
    )
    .bind(account).bind(graph).bind(user).fetch_one(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO extraction_cache(account_id,content_hash,task,model,prompt_version,response_content) \
         VALUES($1,'h','extract_entities','m',1,'{}')",
    )
    .bind(account).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id) \
         VALUES(gen_random_uuid(),'conversation',$2,'upsert','{}',1,now(),'cloud',$1)",
    )
    .bind(graph).bind(conversation.to_string()).execute(pool).await.unwrap();

    Fixture {
        account,
        graph,
        work_atoms: vec![w0, w1, orphan],
        coding_atom,
        coding_item,
        kept_session_atom,
        shared_reef,
        work_reef,
        work_blob_keys: vec![k0, k1, k2, superseded, raw],
        conversation,
        assistant_message,
        query,
        mcp_task,
    }
}

const EXPECTED: Survey = Survey {
    source_items: 1,
    atoms: 3,
    coding_sessions: 2,
    sessions_kept: 1,
    session_op_log: 2,
    reefs_to_delete: 1,
    blob_keys: 5,
    blob_keys_skipped: 1,
    collateral: Collateral {
        graphs: 1,
        accounts: 1,
        conversations: 1,
        assistant_messages: 1,
        query_answers: 1,
        query_history: 1,
        journal_rollups: 0,
        mcp_tasks: 1,
        extraction_cache: 1,
        op_log: 1,
    },
};

/// Row counts of every table the purge may write, and the state of the
/// derived surfaces it may invalidate in place.
async fn snapshot(pool: &PgPool) -> Vec<i64> {
    let mut out = Vec::new();
    for table in ["atoms", "source_items", "source_versions", "nodes", "node_atoms", "coding_sessions", "op_log", "jobs",
                  "source_purge_events", "source_purge_dependencies", "query_history", "extraction_cache"] {
        out.push(sqlx::query_scalar(&format!("SELECT count(*) FROM {table}")).fetch_one(pool).await.unwrap());
    }
    for sql in ["SELECT count(*) FROM conversations WHERE title<>''",
                "SELECT count(*) FROM conversation_messages WHERE text<>'[Evidence no longer available]'",
                "SELECT count(*) FROM queries WHERE answer IS NOT NULL",
                "SELECT count(*) FROM mcp_tasks WHERE status<>'failed'"] {
        out.push(sqlx::query_scalar(sql).fetch_one(pool).await.unwrap());
    }
    out
}

async fn exists(pool: &PgPool, sql: &str, id: Uuid) -> bool {
    sqlx::query_scalar(&format!("SELECT EXISTS({sql})")).bind(id).fetch_one(pool).await.unwrap()
}

async fn assert_purged(pool: &PgPool, f: &Fixture) {
    for a in &f.work_atoms {
        assert!(!exists(pool, "SELECT 1 FROM atoms WHERE atom_id=$1", *a).await, "work atom {a} survived");
    }
    let work: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM source_items WHERE source_type='work_session' AND graph_id=$1), \
                (SELECT count(*) FROM coding_sessions WHERE session_kind='work' AND session_id IN ('w1','w2') AND graph_id=$1), \
                (SELECT count(*) FROM op_log WHERE entity='coding_session' AND entity_id IN ('w1','w2') AND graph_id=$1)",
    )
    .bind(f.graph).fetch_one(pool).await.unwrap();
    assert_eq!(work, (0, 0, 0), "work source items, sessions and session history are gone");

    assert!(exists(pool, "SELECT 1 FROM atoms WHERE atom_id=$1", f.coding_atom).await, "coding atom kept");
    assert!(exists(pool, "SELECT 1 FROM source_items WHERE source_item_id=$1", f.coding_item).await, "coding source kept");
    let coding: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coding_sessions WHERE session_id='c1' AND graph_id=$1), \
                (SELECT count(*) FROM op_log WHERE entity='coding_session' AND entity_id='c1' AND graph_id=$1)",
    )
    .bind(f.graph).fetch_one(pool).await.unwrap();
    assert_eq!(coding, (1, 1), "the coding session and its history are kept");

    // The 'work' session that also holds a coding atom is kept, with it.
    let kept: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM coding_sessions WHERE session_id='w3' AND session_kind='work' AND graph_id=$1), \
                (SELECT count(*) FROM op_log WHERE entity='coding_session' AND entity_id='w3' AND graph_id=$1)",
    )
    .bind(f.graph).fetch_one(pool).await.unwrap();
    assert_eq!(kept, (1, 1), "a work session with a coding atom is kept, with its history");
    assert!(exists(pool, "SELECT 1 FROM atoms WHERE atom_id=$1", f.kept_session_atom).await, "its coding atom is kept");

    assert!(exists(pool, "SELECT 1 FROM nodes WHERE node_id=$1", f.shared_reef).await, "a reef with coding evidence stays");
    assert!(exists(pool, "SELECT 1 FROM node_atoms WHERE node_id=$1", f.shared_reef).await, "and keeps its coding link");
    assert!(!exists(pool, "SELECT 1 FROM nodes WHERE node_id=$1", f.work_reef).await, "a work-only reef is deleted");

    for key in &f.work_blob_keys {
        let queued: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE kind='DeleteSourceBlob' \
             AND (payload->>'graph_id')::uuid=$1 AND payload->>'blob_key'=$2)",
        )
        .bind(f.graph).bind(key).fetch_one(pool).await.unwrap();
        assert!(queued, "blob {key} is queued for deletion");
    }
    let malformed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE payload->>'blob_key'=$1)")
        .bind(MALFORMED_KEY).fetch_one(pool).await.unwrap();
    assert!(!malformed, "a malformed key is skipped, never queued");
}

/// The accepted collateral was applied: what the dry run reported is what
/// the deletion invalidated.
async fn assert_collateral_applied(pool: &PgPool, f: &Fixture) {
    let title: String = sqlx::query_scalar("SELECT title FROM conversations WHERE id=$1")
        .bind(f.conversation).fetch_one(pool).await.unwrap();
    assert_eq!(title, "", "the conversation title is blanked");
    let text: String = sqlx::query_scalar("SELECT text FROM conversation_messages WHERE id=$1")
        .bind(f.assistant_message).fetch_one(pool).await.unwrap();
    assert_eq!(text, "[Evidence no longer available]", "the assistant answer is overwritten");
    let answer: Option<String> = sqlx::query_scalar("SELECT answer FROM queries WHERE id=$1")
        .bind(f.query).fetch_one(pool).await.unwrap();
    assert_eq!(answer, None, "the account's query answer is nulled");
    let status: String = sqlx::query_scalar("SELECT status FROM mcp_tasks WHERE id=$1")
        .bind(f.mcp_task).fetch_one(pool).await.unwrap();
    assert_eq!(status, "failed", "the MCP task is marked failed");
    let gone: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM query_history WHERE graph_id=$1), \
                (SELECT count(*) FROM extraction_cache WHERE account_id=$2), \
                (SELECT count(*) FROM op_log WHERE graph_id=$1 AND entity IN ('query','conversation','conversation_message'))",
    )
    .bind(f.graph).bind(f.account).fetch_one(pool).await.unwrap();
    assert_eq!(gone, (0, 0, 0), "query history, extraction cache and conversation op_log rows are deleted");
}

async fn exercise(plane: Plane) {
    let Some(base) = base_options() else { return };
    let (pool, _name) = fresh_database(&base, plane).await;
    let f = seed(&pool, plane).await;

    // Dry run: exact counts (including the invalidation), no writes.
    let before = snapshot(&pool).await;
    let dry = purge_database(&pool, None, &DRY).await.unwrap();
    assert_eq!(dry.before, EXPECTED);
    assert_eq!(dry.affected, vec![(f.graph, f.account)]);
    assert!(dry.clean(&DRY));
    assert_eq!(snapshot(&pool).await, before, "a dry run writes nothing");

    // Execute: work data gone, coding data and the shared reef kept, blobs queued.
    let run = purge_database(&pool, (plane == Plane::Control).then_some(&pool), &EXECUTE).await.unwrap();
    assert_eq!(run.before, EXPECTED);
    assert_eq!(run.purged.errors, 0);
    assert_eq!(run.purged.source_items, 1);
    assert_eq!(run.purged.leftover_sources, 1);
    assert_eq!(run.purged.atoms, 3);
    assert_eq!(run.purged.session_op_log, 2);
    assert_eq!(run.purged.coding_sessions, 1, "the cascade removed w1 with its last atom");
    assert_eq!(run.purged.superseded_blobs_queued, 1);
    assert_eq!(run.purged.superseded_blobs_skipped, 1);
    assert!(run.after.is_empty(), "nothing remains: {:?}", run.after);
    assert_eq!(run.after.sessions_kept, 1, "the kept session is still reported");
    assert_eq!(run.after.collateral, Collateral::default(), "no graph holds work data any more");
    assert!(run.clean(&EXECUTE));
    assert_purged(&pool, &f).await;
    assert_collateral_applied(&pool, &f).await;

    // The worker's drain of the queued purge dependencies (what
    // `workers::source_blob_cleanup` runs per account) completes and keeps
    // the reef that still has coding evidence.
    drain(&pool, f.account).await;
    let stuck: i64 = sqlx::query_scalar("SELECT count(*) FROM source_purge_dependencies WHERE processed_at IS NULL")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(stuck, 0, "the worker drains every purge dependency");
    assert_purged(&pool, &f).await;

    // Rerun: nothing to do, and still clean.
    let after_first = snapshot(&pool).await;
    assert!(survey(&pool, None).await.unwrap().is_empty());
    let rerun = purge_database(&pool, None, &EXECUTE).await.unwrap();
    assert!(rerun.before.is_empty() && rerun.after.is_empty());
    assert_eq!((rerun.purged.source_items, rerun.purged.leftover_sources, rerun.purged.atoms, rerun.purged.coding_sessions), (0, 0, 0, 0));
    assert!(rerun.clean(&EXECUTE));
    assert_eq!(snapshot(&pool).await, after_first, "a rerun writes nothing");
}

#[tokio::test]
async fn control_plane_purge_removes_only_work_session_data() {
    exercise(Plane::Control).await;
}

#[tokio::test]
async fn tenant_plane_purge_removes_only_work_session_data() {
    exercise(Plane::Tenant).await;
}

/// `--only-graph` touches one ocean: another account's graph keeps its work
/// data and its derived surfaces.
#[tokio::test]
async fn only_graph_leaves_other_accounts_untouched() {
    let Some(base) = base_options() else { return };
    let (pool, _name) = fresh_database(&base, Plane::Control).await;
    let a = seed(&pool, Plane::Control).await;
    let b = seed(&pool, Plane::Control).await;
    let only_a = PurgeOpts { execute: true, only_graph: Some(a.graph), batch: 1 };

    let dry = purge_database(&pool, None, &PurgeOpts { execute: false, ..only_a }).await.unwrap();
    assert_eq!(dry.before, EXPECTED, "the dry run counts only ocean A");
    assert_eq!(dry.affected, vec![(a.graph, a.account)]);

    let run = purge_database(&pool, Some(&pool), &only_a).await.unwrap();
    assert_eq!(run.purged.errors, 0);
    assert!(run.after.is_empty(), "{:?}", run.after);
    assert_purged(&pool, &a).await;
    assert_collateral_applied(&pool, &a).await;

    assert_eq!(survey(&pool, Some(b.graph)).await.unwrap(), EXPECTED, "ocean B still holds all its work data");
    for atom in &b.work_atoms {
        assert!(exists(&pool, "SELECT 1 FROM atoms WHERE atom_id=$1", *atom).await, "B's work atom {atom} kept");
    }
    let title: String = sqlx::query_scalar("SELECT title FROM conversations WHERE id=$1")
        .bind(b.conversation).fetch_one(&pool).await.unwrap();
    assert_eq!(title, "Desktop chat", "B's conversation is not invalidated");
    let answer: Option<String> = sqlx::query_scalar("SELECT answer FROM queries WHERE id=$1")
        .bind(b.query).fetch_one(&pool).await.unwrap();
    assert_eq!(answer.as_deref(), Some("an answer"), "B's account keeps its query answers");
    let status: String = sqlx::query_scalar("SELECT status FROM mcp_tasks WHERE id=$1")
        .bind(b.mcp_task).fetch_one(&pool).await.unwrap();
    assert_eq!(status, "working", "B's MCP task is untouched");
}

#[tokio::test]
async fn preflight_refuses_a_role_under_rls() {
    let Some(base) = base_options() else { return };
    let (pool, name) = fresh_database(&base, Plane::Control).await;
    let role = name.role("desktop_purge");
    sqlx::query(&format!("CREATE ROLE {role} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD 'fixture-only'"))
        .execute(&pool).await.unwrap();
    sqlx::query(&format!("GRANT coralswarm_app TO {role}")).execute(&pool).await.unwrap();
    let app = PgPoolOptions::new().max_connections(1)
        .connect_with(base.clone().database(&name).username(&role).password("fixture-only"))
        .await.unwrap();
    let err = purge_database(&app, None, &DRY).await.unwrap_err();
    assert!(err.to_string().contains("REFUSING"), "{err}");
    app.close().await;
    sqlx::query(&format!("DROP OWNED BY {role}")).execute(&pool).await.unwrap();
    sqlx::query(&format!("DROP ROLE {role}")).execute(&pool).await.unwrap();
}

async fn drain(pool: &PgPool, account: Uuid) {
    loop {
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account).await.unwrap();
        let drained: i32 = sqlx::query_scalar("SELECT library_drain_purge(100)").fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        if drained == 0 {
            return;
        }
    }
}

fn run_binary(url: &str, args: &[&str], env: &[(&str, &str)]) -> (i32, String) {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_purge_desktop_data"));
    cmd.args(args)
        .env("MIGRATOR_DATABASE_URL", url)
        .env_remove("DATABASE_URL")
        .env_remove("MASTER_KEK_HEX");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    (out.status.code().unwrap(), String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr))
}

const DRY_SUMMARY: &str = "summary: db=control mode=dry-run source_items=1 atoms=3 coding_sessions=2 sessions_kept=1 \
op_log=2 reefs_to_delete=1 blob_keys=5 blob_keys_skipped=1 inv_graphs=1 inv_accounts=1 inv_conversations=1 \
inv_assistant_messages=1 inv_query_answers=1 inv_query_history=1 inv_journal_rollups=0 inv_mcp_tasks=1 \
inv_extraction_cache=1 inv_op_log=1";

const ZERO_SUMMARY: &str = "summary: db=TOTAL mode=dry-run source_items=0 atoms=0 coding_sessions=0 sessions_kept=1 \
op_log=0 reefs_to_delete=0 blob_keys=0 blob_keys_skipped=0 inv_graphs=0 inv_accounts=0 inv_conversations=0 \
inv_assistant_messages=0 inv_query_answers=0 inv_query_history=0 inv_journal_rollups=0 inv_mcp_tasks=0 \
inv_extraction_cache=0 inv_op_log=0";

#[tokio::test]
async fn binary_dry_run_execute_and_rerun() {
    let Some(base) = base_options() else { return };
    let (pool, name) = fresh_database(&base, Plane::Control).await;
    let f = seed(&pool, Plane::Control).await;
    let url = database_url(&name);

    let (code, out) = run_binary(&url, &["--execute", "--dry-run"], &[]);
    assert_eq!(code, 2, "{out}");

    let before = snapshot(&pool).await;
    let (code, out) = run_binary(&url, &[], &[]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("work_session source_items: 1"), "{out}");
    assert!(out.contains(DRY_SUMMARY), "{out}");
    assert!(out.contains(&format!("graph {} (account {})", f.graph, f.account)), "{out}");
    assert_eq!(snapshot(&pool).await, before, "the binary's dry run writes nothing");

    let (code, out) = run_binary(&url, &["--execute"], &[]);
    assert_eq!(code, 0, "{out}");
    assert_purged(&pool, &f).await;
    assert_collateral_applied(&pool, &f).await;

    let (code, out) = run_binary(&url, &["--dry-run"], &[]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains(ZERO_SUMMARY), "{out}");
}

/// `--only` applies the `routing::list_active` gate, and the `tenant_meta`
/// fingerprint refuses a DSN that points at another org's database.
#[tokio::test]
async fn binary_only_refuses_inactive_revoked_and_swapped_tenants() {
    let Some(base) = base_options() else { return };
    let (control, control_name) = fresh_database(&base, Plane::Control).await;
    let (tenant, tenant_name) = fresh_database(&base, Plane::Tenant).await;
    sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_somebody_else')")
        .execute(&tenant).await.unwrap();

    // Routing rows without the billing/account harness: replica mode skips
    // the binding triggers and FKs for this fixture connection only.
    let mut conn = control.acquire().await.unwrap();
    sqlx::query("SET session_replication_role = replica").execute(&mut *conn).await.unwrap();
    for (org, slug, secret, status) in [
        ("org_suspended", "suspended-co", "TENANT_DB_SUSPENDED_CO", "suspended"),
        ("org_revoked", "revoked-co", "TENANT_DB_REVOKED_CO", "active"),
        ("org_routing", "swapped-co", "TENANT_DB_SWAPPED_CO", "active"),
    ] {
        sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES($1,$2,$3,$4,$5)")
            .bind(org).bind(Uuid::new_v4()).bind(slug).bind(secret).bind(status)
            .execute(&mut *conn).await.unwrap();
    }
    sqlx::query("INSERT INTO org_revocations(clerk_org_id) VALUES('org_revoked')").execute(&mut *conn).await.unwrap();
    sqlx::query("SET session_replication_role = DEFAULT").execute(&mut *conn).await.unwrap();
    drop(conn);

    let url = database_url(&control_name);
    let tenant_url = database_url(&tenant_name);
    let env = [("TENANT_DB_SWAPPED_CO", tenant_url.as_str())];

    let (code, out) = run_binary(&url, &["--only", "suspended-co"], &env);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("is not active or its org is revoked"), "{out}");

    let (code, out) = run_binary(&url, &["--only", "org_revoked"], &env);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("is not active or its org is revoked"), "{out}");

    let (code, out) = run_binary(&url, &["--only", "swapped-co"], &env);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("DSN-swap guard"), "{out}");
    assert!(!out.contains("db=tenant:swapped-co"), "the swapped database is never surveyed: {out}");
}

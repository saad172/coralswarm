//! Dynamic half of the capture-funnel coverage guard (the static half is
//! `tests/capture_funnel_guard.rs`, which proves every connector writes
//! through [`store_capture_tx`]). Each connector's exact capture
//! configuration — header shape, sync ops, scope, provenance — is run through the core on both planes, as the table owner
//! (how prod tenant DBs connect) and as the restricted app role, and must
//! reach every stage: a source version, standing admission, chunks linked to
//! the version, fallback titles and excerpts, and an embed job covering every
//! chunk. A re-capture of unchanged content must add nothing.
//!
//! Requires `ORGT_TEST_BASE_URL` (a local PostgreSQL); with
//! `REQUIRE_PG_SUITE=1` a missing database fails instead of skipping.

use super::*;
use crate::store::PostgresStore;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plane {
    Tenant,
    Control,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Owner,
    App,
}

struct Fixture {
    admin: PgPool,
    owner: PgPool,
    pool: PgPool,
    db: crate::test_db::TestDatabase,
    account: Uuid,
    user: Uuid,
    graph: Uuid,
}

impl Fixture {
    async fn new(plane: Plane, role: Role) -> Option<Self> {
        let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "ORGT_TEST_BASE_URL required");
            eprintln!("capture_funnel pg_tests skipped: set ORGT_TEST_BASE_URL");
            return None;
        };
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let admin = PgPoolOptions::new().max_connections(2).connect_with(opts.clone()).await.unwrap();
        let db = crate::test_db::TestDatabase::new(&opts, "capture_funnel");
        sqlx::query(&format!("CREATE DATABASE {db}")).execute(&admin).await.unwrap();
        let owner = PgPoolOptions::new().max_connections(4).connect_with(opts.clone().database(&db)).await.unwrap();
        match plane {
            Plane::Tenant => sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap(),
            Plane::Control => sqlx::migrate!("./migrations_control").run(&owner).await.unwrap(),
        }
        let pool = match role {
            Role::Owner => owner.clone(),
            Role::App => {
                let set_role = match plane {
                    Plane::Tenant => "SET ROLE coralswarm_tenant_app",
                    Plane::Control => "SET ROLE coralswarm_app",
                };
                let pool = PgPoolOptions::new()
                    .max_connections(4)
                    .after_connect(move |conn, _| {
                        Box::pin(async move {
                            sqlx::query(set_role).execute(conn).await?;
                            Ok(())
                        })
                    })
                    .connect_with(opts.database(&db))
                    .await
                    .unwrap();
                let restricted: bool = sqlx::query_scalar(
                    "SELECT NOT rolsuper AND NOT rolbypassrls FROM pg_roles WHERE rolname=current_user",
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                assert!(restricted, "the app role must be subject to RLS");
                pool
            }
        };
        let user = Uuid::new_v4();
        let account = Uuid::new_v4();
        if plane == Plane::Control {
            sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
                .bind(user)
                .bind(format!("{user}@funnel.test"))
                .execute(&owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)")
                .bind(account)
                .bind(user)
                .execute(&owner)
                .await
                .unwrap();
        }
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'funnel') RETURNING id")
            .bind(account)
            .fetch_one(&owner)
            .await
            .unwrap();
        Some(Self { admin, owner, pool, db, account, user, graph })
    }

    async fn done(self) {
        self.pool.close().await;
        self.owner.close().await;
        sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db))
            .execute(&self.admin)
            .await
            .ok();
    }
}

/// One connector's capture configuration, as its writer builds it.
struct Shape {
    connector: &'static str,
    source_type: &'static str,
    namespace: &'static str,
    representation: &'static str,
    header: Header,
    hints: CaptureHints,
    scope: &'static str,
    sync_ops: bool,
    with_actor: bool,
    expect_title: Option<&'static str>,
}

const BODY: &str = "We decided to ship the search pilot on Friday. Morgan owns the rollout and the \
    feature flag.\n\nCasey will watch error rates for a week before widening the cohort.";

fn shapes() -> Vec<Shape> {
    vec![
        // `ingest::submit`'s private arm (HTTP, MCP, sessions, every in-process caller).
        Shape {
            connector: "http_funnel",
            source_type: "note",
            namespace: "declared:user:x",
            representation: "normalized_text",
            header: Header::Recorded("Project: search · 2026-10-18".into()),
            hints: CaptureHints::default(),
            scope: "private",
            sync_ops: true,
            with_actor: true,
            expect_title: Some("We decided to ship the search pilot on Friday"),
        },
        // `routes::composio` (through the HTTP funnel, with recipe hints).
        Shape {
            connector: "composio",
            source_type: "linear_comment",
            namespace: "linear:workspace:w",
            representation: "normalized_text",
            header: Header::None,
            hints: CaptureHints {
                chunk: Some(ChunkStrategy::Generic),
                title: Some("Comment on COR-7: Ship search".into()),
                ..CaptureHints::default()
            },
            scope: "private",
            sync_ops: true,
            with_actor: true,
            expect_title: Some("Comment on COR-7: Ship search"),
        },
        // `connectors::github_backfill`.
        Shape {
            connector: "github",
            source_type: "github_pr",
            namespace: "github:repository:1",
            representation: "normalized_text",
            header: Header::None,
            hints: CaptureHints::default(),
            scope: "private",
            sync_ops: true,
            with_actor: true,
            expect_title: Some("We decided to ship the search pilot on Friday"),
        },
        // `routes::slack_events` (unknown actor, as for an unmapped author).
        Shape {
            connector: "slack",
            source_type: "slack",
            namespace: "slack:workspace:T1",
            representation: "normalized_text",
            header: Header::Inline("Alice in #eng · 2026-10-18 09:00 UTC".into()),
            hints: CaptureHints::default(),
            scope: "account",
            sync_ops: false,
            with_actor: false,
            expect_title: Some("We decided to ship the search pilot on Friday"),
        },
        // `workers::huddle` (Generic chunks for a `transcript` source).
        Shape {
            connector: "huddle",
            source_type: "transcript",
            namespace: "slack:workspace:T1",
            representation: "provider_summary",
            header: Header::Recorded("Meeting on 2026-10-18 with Alice, Bob. Duration: 12 min.".into()),
            hints: CaptureHints { chunk: Some(ChunkStrategy::Generic), ..CaptureHints::default() },
            scope: "account",
            sync_ops: false,
            with_actor: true,
            expect_title: Some("We decided to ship the search pilot on Friday"),
        },
        // `connectors::google_chat`.
        Shape {
            connector: "google_chat",
            source_type: "google_chat",
            namespace: "google_chat:space:spaces/A",
            representation: "normalized_text",
            header: Header::Inline("Alice in Eng · 2026-10-18 09:00 UTC".into()),
            hints: CaptureHints::default(),
            scope: "account",
            sync_ops: false,
            with_actor: true,
            expect_title: Some("We decided to ship the search pilot on Friday"),
        },
    ]
}

/// The source's own event time: fixed, so a re-capture is unchanged.
fn occurred() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-10-18T09:00:00Z").unwrap().with_timezone(&Utc)
}

async fn capture(fx: &Fixture, shape: &Shape, source_id: &str) -> Stored {
    capture_then(fx, shape, source_id, true).await
}

/// [`capture`], then commit or roll back the capture's transaction.
async fn capture_then(fx: &Fixture, shape: &Shape, source_id: &str, commit: bool) -> Stored {
    let store = PostgresStore::new(fx.pool.clone());
    let participants = vec!["Alice".to_string()];
    let metadata = serde_json::json!({"connector": shape.connector});
    let content_hash = crate::services::source_reingest::content_hash_hex(BODY);
    let strategy = chunk_strategy(shape.source_type, None, shape.hints.chunk.as_ref());
    let chunks = coralswarm_core::chunker::chunk(BODY, strategy);
    let prepared = Prepared::new(source_id, shape.source_type, BODY, &chunks, &shape.header, shape.hints.title.as_deref());
    let actor = shape.with_actor.then_some(fx.user);
    let key = embed_key(fx.graph, shape.source_type, source_id, &content_hash);
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&fx.pool, fx.account).await.unwrap();
    let mut uploads = Vec::new();
    let stored = store_capture_tx(
        &mut tx,
        Destination { graph_id: fx.graph, account_id: fx.account, org_id: None, dek: &[7; 32], r2: None },
        &CaptureRequest {
            source: CaptureSource {
                source_type: shape.source_type,
                provider_namespace: shape.namespace,
                source_id,
                normalized_text: BODY,
                representation: shape.representation,
                metadata: &metadata,
                occurred_at: Some(occurred()),
                participants: &participants,
                ingested_by: actor,
                scope: shape.scope,
                duration_ms: None,
                segments: None,
            },
            origin: None,
            chunks: &prepared,
            atoms: AtomFields {
                scope: shape.scope,
                content_hash: Some(&content_hash),
                channel_id: Some("C1"),
                channel_name: Some("eng"),
                author_id: Some("U1"),
                author_name: Some("Alice"),
                ..AtomFields::new(shape.source_type, &participants, occurred(), actor)
            },
            embed_key: key,
            sync_ops: shape.sync_ops.then_some(&store as &dyn crate::store::Store),
        },
        &mut uploads,
    )
    .await
    .unwrap_or_else(|e| panic!("{}: {e}", shape.connector));
    if commit {
        tx.commit().await.unwrap();
    } else {
        tx.rollback().await.unwrap();
    }
    assert!(uploads.is_empty(), "dev mode uploads nothing");
    stored
}

fn meet_origin(record: &str) -> SourceOrigin {
    SourceOrigin::native("google_meet", "transcript", "google_meet_attendee_capture").unwrap()
        .with_container("conference_record", record, "google_meet:conference_records", None).unwrap()
}

async fn capture_transcript(fx: &Fixture, record: &str, body: &str, native: bool) -> Stored {
    try_capture_transcript(fx, record, body, native, true).await.unwrap()
}

async fn try_capture_transcript(fx: &Fixture, record: &str, body: &str, native: bool, commit: bool) -> Result<Stored, FunnelError> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&fx.pool, fx.account).await.unwrap();
    let stored = capture_transcript_tx(fx, &mut tx, record, body, native, "declared:user:meet-test").await;
    if commit && stored.is_ok() { tx.commit().await.unwrap(); } else { tx.rollback().await.unwrap(); }
    stored
}

async fn capture_transcript_tx(
    fx: &Fixture, tx: &mut Transaction<'_, Postgres>, record: &str, body: &str, native: bool, namespace: &str,
) -> Result<Stored, FunnelError> {
    let source_id = format!("meet:{record}");
    let origin = meet_origin(record);
    let participants = vec!["Alice".into()];
    let metadata = serde_json::json!({"body_label": body.len()});
    let chunks = coralswarm_core::chunker::chunk(body, ChunkStrategy::Transcript);
    let prepared = Prepared::new(&source_id, "meet", body, &chunks, &Header::Recorded("Meeting with Alice".into()), None);
    let store = PostgresStore::new(fx.pool.clone());
    let mut uploads = Vec::new();
    let stored = store_capture_tx(
        tx,
        Destination { graph_id: fx.graph, account_id: fx.account, org_id: None, dek: &[7; 32], r2: None },
        &CaptureRequest {
            source: CaptureSource {
                source_type: "meet", provider_namespace: namespace, source_id: &source_id,
                normalized_text: body, representation: "normalized_text", metadata: &metadata,
                occurred_at: Some(occurred()), participants: &participants, ingested_by: Some(fx.user),
                scope: "private", duration_ms: None, segments: None,
            },
            origin: native.then_some(&origin), chunks: &prepared,
            atoms: AtomFields::new("meet", &participants, occurred(), Some(fx.user)),
            embed_key: embed_key(fx.graph, "meet", &source_id, &crate::services::source_reingest::content_hash_hex(body)),
            sync_ops: Some(&store),
        },
        &mut uploads,
    ).await;
    assert!(uploads.is_empty());
    stored
}

/// Includes authoritative bytes/pointers, projection, family membership and
/// downstream work. A replay must leave every one of them unchanged.
async fn transcript_snapshot(fx: &Fixture) -> serde_json::Value {
    sqlx::query_scalar(
        "SELECT jsonb_build_object(
          'items',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.source_item_id) FROM source_items s WHERE s.graph_id=$1),
          'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY v.version_id) FROM source_versions v WHERE v.graph_id=$1),
          'chunks',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.source_version_id,c.atom_id) FROM source_version_chunks c WHERE c.graph_id=$1),
          'atoms',(SELECT jsonb_agg(to_jsonb(a) ORDER BY a.atom_id) FROM atoms a WHERE a.graph_id=$1),
          'meetings',(SELECT jsonb_agg(to_jsonb(m) ORDER BY m.source_id) FROM meeting_transcripts m WHERE m.graph_id=$1),
          'admissions',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.source_version_id) FROM corpus_capture_admissions c WHERE c.source_version_id IN (SELECT v.version_id FROM source_versions v WHERE v.graph_id=$1)),
          'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY j.id) FROM jobs j WHERE j.account_id=$2),
          'ops',(SELECT jsonb_agg(to_jsonb(o) ORDER BY o.op_id) FROM op_log o WHERE o.graph_id=$1))",
    ).bind(fx.graph).bind(fx.account).fetch_one(&fx.owner).await.unwrap()
}

async fn assert_serialized_transcript_race(fx: &Fixture, record: &str, first_body: &str, second_body: &str, commit_first: bool) {
    let mut first_tx = crate::tenancy::scope::begin_tenant_system_scoped(&fx.pool, fx.account).await.unwrap();
    let first = capture_transcript_tx(fx, &mut first_tx, record, first_body, true, "declared:user:meet-test").await.unwrap();
    let mut second_tx = crate::tenancy::scope::begin_tenant_system_scoped(&fx.pool, fx.account).await.unwrap();
    let second_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *second_tx).await.unwrap();
    let (second, ()) = tokio::join!(
        async {
            let stored = capture_transcript_tx(fx, &mut second_tx, record, second_body, true, "declared:user:meet-test").await.unwrap();
            second_tx.commit().await.unwrap();
            stored
        },
        async {
            // Witness PostgreSQL's actual advisory-lock wait before releasing
            // the first transaction. Elapsed time alone never counts as proof.
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted)")
                        .bind(second_pid).fetch_one(&fx.owner).await.unwrap();
                    if waiting { break; }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }).await.expect("second delivery must wait on the first capture's identity lock");
            if commit_first { first_tx.commit().await.unwrap(); } else { first_tx.rollback().await.unwrap(); }
        },
    );
    if commit_first {
        assert_eq!((second.source_item_id, second.version_id), (first.source_item_id, first.version_id));
        assert!(!second.version_changed && second.written().is_empty() && second.enqueued_in_transaction == 0);
        assert_eq!(second.reconciled.linked, first.reconciled.linked);
    } else {
        assert!(second.version_changed && second.version_id != first.version_id && second.source_item_id != first.source_item_id);
        assert!(!second.written().is_empty());
    }
    let versions: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1 AND source_id=$2")
        .bind(fx.graph).bind(format!("meet:{record}")).fetch_one(&fx.owner).await.unwrap();
    assert_eq!(versions, 1, "only the committed winner may have a version");
}

async fn native_meet_is_first_write_wins(plane: Plane, role: Role) {
    let Some(fx) = Fixture::new(plane, role).await else { return };
    let short = "Alice: First original transcript.";
    let long = (0..400).map(|i| format!("Bob: Alternate transcript turn {i} has unrelated words and details.\n")).collect::<String>();
    // Both directions matter: later chunks must neither append to a short
    // original nor retire surplus positions of a long original.
    for (record, first_body, changed_body) in [("conferenceRecords/short", short, long.as_str()), ("conferenceRecords/long", long.as_str(), short)] {
        let first = capture_transcript(&fx, record, first_body, true).await;
        assert!(first.version_changed && !first.inserted.is_empty());
        if record.ends_with("long") { assert!(first.inserted.len() > 1); }
        let snapshot = transcript_snapshot(&fx).await;
        for body in [changed_body, first_body] {
            let replay = capture_transcript(&fx, record, body, true).await;
            assert_eq!((replay.source_item_id, replay.version_id), (first.source_item_id, first.version_id));
            assert!(!replay.version_changed && replay.written().is_empty() && replay.inserted.is_empty() && replay.enqueued_in_transaction == 0);
            let mut original = first.reconciled.linked.clone();
            let mut reused = replay.reconciled.linked;
            original.sort(); reused.sort();
            assert_eq!(reused, original);
            assert_eq!(transcript_snapshot(&fx).await, snapshot, "replay may not change the committed capture or enqueue work");
        }
        for (native, namespace) in [(false, "declared:user:meet-test"), (false, "other:namespace"), (true, "other:namespace")] {
            let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&fx.pool, fx.account).await.unwrap();
            let err = capture_transcript_tx(&fx, &mut tx, record, changed_body, native, namespace).await.unwrap_err();
            tx.rollback().await.unwrap();
            assert_eq!(err.stage, "source version", "an existing native artifact cannot be made mutable or claimed by another namespace");
            assert_eq!(transcript_snapshot(&fx).await, snapshot);
        }
    }
    // The same public source type without the trusted native origin retains
    // the generic edit path, including replacement and surplus retirement.
    let generic = capture_transcript(&fx, "conferenceRecords/generic", &long, false).await;
    let edited = capture_transcript(&fx, "conferenceRecords/generic", short, false).await;
    assert!(edited.version_changed && edited.version_id != generic.version_id);
    assert!(!edited.reconciled.replaced.is_empty() && !edited.reconciled.retired.is_empty());
    let before = transcript_snapshot(&fx).await;
    let foreign = try_capture_transcript(&fx, "conferenceRecords/generic", &long, true, true).await.unwrap_err();
    assert_eq!(foreign.stage, "source version", "an untrusted prior capture is not the native adapter's replay");
    assert_eq!(transcript_snapshot(&fx).await, before);

    // Changed deliveries must really overlap and wait: the first commit wins,
    // while a rolled-back first attempt must never become a replay target.
    assert_serialized_transcript_race(&fx, "conferenceRecords/race", short, &long, true).await;
    assert_serialized_transcript_race(&fx, "conferenceRecords/rollback", short, &long, false).await;

    // Reusing a committed partial snapshot never fills missing positions
    // from the later delivery's changed bytes.
    let partial = capture_transcript(&fx, "conferenceRecords/partial", &long, true).await;
    sqlx::query("DELETE FROM atoms WHERE graph_id=$1 AND atom_id=$2")
        .bind(fx.graph).bind(partial.inserted[0]).execute(&fx.owner).await.unwrap();
    let before = transcript_snapshot(&fx).await;
    let replay = capture_transcript(&fx, "conferenceRecords/partial", short, true).await;
    assert_eq!(replay.version_id, partial.version_id);
    assert_eq!(replay.reconciled.linked.len(), partial.inserted.len() - 1);
    assert_eq!(transcript_snapshot(&fx).await, before);

    // A foreign namespace or a legacy atom with no source version must fail
    // closed without claiming or changing the holding.
    sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'meet','foreign:namespace','meet:conferenceRecords/foreign')")
        .bind(fx.graph).execute(&fx.owner).await.unwrap();
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'meet','meet:conferenceRecords/legacy:chunk:0','dev:legacy','Alice: Original legacy transcript',$2)")
        .bind(fx.graph).bind(occurred()).execute(&fx.owner).await.unwrap();
    let before = transcript_snapshot(&fx).await;
    for record in ["conferenceRecords/foreign", "conferenceRecords/legacy"] {
        let err = try_capture_transcript(&fx, record, short, true, true).await.unwrap_err();
        assert_eq!(err.stage, "source version");
        assert_eq!(transcript_snapshot(&fx).await, before);
    }
    fx.done().await;
}

#[tokio::test]
async fn native_meet_is_first_write_wins_on_both_planes_and_roles() {
    for plane in [Plane::Control, Plane::Tenant] {
        for role in [Role::Owner, Role::App] {
            native_meet_is_first_write_wins(plane, role).await;
        }
    }
}

async fn assert_every_stage(fx: &Fixture, shape: &Shape, source_id: &str, stored: &Stored, label: &str) {
    let c = shape.connector;
    let like = format!("{source_id}:chunk:%");
    let atoms: Vec<(Uuid, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>)> =
        sqlx::query_as(
            "SELECT atom_id,title,title_kind,excerpt,content_inline,author_name FROM atoms
              WHERE graph_id=$1 AND source_type=$2 AND source_id LIKE $3 ORDER BY source_id",
        )
        .bind(fx.graph)
        .bind(shape.source_type)
        .bind(&like)
        .fetch_all(&fx.owner)
        .await
        .unwrap();
    assert!(!atoms.is_empty(), "{label} {c}: chunks written");
    assert_eq!(atoms[0].1.as_deref(), shape.expect_title, "{label} {c}: title of the first chunk");
    for (id, title, kind, excerpt, inline, author) in &atoms {
        assert!(title.as_deref().is_some_and(|t| !t.contains(" in #") && !t.starts_with("Meeting on")),
            "{label} {c}: title of {id} comes from the body: {title:?}");
        if shape.hints.title.is_some() {
            assert_eq!(title.as_deref(), shape.expect_title, "{label} {c}: hinted title on every chunk");
        }
        let expected_kind = if shape.hints.title.is_some() { "source" } else { "fallback" };
        assert_eq!(kind.as_deref(), Some(expected_kind), "{label} {c}: title kind (a hinted title is the source's own)");
        assert!(excerpt.as_deref().is_some_and(|e| !e.contains(" in #") && !e.starts_with("Meeting on")),
            "{label} {c}: excerpt from the body: {excerpt:?}");
        assert!(inline.is_some(), "{label} {c}: body stored");
        assert_eq!(author.as_deref(), Some("Alice"), "{label} {c}: provenance");
    }
    let ids: Vec<Uuid> = atoms.iter().map(|a| a.0).collect();
    let (versions, linked, admitted): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM source_versions WHERE graph_id=$1 AND source_id=$2),
                (SELECT count(*) FROM source_version_chunks WHERE graph_id=$1 AND source_version_id=$3 AND atom_id=ANY($4)),
                (SELECT count(*) FROM corpus_capture_admissions WHERE source_version_id=$3)",
    )
    .bind(fx.graph)
    .bind(source_id)
    .bind(stored.version_id)
    .bind(&ids)
    .fetch_one(&fx.owner)
    .await
    .unwrap();
    assert_eq!(versions, 1, "{label} {c}: one source version");
    assert_eq!(linked as usize, ids.len(), "{label} {c}: every chunk linked to the version");
    assert_eq!(admitted, 1, "{label} {c}: the version is standing-admissible");
    let embedded: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT jsonb_array_elements_text(payload->'atom_ids')::uuid FROM jobs WHERE kind='EmbedAtoms'",
    )
    .fetch_all(&fx.owner)
    .await
    .unwrap();
    assert!(ids.iter().all(|id| embedded.contains(id)), "{label} {c}: every chunk has an embed job");
    let ops: i64 = sqlx::query_scalar("SELECT count(*) FROM op_log WHERE entity_id=ANY($1)")
        .bind(ids.iter().map(Uuid::to_string).collect::<Vec<_>>())
        .fetch_one(&fx.owner)
        .await
        .unwrap();
    assert_eq!(ops > 0, shape.sync_ops, "{label} {c}: sync ops exactly where the connector always wrote them");
}

async fn every_connector_reaches_every_stage(plane: Plane, role: Role) {
    let Some(fx) = Fixture::new(plane, role).await else { return };
    let label = format!("{plane:?}/{role:?}");
    for shape in shapes() {
        let source_id = format!("{}-src", shape.connector);
        let first = capture(&fx, &shape, &source_id).await;
        assert!(first.version_changed && !first.inserted.is_empty(), "{label} {}: first capture writes", shape.connector);
        assert_every_stage(&fx, &shape, &source_id, &first, &label).await;
        let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='EmbedAtoms'").fetch_one(&fx.owner).await.unwrap();
        // Unchanged content: same version, no new rows, nothing to embed.
        let again = capture(&fx, &shape, &source_id).await;
        assert_eq!(again.version_id, first.version_id, "{label} {}: unchanged content re-adopts the version", shape.connector);
        assert!(!again.version_changed && again.inserted.is_empty() && again.written().is_empty());
        assert_eq!(again.duplicates(), first.inserted.len(), "{label} {}: every position adopted", shape.connector);
        let after: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='EmbedAtoms'").fetch_one(&fx.owner).await.unwrap();
        assert_eq!(after, jobs, "{label} {}: no re-embed for unchanged content", shape.connector);
    }
    fx.done().await;
}

/// ADR-0016 §4: the embed job is enqueued in the capture's own transaction
/// for every caller, so it commits with the rows it embeds and a rollback
/// leaves neither. Before, the HTTP door enqueued after its commit and a
/// failed enqueue left committed corals with no embed job.
async fn the_embed_job_commits_and_rolls_back_with_the_capture(plane: Plane, role: Role) {
    let Some(fx) = Fixture::new(plane, role).await else { return };
    let label = format!("{plane:?}/{role:?}");
    let landed = |source_id: String| {
        let (pool, graph) = (fx.owner.clone(), fx.graph);
        async move {
            sqlx::query_as::<_, (i64, i64)>(
                "SELECT (SELECT count(*) FROM jobs WHERE kind='EmbedAtoms' AND account_id=$3),
                        (SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_id LIKE $2 || ':chunk:%')",
            )
            .bind(graph)
            .bind(source_id)
            .bind(fx.account)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    for shape in shapes() {
        let c = shape.connector;
        let source_id = format!("{c}-rolled-back");
        let (jobs_before, _) = landed(source_id.clone()).await;
        let rolled_back = capture_then(&fx, &shape, &source_id, false).await;
        assert_eq!(rolled_back.enqueued_in_transaction, 1, "{label} {c}: the job was enqueued in the capture");
        assert_eq!(landed(source_id.clone()).await, (jobs_before, 0), "{label} {c}: a rollback leaves no job and no row");
        let committed = capture(&fx, &shape, &source_id).await;
        assert_eq!(committed.enqueued_in_transaction, 1, "{label} {c}");
        let (jobs_after, rows) = landed(source_id.clone()).await;
        assert_eq!(jobs_after, jobs_before + 1, "{label} {c}: the commit keeps exactly its own job");
        assert_eq!(rows as usize, committed.inserted.len(), "{label} {c}");
    }
    fx.done().await;
}

#[tokio::test]
async fn the_embed_job_commits_and_rolls_back_with_the_capture_on_both_planes_and_roles() {
    for plane in [Plane::Control, Plane::Tenant] {
        for role in [Role::Owner, Role::App] {
            the_embed_job_commits_and_rolls_back_with_the_capture(plane, role).await;
        }
    }
}

#[tokio::test]
async fn every_connector_reaches_every_stage_on_the_tenant_plane_as_owner() {
    every_connector_reaches_every_stage(Plane::Tenant, Role::Owner).await;
}

#[tokio::test]
async fn every_connector_reaches_every_stage_on_the_tenant_plane_as_app_role() {
    every_connector_reaches_every_stage(Plane::Tenant, Role::App).await;
}

#[tokio::test]
async fn every_connector_reaches_every_stage_on_the_consumer_plane_as_owner() {
    every_connector_reaches_every_stage(Plane::Control, Role::Owner).await;
}

#[tokio::test]
async fn every_connector_reaches_every_stage_on_the_consumer_plane_as_app_role() {
    every_connector_reaches_every_stage(Plane::Control, Role::App).await;
}

/// A trusted chunk hint changes how the source is split; the default keeps
/// the source type's strategy.
#[tokio::test]
async fn a_chunk_hint_is_honored_and_titles_follow_the_body_on_replacement() {
    let Some(fx) = Fixture::new(Plane::Tenant, Role::App).await else { return };
    let mut shape = shapes().remove(0);
    let quoted = "Thanks, rolling back now.\n> Did the deploy break login?\n> It did for SSO users.";
    shape.hints = CaptureHints { chunk: Some(ChunkStrategy::Email), ..CaptureHints::default() };
    let email = coralswarm_core::chunker::chunk(quoted, chunk_strategy("note", None, shape.hints.chunk.as_ref()));
    let generic = coralswarm_core::chunker::chunk(quoted, chunk_strategy("note", None, None));
    assert_eq!((email.len(), generic.len()), (2, 1), "the hint, not the source type, decides");
    // An edit replaces the chunk in place with a title from the new body.
    let first = capture(&fx, &shape, "edit-src").await;
    let store = PostgresStore::new(fx.pool.clone());
    let edited = "We decided to delay the search pilot until Monday. Morgan still owns it.";
    let chunks = coralswarm_core::chunker::chunk(edited, ChunkStrategy::Generic);
    let prepared = Prepared::new("edit-src", "note", edited, &chunks, &shape.header, None);
    let participants = vec!["Alice".to_string()];
    let metadata = serde_json::json!({"connector": "http_funnel"});
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&fx.pool, fx.account).await.unwrap();
    let stored = store_capture_tx(
        &mut tx,
        Destination { graph_id: fx.graph, account_id: fx.account, org_id: None, dek: &[7; 32], r2: None },
        &CaptureRequest {
            source: CaptureSource {
                source_type: "note", provider_namespace: shape.namespace, source_id: "edit-src",
                normalized_text: edited, representation: "normalized_text", metadata: &metadata,
                occurred_at: Some(Utc::now()), participants: &participants, ingested_by: Some(fx.user),
                scope: "private", duration_ms: None, segments: None,
            },
            origin: None,
            chunks: &prepared,
            atoms: AtomFields::new("note", &participants, Utc::now(), Some(fx.user)),
            embed_key: "embed:edit".into(),
            sync_ops: Some(&store),
        },
        &mut Vec::new(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(stored.version_changed && stored.version_id != first.version_id);
    assert_eq!(stored.reconciled.replaced, vec![first.inserted[0]], "position 0 replaced in place");
    let title: Option<String> = sqlx::query_scalar("SELECT title FROM atoms WHERE atom_id=$1")
        .bind(first.inserted[0])
        .fetch_one(&fx.owner)
        .await
        .unwrap();
    assert_eq!(title.as_deref(), Some("We decided to delay the search pilot until Monday"));
    fx.done().await;
}

/// Recovery for a chunk the old chunker let through over the embedder's
/// input limit (prod, 2026-10-23: one Gmail body, one atom). Stamped
/// `blocked:input_too_long` by the embed path, it is re-chunked from its
/// version's text: the position is replaced in place, the rest of its text
/// lands at new positions, all linked to the SAME version and queued for
/// embedding, and nothing is dropped.
async fn oversized_chunk_is_rechunked(plane: Plane) {
    let Some(fx) = Fixture::new(plane, Role::Owner).await else { return };
    let long = (0..80)
        .map(|i| format!("Paragraph {i}. The bond walkthrough for the north campus moves to the second week of March and Morgan owns the agenda."))
        .collect::<Vec<_>>()
        .join("\n\n");
    assert!(long.len() > 9000);
    // The old Email strategy's output: one chunk, whatever its length.
    let whole = vec![coralswarm_core::chunker::Chunk {
        text: long.clone(),
        position: 0,
        speaker: None,
        parent_message_id: None,
        token_count: 0,
        symbols: Vec::new(),
    }];
    let header = Header::Recorded("From: Morgan · 2026-10-22".into());
    let prepared = Prepared::new("msg-1", "gmail", &long, &whole, &header, None);
    let participants = vec!["Morgan".to_string()];
    let metadata = serde_json::json!({"connector": "gmail"});
    let content_hash = crate::services::source_reingest::content_hash_hex(&long);
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&fx.pool, fx.account).await.unwrap();
    let stored = store_capture_tx(
        &mut tx,
        Destination { graph_id: fx.graph, account_id: fx.account, org_id: None, dek: &[7; 32], r2: None },
        &CaptureRequest {
            source: CaptureSource {
                source_type: "gmail", provider_namespace: "gmail:user:u", source_id: "msg-1",
                normalized_text: &long, representation: "normalized_text", metadata: &metadata,
                occurred_at: Some(occurred()), participants: &participants, ingested_by: Some(fx.user),
                scope: "private", duration_ms: None, segments: None,
            },
            origin: None,
            chunks: &prepared,
            atoms: AtomFields {
                content_hash: Some(&content_hash),
                author_name: Some("Morgan"),
                ..AtomFields::new("gmail", &participants, occurred(), Some(fx.user))
            },
            embed_key: "embed:msg-1".into(),
            sync_ops: None,
        },
        &mut Vec::new(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let oversized = stored.inserted[0];
    sqlx::query("UPDATE atoms SET embed_model_next=$2,embed_attempts=$3,embed_failed_at=now() WHERE atom_id=$1")
        .bind(oversized)
        .bind(crate::workers::embed::BLOCKED_INPUT_TOO_LONG)
        .bind(crate::workers::embed::MAX_EMBED_ATTEMPTS)
        .execute(&fx.owner)
        .await
        .unwrap();
    sqlx::query("DELETE FROM jobs").execute(&fx.owner).await.unwrap();

    let dry = crate::chunk_reconcile::backfill::rechunk_oversized(&fx.owner, None, Some(fx.graph), false)
        .await
        .unwrap();
    let expected = coralswarm_core::chunker::chunk(&long, ChunkStrategy::Email).len() as u64;
    assert!(expected > 1);
    assert_eq!(
        (dry.atoms_stamped, dry.atoms_unbound, dry.versions, dry.positions_before, dry.positions_after),
        (1, 0, 1, 1, expected)
    );
    let atoms_after_dry: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1")
        .bind(fx.graph)
        .fetch_one(&fx.owner)
        .await
        .unwrap();
    assert_eq!(atoms_after_dry, 1, "a dry run writes nothing");

    let done = crate::chunk_reconcile::backfill::rechunk_oversized(&fx.owner, None, Some(fx.graph), true)
        .await
        .unwrap();
    assert_eq!((done.versions, done.versions_failed, done.still_stamped), (1, 0, 0));
    assert_eq!((done.replaced, done.inserted), (1, expected - 1));
    let rows: Vec<(Uuid, String, String, Option<i32>, Option<String>, i32, Option<String>, Vec<String>)> = sqlx::query_as(
        "SELECT a.atom_id,a.source_id,a.content_inline,a.header_len,a.embed_model_next,a.embed_attempts,a.author_name,a.participants
           FROM atoms a JOIN source_version_chunks c ON c.graph_id=a.graph_id AND c.atom_id=a.atom_id
          WHERE c.source_version_id=$1
          ORDER BY substring(a.source_id FROM '[0-9]+$')::int",
    )
    .bind(stored.version_id)
    .fetch_all(&fx.owner)
    .await
    .unwrap();
    assert_eq!(rows.len() as u64, expected, "every position linked to the same version");
    assert_eq!(rows[0].0, oversized, "the oversized position is replaced in place");
    let mut bodies = String::new();
    for (i, (_, source_id, text, header_len, stamp, attempts, author, people)) in rows.iter().enumerate() {
        assert_eq!(source_id, &format!("msg-1:chunk:{i}"));
        assert!(text.len() <= 8192, "position {i} is {} bytes", text.len());
        assert!(text.starts_with("From: Morgan · 2026-10-22\n\n"), "the header is carried");
        assert_eq!(*header_len, Some("From: Morgan · 2026-10-22".len() as i32 + 2));
        assert_eq!((stamp.as_deref(), *attempts), (None, 0), "a fresh embedding ladder");
        assert_eq!((author.as_deref(), people.clone()), (Some("Morgan"), vec!["Morgan".to_string()]));
        bodies.push_str(crate::store::strip_header_prefix(text, header_len.unwrap()));
        bodies.push(' ');
    }
    for i in 0..80 {
        assert!(bodies.contains(&format!("Paragraph {i}.")), "paragraph {i} lost");
    }
    let queued: Vec<serde_json::Value> = sqlx::query_scalar("SELECT payload->'atom_ids' FROM jobs WHERE kind='EmbedAtoms'")
        .fetch_all(&fx.owner)
        .await
        .unwrap();
    let queued: std::collections::BTreeSet<Uuid> = queued
        .into_iter()
        .flat_map(|ids| serde_json::from_value::<Vec<Uuid>>(ids).unwrap())
        .collect();
    assert_eq!(queued, rows.iter().map(|r| r.0).collect(), "every written position is queued for embedding");
    let versions: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
        .bind(fx.graph)
        .fetch_one(&fx.owner)
        .await
        .unwrap();
    assert_eq!(versions, 1, "no new version: the text did not change");

    let again = crate::chunk_reconcile::backfill::rechunk_oversized(&fx.owner, None, Some(fx.graph), true)
        .await
        .unwrap();
    assert_eq!(again.atoms_stamped, 0);
    fx.done().await;
}

#[tokio::test]
async fn an_oversized_chunk_is_rechunked_on_the_tenant_plane() {
    oversized_chunk_is_rechunked(Plane::Tenant).await;
}

#[tokio::test]
async fn an_oversized_chunk_is_rechunked_on_the_consumer_plane() {
    oversized_chunk_is_rechunked(Plane::Control).await;
}

//! The `OrgShared` half of the ingest door against a real database: a grant
//! allows only the oceans its routing record names, only while that record is
//! unchanged, the record is locked only at the capture's seal, and a routed
//! capture follows the door's rules (abort on a failed upload, the
//! meeting-summary enqueue).
//!
//! Requires `ORGT_TEST_BASE_URL` (a local PostgreSQL); with
//! `REQUIRE_PG_SUITE=1` a missing database fails instead of skipping.

use super::*;
use coralswarm_core::chunker::chunk;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

struct Fixture {
    admin: PgPool,
    pool: PgPool,
    /// Connect options for the fixture's database, for a pool of another size.
    opts: PgConnectOptions,
    db: crate::test_db::TestDatabase,
    account: Uuid,
    /// The account's owner.
    user: Uuid,
    /// The account's own primary ocean: not routed to.
    home: Uuid,
    /// The ocean `T1`/`C1` is routed to.
    routed: Uuid,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "ORGT_TEST_BASE_URL required");
            eprintln!("ingest pg_tests skipped: set ORGT_TEST_BASE_URL");
            return None;
        };
        let opts = PgConnectOptions::from_str(&base).unwrap();
        let admin = PgPoolOptions::new().max_connections(2).connect_with(opts.clone()).await.unwrap();
        let db = crate::test_db::TestDatabase::new(&opts, "ingest_door");
        sqlx::query(&format!("CREATE DATABASE {db}")).execute(&admin).await.unwrap();
        let opts = opts.database(&db);
        let pool = PgPoolOptions::new().max_connections(4).connect_with(opts.clone()).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let user: Uuid = sqlx::query_scalar("INSERT INTO users(id,email) VALUES(gen_random_uuid(),$1) RETURNING id")
            .bind(format!("{}@door.test", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .unwrap();
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user)
            .fetch_one(&pool)
            .await
            .unwrap();
        let graph = |primary: bool| {
            sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO graphs(account_id,name,kind,is_primary) VALUES($1,'ocean','personal',$2) RETURNING id",
            )
            .bind(account)
            .bind(primary)
            .fetch_one(&pool)
        };
        let home = graph(true).await.unwrap();
        let routed = graph(false).await.unwrap();
        sqlx::query(
            "INSERT INTO slack_watched_channels(account_id,user_id,mode,graph_id,workspace_id,channel_id,display_name) \
             VALUES($1,$2,'single',$3,'T1','C1','general')",
        )
        .bind(account)
        .bind(user)
        .bind(routed)
        .execute(&pool)
        .await
        .unwrap();
        Some(Self { admin, pool, opts, db, account, user, home, routed })
    }

    /// Make `user` a member of the account, as an org account's roster has it.
    async fn join(&self) {
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member') ON CONFLICT DO NOTHING")
            .bind(self.account)
            .bind(self.user)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn leave(&self) {
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
            .bind(self.account)
            .bind(self.user)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    /// A mailbox opt-in (`DIRECT_MESSAGE`, `single`, owned by `user`) routed
    /// to `routed`: the one Chat route whose verification asks the roster.
    async fn mailbox_route(&self, space_id: &str) {
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces(account_id,space_id,space_type,mode,graph_id,owner_user_id) \
             VALUES($1,$2,'DIRECT_MESSAGE','single',$3,$4)",
        )
        .bind(self.account)
        .bind(space_id)
        .bind(self.routed)
        .bind(self.user)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    async fn grant(&self) -> OrgSharedGrant {
        self.grant_to(self.routed).await
    }

    /// The grant for `T1`/`C1`, whose row currently routes to `ocean`.
    async fn grant_to(&self, ocean: Uuid) -> OrgSharedGrant {
        let (grant, unmapped) = OrgSharedGrant::slack_channel(
            &self.pool, &self.pool, &TenantId::Consumer, self.account, crate::routes::slack::AccountPlane::Personal, "T1", "C1",
        )
        .await
        .unwrap();
        assert_eq!((grant.oceans(), unmapped), (&[ocean][..], 0));
        grant
    }

    /// Submit one routed source of `source_type` into `ocean`.
    async fn write(
        &self,
        grant: &OrgSharedGrant,
        ocean: Uuid,
        namespace: &str,
        source_type: &str,
        representation: &str,
        r2: Option<&coralswarm_core::r2::R2Client>,
    ) -> Result<Ingested, IngestError> {
        self.write_on(&self.pool, &TenantId::Consumer, grant, ocean, namespace, source_type, representation, r2).await
    }

    /// [`Self::write`] on a pool and tenant of the test's choosing.
    #[allow(clippy::too_many_arguments)]
    async fn write_on(
        &self,
        pool: &PgPool,
        tenant: &TenantId,
        grant: &OrgSharedGrant,
        ocean: Uuid,
        namespace: &str,
        source_type: &str,
        representation: &str,
        r2: Option<&coralswarm_core::r2::R2Client>,
    ) -> Result<Ingested, IngestError> {
        let text = "Alice: the deploy window opens at ten.\nBob: then we ship after lunch.";
        let source_id = format!("{source_type}-1");
        let chunks = chunk(text, crate::ingest::capture::chunk_strategy(source_type, None, None));
        let prepared = Prepared::new(&source_id, source_type, text, &chunks, &crate::ingest::capture::Header::None, None);
        let metadata = json!({});
        let participants = ["Alice".to_string(), "Bob".to_string()];
        let now = Utc::now();
        submit(IngestRequest::new(Destination::OrgShared(Routed {
            ocean,
            grant,
            writer: SystemWriter { pool, tenant, dek: &[7u8; 32], r2 },
            source: CaptureSource {
                source_type,
                provider_namespace: namespace,
                source_id: &source_id,
                normalized_text: text,
                representation,
                metadata: &metadata,
                occurred_at: Some(now),
                participants: &participants,
                ingested_by: None,
                scope: "account",
                duration_ms: None,
                segments: None,
            },
            chunks: &prepared,
            atoms: AtomFields { scope: "account", ..AtomFields::new(source_type, &participants, now, None) },
            content_hash: &crate::services::source_reingest::content_hash_hex(text),
            sync_ops: None,
        })))
        .await
    }

    /// An active GitHub watch row bound to `routed`, chosen by `user`, and
    /// the grant built from it.
    async fn github_binding(&self) -> (Uuid, OrgSharedGrant) {
        let row_id: Uuid = sqlx::query_scalar(
            "INSERT INTO github_watched_repos(account_id,installation_id,repo_id,owner,name,active,graph_id,owner_user_id,destination_kind) \
             VALUES($1,7,42,'acme','docs',true,$2,$3,'personal') RETURNING id",
        )
        .bind(self.account)
        .bind(self.routed)
        .bind(self.user)
        .fetch_one(&self.pool)
        .await
        .unwrap();
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(self.pool.clone(), None));
        let binding = crate::connectors::github_backfill::load_binding(&self.pool, self.account, row_id).await.unwrap().expect("row");
        let grant = OrgSharedGrant::github_repo(&state, &TenantId::Consumer, &self.pool, self.account, &binding)
            .await
            .expect("bound and reachable");
        assert_eq!((grant.oceans(), grant.github_chooser()), (&[self.routed][..], Some(self.user)));
        (row_id, grant)
    }

    async fn stored(&self, ocean: Uuid) -> (i64, i64) {
        sqlx::query_as(
            "SELECT (SELECT count(*) FROM atoms WHERE graph_id=$1), (SELECT count(*) FROM source_items WHERE graph_id=$1)",
        )
        .bind(ocean)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn jobs(&self, kind: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind=$1")
            .bind(kind)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    async fn done(self) {
        self.pool.close().await;
        sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db))
            .execute(&self.admin)
            .await
            .ok();
    }
}

#[tokio::test]
async fn a_grant_never_reaches_an_ocean_its_route_does_not_name_pg() {
    let Some(f) = Fixture::new().await else { return };
    let grant = f.grant().await;

    let refused = f.write(&grant, f.home, "slack:workspace:T1", "slack", "normalized_text", None).await;
    assert!(matches!(refused, Err(IngestError::Forbidden(_))), "{:?}", refused.as_ref().err());
    assert_eq!(f.stored(f.home).await, (0, 0), "nothing lands in an ocean the grant does not name");

    let ingested = f.write(&grant, f.routed, "slack:workspace:T1", "slack", "normalized_text", None).await;
    let ingested = ingested.unwrap_or_else(|r| panic!("the routed ocean accepts the write: {r}"));
    assert!(!ingested.body.atom_ids.is_empty());
    assert_eq!(ingested.body.jobs_enqueued, 1, "the response counts the embed job it enqueued");
    assert_eq!(f.jobs("EmbedAtoms").await, 1, "the embed job commits with the capture");

    // The same source again: nothing is rewritten, so nothing is enqueued.
    let again = f.write(&grant, f.routed, "slack:workspace:T1", "slack", "normalized_text", None).await.unwrap();
    assert!(again.body.atom_ids.is_empty());
    assert_eq!(again.body.jobs_enqueued, 0, "a duplicate capture reports no job, not a guess");
    assert_eq!(f.jobs("EmbedAtoms").await, 1);
    f.done().await;
}

#[tokio::test]
async fn a_route_changed_after_the_lookup_refuses_the_write_pg() {
    let Some(f) = Fixture::new().await else { return };
    let grant = f.grant().await;

    sqlx::query("UPDATE slack_watched_channels SET graph_id=$1 WHERE account_id=$2")
        .bind(f.home)
        .bind(f.account)
        .execute(&f.pool)
        .await
        .unwrap();
    let repointed = f.write(&grant, f.routed, "slack:workspace:T1", "slack", "normalized_text", None).await;
    assert!(matches!(repointed, Err(IngestError::Forbidden(_))), "{:?}", repointed.as_ref().err());

    sqlx::query("DELETE FROM slack_watched_channels WHERE account_id=$1").bind(f.account).execute(&f.pool).await.unwrap();
    let removed = f.write(&grant, f.routed, "slack:workspace:T1", "slack", "normalized_text", None).await;
    assert!(matches!(removed, Err(IngestError::Forbidden(_))), "{:?}", removed.as_ref().err());

    assert_eq!(f.stored(f.routed).await, (0, 0));
    assert_eq!(f.stored(f.home).await, (0, 0));
    f.done().await;
}

#[tokio::test]
async fn a_source_outside_the_grants_namespace_is_refused_pg() {
    let Some(f) = Fixture::new().await else { return };
    let grant = f.grant().await;
    let refused = f.write(&grant, f.routed, "slack:workspace:T2", "slack", "normalized_text", None).await;
    assert!(matches!(refused, Err(IngestError::Invalid(_))), "{:?}", refused.as_ref().err());
    assert!(refused.as_ref().err().is_some_and(IngestError::is_permanent), "a connector must not retry it");
    assert_eq!(f.stored(f.routed).await, (0, 0));
    f.done().await;
}

/// The door's one meeting rule on the routed path: a transcript the capture
/// projects as a meeting queues its summary. A provider's own summary (the
/// huddle shape, `provider_summary`) is not projected, so it queues none.
#[tokio::test]
async fn a_routed_meeting_transcript_queues_its_summary_pg() {
    let Some(f) = Fixture::new().await else { return };
    let grant = f.grant().await;

    f.write(&grant, f.routed, "slack:workspace:T1", "transcript", "provider_summary", None).await.unwrap_or_else(|r| panic!("{r}"));
    assert_eq!(f.jobs("GenerateMeetingSummary").await, 0, "a provider summary is not a meeting transcript");

    f.write(&grant, f.routed, "slack:workspace:T1", "meet", "normalized_text", None).await.unwrap_or_else(|r| panic!("{r}"));
    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='GenerateMeetingSummary' AND payload->>'graph_id'=$1 AND payload->>'source_id'='meet-1'",
    )
    .bind(f.routed.to_string())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(queued, 1);
    f.done().await;
}

/// A routed capture during an outage answers what the private arm does:
/// [`IngestError::StorageUnavailable`], which every connector retries. Both
/// the source's body failing and a chunk body failing after it landed.
#[tokio::test]
async fn a_failed_upload_aborts_the_routed_capture_with_nothing_stored_pg() {
    let Some(f) = Fixture::new().await else { return };
    let grant = f.grant().await;
    for accept in [0, 1] {
        // A local S3 stand-in: the first `accept` uploads land, then every
        // one is refused.
        let (r2, store) = crate::test_object_store::failing_after(accept).await;
        let refused = f.write(&grant, f.routed, "slack:workspace:T1", "slack", "normalized_text", Some(&r2)).await;
        assert!(
            matches!(refused, Err(IngestError::StorageUnavailable { cleanup_status: "completed" })),
            "accept {accept}: one outage, one shape: {:?}",
            refused.as_ref().err()
        );
        assert!(refused.as_ref().err().is_some_and(|e| !e.is_permanent() && !e.is_invalid() && !e.is_forbidden()));
        if accept == 0 {
            assert_eq!(store.puts(), 1, "no chunk body follows the refused source body");
        } else {
            assert!(store.puts() > accept, "a chunk body was refused after the source's landed");
        }
        assert_eq!(store.deletes(), accept, "accept {accept}: what landed is deleted again");
    }
    assert_eq!(f.stored(f.routed).await, (0, 0), "an aborted capture leaves no row, bodiless or not");
    assert_eq!(f.jobs("EmbedAtoms").await, 0);
    f.done().await;
}

/// One statement on its own connection, bounded by a lock timeout: `Ok` if it
/// ran, `Err(true)` if it waited on a lock the whole time (SQLSTATE 55P03),
/// `Err(false)` for any other failure. Rolled back either way.
async fn attempt(pool: &PgPool, sql: &str, binds: &[Uuid]) -> Result<(), bool> {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL lock_timeout = '300ms'").execute(&mut *tx).await.unwrap();
    let mut query = sqlx::query(sql);
    for bind in binds {
        query = query.bind(bind);
    }
    let result = query.execute(&mut *tx).await;
    tx.rollback().await.ok();
    result.map(|_| ()).map_err(|e| e.as_database_error().and_then(|e| e.code()).as_deref() == Some("55P03"))
}

/// The capture transaction at its seal: the grant check done and the row
/// share-locked, the commit not yet issued. `submit_routed` holds the lock
/// for exactly this window.
async fn sealing_capture(f: &Fixture, grant: &OrgSharedGrant, ocean: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&f.pool, f.account).await.unwrap();
    assert!(grant.verify_tx(&mut tx, ocean).await.unwrap(), "the grant verifies before anything changes");
    tx
}

/// Run `write` paused at the seam before its seal (rows and uploads done,
/// the routing row not yet checked or locked), run `meanwhile` there, then
/// let the write finish and return its result.
async fn paused_before_seal<W, M>(write: W, meanwhile: M) -> Result<Ingested, IngestError>
where
    W: std::future::Future<Output = Result<Ingested, IngestError>>,
    M: std::future::Future<Output = ()>,
{
    let pause = Arc::new(Pause::default());
    let write = BEFORE_SEAL.scope(pause.clone(), write);
    let (result, ()) = tokio::join!(write, async {
        pause.reached.notified().await;
        meanwhile.await;
        pause.resume.notify_one();
    });
    result
}

/// A Slack route a sealing capture has checked is share-locked: an admin's
/// re-point or delete waits for the commit instead of landing under it. Once
/// the capture ends the admin's write goes through, and the grant built
/// before it is refused.
#[tokio::test]
async fn a_slack_route_a_sealing_capture_checked_holds_off_the_admin_pg() {
    let Some(f) = Fixture::new().await else { return };
    let grant = f.grant().await;
    let repoint = "UPDATE slack_watched_channels SET graph_id=$1 WHERE account_id=$2";
    let delete = "DELETE FROM slack_watched_channels WHERE account_id=$1";

    let capture = sealing_capture(&f, &grant, f.routed).await;
    assert_eq!(attempt(&f.pool, repoint, &[f.home, f.account]).await, Err(true), "the re-point waits on the capture");
    assert_eq!(attempt(&f.pool, delete, &[f.account]).await, Err(true), "the delete waits on the capture");
    // A reader is not held up: the lookup's own read still sees the row.
    let (mode, graph): (String, Option<Uuid>) =
        sqlx::query_as("SELECT mode, graph_id FROM slack_watched_channels WHERE account_id=$1")
            .bind(f.account)
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!((mode.as_str(), graph), ("single", Some(f.routed)));
    capture.rollback().await.unwrap();

    assert_eq!(attempt(&f.pool, repoint, &[f.home, f.account]).await, Ok(()), "released with the capture");
    sqlx::query(repoint).bind(f.home).bind(f.account).execute(&f.pool).await.unwrap();
    let refused = f.write(&grant, f.routed, "slack:workspace:T1", "slack", "normalized_text", None).await;
    assert!(matches!(refused, Err(IngestError::Forbidden(_))), "{:?}", refused.as_ref().err());
    assert_eq!(f.stored(f.routed).await, (0, 0));
    f.done().await;
}

/// The lookup is a plain read, so it does not queue behind a sealing capture
/// of the same channel. It used to be `UPDATE ... SET last_resolved_at`,
/// which waited on the capture's share lock.
#[tokio::test]
async fn a_lookup_does_not_wait_behind_a_sealing_capture_of_its_channel_pg() {
    let Some(f) = Fixture::new().await else { return };
    let grant = f.grant().await;
    let capture = sealing_capture(&f, &grant, f.routed).await;

    let lookup = crate::connectors::slack::resolve_channel_route(
        &f.pool, &f.pool, f.account, crate::routes::slack::AccountPlane::Personal, "T1", "C1",
    );
    let (route, destination) = tokio::time::timeout(std::time::Duration::from_secs(3), lookup)
        .await
        .expect("the lookup returns while the capture is open")
        .unwrap();
    assert_eq!(route, Some(("single".to_string(), Some(f.routed))));
    assert!(matches!(destination, crate::connectors::slack::ChannelDestination::Single(g) if g == f.routed));
    // The row itself is still exactly as the capture locked it.
    assert_eq!(attempt(&f.pool, "UPDATE slack_watched_channels SET graph_id=$1 WHERE account_id=$2", &[f.home, f.account]).await, Err(true));
    capture.rollback().await.unwrap();
    f.done().await;
}

/// An Org-plane account still served from the consumer plane: the control
/// and content pools are one pool. The roster re-check runs on the capture's
/// own connection, so a capture needs exactly one connection; with a pool of
/// one, a second acquire would wait for the capture that holds it. Membership
/// removed after the lookup is still caught, in the capture.
#[tokio::test]
async fn an_org_plane_grant_on_the_consumer_plane_captures_on_one_connection_pg() {
    let Some(f) = Fixture::new().await else { return };
    f.join().await;
    f.mailbox_route("spaces/DM1").await;
    let one = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(20))
        .connect_with(f.opts.clone())
        .await
        .unwrap();
    let (grant, _) = OrgSharedGrant::google_chat_space(
        &one, &one, &TenantId::Consumer, f.account, crate::routes::slack::AccountPlane::Org, "spaces/DM1", Some(f.user),
    )
    .await
    .unwrap()
    .expect("routed");
    assert_eq!(grant.oceans(), &[f.routed][..]);

    let write = f.write_on(&one, &TenantId::Consumer, &grant, f.routed, "google_chat:space:spaces/DM1", "google_chat", "normalized_text", None);
    tokio::time::timeout(std::time::Duration::from_secs(10), write)
        .await
        .expect("the capture does not wait on a second connection")
        .unwrap_or_else(|r| panic!("{r}"));
    assert_eq!(f.stored(f.routed).await.0 > 0, true);

    f.leave().await;
    let refused = f
        .write_on(&one, &TenantId::Consumer, &grant, f.routed, "google_chat:space:spaces/DM1", "google_chat_2", "normalized_text", None)
        .await;
    assert!(matches!(refused, Err(IngestError::Forbidden(_))), "a member who left is refused in the capture: {:?}", refused.as_ref().err());
    one.close().await;
    f.done().await;
}

/// On an org tenant the roster is on another database: it is read once when
/// the grant is built, not once per write. A member removed after the build
/// is refused by the next grant, not by the writes under this one.
#[tokio::test]
async fn an_org_tenant_grant_reads_the_roster_once_when_it_is_built_pg() {
    let Some(f) = Fixture::new().await else { return };
    f.join().await;
    f.mailbox_route("spaces/DM2").await;
    let org = TenantId::Org("org_door".to_string());
    let build = || async {
        OrgSharedGrant::google_chat_space(
            &f.pool, &f.pool, &org, f.account, crate::routes::slack::AccountPlane::Org, "spaces/DM2", Some(f.user),
        )
        .await
        .unwrap()
        .expect("routed")
        .0
    };
    let grant = build().await;
    f.write_on(&f.pool, &org, &grant, f.routed, "google_chat:space:spaces/DM2", "google_chat", "normalized_text", None)
        .await
        .unwrap_or_else(|r| panic!("{r}"));

    f.leave().await;
    f.write_on(&f.pool, &org, &grant, f.routed, "google_chat:space:spaces/DM2", "google_chat_2", "normalized_text", None)
        .await
        .unwrap_or_else(|r| panic!("the roster snapshot carries the pass: {r}"));
    let next = build().await;
    let refused = f
        .write_on(&f.pool, &org, &next, f.routed, "google_chat:space:spaces/DM2", "google_chat_3", "normalized_text", None)
        .await;
    assert!(matches!(refused, Err(IngestError::Forbidden(_))), "{:?}", refused.as_ref().err());
    f.done().await;
}

#[tokio::test]
async fn a_chat_route_a_sealing_capture_checked_holds_off_the_admin_pg() {
    let Some(f) = Fixture::new().await else { return };
    sqlx::query(
        "INSERT INTO google_chat_watched_spaces(account_id,space_id,space_type,mode,graph_id) \
         VALUES($1,'spaces/S1','SPACE','single',$2)",
    )
    .bind(f.account)
    .bind(f.routed)
    .execute(&f.pool)
    .await
    .unwrap();
    let (grant, _) = OrgSharedGrant::google_chat_space(
        &f.pool, &f.pool, &TenantId::Consumer, f.account, crate::routes::slack::AccountPlane::Personal, "spaces/S1", None,
    )
    .await
    .unwrap()
    .expect("routed");
    assert_eq!(grant.oceans(), &[f.routed][..]);
    let repoint = "UPDATE google_chat_watched_spaces SET graph_id=$1 WHERE account_id=$2";
    let remove = "UPDATE google_chat_watched_spaces SET active=false, mode=NULL, graph_id=NULL WHERE account_id=$1";

    let capture = sealing_capture(&f, &grant, f.routed).await;
    assert_eq!(attempt(&f.pool, repoint, &[f.home, f.account]).await, Err(true));
    assert_eq!(attempt(&f.pool, remove, &[f.account]).await, Err(true));
    capture.rollback().await.unwrap();

    assert_eq!(attempt(&f.pool, remove, &[f.account]).await, Ok(()));
    sqlx::query(remove).bind(f.account).execute(&f.pool).await.unwrap();
    let refused = f.write(&grant, f.routed, "google_chat:space:spaces/S1", "google_chat", "normalized_text", None).await;
    assert!(matches!(refused, Err(IngestError::Forbidden(_))), "{:?}", refused.as_ref().err());
    assert_eq!(f.stored(f.routed).await, (0, 0));
    f.done().await;
}

/// The GitHub binding is share-locked like the others, and the chooser's
/// reachability is asked again inside the capture: an ocean archived after
/// the lookup refuses the write even though the binding row is unchanged.
#[tokio::test]
async fn a_github_binding_a_sealing_capture_checked_holds_off_the_admin_and_rechecks_reach_pg() {
    let Some(f) = Fixture::new().await else { return };
    let (row_id, grant) = f.github_binding().await;
    let pause = "UPDATE github_watched_repos SET active=false WHERE id=$1";
    let repoint = "UPDATE github_watched_repos SET graph_id=$1 WHERE id=$2";

    let capture = sealing_capture(&f, &grant, f.routed).await;
    assert_eq!(attempt(&f.pool, pause, &[row_id]).await, Err(true));
    assert_eq!(attempt(&f.pool, repoint, &[f.home, row_id]).await, Err(true));
    capture.rollback().await.unwrap();
    assert_eq!(attempt(&f.pool, pause, &[row_id]).await, Ok(()));

    // The row is untouched; the ocean is archived out from under it.
    sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1").bind(f.routed).execute(&f.pool).await.unwrap();
    let refused = f.write(&grant, f.routed, "github:repository:42", "github_pr", "normalized_text", None).await;
    assert!(matches!(refused, Err(IngestError::Forbidden(_))), "{:?}", refused.as_ref().err());
    assert_eq!(f.stored(f.routed).await, (0, 0));

    sqlx::query("UPDATE graphs SET archived_at=NULL WHERE id=$1").bind(f.routed).execute(&f.pool).await.unwrap();
    f.write(&grant, f.routed, "github:repository:42", "github_pr", "normalized_text", None).await.unwrap_or_else(|r| panic!("{r}"));
    f.done().await;
}

/// Before its seal a capture holds no lock on its routing row: the row's own
/// bookkeeping (here the backfill cursor `narrative_cursor`, which the tick
/// writes while captures of the same repo are in flight) goes through at
/// once. With the check at the start of the transaction this UPDATE waited
/// behind the capture's uploads (`lock_timeout` makes it fail fast). The
/// capture then seals and commits as usual.
#[tokio::test]
async fn a_github_rows_bookkeeping_is_not_blocked_by_a_capture_before_its_seal_pg() {
    let Some(f) = Fixture::new().await else { return };
    let (row_id, grant) = f.github_binding().await;
    let cursor = "UPDATE github_watched_repos SET narrative_cursor='{\"prs\":\"2026-10-30\"}'::jsonb, updated_at=now() WHERE id=$1";
    let error_code = "UPDATE github_watched_repos SET sync_error_code='sync_failed', sync_error_at=now() WHERE id=$1";

    let write = f.write(&grant, f.routed, "github:repository:42", "github_pr", "normalized_text", None);
    let ingested = paused_before_seal(write, async {
        assert_eq!(attempt(&f.pool, cursor, &[row_id]).await, Ok(()), "the cursor write does not wait on the open capture");
        assert_eq!(attempt(&f.pool, error_code, &[row_id]).await, Ok(()), "nor does the error-code write");
        // The real cursor write, committed while the capture is open.
        sqlx::query(cursor).bind(row_id).execute(&f.pool).await.unwrap();
    })
    .await
    .unwrap_or_else(|r| panic!("the capture seals and commits after the bookkeeping: {r}"));
    assert!(!ingested.body.atom_ids.is_empty());
    assert!(f.stored(f.routed).await.0 > 0);
    let cursor_kept: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT narrative_cursor FROM github_watched_repos WHERE id=$1").bind(row_id).fetch_one(&f.pool).await.unwrap();
    assert_eq!(cursor_kept, Some(json!({"prs": "2026-10-30"})), "the capture's commit did not undo the cursor");
    f.done().await;
}

/// A Slack route re-pointed while a capture runs: the re-point does not wait
/// (nothing is locked before the seal), and the seal then refuses the
/// capture, so nothing it wrote or uploaded commits anywhere. The row's
/// `last_resolved_at` stamp, which the admin channel actions bump, goes
/// through at once as well.
#[tokio::test]
async fn a_slack_route_re_pointed_during_a_capture_stores_nothing_pg() {
    let Some(f) = Fixture::new().await else { return };
    let grant = f.grant().await;
    let stamp = "UPDATE slack_watched_channels SET last_resolved_at=now() WHERE account_id=$1";
    let repoint = "UPDATE slack_watched_channels SET graph_id=$1 WHERE account_id=$2";

    let write = f.write(&grant, f.routed, "slack:workspace:T1", "slack", "normalized_text", None);
    let refused = paused_before_seal(write, async {
        assert_eq!(attempt(&f.pool, stamp, &[f.account]).await, Ok(()), "the stamp does not wait on the open capture");
        assert_eq!(attempt(&f.pool, repoint, &[f.home, f.account]).await, Ok(()), "the re-point does not wait on the open capture");
        sqlx::query(repoint).bind(f.home).bind(f.account).execute(&f.pool).await.unwrap();
    })
    .await;
    assert!(matches!(refused, Err(IngestError::Forbidden(_))), "the seal refuses the changed route: {:?}", refused.as_ref().err());
    assert_eq!(f.stored(f.routed).await, (0, 0), "the rolled-back capture left nothing in the old ocean");
    assert_eq!(f.stored(f.home).await, (0, 0), "and wrote nothing to the new one");
    assert_eq!(f.jobs("EmbedAtoms").await, 0);

    // The next lookup builds a grant for the new route and writes there.
    let next = f.grant_to(f.home).await;
    f.write(&next, f.home, "slack:workspace:T1", "slack", "normalized_text", None).await.unwrap_or_else(|r| panic!("{r}"));
    assert!(f.stored(f.home).await.0 > 0);
    f.done().await;
}

/// A fan-out ocean is re-derived in the capture: it must still be a member's
/// own primary ocean. One archived after the lookup is refused; the route row
/// alone does not carry the write.
#[tokio::test]
async fn a_fanout_ocean_archived_after_the_lookup_is_refused_pg() {
    let Some(f) = Fixture::new().await else { return };
    sqlx::query(
        "INSERT INTO google_chat_watched_spaces(account_id,space_id,space_type,mode) VALUES($1,'spaces/F1','SPACE','fanout')",
    )
    .bind(f.account)
    .execute(&f.pool)
    .await
    .unwrap();
    let member: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id,name,kind,is_primary,owner_user_id) VALUES($1,'mine','member',true,$2) RETURNING id",
    )
    .bind(f.account)
    .bind(f.user)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    let grant = OrgSharedGrant::google_chat_fanout_for_test(f.account, "spaces/F1", vec![member, f.routed]);

    let refused = f.write(&grant, f.routed, "google_chat:space:spaces/F1", "google_chat", "normalized_text", None).await;
    assert!(matches!(refused, Err(IngestError::Forbidden(_))), "not a member ocean: {:?}", refused.as_ref().err());
    f.write(&grant, member, "google_chat:space:spaces/F1", "google_chat", "normalized_text", None).await.unwrap_or_else(|r| panic!("{r}"));

    sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1").bind(member).execute(&f.pool).await.unwrap();
    let text_changed = f.write(&grant, member, "google_chat:space:spaces/F1", "google_chat_2", "normalized_text", None).await;
    assert!(matches!(text_changed, Err(IngestError::Forbidden(_))), "{:?}", text_changed.as_ref().err());
    f.done().await;
}

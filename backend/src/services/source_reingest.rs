//! Retract-and-re-ingest for an upstream item that was EDITED after we
//! ingested it — the "update" half that `routes::ingest::ingest_atom`'s
//! `ON CONFLICT (graph_id, source_type, source_id) DO NOTHING` insert does
//! not have.
//!
//! # The bug this exists for (Google Calendar, verified in prod 2026-10-06)
//!
//! A Google Calendar event keeps its id for life. Rename it, move it, add an
//! attendee, rewrite the description — the incremental `syncToken` pass
//! reports the SAME `source_id` with new content, `routes::ingest` hashes
//! and chunks it, and `Store::insert_atoms_scoped` skips every chunk
//! (`skipped_duplicate_chunks: 1`). The push webhook logged
//! "sync complete", the cursor advanced past the change (an incremental
//! pass reports a given change exactly once), and the stored coral kept the
//! old title, time and attendees forever. Cancellations already worked
//! because they go through `services::deletion`, not the insert.
//!
//! # How this fixes it
//!
//! The ingest path now persists `atoms.content_hash` (migrations
//! `0271`/`0112`) — the SHA-256 of the whole pre-chunk request content it
//! was already computing for its `embed:` job key. Before a connector
//! ingests a re-fetched item, [`retract_if_changed`] looks up the atoms
//! already stored for that `(graph_id, source_type, source_id)` and:
//!
//! * finds none — first sight, nothing to do; the insert will land it;
//! * finds them all carrying the SAME hash, start time and participants —
//!   an unchanged re-fetch (a retry of a page whose cursor never
//!   advanced, say); nothing to do, the insert's `DO NOTHING` keeps it a
//!   no-op exactly as before — **no churn**;
//! * finds any that differ (or that predate the hash column, NULL) — the
//!   upstream item was edited: retract exactly those atoms with
//!   [`crate::services::deletion::delete_atoms_in_graph_only`], so the
//!   insert that follows creates a fresh coral with the new content.
//!
//! Retract + fresh insert, rather than an in-place `UPDATE`, is deliberate:
//! it reuses the one cascade that already knows how to take a coral apart
//! correctly (its blob, its `node_atoms` links, any reef it was the last
//! coral of, its FTS/embedding rows, the `op_log` tombstone), and the
//! fresh insert reuses the one path that knows how to build one (title from
//! the new summary via `services::atom_title::fallback_title_for_source`,
//! excerpt, `embed:`/extract jobs). An in-place update would have to
//! re-derive every one of those by hand and keep them in step.
//!
//! # Microsoft Calendar has the identical gap
//!
//! A Microsoft Graph calendar event also keeps its `id` across edits (the
//! id only changes when the item is MOVED between folders), and every
//! calendar pass — `workers::microsoft_calendar_sync` on the 30-minute tick
//! and on the manual `POST /v1/connectors/microsoft/sync`, both planes;
//! since fix/cal-deletions a `calendarView/delta` pass that re-reports an
//! edited event once — re-fetches an edited event with the SAME
//! `source_id` and new content, and hit the same `DO NOTHING`. They now go
//! through [`retract_if_changed_item`], gated by [`is_calendar_event`] so a
//! `teams_transcript` item riding the same batch is never treated as an
//! editable event. Not wired: `outlook_everything_mode` (mail messages),
//! `graph_webhook`/`graph_ingest` (Teams messages/transcripts, mail replies
//! — no calendar resource is subscribed), `outlook_schedule`/
//! `teams_meeting_sync` (scheduling + transcript capture, no event atoms).
//!
//! # Scope: ONE graph, never the account
//!
//! The retraction is [`delete_atoms_in_graph_only`] — confined to the
//! named graph, with NO source-identity expansion to sibling/promoted
//! copies elsewhere in the account. On an org account Google gives every
//! attendee's calendar the same event id, and each member holds their OWN
//! atom for it in their OWN member-private ocean: member A's edit (or A's
//! view of the organiser's edit) must only ever replace A's copy, never
//! B's. This is the identical confinement
//! `connectors::google_calendar::propagate_org_member_deletions` uses for
//! cancellations, and it is applied on the personal plane too — the
//! personal-plane cancellation path uses the account-wide
//! `delete_source`, but a human-PROMOTED copy of an event in a shared ocean
//! is a decision of its own and an edit upstream should not silently
//! rewrite it (residual, same as the org deletion path documents).
//!
//! The lookup runs `begin_tenant_system_scoped` — the same scope every
//! background/webhook-driven connector write uses — because there is no
//! acting end user to present, and on a tenant database an account-scoped
//! transaction with no tenant actor cannot see a member's private graph:
//! it would find nothing and "succeed" (the exact failure
//! `google_calendar::tests::org_deletion_propagation_works_as_the_real_tenant_app_role_pg`
//! guards against for cancellations).
//!
//! # Failure contract
//!
//! Any error (lookup, cascade) is returned, never swallowed: the Calendar
//! push/backstop caller treats it like a failed ingest — the item is NOT
//! ingested this pass and the sync cursor is left unchanged, so the next
//! pass re-fetches the same page and tries again (the cascade is
//! idempotent; a retract that already happened is a no-op).
//!
//! The retract and the re-insert are two transactions, not one: if the
//! insert fails transiently AFTER a successful retract, the coral is absent
//! until the next pass re-ingests it (cursor unchanged → it will). That
//! window is accepted rather than engineered away — closing it would mean
//! threading the cascade into `ingest_atom`'s own transaction.

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::NormalizedItem;
use crate::services::blob_store::PromotionBlobStore;
use crate::services::deletion::{self, DeletionError};

/// The content hash `routes::ingest` persists to `atoms.content_hash` —
/// lowercase hex SHA-256 of the raw request content. Kept here as the ONE
/// definition both the writer (`routes::ingest`) and this reader use, so
/// they cannot drift: an ingest that hashed differently from this reader
/// would make every re-fetch look "changed" and churn every atom on every
/// sync.
pub(crate) fn content_hash_hex(content: &str) -> String {
    hex::encode(Sha256::digest(content.as_bytes()))
}

/// One stored chunk atom of the source being re-fetched, as the lookup
/// returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredChunk {
    pub(crate) atom_id: Uuid,
    /// `None` = ingested before `atoms.content_hash` existed — content
    /// unknown, treated as changed (see the module doc).
    pub(crate) content_hash: Option<String>,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) participants: Vec<String>,
}

/// The pure decision [`retract_if_changed`] acts on — split out so the
/// comparison rules are unit-testable without a database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReingestDecision {
    /// Nothing stored for this source in this graph.
    FirstSeen,
    /// Every stored chunk matches the incoming item exactly.
    Unchanged,
    /// At least one stored chunk differs (or has no hash): retract these.
    Changed { stale_atom_ids: Vec<Uuid> },
}

/// What the incoming item looks like, for the comparison.
pub(crate) struct Incoming<'a> {
    pub(crate) content: &'a str,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) participants: &'a [String],
}

/// Decide whether the stored chunks are the same item the connector just
/// re-fetched. Rules:
///
/// * the content hash must be present AND equal (NULL = unknown = changed);
/// * the start/occurrence `timestamp` must be equal to the microsecond —
///   Postgres `timestamptz` keeps microseconds, so a round-tripped value
///   can never differ below that, and Google's times carry whole seconds;
/// * `participants` must be equal AS A SET (order- and duplicate-
///   insensitive): the attendee list is identity, not prose, and a
///   reordering is not an edit worth replacing a coral over.
///
/// `Changed` names EVERY stored chunk, not only the ones that differ: a
/// multi-chunk source is replaced whole, because the new content chunks
/// differently and a partial replacement would leave a chunk from the old
/// text next to chunks from the new.
pub(crate) fn decide(existing: &[StoredChunk], incoming: &Incoming<'_>) -> ReingestDecision {
    if existing.is_empty() {
        return ReingestDecision::FirstSeen;
    }
    let new_hash = content_hash_hex(incoming.content);
    let new_participants = participant_set(incoming.participants);
    let unchanged = existing.iter().all(|c| {
        c.content_hash.as_deref() == Some(new_hash.as_str())
            && c.timestamp.timestamp_micros() == incoming.timestamp.timestamp_micros()
            && participant_set(&c.participants) == new_participants
    });
    if unchanged {
        ReingestDecision::Unchanged
    } else {
        ReingestDecision::Changed {
            stale_atom_ids: existing.iter().map(|c| c.atom_id).collect(),
        }
    }
}

fn participant_set(p: &[String]) -> std::collections::BTreeSet<&str> {
    p.iter().map(String::as_str).collect()
}

/// What [`retract_if_changed`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReingestOutcome {
    FirstSeen,
    Unchanged,
    Replaced {
        atoms_deleted: usize,
        reefs_deleted: usize,
    },
}

/// Look up what is stored in `graph_id` for `(source_type, source_id)` —
/// the bare id and its `:chunk:N` rows, matched with the same escaping rule
/// `services::deletion` uses — and, if the incoming item differs, retract
/// those atoms so the caller's following `ingest_atom` lands the new
/// content as a fresh coral. See the module doc for every rule.
///
/// `blobs`: the R2 transport, so a replaced coral's blob is deleted with it
/// (pass `state.r2` like the retraction routes do; `None` in tests with no
/// R2, exactly as the cancellation path does).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn retract_if_changed(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    account_id: Uuid,
    graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    incoming: &Incoming<'_>,
) -> Result<ReingestOutcome, DeletionError> {
    let existing = stored_chunks(pool, account_id, graph_id, source_type, source_id).await?;
    let stale = match decide(&existing, incoming) {
        ReingestDecision::FirstSeen => return Ok(ReingestOutcome::FirstSeen),
        ReingestDecision::Unchanged => return Ok(ReingestOutcome::Unchanged),
        ReingestDecision::Changed { stale_atom_ids } => stale_atom_ids,
    };
    let outcome = deletion::replace_atoms_in_graph_only(
        pool,
        blobs,
        crate::store::CLOUD_DEVICE_ID,
        graph_id,
        &stale,
        // Sync/push-driven, no CoralSwarm user session — system actor,
        // same as the connector cancellation paths. `control_pool: None`:
        // an upstream EDIT is not a retention event, so no
        // `retraction_audit_log` row (the `connector_audit_log` action
        // vocabulary is CHECK-constrained to real compliance events, and
        // "the user renamed a meeting" is not one).
        crate::services::connector_audit::Actor::System,
        None,
    )
    .await?;
    tracing::info!(
        %graph_id, source_type, source_id,
        atoms_deleted = outcome.atoms_deleted.len(),
        reefs_deleted = outcome.deleted_reef_ids.len(),
        "source_reingest: upstream item changed — stale coral retracted ahead of re-ingest"
    );
    Ok(ReingestOutcome::Replaced {
        atoms_deleted: outcome.atoms_deleted.len(),
        reefs_deleted: outcome.deleted_reef_ids.len(),
    })
}

/// Is `source_type` a calendar EVENT whose upstream edits keep the same id —
/// i.e. an item [`retract_if_changed`] applies to? Google Calendar and
/// Microsoft Graph calendar events only. Deliberately NOT mail messages,
/// Teams channel/chat messages, or meeting transcripts/recordings: a
/// re-fetch of those with different content is not "the same item, edited"
/// (a transcript grows as the meeting runs; a message is immutable and a
/// reply is a new item), so retracting on a content mismatch would be wrong
/// there.
pub(crate) fn is_calendar_event(source_type: &str) -> bool {
    source_type == crate::connectors::google_calendar::SOURCE_TYPE
        || source_type == crate::connectors::microsoft::CALENDAR_SOURCE_TYPE
}

/// [`retract_if_changed`] for a connector [`NormalizedItem`], applied ONLY
/// when [`is_calendar_event`] says so: `Ok(None)` for every other source
/// type (no lookup is even run), `Ok(Some(outcome))` for a calendar event.
/// The one entry point for connector batch loops that carry mixed source
/// types — `workers::microsoft_calendar_sync` ingests `teams_transcript`
/// items alongside calendar events in the same loop — so the gate lives
/// here, once, rather than in each caller.
pub(crate) async fn retract_if_changed_item(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    account_id: Uuid,
    graph_id: Uuid,
    item: &NormalizedItem,
) -> Result<Option<ReingestOutcome>, DeletionError> {
    if !is_calendar_event(&item.source_type) {
        return Ok(None);
    }
    let incoming = Incoming {
        content: &item.content,
        timestamp: item.timestamp,
        participants: &item.participants,
    };
    retract_if_changed(
        pool,
        blobs,
        account_id,
        graph_id,
        &item.source_type,
        &item.source_id,
        &incoming,
    )
    .await
    .map(Some)
}

/// Every chunk atom stored for the source in THIS graph only.
async fn stored_chunks(
    pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    source_type: &str,
    source_id: &str,
) -> Result<Vec<StoredChunk>, DeletionError> {
    // `(atom_id, content_hash, timestamp, participants)` — one row per
    // stored chunk of the source.
    type Row = (Uuid, Option<String>, DateTime<Utc>, Vec<String>);
    let pattern = deletion::escaped_chunk_pattern(source_id);
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await?;
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT atom_id, content_hash, timestamp, participants FROM atoms \
         WHERE graph_id = $1 AND source_type = $2 \
           AND (source_id = $3 OR source_id LIKE $4)",
    )
    .bind(graph_id)
    .bind(source_type)
    .bind(source_id)
    .bind(&pattern)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows
        .into_iter()
        .map(|(atom_id, content_hash, timestamp, participants)| StoredChunk {
            atom_id,
            content_hash,
            timestamp,
            participants,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(hash: Option<&str>, ts: DateTime<Utc>, participants: &[&str]) -> StoredChunk {
        StoredChunk {
            atom_id: Uuid::new_v4(),
            content_hash: hash.map(str::to_string),
            timestamp: ts,
            participants: participants.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    const CONTENT: &str = "Alex <> Stephen\n\nLocation: R4";

    #[test]
    fn content_hash_matches_what_routes_ingest_writes() {
        // `routes::ingest` step 5 is `hex::encode(Sha256::digest(content))`;
        // this reader must produce the identical string or every re-fetch
        // looks edited.
        assert_eq!(
            content_hash_hex(CONTENT),
            hex::encode(Sha256::digest(CONTENT.as_bytes()))
        );
        assert_eq!(content_hash_hex("").len(), 64);
    }

    #[test]
    fn nothing_stored_is_first_seen() {
        let incoming = Incoming { content: CONTENT, timestamp: ts("2026-10-06T10:00:00Z"), participants: &[] };
        assert_eq!(decide(&[], &incoming), ReingestDecision::FirstSeen);
    }

    #[test]
    fn identical_refetch_is_unchanged_so_the_insert_stays_a_no_op() {
        let h = content_hash_hex(CONTENT);
        let t = ts("2026-10-06T10:00:00Z");
        let people = ["Stephen".to_string(), "Alex".to_string()];
        // Stored in a different order, with a duplicate — still the same set.
        let existing = [chunk(Some(&h), t, &["Alex", "Stephen", "Alex"])];
        let incoming = Incoming { content: CONTENT, timestamp: t, participants: &people };
        assert_eq!(decide(&existing, &incoming), ReingestDecision::Unchanged);
    }

    #[test]
    fn renamed_event_is_changed_and_names_every_stored_chunk() {
        let t = ts("2026-10-06T10:00:00Z");
        let old = content_hash_hex("Old title\n\nLocation: R4");
        let existing = [chunk(Some(&old), t, &[]), chunk(Some(&old), t, &[])];
        let incoming = Incoming { content: CONTENT, timestamp: t, participants: &[] };
        match decide(&existing, &incoming) {
            ReingestDecision::Changed { stale_atom_ids } => {
                assert_eq!(stale_atom_ids, vec![existing[0].atom_id, existing[1].atom_id]);
            }
            other => panic!("expected Changed, got {other:?}"),
        }
    }

    #[test]
    fn rescheduled_event_with_same_text_is_changed() {
        let h = content_hash_hex(CONTENT);
        let existing = [chunk(Some(&h), ts("2026-10-06T10:00:00Z"), &[])];
        let incoming = Incoming { content: CONTENT, timestamp: ts("2026-10-07T10:00:00Z"), participants: &[] };
        assert!(matches!(decide(&existing, &incoming), ReingestDecision::Changed { .. }));
    }

    #[test]
    fn attendee_added_with_same_text_is_changed() {
        let h = content_hash_hex(CONTENT);
        let t = ts("2026-10-06T10:00:00Z");
        let existing = [chunk(Some(&h), t, &["Alex"])];
        let people = ["Alex".to_string(), "Stephen".to_string()];
        let incoming = Incoming { content: CONTENT, timestamp: t, participants: &people };
        assert!(matches!(decide(&existing, &incoming), ReingestDecision::Changed { .. }));
    }

    #[test]
    fn pre_migration_atom_without_a_hash_is_treated_as_changed() {
        // Every atom ingested before 0271/0112 has NULL here. Its content is
        // unknowable without reading the blob back, and an incremental
        // Calendar pass only reports an event Google says changed — so
        // "assume changed" is both the safe and the usually-correct call.
        let t = ts("2026-10-06T10:00:00Z");
        let existing = [chunk(None, t, &[])];
        let incoming = Incoming { content: CONTENT, timestamp: t, participants: &[] };
        assert!(matches!(decide(&existing, &incoming), ReingestDecision::Changed { .. }));
    }

    #[test]
    fn one_stale_chunk_among_fresh_ones_still_replaces_the_whole_source() {
        let h = content_hash_hex(CONTENT);
        let t = ts("2026-10-06T10:00:00Z");
        let existing = [chunk(Some(&h), t, &[]), chunk(None, t, &[])];
        let incoming = Incoming { content: CONTENT, timestamp: t, participants: &[] };
        match decide(&existing, &incoming) {
            ReingestDecision::Changed { stale_atom_ids } => assert_eq!(stale_atom_ids.len(), 2),
            other => panic!("expected Changed, got {other:?}"),
        }
    }

    #[test]
    fn gate_admits_only_calendar_events() {
        // The two providers whose events keep their id across edits.
        assert!(is_calendar_event(crate::connectors::google_calendar::SOURCE_TYPE));
        assert!(is_calendar_event(crate::connectors::microsoft::CALENDAR_SOURCE_TYPE));
        assert!(is_calendar_event("microsoft_calendar"), "the constant must be the stored string");
        // Everything else that rides a Microsoft/Google batch loop is NOT an
        // editable item: a transcript grows, a message is immutable, mail
        // and chat replies are new items.
        for st in ["outlook", "teams_transcript", "teams_channel", "teams_chat", "google_gmail", "google_meet", "slack", ""] {
            assert!(!is_calendar_event(st), "{st} must not go through retract_if_changed");
        }
    }

    // ── pg-gated: the real lookup + cascade against Postgres ───────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //     cargo test -p coralswarm-backend --lib services::source_reingest::tests
    //
    // Same `ORGT_TEST_BASE_URL` convention as `connectors::google_calendar`'s
    // own pg tests: absent → the test prints "skipping" and returns Ok, so
    // a run without a database passes VACUOUSLY. Check the output.

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }
    async fn drop_db(base: &str, db: &str) {
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Insert a calendar coral the way `routes::ingest` does — through the
    /// REAL `Store::insert_atoms_scoped`, so this test proves the new
    /// `content_hash` column round-trips through the production write path
    /// and not just through hand-rolled SQL.
    #[allow(clippy::too_many_arguments)]
    async fn ingest_like_prod(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        user_id: Uuid,
        source_type: &str,
        source_id: &str,
        content: &str,
        timestamp: DateTime<Utc>,
        participants: &[String],
    ) -> Uuid {
        use crate::store::Store;
        let store = crate::store::PostgresStore::new(pool.clone());
        let atom_id = Uuid::new_v4();
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
            .await
            .unwrap();
        let hash = content_hash_hex(content);
        let inserted = store
            .insert_atoms_scoped(
                &mut tx,
                &[atom_id],
                graph_id,
                source_type,
                &[format!("{source_id}:chunk:0")],
                &[format!("blob-{atom_id}")],
                participants,
                timestamp,
                user_id,
                "private",
                "english",
                &[Some(content.to_string())],
                &[crate::services::atom_title::fallback_title_for_source(source_type, content)],
                &[Some("fallback".to_string())],
                &[None],
                Some(&hash),
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(inserted, vec![atom_id]);
        atom_id
    }

    async fn atom_count(pool: &PgPool, atom_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Personal plane, end to end against the control tree: an unchanged
    /// re-fetch leaves the coral alone; a rename retracts it (so the
    /// following ingest lands the new title); a pre-migration row (NULL
    /// hash) is retracted too; a different event's coral is never touched.
    #[tokio::test]
    async fn personal_plane_unchanged_is_kept_and_edited_is_retracted_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_plane_unchanged_is_kept_and_edited_is_retracted_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "src_reingest_personal";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let start = ts("2026-10-06T15:00:00Z");
        let people = ["Stephen".to_string()];
        let v1 = "Alex <> Stephen\n\nLocation: R4";
        let atom_v1 = ingest_like_prod(&pool, account_id, graph_id, user_id, "google_calendar", "evt-1", v1, start, &people).await;
        // A calendar edit replaces retrieval chunks, never its immutable
        // source history. Associate this fixture exactly as generic ingest does.
        let source_item:Uuid=sqlx::query_scalar(
            "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'google_calendar','test:calendar','evt-1') RETURNING source_item_id",
        ).bind(graph_id).fetch_one(&pool).await.unwrap();
        let source_version:Uuid=sqlx::query_scalar(
            "INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline) VALUES($1,$2,'google_calendar','test:calendar','evt-1',$3) RETURNING version_id",
        ).bind(graph_id).bind(source_item).bind(v1).fetch_one(&pool).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(source_item).bind(source_version).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
            .bind(graph_id).bind(source_version).bind(atom_v1).execute(&pool).await.unwrap();
        // `content_hash` really landed through the production insert.
        let stored_hash: Option<String> =
            sqlx::query_scalar("SELECT content_hash FROM atoms WHERE atom_id = $1")
                .bind(atom_v1)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored_hash.as_deref(), Some(content_hash_hex(v1).as_str()));
        // A bystander event that must never be touched.
        let other = ingest_like_prod(&pool, account_id, graph_id, user_id, "google_calendar", "evt-2", "Standup", start, &[]).await;
        // A pre-migration coral: same shape, hash NULL.
        let legacy = ingest_like_prod(&pool, account_id, graph_id, user_id, "google_calendar", "evt-legacy", "Old", start, &[]).await;
        sqlx::query("UPDATE atoms SET content_hash = NULL WHERE atom_id = $1")
            .bind(legacy)
            .execute(&pool)
            .await
            .unwrap();

        // 1. Unchanged re-fetch → kept.
        let same = Incoming { content: v1, timestamp: start, participants: &people };
        let out = retract_if_changed(&pool, None, account_id, graph_id, "google_calendar", "evt-1", &same)
            .await
            .unwrap();
        assert_eq!(out, ReingestOutcome::Unchanged);
        assert_eq!(atom_count(&pool, atom_v1).await, 1, "an unchanged re-fetch must not churn the coral");

        // 2. Renamed → retracted, so the caller's next ingest creates the
        //    new coral (titled from the new summary by the ingest path).
        let v2 = "Alex <> Stephen (moved to Friday)\n\nLocation: R4";
        let renamed = Incoming { content: v2, timestamp: start, participants: &people };
        let out = retract_if_changed(&pool, None, account_id, graph_id, "google_calendar", "evt-1", &renamed)
            .await
            .unwrap();
        assert_eq!(out, ReingestOutcome::Replaced { atoms_deleted: 1, reefs_deleted: 0 });
        assert_eq!(atom_count(&pool, atom_v1).await, 0, "the stale coral is gone");
        assert_eq!(atom_count(&pool, other).await, 1, "a different event's coral is untouched");
        let historical_text:String=sqlx::query_scalar("SELECT raw_inline FROM source_versions WHERE version_id=$1")
            .bind(source_version).fetch_one(&pool).await.unwrap();
        assert_eq!(historical_text,v1,"replacing a retrieval chunk must preserve its original source version");
        // And the re-ingest that follows in production lands the new title.
        let atom_v2 = ingest_like_prod(&pool, account_id, graph_id, user_id, "google_calendar", "evt-1", v2, start, &people).await;
        let (title, hash): (Option<String>, Option<String>) =
            sqlx::query_as("SELECT title, content_hash FROM atoms WHERE atom_id = $1")
                .bind(atom_v2)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(title.as_deref(), Some("Alex <> Stephen (moved to Friday)"));
        assert_eq!(hash.as_deref(), Some(content_hash_hex(v2).as_str()));
        let copies: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_id LIKE 'evt-1:chunk:%'",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(copies, 1, "exactly one coral for the event after the edit — no duplicate");

        // 3. First sight of a brand-new event → nothing to retract.
        let fresh = Incoming { content: "New", timestamp: start, participants: &[] };
        let out = retract_if_changed(&pool, None, account_id, graph_id, "google_calendar", "evt-new", &fresh)
            .await
            .unwrap();
        assert_eq!(out, ReingestOutcome::FirstSeen);

        // 4. Pre-migration coral (NULL hash) re-fetched with the SAME text →
        //    unknowable, so replaced once.
        let legacy_same = Incoming { content: "Old", timestamp: start, participants: &[] };
        let out = retract_if_changed(&pool, None, account_id, graph_id, "google_calendar", "evt-legacy", &legacy_same)
            .await
            .unwrap();
        assert_eq!(out, ReingestOutcome::Replaced { atoms_deleted: 1, reefs_deleted: 0 });
        assert_eq!(atom_count(&pool, legacy).await, 0);

        pool.close().await;
        drop_db(&base, db).await;
    }

    /// A fresh tenant-tree database plus a second pool connected AS the real
    /// NOBYPASSRLS `coralswarm_tenant_app` role (`ALTER ROLE ... PASSWORD`
    /// is retried because the migration that creates the role can race the
    /// first statement on a brand-new database). Returns `(owner, app)`:
    /// seed and inspect through `owner`, exercise the code under test
    /// through `app`.
    async fn tenant_tree_as_app_role(base: &str, db: &str, org_id: &str) -> (PgPool, PgPool) {
        recreate(base, db).await.close().await.ok();
        let owner_pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&owner_pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&owner_pool)
            .await
            .unwrap();
        let pw = "src_reingest_tenant_role_test_only_pw";
        let stmt = format!("ALTER ROLE coralswarm_tenant_app WITH PASSWORD '{pw}'");
        let mut ok = false;
        for attempt in 0..10u32 {
            if sqlx::query(&stmt).execute(&owner_pool).await.is_ok() {
                ok = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50 * u64::from(attempt + 1))).await;
        }
        assert!(ok, "set test password on coralswarm_tenant_app");
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(&with_db(base, db))
            .unwrap()
            .username("coralswarm_tenant_app")
            .password(pw);
        let app = PgPoolOptions::new().max_connections(2).connect_with(opts).await.expect("connect AS coralswarm_tenant_app");
        (owner_pool, app)
    }

    /// Org plane, as the REAL NOBYPASSRLS `coralswarm_tenant_app` role: two
    /// members hold their own coral for the SAME event id in their own
    /// member-private oceans. Alice's edit replaces only Alice's; Bob's
    /// survives byte-for-byte. Running as the app role is what proves the
    /// lookup's `begin_tenant_system_scoped` actually sees the member graph
    /// — an account-scoped read would find nothing and report `FirstSeen`,
    /// silently leaving the stale coral in place.
    #[tokio::test]
    async fn org_member_edit_replaces_only_that_members_copy_as_tenant_app_role_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_member_edit_replaces_only_that_members_copy_as_tenant_app_role_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "src_reingest_org_role_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner_pool, app) = tenant_tree_as_app_role(&base, db, "org_src_reingest").await;

        let account_id = Uuid::new_v4();
        let (alice, bob) = (Uuid::new_v4(), Uuid::new_v4());
        let start = ts("2026-10-06T15:00:00Z");
        let v1 = "Planning\n\nLocation: R4";
        let mut graphs = Vec::new();
        let mut atoms = Vec::new();
        for u in [alice, bob] {
            let g: Uuid = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
                 VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
            )
            .bind(account_id)
            .bind(u)
            .fetch_one(&owner_pool)
            .await
            .unwrap();
            let a = ingest_like_prod(&owner_pool, account_id, g, u, "google_calendar", "shared-evt", v1, start, &[]).await;
            graphs.push(g);
            atoms.push(a);
        }

        // Alice's calendar reports the event renamed. Lookup + cascade run
        // as the app role against Alice's graph only.
        let v2 = "Planning (rescheduled)\n\nLocation: R4";
        let renamed = Incoming { content: v2, timestamp: start, participants: &[] };
        let out = retract_if_changed(&app, None, account_id, graphs[0], "google_calendar", "shared-evt", &renamed)
            .await
            .unwrap();
        assert_eq!(out, ReingestOutcome::Replaced { atoms_deleted: 1, reefs_deleted: 0 });

        let left: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM atoms ORDER BY atom_id")
            .fetch_all(&owner_pool)
            .await
            .unwrap();
        assert_eq!(left, vec![atoms[1]], "alice's stale coral retracted as the app role; bob's survives");

        // Bob's own unchanged re-fetch of the same event id: nothing happens
        // to Bob's coral either.
        let same = Incoming { content: v1, timestamp: start, participants: &[] };
        let out = retract_if_changed(&app, None, account_id, graphs[1], "google_calendar", "shared-evt", &same)
            .await
            .unwrap();
        assert_eq!(out, ReingestOutcome::Unchanged);
        assert_eq!(atom_count(&owner_pool, atoms[1]).await, 1);

        app.close().await;
        owner_pool.close().await;
        drop_db(&base, db).await;
    }

    fn item(source_type: &str, source_id: &str, content: &str, timestamp: DateTime<Utc>, participants: &[&str]) -> NormalizedItem {
        NormalizedItem {
            source_type: source_type.to_string(),
            source_id: source_id.to_string(),
            content: content.to_string(),
            participants: participants.iter().map(|s| s.to_string()).collect(),
            timestamp,
            scope: None,
            thread_id: None,
            channel_id: None,
            author_id: None,
            author_name: None,
        }
    }

    /// Microsoft Calendar, org plane, as the real `coralswarm_tenant_app`
    /// role — the exact shape `workers::microsoft_calendar_sync::sync_member`
    /// and the manual `/v1/connectors/microsoft/sync` route hand to
    /// [`retract_if_changed_item`]: a batch of `microsoft_calendar` events
    /// plus the `teams_transcript` items `plan_member_transcripts` adds.
    /// Proves (1) an edited event replaces only THAT member's coral, (2) a
    /// transcript atom sharing the event's source id is never touched even
    /// when its content differs (the gate, not the comparison, protects it),
    /// (3) the other member's copy of the same event id survives, (4) an
    /// unchanged re-fetch — every 30-minute tick, in practice — is a no-op.
    #[tokio::test]
    async fn microsoft_event_edit_replaces_only_that_members_coral_and_skips_transcripts_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping microsoft_event_edit_replaces_only_that_members_coral_and_skips_transcripts_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "src_reingest_ms_org_role_ten";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner_pool, app) = tenant_tree_as_app_role(&base, db, "org_src_reingest_ms").await;
        let ms = crate::connectors::microsoft::CALENDAR_SOURCE_TYPE;

        let account_id = Uuid::new_v4();
        let (alice, bob) = (Uuid::new_v4(), Uuid::new_v4());
        let start = ts("2026-10-07T09:00:00Z");
        let v1 = "Sprint review\n\nAgenda: demo, retro";
        let people = ["Alice".to_string(), "Bob".to_string()];
        let mut graphs = Vec::new();
        let mut events = Vec::new();
        for u in [alice, bob] {
            let g: Uuid = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
                 VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
            )
            .bind(account_id)
            .bind(u)
            .fetch_one(&owner_pool)
            .await
            .unwrap();
            let a = ingest_like_prod(&owner_pool, account_id, g, u, ms, "AAMkAG-evt", v1, start, &people).await;
            graphs.push(g);
            events.push(a);
        }
        // A transcript in alice's ocean whose source id happens to equal the
        // event id (Teams transcripts are keyed by meeting, and a meeting
        // can be keyed by its calendar event). Its content is NOT what the
        // batch now carries — exactly the "re-fetch with different content
        // is not an edit" case the gate exists for.
        let transcript = ingest_like_prod(&owner_pool, account_id, graphs[0], alice, "teams_transcript", "AAMkAG-evt", "Alice: hi\nBob: hello", start, &[]).await;

        // 1. Alice's sweep re-fetches the event renamed with a new attendee.
        let v2 = "Sprint review (moved to Thursday)\n\nAgenda: demo, retro";
        let renamed = item(ms, "AAMkAG-evt", v2, start, &["Alice", "Bob", "Carol"]);
        let out = retract_if_changed_item(&app, None, account_id, graphs[0], &renamed).await.unwrap();
        assert_eq!(out, Some(ReingestOutcome::Replaced { atoms_deleted: 1, reefs_deleted: 0 }));
        assert_eq!(atom_count(&owner_pool, events[0]).await, 0, "alice's stale event coral retracted");
        assert_eq!(atom_count(&owner_pool, transcript).await, 1, "alice's transcript with the same source id untouched");
        assert_eq!(atom_count(&owner_pool, events[1]).await, 1, "bob's copy of the same event id untouched");

        // 2. The transcript item in the same batch, with content that
        //    differs from what is stored: not a calendar event, so no
        //    lookup, no retract — `None`.
        let grown = item("teams_transcript", "AAMkAG-evt", "Alice: hi\nBob: hello\nCarol: sorry I'm late", start, &[]);
        let out = retract_if_changed_item(&app, None, account_id, graphs[0], &grown).await.unwrap();
        assert_eq!(out, None);
        assert_eq!(atom_count(&owner_pool, transcript).await, 1);

        // 3. Bob's own tick re-fetches the event unchanged (attendees in a
        //    different order): no churn.
        let same = item(ms, "AAMkAG-evt", v1, start, &["Bob", "Alice"]);
        let out = retract_if_changed_item(&app, None, account_id, graphs[1], &same).await.unwrap();
        assert_eq!(out, Some(ReingestOutcome::Unchanged));
        assert_eq!(atom_count(&owner_pool, events[1]).await, 1);

        // 4. And the ingest that follows alice's retract in production lands
        //    exactly one coral for the event, titled from the new subject.
        let fresh = ingest_like_prod(&owner_pool, account_id, graphs[0], alice, ms, "AAMkAG-evt", v2, start, &["Alice".into(), "Bob".into(), "Carol".into()]).await;
        let title: Option<String> = sqlx::query_scalar("SELECT title FROM atoms WHERE atom_id = $1")
            .bind(fresh)
            .fetch_one(&owner_pool)
            .await
            .unwrap();
        assert_eq!(title.as_deref(), Some("Sprint review (moved to Thursday)"));
        let copies: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_type = $2 AND source_id LIKE 'AAMkAG-evt:chunk:%'",
        )
        .bind(graphs[0])
        .bind(ms)
        .fetch_one(&owner_pool)
        .await
        .unwrap();
        assert_eq!(copies, 1, "exactly one microsoft_calendar coral for the event after the edit");

        app.close().await;
        owner_pool.close().await;
        drop_db(&base, db).await;
    }
}

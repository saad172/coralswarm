//! GitHub disconnect — the pause-only analog of `slack_disconnect`/
//! `microsoft_disconnect`/`google_disconnect` (docs/connectors/github.md
//! §3.8, "never purge").
//!
//! # Why this is NOT shaped like its siblings
//!
//! Every other connector's disconnect module ultimately DELETES rows (watch
//! channels, watched conversations, watched threads) — withdrawing consent
//! to keep capturing NEW content, while leaving already-ingested atoms
//! completely untouched (their own module docs are explicit about that
//! split). GitHub's contract goes one step further: §3.8 requires that even
//! the WATCH ROW itself survive a disconnect, uninstall, or repo-grant
//! removal — `active = false`, `paused_reason` set, `sync_stopped_at`
//! stamped, and every atom already ingested kept exactly as it is. There is
//! no delete branch here at all; "purge" would be the wrong word for what
//! this module does.
//!
//! Three distinct triggers pause rows, all funnelled through
//! [`pause_repos`]:
//!
//! * **App uninstall** (`installation.deleted` webhook, or the authenticated
//!   `DELETE /v1/connectors/github/installs/{id}` route) — pauses every row
//!   for that installation.
//! * **Repo removed from the grant** (`installation_repositories.removed`
//!   webhook) — pauses only the rows naming that specific `repo_id`.
//! * **Admin toggle-off** (`PUT /v1/connectors/github/repos` with
//!   `active: false` for a repo) — handled by `routes::github`'s own upsert,
//!   not this module (a toggle is a normal field update, not a disconnect
//!   event) — but reuses the SAME `paused_reason` vocabulary below so the
//!   selection API and this module never drift on what a pause means.

use sqlx::PgPool;
use uuid::Uuid;

/// The `github_watched_repos.paused_reason` vocabulary. A `TEXT` column, not
/// an enum — free text, like every other connector's reason field in this
/// tree — but centralized here so every writer uses the same three strings.
pub(crate) mod paused_reason {
    /// The GitHub App was uninstalled from this installation.
    pub(crate) const UNINSTALLED: &str = "uninstalled";
    /// The repo was removed from the installation's grant
    /// (`installation_repositories.removed`) while the App itself stayed
    /// installed.
    pub(crate) const REPO_REMOVED: &str = "repo_removed";
    /// An admin explicitly toggled this repo off via the selection API.
    pub(crate) const ADMIN_TOGGLED_OFF: &str = "admin_toggled_off";
    /// Granted by GitHub (present in `GET /installation/repositories`) but
    /// never yet selected — the §3.3 "pending" landing state. Not produced
    /// by this module (no pause EVENT created it), but shares the same
    /// column and is listed here so every reader of `paused_reason` sees
    /// the complete vocabulary in one place.
    pub(crate) const PENDING_SELECTION: &str = "pending_selection";
    /// The backfill/event/sweep worker hit a 404 fetching this repo — it
    /// vanished (renamed across owners, deleted, or the installation lost
    /// reach without a `installation_repositories.removed` delivery arriving
    /// first). `connectors::github_backfill`/`connectors::github_event_ingest`
    /// are the writers; never produced by this module directly.
    pub(crate) const REPO_UNAVAILABLE: &str = "repo_unavailable";
    /// The retired code tier's per-repo file-count ceiling was exceeded, or
    /// the tree listing came back `truncated: true` (§3.9, §8 item 6). No
    /// writer since the code tier was removed (2026-10-27); kept because
    /// rows paused before then still carry it and the UI renders it.
    #[allow(dead_code)] // no writer since 2026-10-27; still a value the column holds
    pub(crate) const FILE_CEILING_EXCEEDED: &str = "file_ceiling_exceeded";
    /// The human who chose this repo's destination can no longer reach it —
    /// they were removed from the team, left the org, or the ocean was
    /// archived. Written by [`pause_for_unreachable_destination`], from two
    /// places: `routes::teams::remove_team_member` (immediately, on the
    /// roster event) and `connectors::github_backfill`'s per-write
    /// reachability gate (on the next tick, for every other way
    /// reachability can be lost — an org departure, an archived ocean — and
    /// as the backstop for a roster event that never fired).
    ///
    /// The one `paused_reason` an admin can ACT on without contacting
    /// support: re-route the repo, or restore the person's membership.
    pub(crate) const DESTINATION_UNREACHABLE: &str = "destination_unreachable";
}

/// Pause every `active` `github_watched_repos` row for `account_id` +
/// `installation_id`, optionally narrowed to one `repo_id` — the single
/// write path [`pause_repos`]'s three callers (uninstall, repo-removed,
/// explicit disconnect route) all funnel through, so "what counts as a
/// pause" can never drift between them.
///
/// **Never deletes.** Every atom already ingested under a paused row is left
/// completely alone — this function touches `github_watched_repos` only.
/// Idempotent: re-pausing an already-paused row just re-stamps
/// `sync_stopped_at`/`paused_reason` (harmless — `updated_at`/
/// `sync_stopped_at` moving forward on a repeat webhook delivery is
/// expected, not a bug).
///
/// Returns the number of rows this call actually paused (`active` flipped
/// `true` -> `false`) — a caller that expected at least one match (the
/// departure conformance test) can tell "nothing to pause" apart from "this
/// call did the pausing".
pub(crate) async fn pause_repos(
    pool: &PgPool,
    account_id: Uuid,
    installation_id: i64,
    repo_id: Option<i64>,
    reason: &str,
) -> Result<usize, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("pause_repos scoped tx: {e}")))?;

    let result = sqlx::query(
        "UPDATE github_watched_repos \
         SET active = false, paused_reason = $1, sync_stopped_at = now(), updated_at = now() \
         WHERE account_id = $2 AND installation_id = $3 \
           AND ($4::bigint IS NULL OR repo_id = $4) \
           AND active = true",
    )
    .bind(reason)
    .bind(account_id)
    .bind(installation_id)
    .bind(repo_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

/// Pause EVERY active repo across EVERY installation this account has —
/// used by uninstall-all / conformance, not by a single member's OAuth
/// disconnect (that is [`pause_for_member`]).
pub(crate) async fn pause_all_for_account(pool: &PgPool, account_id: Uuid) -> Result<usize, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("pause_all_for_account scoped tx: {e}")))?;
    let result = sqlx::query(
        "UPDATE github_watched_repos \
         SET active = false, paused_reason = $1, sync_stopped_at = now(), updated_at = now() \
         WHERE account_id = $2 AND active = true",
    )
    .bind(paused_reason::UNINSTALLED)
    .bind(account_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

/// Pause active watch rows owned by one member — the
/// `DELETE /v1/oauth/connections/github` arm. Another member's still-valid
/// GitHub connection must not have its repos paused.
pub(crate) async fn pause_for_member(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Result<usize, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("pause_for_member scoped tx: {e}")))?;
    let result = sqlx::query(
        "UPDATE github_watched_repos \
         SET active = false, paused_reason = $1, sync_stopped_at = now(), updated_at = now() \
         WHERE account_id = $2 AND owner_user_id = $3 AND active = true",
    )
    .bind(paused_reason::UNINSTALLED)
    .bind(account_id)
    .bind(owner_user_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

/// Pause every active watch row that `owner_user_id` routed into
/// `graph_id` — they can no longer reach that ocean, so this repo must stop
/// trying to write into it.
///
/// # Why a pause and not just a refusal
///
/// `connectors::github_backfill::destination_reachable` already refuses the
/// WRITE, which is the safety property. But a refusal alone leaves the watch
/// row `active`, so the job queue keeps re-enqueuing a backfill that can only
/// ever fail, three attempts at a time, forever. Pausing converts a permanent
/// refusal into a terminal state an admin can see and fix. The two are belt
/// and braces on purpose: the pause depends on an event or a tick happening,
/// the refusal does not — a row that is somehow still active must STILL be
/// refused, which is why this function never replaces that gate.
///
/// # Scope of the UPDATE
///
/// Keyed on `(account_id, owner_user_id, graph_id)` and `active = true`.
/// Narrow on purpose: another member who routed the same repo into the same
/// ocean is unaffected (their own reachability is their own question), rows
/// the same member pointed at a DIFFERENT ocean are unaffected, and an
/// already-paused row is left with whatever reason paused it first rather
/// than being re-stamped with this one.
///
/// Also records `sync_error_code` in the same statement, so the selection
/// page's failure state and its pause state can never disagree about why a
/// repo stopped. The two columns are separate vocabularies that happen to
/// share this one token — bound separately below rather than aliased, since
/// nothing guarantees they stay in step.
pub(crate) async fn pause_for_unreachable_destination(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    graph_id: Uuid,
) -> Result<usize, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("pause_for_unreachable_destination scoped tx: {e}")))?;
    let result = sqlx::query(
        "UPDATE github_watched_repos \
         SET active = false, paused_reason = $1, sync_stopped_at = now(), \
             sync_error_code = $5, sync_error_at = now(), updated_at = now() \
         WHERE account_id = $2 AND owner_user_id = $3 AND graph_id = $4 AND active = true",
    )
    .bind(paused_reason::DESTINATION_UNREACHABLE)
    .bind(account_id)
    .bind(owner_user_id)
    .bind(graph_id)
    .bind(crate::connectors::github_backfill::SyncErrorCode::DestinationUnreachable.as_str())
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

#[cfg(test)]
mod tests {
    use super::paused_reason;

    #[test]
    fn paused_reason_vocabulary_is_distinct() {
        let all = [
            paused_reason::UNINSTALLED,
            paused_reason::REPO_REMOVED,
            paused_reason::ADMIN_TOGGLED_OFF,
            paused_reason::PENDING_SELECTION,
            paused_reason::REPO_UNAVAILABLE,
            paused_reason::FILE_CEILING_EXCEEDED,
            paused_reason::DESTINATION_UNREACHABLE,
        ];
        let unique: std::collections::HashSet<_> = all.iter().collect();
        assert_eq!(unique.len(), all.len(), "every paused_reason string must be distinct");
    }
}

/// pg-gated integration tests: proves pause behaviour against a REAL
/// Postgres database.
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib github_disconnect_pg_tests
#[cfg(test)]
mod github_disconnect_pg_tests {
    use super::*;
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
    async fn drop_db(admin_conn: &mut PgConnection, db: &str) {
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    async fn seed_graph(pool: &sqlx::PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) \
             VALUES ($1, 'test ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    #[allow(clippy::too_many_arguments)]
    async fn seed_repo(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        installation_id: i64,
        repo_id: i64,
        graph_id: Uuid,
        active: bool,
    ) {
        sqlx::query(
            "INSERT INTO github_watched_repos \
                (account_id, installation_id, repo_id, owner, name, active, graph_id, destination_kind) \
             VALUES ($1, $2, $3, 'acme', 'repo', $4, $5, 'personal')",
        )
        .bind(account_id)
        .bind(installation_id)
        .bind(repo_id)
        .bind(active)
        .bind(graph_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Departure (contract conformance test 7): removing ONE repo from the
    /// grant pauses exactly that repo and nothing else — a sibling repo on
    /// the SAME installation must survive untouched.
    #[tokio::test]
    async fn removing_one_repo_pauses_only_that_repo_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping removing_one_repo_pauses_only_that_repo_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gh_disconnect_one_repo";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gh_disconnect_one_repo')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id).await;
        seed_repo(&pool, account_id, 111, 1, graph_id, true).await;
        seed_repo(&pool, account_id, 111, 2, graph_id, true).await;

        let paused = pause_repos(&pool, account_id, 111, Some(1), paused_reason::REPO_REMOVED)
            .await
            .unwrap();
        assert_eq!(paused, 1);

        let (active_1, reason_1): (bool, Option<String>) =
            sqlx::query_as("SELECT active, paused_reason FROM github_watched_repos WHERE repo_id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!active_1);
        assert_eq!(reason_1.as_deref(), Some(paused_reason::REPO_REMOVED));

        let active_2: bool = sqlx::query_scalar("SELECT active FROM github_watched_repos WHERE repo_id = 2")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(active_2, "the sibling repo must survive untouched");

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// Disconnect does not remove content (contract conformance test 8):
    /// pausing never deletes the `github_watched_repos` row, and this
    /// module has no code path that touches `atoms` at all.
    #[tokio::test]
    async fn pause_never_deletes_the_watch_row_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping pause_never_deletes_the_watch_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gh_disconnect_keeps_row";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gh_disconnect_keeps_row')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id).await;
        seed_repo(&pool, account_id, 222, 9, graph_id, true).await;

        pause_all_for_account(&pool, account_id).await.unwrap();

        let row_count: i64 = sqlx::query_scalar("SELECT count(*) FROM github_watched_repos WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row_count, 1, "the row must survive a disconnect — paused, never deleted");

        let (active, stopped_at): (bool, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as("SELECT active, sync_stopped_at FROM github_watched_repos WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!active);
        assert!(stopped_at.is_some());

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }
}

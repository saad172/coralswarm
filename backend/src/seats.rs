//! Team-seat provenance: why a seat exists, and what it takes to remove it.
//!
//! Auto-seating grants a team-ocean seat because some external container (a
//! Slack channel, today) was routed into that ocean. A seat granted for a
//! reason has to disappear when the reason does — otherwise "routing a
//! channel seats its members" is a one-way ratchet, and leaving a channel
//! quietly leaves the person's read access to that ocean intact forever.
//!
//! The whole design is in migration `0106_team_seat_grants`'s header. The
//! two rules worth repeating at the point of use:
//!
//! 1. **A seat survives while ANY grant survives.** One person seated by two
//!    routed channels keeps their seat after leaving one of them. This is
//!    the case a `granted_by_slack` boolean on `team_members` could not
//!    express, and the reason this is a table.
//!
//! 2. **Revocation only ever fires as a consequence of deleting a grant we
//!    recorded.** [`revoke_seat_grant`] refuses to touch `team_members`
//!    unless its DELETE actually removed a row. A seat with no grants at all
//!    is not "safe to revoke" — it is "not ours to revoke", and the two must
//!    never be conflated. That is the difference between this feature and a
//!    feature that silently deletes hand-granted access.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// `source` for a seat granted because a Slack channel routes into the team's
/// ocean. Namespaced so a second connector granting seats for its own reason
/// can never collide with a Slack channel id.
pub(crate) fn slack_channel_source(channel_id: &str) -> String {
    format!("slack:{channel_id}")
}

/// `source` for a seat granted because a Microsoft Teams channel routes into
/// the team's ocean (m365 Train 2 PR 2.4, decision #3). Same namespacing
/// rationale as [`slack_channel_source`] — a Teams channel id can never
/// collide with a Slack one under a shared `(team_id, user_id, source)` key.
pub(crate) fn teams_channel_source(channel_id: &str) -> String {
    format!("teams:{channel_id}")
}

/// `source` for a seat granted because a Google Chat SPACE routes into the
/// team's ocean (gsuite Train G4 PR 4, design doc §3.4/D7). Same namespacing
/// rationale as [`slack_channel_source`]/[`teams_channel_source`].
///
/// `space_id` is Chat's own `"spaces/{id}"` resource name, passed through
/// verbatim rather than trimmed to the bare id: it is globally unique and
/// stable, it is what every other Chat table and every Chat API call keys
/// on, and trimming it would make the grant's `source` un-joinable back to
/// `google_chat_watched_spaces.space_id` without a string rebuild. The
/// resulting literal is `"google_chat:spaces/{id}"`, which can collide with
/// neither a Slack channel id nor a Teams one under
/// `team_seat_grants`' shared `(team_id, user_id, source)` key.
pub(crate) fn google_chat_space_source(space_id: &str) -> String {
    format!("google_chat:{space_id}")
}

/// `source` for a seat granted because a GitHub repository routes into the
/// team's ocean (GitHub connector PR A, docs/connectors/github.md §3.4 —
/// routing a repo to a team ocean auto-seats its mapped collaborators, I4).
/// Same namespacing rationale as [`slack_channel_source`]/
/// [`teams_channel_source`]/[`google_chat_space_source`] — a GitHub repo id
/// can never collide with any other connector's source under
/// `team_seat_grants`' shared `(team_id, user_id, source)` key.
pub(crate) fn github_repo_source(repo_id: i64) -> String {
    format!("github:{repo_id}")
}

/// `source` recorded when auto-seating finds a person ALREADY holding an
/// active seat it did not create.
///
/// Load-bearing, not bookkeeping: this row is never deleted by any connector
/// event, so a `(team_id, user_id)` that has one can never reach an empty
/// grant set, and [`revoke_seat_grant`]'s revocation branch is therefore
/// unreachable for it. That is how a manually-granted seat survives being
/// auto-granted a second time and then losing the automatic reason.
pub(crate) const SOURCE_PREEXISTING: &str = "preexisting";

/// What [`revoke_seat_grant`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RevokeOutcome {
    /// No grant existed for this `(team, user, source)`, so nothing was
    /// removed and the seat was not examined. Either we never granted it, or
    /// this is a replay of an event already processed. **The seat is left
    /// exactly as it was** — see rule 2 in this module's header.
    NotOurs,
    /// The grant was removed, but other reasons for this seat remain (another
    /// routed channel, or a `preexisting` marker). The seat stands.
    GrantDropped { remaining: i64 },
    /// The grant was removed, it was the last one, and the seat was revoked.
    SeatRevoked,
    /// The grant was removed and it was the last one, but the seat was left
    /// alone because the member holds it as an `admin`. Auto-seating never
    /// grants 'admin', so it must never take one away.
    AdminPreserved,
}

/// Record that `source` is a reason for `user_id` to hold a seat on `team_id`.
///
/// Idempotent — the roster reconciler re-asserts every grant on every sweep,
/// and a `member_joined_channel` event Slack redelivers must not be an error.
///
/// `history_from` (m365 enterprise train PR 4.1, migration
/// `0064_team_seat_grants_history_from`) is the retrieval-window floor this
/// grant vouches for: `None` = full history, `Some(ts)` = "atoms timestamped
/// before `ts` are outside what THIS grant justifies" (the seat's actual
/// window is the widest any surviving grant justifies — resolved at read
/// time by `tenant_rls_visible_graph_windows()`, never here). Every caller's
/// choice of value is documented at its own call site; the two repeating
/// rules are "founding roster / auto-seated-by-routing → `None`" and
/// "discovered later → `Some(now())`" (`docs/plans/
/// org-connectors-and-channel-ingest.md` §4/§5).
///
/// Because the INSERT is `ON CONFLICT ... DO NOTHING`, `history_from` is
/// ONLY EVER the value stamped the FIRST time this exact `(team_id, user_id,
/// source)` grant is recorded — a re-sweep that re-asserts an existing grant
/// passes whatever value it likes here and it is silently ignored. This is
/// load-bearing, not an oversight: a grant's window must never be narrowed
/// (or widened) by a later sweep re-observing the same reason, only by an
/// admin's explicit action or by a NEW distinct grant appearing.
/// Returns `true` when this call actually inserted a new grant row, `false`
/// when `(team_id, user_id, source)` already existed (the `ON CONFLICT ...
/// DO NOTHING` no-op) — added (m365 enterprise train PR 4.2) so a caller
/// that wants to audit GENUINE grants (not every idempotent re-assertion a
/// sweep performs) can tell the two apart without a separate lookup. Every
/// existing caller that ignored the previous `Result<(), _>` value keeps
/// compiling unchanged; only a caller that inspects the `Ok` payload needs
/// to change.
pub(crate) async fn record_seat_grant(
    pool: &PgPool,
    account_id: Uuid,
    team_id: Uuid,
    user_id: Uuid,
    source: &str,
    history_from: Option<DateTime<Utc>>,
) -> Result<bool, sqlx::Error> {
    // `team_seat_grants` is on the tenant plane's `member_ocean_isolation`
    // roster too (migration `migrations_tenant/0017_team_seat_grants`,
    // anchored `team_id -> teams -> graph_id` — added AFTER 0009, easy to
    // miss by only reading that migration). Its policy has no explicit
    // `WITH CHECK`, so a bare policy reuses `USING` for INSERT too: under
    // plain `begin_account_scoped` (no `app.tenant_actor`), a team-kind
    // graph is invisible to `tenant_rls_visible_graph_ids()`, so this INSERT
    // would silently affect zero rows once a tenant connection is no longer
    // owner-exempt. `begin_tenant_system_scoped` is required here for the
    // same reason it is in `revoke_seat_grant` just below.
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("record_seat_grant scoped tx: {e}")))?;
    let inserted: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO team_seat_grants (account_id, team_id, user_id, source, history_from) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (team_id, user_id, source) DO NOTHING \
         RETURNING team_id",
    )
    .bind(account_id)
    .bind(team_id)
    .bind(user_id)
    .bind(source)
    .bind(history_from)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(inserted.is_some())
}

/// Drop one reason for a seat, and revoke the seat if it was the last one.
///
/// The ordering is the safety property. The DELETE comes first and its row
/// count is the gate: if it removed nothing, this function returns
/// [`RevokeOutcome::NotOurs`] without ever reading or writing `team_members`.
/// Counting grants first and revoking on zero would revoke every seat that
/// predates this table.
pub(crate) async fn revoke_seat_grant(
    pool: &PgPool,
    account_id: Uuid,
    team_id: Uuid,
    user_id: Uuid,
    source: &str,
) -> Result<RevokeOutcome, sqlx::Error> {
    // `team_members` is on the tenant plane's `0009_tenant_rls` roster
    // (org-slack step 4): `begin_account_scoped` alone sets only
    // `app.account_id`, which its `member_ocean_isolation` policy never
    // treats as sufficient for a team ocean — a plain scope here would
    // silently touch zero rows on an org tenant, exactly the "unscoped read
    // returns nothing in prod, passes every test run as the owner" failure
    // class this file's own header warns about elsewhere.
    // `begin_tenant_system_scoped` additionally sets `app.tenant_actor =
    // 'system'`, harmless on the consumer plane (0083's policies never read
    // that GUC).
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("revoke_seat_grant scoped tx: {e}")))?;

    let dropped = sqlx::query(
        "DELETE FROM team_seat_grants \
         WHERE team_id = $1 AND user_id = $2 AND source = $3",
    )
    .bind(team_id)
    .bind(user_id)
    .bind(source)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    // The gate. Absence of provenance means "not ours to revoke".
    if dropped == 0 {
        tx.commit().await?;
        return Ok(RevokeOutcome::NotOurs);
    }

    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM team_seat_grants WHERE team_id = $1 AND user_id = $2",
    )
    .bind(team_id)
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await?;

    if remaining > 0 {
        tx.commit().await?;
        return Ok(RevokeOutcome::GrantDropped { remaining });
    }

    // Last reason gone. `role <> 'admin'` is the second guard: auto-seating
    // only ever writes 'member', so an admin seat was established some other
    // way and is not this system's to remove. `departing_at IS NULL` keeps
    // this idempotent against a redelivered event.
    let revoked = sqlx::query(
        "UPDATE team_members SET departing_at = now() \
         WHERE team_id = $1 AND user_id = $2 AND departing_at IS NULL AND role <> 'admin'",
    )
    .bind(team_id)
    .bind(user_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    tx.commit().await?;

    if revoked > 0 {
        Ok(RevokeOutcome::SeatRevoked)
    } else {
        // Either an admin (guarded above) or an already-departing row. Both
        // are "the seat is not standing because of us" — reported distinctly
        // from `SeatRevoked` so a caller never claims an access change it
        // did not make.
        Ok(RevokeOutcome::AdminPreserved)
    }
}

/// The team whose ocean `channel_id` routes into, if it routes into one at all.
///
/// Only a `mode = 'single'` pick that lands on a graph some team owns can
/// grant seats — a fan-out pick seats nobody (each person's content goes to
/// their own ocean), and a channel with no row has made no choice. Returning
/// `None` for both is what keeps every membership event a no-op until an
/// admin has actually routed the channel somewhere seat-bearing.
pub(crate) async fn resolve_channel_team(
    pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("resolve_channel_team scoped tx: {e}")))?;
    let team_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT t.id FROM slack_watched_channels swc \
         JOIN teams t ON t.graph_id = swc.graph_id AND t.account_id = swc.account_id \
         WHERE swc.account_id = $1 AND swc.workspace_id = $2 AND swc.channel_id = $3 \
           AND swc.mode = 'single' AND swc.graph_id IS NOT NULL \
         LIMIT 1",
    )
    .bind(account_id)
    .bind(workspace_id)
    .bind(channel_id)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    tx.rollback().await.ok();
    Ok(team_id)
}

/// Resolve a Slack member to a CoralSwarm user **without touching the Slack
/// API** — the confirmed `connector_identities` directory only.
///
/// Deliberately not `connectors::slack::resolve_verified_slack_identity`.
/// That function falls through to a live `users.info` call for anyone not
/// already linked, which is exactly wrong on a departure path: a person we
/// have never linked holds no grant, so the answer cannot change the outcome
/// — and paying a rate-limited API call (and writing a fresh identity link)
/// for somebody on their way OUT of a channel is work in the wrong
/// direction.
pub(crate) async fn resolve_linked_user_directory_only(
    pool: &PgPool,
    workspace_id: &str,
    external_user_id: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT user_id FROM connector_identities \
         WHERE provider = 'slack' AND workspace_id = $1 AND external_user_id = $2 \
           AND user_id IS NOT NULL",
    )
    .bind(workspace_id)
    .bind(external_user_id)
    .fetch_optional(pool)
    .await
    .map(Option::flatten)
}

/// Every `user_id` currently granted a seat on `team_id` because of `source`.
///
/// The reconciler diffs this against a fresh roster walk to find grants whose
/// reason has quietly disappeared — the people who left a channel while the
/// backend was down, or before `member_left_channel` was handled at all.
pub(crate) async fn users_granted_by_source(
    pool: &PgPool,
    account_id: Uuid,
    team_id: Uuid,
    source: &str,
) -> Result<Vec<Uuid>, sqlx::Error> {
    // `team_seat_grants` is RLS-covered on the tenant plane too (see
    // `record_seat_grant`'s doc comment above) — `begin_tenant_system_scoped`
    // for the same reason.
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("users_granted_by_source scoped tx: {e}")))?;
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT user_id FROM team_seat_grants WHERE team_id = $1 AND source = $2",
    )
    .bind(team_id)
    .bind(source)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(u,)| u).collect())
}

/// The founding-sweep decision for one `(team_id, source)`: `None` when this
/// pass seats the FOUNDING roster (full history), `Some(now)` when the source
/// was already founded and anyone newly granted this pass is a late joiner.
///
/// "Founded" is durable (m365 enterprise train PR 4.1, CodeRabbit round 1 on
/// #680): grant-emptiness alone is NOT the test, because `revoke_seat_grant`
/// deletes rows — a channel whose every member left would otherwise look
/// brand-new to the next sweep, and the next person to join would be
/// mis-stamped a founding member of a long-routed source (full ocean
/// history). The durable marker is a `seat_grant_sources` row (migration
/// `0064`/`0184`); surviving grants are kept as a belt-and-braces second
/// signal so deployments that predate the marker (grants exist, no
/// `seat_grant_sources` row yet) classify correctly without a backfill.
///
/// Callers that actually seat someone on a `None` result must then call
/// [`record_source_founding`] — founding is only durable once recorded.
pub(crate) async fn founding_history_from(
    pool: &PgPool,
    account_id: Uuid,
    team_id: Uuid,
    source: &str,
) -> Result<Option<chrono::DateTime<Utc>>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("founding_history_from scoped tx: {e}")))?;
    let founded: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM seat_grant_sources WHERE team_id = $1 AND source = $2) \
             OR EXISTS (SELECT 1 FROM team_seat_grants   WHERE team_id = $1 AND source = $2)",
    )
    .bind(team_id)
    .bind(source)
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(if founded { Some(Utc::now()) } else { None })
}

/// Record that `(team_id, source)` has had its founding roster seated. Called
/// after a pass that granted on a [`founding_history_from`] `None` result —
/// and by the routing-time auto-seat, which founds a source definitionally.
/// Idempotent; the row is never deleted by grant revocation.
pub(crate) async fn record_source_founding(
    pool: &PgPool,
    account_id: Uuid,
    team_id: Uuid,
    source: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("record_source_founding scoped tx: {e}")))?;
    sqlx::query(
        "INSERT INTO seat_grant_sources (account_id, team_id, source) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (team_id, source) DO NOTHING",
    )
    .bind(account_id)
    .bind(team_id)
    .bind(source)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

#[cfg(test)]
mod history_from_grant_population_pg_tests {
    //! m365 enterprise train PR 4.1 ("history_from"): proves
    //! [`record_seat_grant`]'s `history_from` contract directly against a
    //! real tenant database — the primitive both reconcilers
    //! (`workers::teams_seat_reconcile`, `workers::slack_seat_reconcile`)
    //! compose into their "founding sweep vs late joiner" rule (each
    //! reconciler's own `reconcile_channel` doc comment states the rule; this
    //! module proves the building block that rule depends on: the value
    //! passed in is stored on first insert and NEVER touched again).
    //!
    //! In-crate (not `backend/tests/*_pg.rs`) because `record_seat_grant`/
    //! `users_granted_by_source` are `pub(crate)`.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib history_from_grant_population_pg_tests

    use super::{
        founding_history_from, record_seat_grant, record_source_founding, revoke_seat_grant,
        users_granted_by_source,
    };
    use chrono::{Duration, SubsecRound, Utc};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

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

    /// A minimal team ocean: `graphs(kind='team')` + `teams` — the
    /// precondition `team_seat_grants.team_id`'s FK requires.
    async fn seed_team(pool: &PgPool, account_id: Uuid, creator: Uuid) -> Uuid {
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Team ocean', 'team') \
             RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar(
            "INSERT INTO teams (account_id, graph_id, name, created_by) \
             VALUES ($1, $2, 'Eng', $3) RETURNING id",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(creator)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn history_from_of(pool: &PgPool, team_id: Uuid, user_id: Uuid, source: &str) -> Option<chrono::DateTime<Utc>> {
        sqlx::query_scalar(
            "SELECT history_from FROM team_seat_grants \
             WHERE team_id = $1 AND user_id = $2 AND source = $3",
        )
        .bind(team_id)
        .bind(user_id)
        .bind(source)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// The founding-sweep rule: `users_granted_by_source` empty BEFORE
    /// granting means every member this pass seats gets `None` (full
    /// history) — exactly the pre-check both reconcilers run before their
    /// grant loop.
    #[tokio::test]
    async fn founding_sweep_stamps_full_history_for_everyone_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping founding_sweep_stamps_full_history_for_everyone_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_hf_founding_sweep";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_hf_founding')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let creator = Uuid::new_v4();
        let team_id = seed_team(&pool, account_id, creator).await;
        let source = "test:founding-source";

        // The pre-check itself: empty before anyone is granted.
        let pre = users_granted_by_source(&pool, account_id, team_id, source).await.unwrap();
        assert!(pre.is_empty(), "no grants exist yet for this (team_id, source)");
        let history_from = if pre.is_empty() { None } else { Some(Utc::now()) };
        assert_eq!(history_from, None);

        let (user_1, user_2) = (Uuid::new_v4(), Uuid::new_v4());
        for u in [user_1, user_2] {
            record_seat_grant(&pool, account_id, team_id, u, source, history_from)
                .await
                .expect("grant founding member");
        }

        for u in [user_1, user_2] {
            assert_eq!(
                history_from_of(&pool, team_id, u, source).await,
                None,
                "every member seated in the founding sweep must be stamped full history"
            );
        }

        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// The late-joiner rule: once ANY grant exists for a `(team_id,
    /// source)`, a NEW member discovered in a subsequent sweep is stamped
    /// `Some(now())`, while re-granting an ALREADY-seated member (the
    /// idempotent re-assert every sweep performs) leaves their ORIGINAL
    /// `history_from` untouched — `ON CONFLICT ... DO NOTHING` never
    /// narrows or widens an existing grant.
    #[tokio::test]
    async fn late_joiner_is_stamped_now_and_do_nothing_never_restamps_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping late_joiner_is_stamped_now_and_do_nothing_never_restamps_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_hf_late_joiner";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_hf_late')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let creator = Uuid::new_v4();
        let team_id = seed_team(&pool, account_id, creator).await;
        let source = "test:late-source";

        // Founding sweep: one member, full history.
        let founder = Uuid::new_v4();
        record_seat_grant(&pool, account_id, team_id, founder, source, None)
            .await
            .expect("grant founder");

        // A later sweep: pre-check now sees the founder, so this is NOT the
        // founding sweep — a newly discovered member gets `Some(now())`.
        let pre = users_granted_by_source(&pool, account_id, team_id, source).await.unwrap();
        assert_eq!(pre, vec![founder]);
        // Truncated to microsecond precision — `TIMESTAMPTZ` only carries
        // that much, and comparing a nanosecond-precision `Utc::now()`
        // against the round-tripped-through-Postgres value would spuriously
        // fail below over a precision mismatch, not a real difference.
        let now = Utc::now().trunc_subsecs(6);
        let history_from = if pre.is_empty() { None } else { Some(now) };
        assert_eq!(history_from, Some(now));

        let late_joiner = Uuid::new_v4();
        record_seat_grant(&pool, account_id, team_id, late_joiner, source, history_from)
            .await
            .expect("grant late joiner");
        // The re-assert of the ALREADY-granted founder, same pass, same
        // `history_from` value the late joiner got — this is exactly what
        // both reconcilers do (one shared value for the whole batch).
        record_seat_grant(&pool, account_id, team_id, founder, source, history_from)
            .await
            .expect("re-assert founder (idempotent)");

        assert_eq!(
            history_from_of(&pool, team_id, late_joiner, source).await,
            Some(now),
            "the newly discovered member must be stamped this sweep's `now()`"
        );
        assert_eq!(
            history_from_of(&pool, team_id, founder, source).await,
            None,
            "DO NOTHING must leave the founder's original NULL (full history) untouched — \
             re-asserting an existing grant must never narrow it"
        );

        // A THIRD sweep, later still, with a DIFFERENT `now()` — proves this
        // isn't narrowly true only immediately after the insert.
        let later_sweep_ts = now + Duration::hours(1);
        record_seat_grant(&pool, account_id, team_id, late_joiner, source, Some(later_sweep_ts))
            .await
            .expect("re-assert late joiner on a later sweep");
        assert_eq!(
            history_from_of(&pool, team_id, late_joiner, source).await,
            Some(now),
            "DO NOTHING must leave the late joiner's ORIGINAL stamp untouched even when a \
             later sweep passes a different `now()`"
        );

        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// The durable-founding fix (CodeRabbit round 1 on #680): after the
    /// LAST grant for a source is revoked, the source must NOT look
    /// brand-new — the next joiner is a late joiner of a long-routed
    /// channel, not a founding member entitled to full ocean history.
    #[tokio::test]
    async fn founding_survives_full_grant_revocation_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping founding_survives_full_grant_revocation_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_hf_founding_durable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_hf_durable')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let creator = Uuid::new_v4();
        let team_id = seed_team(&pool, account_id, creator).await;
        let source = "test:durable-source";

        // Nothing yet: founding pass.
        assert_eq!(
            founding_history_from(&pool, account_id, team_id, source).await.unwrap(),
            None,
            "an unfounded source with no grants is a founding pass"
        );

        // Found it: one member seated, marker recorded (the sequence the
        // reconcilers and the routing-time auto-seat both perform).
        let founder = Uuid::new_v4();
        record_seat_grant(&pool, account_id, team_id, founder, source, None).await.unwrap();
        record_source_founding(&pool, account_id, team_id, source).await.unwrap();
        // Idempotent.
        record_source_founding(&pool, account_id, team_id, source).await.unwrap();

        assert!(
            founding_history_from(&pool, account_id, team_id, source).await.unwrap().is_some(),
            "a founded source classifies later members as late joiners"
        );

        // Everyone leaves: the grant is deleted...
        revoke_seat_grant(&pool, account_id, team_id, founder, source).await.unwrap();
        assert!(
            users_granted_by_source(&pool, account_id, team_id, source).await.unwrap().is_empty(),
            "precondition: no surviving grants"
        );
        // ...and the founding must survive it.
        assert!(
            founding_history_from(&pool, account_id, team_id, source).await.unwrap().is_some(),
            "a fully-vacated source must stay founded — the next joiner is a late joiner, \
             not a founding member (the round-1 finding this test exists for)"
        );

        // Belt-and-braces legacy path: a source with surviving grants but
        // NO marker (deployments that predate `seat_grant_sources`) still
        // classifies as founded.
        let legacy_source = "test:legacy-no-marker";
        record_seat_grant(&pool, account_id, team_id, founder, legacy_source, None).await.unwrap();
        assert!(
            founding_history_from(&pool, account_id, team_id, legacy_source).await.unwrap().is_some(),
            "surviving grants alone (no marker) must still classify the source as founded"
        );

        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

//! The `org_join_requests` queue (CS-437, org-onboarding plan §3 items 4 +
//! 14, `docs/plans/org-onboarding-from-the-invite.md`, D3/D9/D10/D19) —
//! everything below `routes::org_join_requests` that is not HTTP shape:
//! enqueue (with D10's durable rejection suppression and the personal-org
//! refusal), the two admin reads, and the guarded decision write.
//!
//! `routes::org_join_requests::approve` composes this module's
//! [`mark_decided`] with `billing::slot_pool::reserve_slot`/
//! `release_reservation` and `routes::org_invites::provision_clerk_membership`
//! / `mirror_local_membership` — the actual D3 Clerk-first sequence lives
//! in the route handler, not here, because this module must never reference
//! `clerk_provision::ensure_org_membership` (the whole point of factoring
//! that call into `routes::org_invites` is that
//! `scripts/check_ensure_org_membership_callers.sh` only allowlists that one
//! file, and a service module reused by MCP tools or a future admin batch
//! action is exactly the kind of indirect caller that guard exists to
//! catch).
//!
//! Every single-account read/write against `org_join_requests` opens
//! [`crate::tenancy::scope::begin_account_scoped`] first — migration 0238
//! enables RLS with a strict `app.account_id` predicate, and
//! `state.control_db()` is `coralswarm_app` (`rolbypassrls = f`).
//! [`list_open_for_user`] is the one cross-account read: it discovers
//! account ids via the SECURITY DEFINER
//! `resolve_open_join_request_account_ids` (migration 0239), then iterates
//! those accounts under a real scoped transaction.

use serde_json::Value as Json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::scope::begin_account_scoped;

#[derive(Debug, thiserror::Error)]
pub enum JoinRequestError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
}

/// [`enqueue_join_request`]'s result. Every branch renders identically to
/// the requester — `routes::org_join_requests::create` sends the same
/// `{"status": "sent_to_admin"}` 202 body for all three (D9/D10: the
/// requester never learns the pool state or a rejection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueOutcome {
    /// A fresh open request was inserted.
    Queued(Uuid),
    /// This person already had an open request for this org — no second row
    /// (the partial unique index's own `ON CONFLICT` target).
    AlreadyOpen(Uuid),
    /// A durable rejected row already exists for this `(account_id,
    /// user_id)` (D10), or the target account is a personal org (D19: never
    /// receives a join request at all) — nothing was written.
    Suppressed,
}

/// `accounts.id` is a personal org iff `personal_orgs` carries a row for its
/// `clerk_org_id` — the same "org id is a selector naming a Clerk user, not
/// an org with members" shape `personal_orgs`' own migration (0208) header
/// describes. Unlike `store::Store::account_is_real_org` (which additionally
/// checks "is this the CALLER's OWN personal org", scoped by a `user_id`
/// argument — wrong for this module, which needs "is this account a
/// personal org for ANYONE at all", independent of who is asking), this is
/// a direct, caller-agnostic existence check.
pub(crate) async fn is_personal_org(control: &PgPool, account_id: Uuid) -> Result<bool, JoinRequestError> {
    let is_personal: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM personal_orgs po \
             JOIN accounts a ON a.clerk_org_id = po.clerk_org_id \
             WHERE a.id = $1 \
         )",
    )
    .bind(account_id)
    .fetch_one(control)
    .await?;
    Ok(is_personal)
}

/// **D10's durable per-org suppression, as one predicate.**
///
/// Shared by the two places that must agree about it, and `const` rather
/// than duplicated because they are on opposite paths and a drift between
/// them is invisible until it admits somebody:
///
/// * [`enqueue_join_request`] below, on the QUEUING path — "a rejected
///   person never gets a fresh open row, however many times they ask".
/// * [`has_rejected_decision`], read by
///   `routes::org_join_requests::create`'s auto-add branch on the ADDING
///   path — "a rejected person is never added without an admin, however
///   many gates have since opened".
///
/// `$1` is the account id, `$2` the user id. Deliberately unbounded in
/// time: D10's suppression does not expire, and a rejection from a year ago
/// still refuses.
pub(crate) const REJECTED_DECISION_EXISTS_SQL: &str = "SELECT EXISTS ( \
     SELECT 1 FROM org_join_requests \
      WHERE account_id = $1 AND user_id = $2 AND decision = 'rejected' \
 )";

/// Has an admin already rejected this person for this org?
///
/// The auto-add half of [`REJECTED_DECISION_EXISTS_SQL`]. Opens its own
/// account-scoped transaction because it is read BEFORE, and independently
/// of, any enqueue — on the path that adds a member outright, no enqueue
/// ever happens.
pub(crate) async fn has_rejected_decision(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, JoinRequestError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let rejected: bool = sqlx::query_scalar(REJECTED_DECISION_EXISTS_SQL)
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(rejected)
}

/// Queue a join request for `user_id` against `account_id`. `source` must be
/// one of `org_join_requests`'s own five-value `CHECK` domain (migration
/// 0238) — this function does not validate it; callers pass a
/// compile-time-known literal (`"self_request"`, `"tombstoned_returning"`,
/// …), so an invalid value is a caller bug the `CHECK` constraint itself
/// catches loudly (`sqlx::Error`) rather than something this function needs
/// to pre-validate.
///
/// D19/D10: suppression is checked FIRST, before the insert attempt — a
/// personal-org target, or a person with a durable rejected row for this
/// org, never gets a fresh open row, however many times they ask.
pub(crate) async fn enqueue_join_request(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    source: &str,
    evidence: Json,
) -> Result<EnqueueOutcome, JoinRequestError> {
    if is_personal_org(control, account_id).await? {
        return Ok(EnqueueOutcome::Suppressed);
    }

    let mut tx = begin_account_scoped(control, account_id).await?;

    // D10: a rejected row is the durable, per-org suppression — checked
    // regardless of how long ago it was decided, and regardless of whether
    // an open row exists today (it never does at the same time — decided
    // and open are mutually exclusive per person per org, `0238`'s own
    // partial unique index only covers `decision IS NULL` rows).
    let rejected: bool = sqlx::query_scalar(REJECTED_DECISION_EXISTS_SQL)
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await?;
    if rejected {
        tx.commit().await?;
        return Ok(EnqueueOutcome::Suppressed);
    }

    let inserted: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO org_join_requests (account_id, user_id, source, evidence) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (account_id, user_id) WHERE decision IS NULL DO NOTHING \
         RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(source)
    .bind(evidence)
    .fetch_optional(&mut *tx)
    .await?;

    match inserted {
        Some(id) => {
            tx.commit().await?;
            Ok(EnqueueOutcome::Queued(id))
        }
        None => {
            // The `ON CONFLICT ... DO NOTHING` arm fired — an open row for
            // this pair already exists (the ONLY thing the partial unique
            // index can conflict on). Read it back rather than re-deriving
            // its id another way. `fetch_optional`: an admin can approve or
            // reject that row between the INSERT and this SELECT, so
            // `decision IS NULL` can miss; that is the same 202 the
            // requester already gets for `Suppressed`.
            let existing: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM org_join_requests \
                  WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
            )
            .bind(account_id)
            .bind(user_id)
            .fetch_optional(&mut *tx)
            .await?;
            tx.commit().await?;
            match existing {
                Some(id) => Ok(EnqueueOutcome::AlreadyOpen(id)),
                None => Ok(EnqueueOutcome::Suppressed),
            }
        }
    }
}

/// One row of the admin queue read ([`list_open`]/[`list_recent_decided`]) —
/// carries the requester's name/email (a `LEFT JOIN users`, never a second
/// round trip) so `routes::org_join_requests::list` does not need its own
/// query on top of this one.
#[derive(Debug, Clone)]
pub(crate) struct JoinRequestSummary {
    pub id: Uuid,
    pub user_id: Uuid,
    pub user_name: Option<String>,
    pub user_email: Option<String>,
    pub source: String,
    pub evidence: Json,
    pub requested_at: chrono::DateTime<chrono::Utc>,
    pub decided_at: Option<chrono::DateTime<chrono::Utc>>,
    pub decided_by: Option<Uuid>,
    pub decision: Option<String>,
}

const SUMMARY_SELECT: &str = "\
    SELECT r.id, r.user_id, u.name, u.email, r.source, r.evidence, r.requested_at, \
           r.decided_at, r.decided_by, r.decision \
      FROM org_join_requests r \
      LEFT JOIN users u ON u.id = r.user_id";

/// The raw tuple shape [`SUMMARY_SELECT`]'s ten columns decode into — named
/// once so `list_open`/`list_recent_decided`/`get_open` (all three run the
/// SAME select, just with a different `WHERE`) and [`summary_from_row`]
/// share one type instead of repeating the ten-tuple inline at every call
/// site (clippy `type_complexity`).
type SummaryRow = (
    Uuid,
    Uuid,
    Option<String>,
    Option<String>,
    String,
    Json,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<Uuid>,
    Option<String>,
);

/// Count of OPEN requests for `account_id` — the admin badge's own read
/// (`GET /v1/orgs/join-requests/count`, CS-438, D13). A bare `COUNT(*)`
/// rather than `list_open(..).len()`: the badge is meant to stay cheap on
/// every page load even once a queue an admin has not looked at in a while
/// accumulates real `evidence`/name/email payloads per row.
pub(crate) async fn count_open(control: &PgPool, account_id: Uuid) -> Result<i64, JoinRequestError> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control, account_id).await?;
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM org_join_requests WHERE account_id = $1 AND decision IS NULL",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(count)
}

/// Every OPEN request for `account_id`, oldest first — the admin queue's
/// primary read. `routes::org_join_requests::list` renders these ahead of
/// [`list_recent_decided`]'s rows (open first, then recent decided, per
/// this story's own route spec).
pub(crate) async fn list_open(control: &PgPool, account_id: Uuid) -> Result<Vec<JoinRequestSummary>, JoinRequestError> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control,account_id).await?;
    let value = list_open_tx(&mut tx,account_id).await?;
    tx.rollback().await?;
    Ok(value)
}

/// [`list_open`], capped to the OLDEST `limit` rows — CS-449 (item 38)'s
/// fix for the two per-candidate re-gating loops
/// (`routes::org_auto_add::queued_preview`, `routes::org_join_requests::
/// drain_org`) that would otherwise re-gate the WHOLE backlog on every
/// call. See `services::org_auto_add::MAX_AUTO_ADD_BATCH_CANDIDATES`'s own
/// doc comment for why 200 and why shared. Oldest first, same as
/// [`list_open`], so a capped read always considers the SAME candidates on
/// every call (the ones that have been waiting longest) rather than a
/// shifting, unfair window over a backlog larger than the cap.
pub(crate) async fn list_open_capped(
    control: &PgPool,
    account_id: Uuid,
    limit: i64,
) -> Result<Vec<JoinRequestSummary>, JoinRequestError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let rows = sqlx::query_as::<_, SummaryRow>(&format!(
        "{SUMMARY_SELECT} WHERE r.account_id = $1 AND r.decision IS NULL ORDER BY r.requested_at ASC LIMIT $2"
    ))
    .bind(account_id)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows.into_iter().map(summary_from_row).collect())
}

/// The OPEN subset of `ids` for `account_id`, oldest first — CS-449 (item
/// 38)'s `admit-queued` own read.
///
/// Ordering is by `requested_at`, not by the order `ids` arrived in: an
/// admin's client is expected to send them oldest-first already (it got the
/// list from [`list_open`] via the preview), but the SERVER is what must
/// make "oldest first" true (founder decision, 2026-09-22: "admit what
/// fits, oldest first" is not a request the client gets to reorder,
/// accidentally or otherwise).
///
/// An id that is not an OPEN request for THIS account — wrong account,
/// already decided, or simply unknown — is silently absent from the
/// result; `routes::org_auto_add::admit_queued` treats that as "ignore it"
/// per its own contract, never as an error.
pub(crate) async fn list_open_by_ids(
    control: &PgPool,
    account_id: Uuid,
    ids: &[Uuid],
) -> Result<Vec<JoinRequestSummary>, JoinRequestError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut tx = begin_account_scoped(control, account_id).await?;
    let rows = sqlx::query_as::<_, SummaryRow>(&format!(
        "{SUMMARY_SELECT} WHERE r.account_id = $1 AND r.decision IS NULL AND r.id = ANY($2) \
           ORDER BY r.requested_at ASC"
    ))
    .bind(account_id)
    .bind(ids)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows.into_iter().map(summary_from_row).collect())
}

/// Patch an OPEN request's `evidence` (JSONB merge, right side wins — same
/// shape [`mark_decided`] uses for its own patch) and, optionally, its
/// `source` — WITHOUT deciding it. CS-449 (item 38)'s re-gate paths use
/// this to record why a still-legitimately-queued candidate did not admit
/// (pool full, or one of [`crate::services::org_auto_add::FallReason::
/// source_override`]'s two overridable reasons), the same fields `create`
/// itself writes for a FRESH insert — see that route's own "Item 9" comment
/// block. `decision IS NULL` guards against a race with an admin's own
/// approve/reject landing between the re-gate read and this write, the same
/// reason [`mark_decided`] itself is guarded.
pub(crate) async fn patch_open_request(
    control: &PgPool,
    id: Uuid,
    account_id: Uuid,
    source: Option<&str>,
    evidence_patch: Json,
) -> Result<bool, JoinRequestError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let result = sqlx::query(
        "UPDATE org_join_requests \
            SET source = COALESCE($3, source), \
                evidence = evidence || $4 \
          WHERE id = $1 AND account_id = $2 AND decision IS NULL",
    )
    .bind(id)
    .bind(account_id)
    .bind(source)
    .bind(evidence_patch)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() > 0)
}

/// Decided requests for `account_id` in the last `since_days` days, most
/// recently decided first — `include_decided=<n days>` on
/// `GET /v1/orgs/join-requests`.
pub(crate) async fn list_recent_decided(
    control: &PgPool,
    account_id: Uuid,
    since_days: i64,
) -> Result<Vec<JoinRequestSummary>, JoinRequestError> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control, account_id).await?;
    let rows = sqlx::query_as::<_, SummaryRow>(&format!(
        "{SUMMARY_SELECT} WHERE r.account_id = $1 AND r.decision IS NOT NULL \
           AND r.decided_at >= now() - ($2 || ' days')::interval \
         ORDER BY r.decided_at DESC"
    ))
    .bind(account_id)
    .bind(since_days.max(0).to_string())
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows.into_iter().map(summary_from_row).collect())
}

fn summary_from_row(row: SummaryRow) -> JoinRequestSummary {
    JoinRequestSummary {
        id: row.0,
        user_id: row.1,
        user_name: row.2,
        user_email: row.3,
        source: row.4,
        evidence: row.5,
        requested_at: row.6,
        decided_at: row.7,
        decided_by: row.8,
        decision: row.9,
    }
}

/// One request, scoped to `account_id` (an admin route must never resolve a
/// join-request id belonging to a DIFFERENT org — this is the guard against
/// that: a mismatched `account_id` reads back `None`, same as a nonexistent
/// id, so the route's own 404 covers both).
pub(crate) async fn get_open(
    control: &PgPool,
    id: Uuid,
    account_id: Uuid,
) -> Result<Option<JoinRequestSummary>, JoinRequestError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let row = sqlx::query_as::<_, SummaryRow>(&format!(
        "{SUMMARY_SELECT} WHERE r.id = $1 AND r.account_id = $2 AND r.decision IS NULL"
    ))
    .bind(id)
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row.map(summary_from_row))
}

/// One open request for `user_id` — `GET /v1/me/join-requests`'s own read,
/// carrying the target org's `clerk_org_id` (never `account_id`, which
/// means nothing to the requester) so the route can resolve a display name
/// against Clerk.
#[derive(Debug, Clone)]
pub(crate) struct MyOpenJoinRequest {
    pub id: Uuid,
    pub clerk_org_id: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
}

pub(crate) async fn list_open_for_user(
    control: &PgPool,
    user_id: Uuid,
) -> Result<Vec<MyOpenJoinRequest>, JoinRequestError> {
    // Cross-account on purpose: the requester is not a member of the orgs
    // they asked to join, so no single `app.account_id` can see every row.
    // Discover account ids via the narrow SECURITY DEFINER resolver
    // (bare uuid only — same 0084/0086 shape), then re-read each row
    // inside `begin_account_scoped`.
    let account_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT * FROM resolve_open_join_request_account_ids($1)")
            .bind(user_id)
            .fetch_all(crate::tenancy::scope::request_read(control))
            .await?;

    let mut out = Vec::new();
    for account_id in account_ids {
        let mut tx = crate::tenancy::scope::begin_request_account_read(control, account_id).await?;
        let rows: Vec<(Uuid, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT r.id, a.clerk_org_id, r.requested_at \
               FROM org_join_requests r \
               JOIN accounts a ON a.id = r.account_id \
              WHERE r.user_id = $1 AND r.account_id = $2 AND r.decision IS NULL",
        )
        .bind(user_id)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        out.extend(
            rows.into_iter()
                .map(|(id, clerk_org_id, requested_at)| MyOpenJoinRequest { id, clerk_org_id, requested_at }),
        );
    }
    out.sort_by_key(|r| r.requested_at);
    Ok(out)
}

/// Guarded decision write — `WHERE decision IS NULL` makes this idempotent
/// under a double click/retry: a second call against an already-decided row
/// affects 0 rows and returns `false` rather than erroring or overwriting
/// the first decision (same "guard clause substitutes for a lock at this
/// granularity" reasoning `billing::slot_pool::release_reservation`'s own
/// doc comment gives for its identical shape). `evidence_patch` is merged
/// (JSONB `||`, right side wins) into the row's existing `evidence` rather
/// than replacing it — `routes::org_join_requests::create` may have already
/// written `email`/`tombstoned`; the decision only ADDS `slot`/`tier`/
/// `pool_full`, never erases what enqueue recorded.
pub(crate) async fn mark_decided(
    control: &PgPool,
    id: Uuid,
    account_id: Uuid,
    decided_by: Uuid,
    decision: &str,
    evidence_patch: Json,
) -> Result<bool, JoinRequestError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let result = sqlx::query(
        "UPDATE org_join_requests \
            SET decision = $4, decided_at = now(), decided_by = $3, \
                evidence = evidence || $5 \
          WHERE id = $1 AND account_id = $2 AND decision IS NULL",
    )
    .bind(id)
    .bind(account_id)
    .bind(decided_by)
    .bind(decision)
    .bind(evidence_patch)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() > 0)
}

pub(crate) async fn list_open_tx(conn: &mut sqlx::PgConnection, account_id: Uuid) -> Result<Vec<JoinRequestSummary>, JoinRequestError> {
    let rows = sqlx::query_as::<_, SummaryRow>(&format!(
        "{SUMMARY_SELECT} WHERE r.account_id = $1 AND r.decision IS NULL ORDER BY r.requested_at ASC"
    ))
    .bind(account_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(summary_from_row).collect())
}

/// The existing decided-history projection in a caller-owned snapshot. The
/// caller supplies its model clock; PostgreSQL retains the legacy calendar-day
/// interval in the transaction timezone. This opens no transaction/pool.
pub(crate) async fn list_recent_decided_tx(
    conn: &mut sqlx::PgConnection,
    account_id: Uuid,
    as_of: chrono::DateTime<chrono::Utc>,
    since_days: i64,
) -> Result<Vec<JoinRequestSummary>, JoinRequestError> {
    let rows = sqlx::query_as::<_, SummaryRow>(&format!(
        "{SUMMARY_SELECT} WHERE r.account_id=$1 AND r.decision IS NOT NULL AND r.decided_at >= $2::timestamptz - ($3 || ' days')::interval ORDER BY r.decided_at DESC"
    ))
    .bind(account_id).bind(as_of).bind(since_days.max(0).to_string()).fetch_all(conn).await?;
    Ok(rows.into_iter().map(summary_from_row).collect())
}

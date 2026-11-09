//! Shared Microsoft disconnect/purge routine — the analog of
//! [`crate::connectors::slack_disconnect`] for the Microsoft Graph connector.
//!
//! # The gap this closes
//!
//! Before this module existed, disconnecting a Microsoft connection
//! (`routes::oauth::delete_connection`'s `microsoft` arm — the only surface
//! that removes a Microsoft `oauth_tokens` row today; see that function's own
//! doc comment) deleted `oauth_tokens` and nothing else. Every downstream
//! table this connector writes survived:
//!
//! * `outlook_watched_conversations` — opted-in Outlook threads.
//! * `teams_watched_resources` — opted-in Teams channels/chats.
//! * `outlook_workflows` — per-user auto-ingest rules.
//! * `graph_subscriptions` — the live Graph change-notification subscriptions
//!   backing all of the above.
//!
//! This is precisely the "Disconnect didn't stop ingest" incident class
//! `slack_disconnect`'s own header documents for Slack, and it is WORSE here
//! for one specific shape: a `teams_watched_resources` row installed via
//! Resource-Specific Consent (`auth_mode = 'app'`) is kept alive entirely by
//! `MICROSOFT_CLIENT_ID`/`MICROSOFT_CLIENT_SECRET` client-credentials tokens
//! (`connectors::microsoft::app_token`) — it has NO dependency on the
//! account's own `oauth_tokens` row at all. `workers::graph_subscriptions`'s
//! tenant-loop ticks (`ensure_teams_subscriptions_for_account`, the 6-hour
//! backstop resync, `tick_backfill_continue`) sweep every account with ANY
//! `teams_watched_resources` row, unconditionally of whether that account
//! still has a Microsoft OAuth connection — so deleting only `oauth_tokens`
//! left RSC-installed Teams channels ingesting FOREVER after "Disconnect".
//! See [`purge_microsoft_connection`]'s own doc for the two-part fix.
//!
//! # What this deliberately does NOT purge
//!
//! Same contract as `slack_disconnect`: **`connector_identities` and every
//! captured atom are left untouched.** Disconnecting withdraws consent to
//! keep CAPTURING new content — it is not an erasure request. A caller that
//! also wants already-ingested content gone needs the separately-reviewed
//! `services::deletion` path, not this one.
//!
//! `oauth_tokens` itself is also not touched here — every caller already owns
//! tearing that down its own way, and (same reasoning as `slack_disconnect`'s
//! own header) needs whatever token material it holds BEFORE that row
//! disappears, if it wants a best-effort upstream cleanup at all.

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::{microsoft, TokenOwner};
use crate::tenancy::scope::begin_tenant_system_scoped;

/// What [`purge_microsoft_connection`] actually did — logged by its caller,
/// and asserted on directly by this module's own pg tests. Mirrors
/// `slack_disconnect::PurgeOutcome`'s "zero is ambiguous, a `_failed` flag
/// breaks the tie" shape.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PurgeOutcome {
    /// `outlook_watched_conversations` rows deleted.
    pub(crate) watched_conversations_deleted: usize,
    /// `true` when that DELETE itself errored (not the same as "zero rows").
    pub(crate) watched_conversations_delete_failed: bool,
    /// `teams_watched_resources` rows deleted.
    pub(crate) teams_watched_deleted: usize,
    pub(crate) teams_watched_delete_failed: bool,
    /// `outlook_workflows` rows deleted.
    pub(crate) workflows_deleted: usize,
    pub(crate) workflows_delete_failed: bool,
    /// `graph_subscriptions` rows deleted locally — this is the count that
    /// actually stops live ingest (see [`crate::routes::graph_webhook`]'s
    /// `subscription_by_id` lookup: a missing row makes an inbound
    /// notification a silent no-op) and, for RSC/app-mode Teams resources,
    /// removes the resource the tenant-loop's `ensure_teams_subscriptions_
    /// for_account` would otherwise keep re-creating forever.
    pub(crate) subscriptions_deleted: usize,
    pub(crate) subscriptions_delete_failed: bool,
    /// Best-effort upstream `DELETE /subscriptions/{id}` attempts made.
    /// App-only (RSC/transcript) subscriptions are always attempted (a fresh
    /// client-credentials token per tenant). Delegated (mail) subscriptions
    /// are attempted too, as of CS-452, WHEN the caller supplied a usable
    /// `delegated_access_token` — see [`purge_microsoft_connection`]'s own
    /// doc comment for that parameter.
    pub(crate) subscriptions_upstream_attempted: usize,
    /// Of those, how many errored. Never fatal to the purge — Graph having
    /// already forgotten a subscription (expired, or removed some other way)
    /// is an ordinary, expected outcome, not a purge failure.
    pub(crate) subscriptions_upstream_failed: usize,
}

impl PurgeOutcome {
    /// `true` when any of the LOCAL DB deletes failed — the upstream Graph
    /// best-effort calls are excluded on purpose, same as `slack_disconnect::
    /// PurgeOutcome::purge_had_a_failure`'s own reasoning: a failed upstream
    /// call (an already-expired subscription, a revoked app registration) is
    /// expected often enough that it does not, on its own, mean the purge
    /// needs operator attention. Callers should log a `connector_health`
    /// warning when this is `true`.
    pub(crate) fn purge_had_a_failure(&self) -> bool {
        self.watched_conversations_delete_failed
            || self.teams_watched_delete_failed
            || self.workflows_delete_failed
            || self.subscriptions_delete_failed
    }
}

/// The one shared Microsoft disconnect purge.
///
/// `tenant_pool` is the CONTENT plane for this account — the consumer pool
/// for Personal, the org's own tenant database for Org (the same plane
/// `connectors::microsoft`'s own RLS-scoping note documents for every one of
/// these four tables). Every caller is responsible for resolving it
/// correctly for its own plane; this function does not (and cannot, from a
/// bare `PgPool` alone) tell Personal and Org apart.
///
/// `owner` selects which rows this disconnect is responsible for:
///
/// * [`TokenOwner::AccountWide`] — every Personal-plane Microsoft connection.
///   Purges EVERY row across all four tables for `account_id`: there is
///   exactly one Microsoft connection per PERSONAL account, so "disconnect
///   Microsoft" and "purge everything Microsoft-owned for this account" are
///   the same operation.
/// * [`TokenOwner::Member`] — real, reachable as of m365 enterprise train PR
///   3.3 (per-member Outlook opt-in on the org plane) — and, as of CS-452,
///   the shape `routes::oauth::delete_connection`'s own caller actually
///   passes once an org account's Microsoft flag is on (previously that
///   caller hardcoded `TokenOwner::AccountWide` unconditionally, on a stale
///   "not yet built" premise this doc comment used to carry too — see that
///   call site's own CS-452 comment). Scopes `outlook_watched_conversations`/
///   `teams_watched_resources`/`outlook_workflows` to the rows THAT member
///   owns (`ingested_by`/`user_id`), never another member's.
///   `graph_subscriptions` carries `owner_user_id` (migration `0190`/`0068`):
///   a `Member` purge removes that member's OWN mail subscription rows
///   (`owner_user_id = $2`) PLUS the Teams-resource subscriptions matching
///   resources it just un-watched (computed from `teams_watched_resources`,
///   which was already per-member). Transcript subscriptions remain
///   account-wide (app-only, never per-member) and are left alone by a
///   `Member` purge.
///
/// `delegated_access_token` (CS-452): the disconnecting owner's OWN
/// decrypted Microsoft `access_token`, captured by the caller from the
/// `oauth_tokens` row's `RETURNING` clause BEFORE that row was deleted
/// (`routes::oauth::delete_connection`'s own comment on why that ordering
/// matters — once the row is gone, nothing can recover the token needed to
/// delete a delegated subscription upstream). `None` when the caller has no
/// usable token (decrypt failure, no DEK, or a caller — e.g. a future
/// non-oauth-triggered purge — with no token to give at all): the delegated
/// upstream delete is then skipped exactly as it always was pre-CS-452,
/// never fatal to the purge either way. Used ONLY for subscriptions this
/// same purge's own step 4 just found with an EMPTY `tenant_id` (delegated —
/// app-only/RSC subscriptions keep using `app_token` per `tenant_id`,
/// unaffected by this parameter).
pub(crate) async fn purge_microsoft_connection(
    http: &reqwest::Client,
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
    delegated_access_token: Option<&str>,
) -> PurgeOutcome {
    let mut outcome = PurgeOutcome::default();
    let owner_uid = owner.owner_user_id();

    // ── 1. outlook_watched_conversations ────────────────────────────────────
    match delete_watched_conversations(tenant_pool, account_id, owner_uid).await {
        Ok(n) => outcome.watched_conversations_deleted = n,
        Err(e) => {
            tracing::warn!(
                error = %e,
                %account_id,
                connector_health = true,
                "microsoft disconnect purge: failed to delete outlook_watched_conversations rows \
                 — continuing with the rest of the purge"
            );
            outcome.watched_conversations_delete_failed = true;
        }
    }

    // ── 2. teams_watched_resources — also the source of which Teams-resource
    // subscriptions step 4 below is responsible for ─────────────────────────
    let removed_teams_resources = match delete_teams_watched(tenant_pool, account_id, owner_uid).await {
        Ok(rows) => {
            outcome.teams_watched_deleted = rows.len();
            rows
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                %account_id,
                connector_health = true,
                "microsoft disconnect purge: failed to delete teams_watched_resources rows — \
                 continuing. An AccountWide purge still removes every graph_subscriptions row \
                 in step 4; a Member purge cannot, because step 4 derives its resource list \
                 from the rows this DELETE was supposed to return"
            );
            outcome.teams_watched_delete_failed = true;
            if owner_uid.is_some() {
                // Member path: step 4's resource list is derived from the rows
                // this DELETE returns, so an empty error-fallback silently skips
                // that member's subscription cleanup. Surface it through the
                // same failure signal so `purge_had_a_failure` doesn't read
                // green for a purge that left live subscriptions behind.
                outcome.subscriptions_delete_failed = true;
            }
            Vec::new()
        }
    };

    // ── 3. outlook_workflows ─────────────────────────────────────────────────
    match delete_workflows(tenant_pool, account_id, owner_uid).await {
        Ok(n) => outcome.workflows_deleted = n,
        Err(e) => {
            tracing::warn!(
                error = %e,
                %account_id,
                connector_health = true,
                "microsoft disconnect purge: failed to delete outlook_workflows rows — continuing"
            );
            outcome.workflows_delete_failed = true;
        }
    }

    // ── 4. graph_subscriptions — the row whose ABSENCE is what actually stops
    // live ingest (`routes::graph_webhook::process_notification`'s
    // `subscription_by_id` lookup silently discards a notification for an
    // unknown subscription id) and, for RSC/app-mode Teams resources, is what
    // stops the tenant loop's `ensure_teams_subscriptions_for_account` from
    // re-creating one forever. ────────────────────────────────────────────────
    let teams_resources: Vec<String> = removed_teams_resources
        .iter()
        .filter_map(|(kind, team_id, channel_id, chat_id)| {
            microsoft::teams_subscription_resource(kind, team_id, channel_id, chat_id)
        })
        .collect();
    match delete_subscriptions(tenant_pool, account_id, owner_uid, &teams_resources).await {
        Ok(rows) => {
            outcome.subscriptions_deleted = rows.len();
            // Best-effort upstream DELETE, run AFTER the local rows are
            // already gone (same ordering discipline as `slack_disconnect`'s
            // own conversations.leave step: local DB truth commits first,
            // remote cleanup is strictly cosmetic on top of it). App-only
            // (RSC/transcript) resources mint a fresh client-credentials
            // token per tenant — no dependency on the `oauth_tokens` row this
            // disconnect already removed. Delegated (mail) subscriptions
            // (CS-452 — previously NEVER attempted upstream here at all, on
            // the theory this disconnect "no longer holds a live delegated
            // access token to attempt one with" — false: the caller captures
            // it from the SAME `oauth_tokens` row's `RETURNING` clause before
            // that row disappears, see `delegated_access_token`'s own doc
            // comment above) are now attempted too, WHEN the caller supplied
            // one — Graph still expires them on its own short TTL as a
            // backstop either way, so a caller with no token to give (or a
            // decrypt failure) degrades to exactly the pre-CS-452 skip, not a
            // purge failure. The whole upstream phase shares one total time
            // budget: this runs on the disconnect request thread AFTER the
            // control-plane commit, so an account with tens of subscriptions
            // must not hold the HTTP handler open for minutes minting a
            // token + DELETE per row (each call alone carries a 20s timeout).
            // On budget exhaustion the remaining deletes are skipped, not
            // failed-the-purge: Graph expires subscriptions on its own short
            // TTL, and the local rows — the load-bearing half — are already
            // gone above.
            let total_upstream = rows
                .iter()
                .filter(|(_, _, t)| !t.is_empty() || delegated_access_token.is_some())
                .count();
            let upstream_phase = async {
                for (subscription_id, resource, tenant_id) in &rows {
                    if tenant_id.is_empty() {
                        let Some(token) = delegated_access_token else {
                            // No token to attempt with (no caller-supplied
                            // credential, or the post-commit decrypt failed) —
                            // logged so this orphan is findable rather than
                            // silently relying on Graph's own TTL expiry (see
                            // item 3's own "why this matters" in CS-452's
                            // brief: a longer subscription lifetime makes this
                            // orphan hold Graph's one-subscription-per-channel
                            // slot for correspondingly longer).
                            tracing::info!(
                                %account_id,
                                %subscription_id,
                                %resource,
                                "microsoft disconnect purge: no delegated access token available \
                                 — delegated subscription upstream delete skipped; Graph expires \
                                 it on its own short TTL"
                            );
                            continue;
                        };
                        outcome.subscriptions_upstream_attempted += 1;
                        if let Err(e) =
                            revoke_delegated_subscription_upstream(http, token, subscription_id)
                                .await
                        {
                            outcome.subscriptions_upstream_failed += 1;
                            tracing::info!(
                                error = %e,
                                %account_id,
                                %subscription_id,
                                %resource,
                                "microsoft disconnect purge: delegated upstream subscription \
                                 delete failed — best-effort, local row is already gone either way"
                            );
                        }
                        continue;
                    }
                    outcome.subscriptions_upstream_attempted += 1;
                    if let Err(e) =
                        revoke_subscription_upstream(http, tenant_id, subscription_id).await
                    {
                        outcome.subscriptions_upstream_failed += 1;
                        tracing::info!(
                            error = %e,
                            %account_id,
                            %subscription_id,
                            %resource,
                            "microsoft disconnect purge: upstream subscription delete failed — \
                             best-effort, local row is already gone either way"
                        );
                    }
                }
            };
            if tokio::time::timeout(std::time::Duration::from_secs(20), upstream_phase)
                .await
                .is_err()
            {
                tracing::info!(
                    %account_id,
                    attempted = outcome.subscriptions_upstream_attempted,
                    total = total_upstream,
                    "microsoft disconnect purge: upstream delete budget exhausted — remaining \
                     subscription deletes skipped (best-effort; Graph expires them on TTL)"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                %account_id,
                connector_health = true,
                "microsoft disconnect purge: failed to delete graph_subscriptions rows — this is \
                 the failure that matters most: a surviving row here means live notifications and \
                 the tenant-loop renewal sweep can keep this 'disconnected' account ingesting"
            );
            outcome.subscriptions_delete_failed = true;
        }
    }

    outcome
}

/// Delete `outlook_watched_conversations` rows for `account_id`, scoped to
/// `owner_uid` when `Some` (a per-member purge) or every row when `None`
/// (account-wide). No `owner_user_id IS NULL OR owner_user_id = $n` fallback
/// hazard here — unlike a credential read, this is a management/deletion
/// surface (same distinction `routes::oauth::delete_connection`'s own
/// `connector_installs` DELETE draws), and `owner_uid` is never client input.
async fn delete_watched_conversations(
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner_uid: Option<Uuid>,
) -> Result<usize, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_watched_conversations scoped tx: {e}")))?;
    let result = sqlx::query(
        "DELETE FROM outlook_watched_conversations \
         WHERE account_id = $1 AND ($2::uuid IS NULL OR ingested_by = $2)",
    )
    .bind(account_id)
    .bind(owner_uid)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

/// Delete `teams_watched_resources` rows, same owner-scoping rule as
/// [`delete_watched_conversations`]. `RETURNING` the resource-composing
/// columns so the caller can compute exactly which `graph_subscriptions`
/// rows a `Member`-scoped purge is responsible for — see
/// [`purge_microsoft_connection`]'s own doc comment.
async fn delete_teams_watched(
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner_uid: Option<Uuid>,
) -> Result<Vec<(String, String, String, String)>, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_teams_watched scoped tx: {e}")))?;
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "DELETE FROM teams_watched_resources \
         WHERE account_id = $1 AND ($2::uuid IS NULL OR user_id = $2) \
         RETURNING kind, team_id, channel_id, chat_id",
    )
    .bind(account_id)
    .bind(owner_uid)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

/// Delete `outlook_workflows` rows, same owner-scoping rule as
/// [`delete_watched_conversations`].
async fn delete_workflows(
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner_uid: Option<Uuid>,
) -> Result<usize, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_workflows scoped tx: {e}")))?;
    let result = sqlx::query(
        "DELETE FROM outlook_workflows \
         WHERE account_id = $1 AND ($2::uuid IS NULL OR user_id = $2)",
    )
    .bind(account_id)
    .bind(owner_uid)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

/// Delete `graph_subscriptions` rows: every row for `account_id` when
/// `owner_uid` is `None` (account-wide — the only shape a Personal-plane
/// disconnect ever needs, see [`purge_microsoft_connection`]'s own doc
/// comment); when `owner_uid` is `Some` (a `Member` purge), rows this member
/// OWNS (`owner_user_id = $2` — that member's own mail subscription(s),
/// migration `0190`/`0068`, m365 enterprise train PR 3.3) UNION rows whose
/// `resource` matches one of `teams_resources` (this disconnect's own
/// just-purged Teams resources, which stay account-wide-keyed — see
/// `connectors::microsoft::ensure_subscription_for`'s own doc comment on why
/// Teams never needed an owner dimension). Transcript subscriptions
/// (app-only, always account-wide) are never matched by either arm of a
/// `Member` purge and are left alone, same as before this PR.
async fn delete_subscriptions(
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner_uid: Option<Uuid>,
    teams_resources: &[String],
) -> Result<Vec<(String, String, String)>, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_subscriptions scoped tx: {e}")))?;
    let rows: Vec<(String, String, String)> = match owner_uid {
        None => {
            sqlx::query_as(
                "DELETE FROM graph_subscriptions WHERE account_id = $1 \
                 RETURNING subscription_id, resource, tenant_id",
            )
            .bind(account_id)
            .fetch_all(&mut *tx)
            .await?
        }
        Some(owner_uid) => {
            sqlx::query_as(
                "DELETE FROM graph_subscriptions \
                 WHERE account_id = $1 \
                   AND (owner_user_id = $2 OR resource = ANY($3)) \
                 RETURNING subscription_id, resource, tenant_id",
            )
            .bind(account_id)
            .bind(owner_uid)
            .bind(teams_resources)
            .fetch_all(&mut *tx)
            .await?
        }
    };
    tx.commit().await?;
    Ok(rows)
}

/// Best-effort `DELETE /subscriptions/{id}` against Graph using a freshly
/// minted app-only (client-credentials) token for `tenant_id` — the same
/// token acquisition [`connectors::microsoft::delete_subscription_for`] uses
/// for its own upstream call, kept separate here only because this module
/// already holds the subscription id/tenant from its own bulk `RETURNING`
/// and re-querying per resource would cost an extra round trip per row for
/// no benefit.
async fn revoke_subscription_upstream(
    http: &reqwest::Client,
    tenant_id: &str,
    subscription_id: &str,
) -> Result<(), String> {
    let token = microsoft::app_token(http, tenant_id)
        .await
        .map_err(|e| e.to_string())?;
    let resp = http
        .delete(format!(
            "https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
        ))
        .bearer_auth(token)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
        // 404: Graph already forgot it — same "already gone" success case
        // `delete_subscription_row`'s own doc comment treats as a no-op.
        Ok(())
    } else {
        Err(format!("upstream delete failed: {}", resp.status()))
    }
}

/// [`revoke_subscription_upstream`]'s delegated twin (CS-452): the SAME
/// best-effort `DELETE /subscriptions/{id}`, but bearing the caller-supplied
/// DELEGATED access token directly rather than minting an app-only
/// (client-credentials) one — delegated (mail) subscriptions were never
/// created with, and can never be deleted with, an app token; Graph requires
/// the token that carries the delegated `Mail.Read`-class scope the
/// subscription was created under. `access_token` is always the disconnecting
/// owner's OWN token (`purge_microsoft_connection`'s `delegated_access_token`
/// parameter — see its own doc comment for exactly how the caller captured
/// it before the `oauth_tokens` row it lived in was deleted), never a client
/// parameter.
async fn revoke_delegated_subscription_upstream(
    http: &reqwest::Client,
    access_token: &str,
    subscription_id: &str,
) -> Result<(), String> {
    let resp = http
        .delete(format!(
            "https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
        ))
        .bearer_auth(access_token)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
        // 404: Graph already forgot it — same "already gone" success case
        // `revoke_subscription_upstream`'s own arm above treats as a no-op.
        Ok(())
    } else {
        Err(format!("upstream delete failed: {}", resp.status()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_wide_owner_uid_is_none() {
        assert_eq!(TokenOwner::AccountWide.owner_user_id(), None);
    }

    #[test]
    fn member_owner_uid_is_the_wrapped_id() {
        let id = Uuid::new_v4();
        assert_eq!(TokenOwner::Member(id).owner_user_id(), Some(id));
    }

    #[test]
    fn purge_had_a_failure_is_false_for_a_clean_outcome() {
        assert!(!PurgeOutcome::default().purge_had_a_failure());
    }

    #[test]
    fn purge_had_a_failure_ignores_upstream_only_failures() {
        // Every local DB delete succeeded; only the best-effort upstream
        // Graph call failed. Must NOT read as a purge failure — see
        // `PurgeOutcome::purge_had_a_failure`'s own doc comment.
        let outcome = PurgeOutcome {
            subscriptions_upstream_attempted: 2,
            subscriptions_upstream_failed: 2,
            ..PurgeOutcome::default()
        };
        assert!(!outcome.purge_had_a_failure());
    }

    #[test]
    fn purge_had_a_failure_is_true_for_any_local_delete_failure() {
        for outcome in [
            PurgeOutcome { watched_conversations_delete_failed: true, ..PurgeOutcome::default() },
            PurgeOutcome { teams_watched_delete_failed: true, ..PurgeOutcome::default() },
            PurgeOutcome { workflows_delete_failed: true, ..PurgeOutcome::default() },
            PurgeOutcome { subscriptions_delete_failed: true, ..PurgeOutcome::default() },
        ] {
            assert!(outcome.purge_had_a_failure());
        }
    }
}

/// pg-gated integration tests: proves the purge against a REAL tenant
/// Postgres database. Tenant-plane only (not `migrations_control`) — same
/// "no genuine control/tenant behavior divergence to prove for these four
/// tables" reasoning `microsoft::backfill_cursor_pg_tests`'s own module doc
/// gives for its identical single-tenant-DB fixture: none of
/// `outlook_watched_conversations`/`teams_watched_resources`/
/// `outlook_workflows`/`graph_subscriptions` carry a local FK on
/// `account_id` or on their owner column on the tenant plane (`accounts`/
/// `users` don't exist as tables there at all — see `migrations_tenant/
/// 0001_tenant_baseline.sql`); only `graph_id` is FK'd, to the tenant's own
/// local `graphs` table, seeded directly below with a bare `account_id` UUID.
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib microsoft_disconnect_pg_tests
#[cfg(test)]
mod microsoft_disconnect_pg_tests {
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
    async fn build_tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }
    async fn seed_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) \
             VALUES ($1, 'test ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    async fn seed_watched_conversation(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        ingested_by: Uuid,
        conversation_id: &str,
    ) {
        sqlx::query(
            "INSERT INTO outlook_watched_conversations \
                 (account_id, graph_id, ingested_by, conversation_id) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(ingested_by)
        .bind(conversation_id)
        .execute(pool)
        .await
        .unwrap();
    }
    #[allow(clippy::too_many_arguments)]
    async fn seed_teams_watched(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        user_id: Uuid,
        kind: &str,
        team_id: &str,
        channel_id: &str,
        chat_id: &str,
        tenant_id: &str,
        auth_mode: &str,
    ) {
        sqlx::query(
            "INSERT INTO teams_watched_resources \
                 (account_id, graph_id, user_id, kind, team_id, channel_id, chat_id, tenant_id, auth_mode) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(user_id)
        .bind(kind)
        .bind(team_id)
        .bind(channel_id)
        .bind(chat_id)
        .bind(tenant_id)
        .bind(auth_mode)
        .execute(pool)
        .await
        .unwrap();
    }
    async fn seed_workflow(pool: &PgPool, account_id: Uuid, graph_id: Uuid, user_id: Uuid, name: &str) {
        sqlx::query(
            "INSERT INTO outlook_workflows (account_id, graph_id, user_id, name, filter) \
             VALUES ($1, $2, $3, $4, '{}'::jsonb)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(user_id)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    }
    async fn seed_subscription(
        pool: &PgPool,
        account_id: Uuid,
        subscription_id: &str,
        resource: &str,
        tenant_id: &str,
    ) {
        sqlx::query(
            "INSERT INTO graph_subscriptions \
                 (account_id, subscription_id, resource, client_state, expires_at, tenant_id) \
             VALUES ($1, $2, $3, 'test-client-state', now() + interval '1 day', $4)",
        )
        .bind(account_id)
        .bind(subscription_id)
        .bind(resource)
        .bind(tenant_id)
        .execute(pool)
        .await
        .unwrap();
    }
    async fn count(pool: &PgPool, table: &str, account_id: Uuid) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE account_id = $1"))
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    async fn subscription_row_exists(pool: &PgPool, subscription_id: &str) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM graph_subscriptions WHERE subscription_id = $1)",
        )
        .bind(subscription_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    /// Same as [`seed_subscription`], but with `owner_user_id` set (m365
    /// enterprise train PR 3.3, migration `0190`/`0068`) — an org member's own
    /// delegated mail subscription, not an account-wide one. A separate
    /// helper (not an added parameter on `seed_subscription`) so the existing
    /// AccountWide-shaped tests above stay byte-identical, unaware this
    /// column exists at all.
    async fn seed_subscription_owned(
        pool: &PgPool,
        account_id: Uuid,
        subscription_id: &str,
        resource: &str,
        owner_user_id: Uuid,
    ) {
        sqlx::query(
            "INSERT INTO graph_subscriptions \
                 (account_id, subscription_id, resource, client_state, expires_at, tenant_id, owner_user_id) \
             VALUES ($1, $2, $3, 'test-client-state', now() + interval '1 day', '', $4)",
        )
        .bind(account_id)
        .bind(subscription_id)
        .bind(resource)
        .bind(owner_user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Account-wide disconnect (`TokenOwner::AccountWide` — the only variant
    /// any Microsoft caller constructs today) must purge EVERY row across all
    /// four tables for the account: a watched conversation, TWO Teams
    /// resources (one delegated, one app/RSC) each with their own live
    /// subscription, a mail subscription, and a workflow rule — regardless of
    /// which user_id/ingested_by owns each row. This is also the "Disconnect
    /// didn't stop ingest" assertion this PR exists to pre-empt: a purged
    /// `graph_subscriptions` row is exactly what makes a redelivered/late
    /// notification for it a silent no-op in `routes::graph_webhook::
    /// process_notification`'s own `subscription_by_id` lookup (not exercised
    /// directly here — that lookup goes through a control-plane-only
    /// SECURITY DEFINER resolver with no tenant-tree counterpart, per
    /// `connectors::microsoft`'s own module doc — so the row's ABSENCE, which
    /// this test asserts directly, is what that lookup's `None` result
    /// depends on).
    #[tokio::test]
    async fn account_wide_purge_removes_every_row_across_all_four_tables_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping account_wide_purge_removes_every_row_across_all_four_tables_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t37_accountwide";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let tenant = build_tenant_pool(&base, db, "org_t37_accountwide").await;

        let account_id = Uuid::new_v4();
        let user_a = Uuid::new_v4();
        let user_b = Uuid::new_v4();
        let graph_id = seed_graph(&tenant, account_id).await;

        seed_watched_conversation(&tenant, account_id, graph_id, user_a, "conv-1").await;
        seed_teams_watched(&tenant, account_id, graph_id, user_a, "channel", "T1", "C1", "", "", "delegated")
            .await;
        seed_teams_watched(&tenant, account_id, graph_id, user_b, "chat", "", "", "CHAT1", "tenant-xyz", "app")
            .await;
        seed_workflow(&tenant, account_id, graph_id, user_a, "rule-1").await;

        seed_subscription(&tenant, account_id, "sub-mail", "me/mailFolders('inbox')/messages", "").await;
        seed_subscription(&tenant, account_id, "sub-teams-channel", "teams/T1/channels/C1/messages", "").await;
        seed_subscription(&tenant, account_id, "sub-teams-chat", "chats/CHAT1/messages", "tenant-xyz").await;

        let outcome = purge_microsoft_connection(
            &reqwest::Client::new(),
            &tenant,
            account_id,
            TokenOwner::AccountWide,
            None,
        )
        .await;

        assert_eq!(outcome.watched_conversations_deleted, 1);
        assert_eq!(outcome.teams_watched_deleted, 2);
        assert_eq!(outcome.workflows_deleted, 1);
        assert_eq!(
            outcome.subscriptions_deleted, 3,
            "all 3 subscription rows for the account, regardless of which user/resource each backs"
        );
        assert!(!outcome.purge_had_a_failure());

        assert_eq!(count(&tenant, "outlook_watched_conversations", account_id).await, 0);
        assert_eq!(count(&tenant, "teams_watched_resources", account_id).await, 0);
        assert_eq!(count(&tenant, "outlook_workflows", account_id).await, 0);
        assert_eq!(count(&tenant, "graph_subscriptions", account_id).await, 0);
        assert!(
            !subscription_row_exists(&tenant, "sub-teams-channel").await,
            "the purged subscription row must be gone — this is the mechanism that makes a \
             redelivered/late notification for it a silent no-op instead of live ingest"
        );

        tenant.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// `TokenOwner::Member`-scoped purge (Train 3.2 forward-compat plumbing,
    /// not yet reachable from any production caller — see this module's own
    /// doc comment) must touch ONLY the disconnecting member's own watched
    /// conversation/Teams resource/workflow and that member's own
    /// Teams-resource subscription, leaving another member's rows AND the
    /// account's shared mail subscription (unattributable to one member with
    /// today's schema) completely untouched.
    #[tokio::test]
    async fn member_scoped_purge_touches_only_that_members_rows_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_scoped_purge_touches_only_that_members_rows_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t37_memberscoped";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let tenant = build_tenant_pool(&base, db, "org_t37_memberscoped").await;

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();
        let other_member = Uuid::new_v4();
        let graph_id = seed_graph(&tenant, account_id).await;

        // `member`'s own rows — must be purged.
        seed_watched_conversation(&tenant, account_id, graph_id, member, "conv-member").await;
        seed_teams_watched(&tenant, account_id, graph_id, member, "channel", "T1", "C1", "", "", "delegated")
            .await;
        seed_workflow(&tenant, account_id, graph_id, member, "rule-member").await;
        seed_subscription(&tenant, account_id, "sub-member-channel", "teams/T1/channels/C1/messages", "")
            .await;

        // `other_member`'s own rows — must survive untouched.
        seed_watched_conversation(&tenant, account_id, graph_id, other_member, "conv-other").await;
        seed_teams_watched(
            &tenant, account_id, graph_id, other_member, "chat", "", "", "CHAT2", "tenant-xyz", "app",
        )
        .await;
        seed_workflow(&tenant, account_id, graph_id, other_member, "rule-other").await;
        seed_subscription(&tenant, account_id, "sub-other-chat", "chats/CHAT2/messages", "tenant-xyz").await;

        // The account's shared mail subscription — not attributable to
        // either member; must survive a Member-scoped purge.
        seed_subscription(&tenant, account_id, "sub-mail", "me/mailFolders('inbox')/messages", "").await;

        let outcome = purge_microsoft_connection(
            &reqwest::Client::new(),
            &tenant,
            account_id,
            TokenOwner::Member(member),
            None,
        )
        .await;

        assert_eq!(outcome.watched_conversations_deleted, 1);
        assert_eq!(outcome.teams_watched_deleted, 1);
        assert_eq!(outcome.workflows_deleted, 1);
        assert_eq!(
            outcome.subscriptions_deleted, 1,
            "only the disconnecting member's own Teams-resource subscription — not the account's \
             shared mail subscription, not the other member's Teams subscription"
        );
        assert!(!outcome.purge_had_a_failure());

        assert_eq!(
            count(&tenant, "outlook_watched_conversations", account_id).await,
            1,
            "the other member's watched conversation must survive"
        );
        assert_eq!(
            count(&tenant, "teams_watched_resources", account_id).await,
            1,
            "the other member's watched Teams resource must survive"
        );
        assert_eq!(
            count(&tenant, "outlook_workflows", account_id).await,
            1,
            "the other member's workflow must survive"
        );
        assert_eq!(
            count(&tenant, "graph_subscriptions", account_id).await,
            2,
            "the other member's Teams subscription AND the shared mail subscription must both survive"
        );

        let member_conv: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outlook_watched_conversations WHERE account_id = $1 AND ingested_by = $2",
        )
        .bind(account_id)
        .bind(member)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(member_conv, 0, "the disconnecting member's own conversation must be gone");

        assert!(
            subscription_row_exists(&tenant, "sub-mail").await,
            "the shared mail subscription must never be touched by a Member purge"
        );

        tenant.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// m365 enterprise train PR 3.3: closes the gap the test above's own
    /// "shared mail subscription" case does NOT cover — a mail subscription
    /// that DOES carry this member's `owner_user_id` (migration `0190`/
    /// `0068`, real once org-plane Outlook opt-in creates one per member)
    /// must be purged along with the rest of that member's rows; a DIFFERENT
    /// member's own mail subscription must survive untouched. This is the
    /// exact gap `purge_microsoft_connection`'s own doc comment used to name
    /// ("graph_subscriptions has no per-member column... a Member purge...
    /// leaves mail/transcript subscriptions alone").
    #[tokio::test]
    async fn member_scoped_purge_also_removes_that_members_own_mail_subscription_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_scoped_purge_also_removes_that_members_own_mail_subscription_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t33_memberscoped_mail_sub";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let tenant = build_tenant_pool(&base, db, "org_t33_memberscoped_mail_sub").await;

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();
        let other_member = Uuid::new_v4();

        seed_subscription_owned(
            &tenant,
            account_id,
            "sub-member-mail",
            "me/mailFolders('inbox')/messages",
            member,
        )
        .await;
        seed_subscription_owned(
            &tenant,
            account_id,
            "sub-other-member-mail",
            "me/mailFolders('inbox')/messages",
            other_member,
        )
        .await;
        // A legacy AccountWide mail subscription (no owner) must also survive
        // a Member-scoped purge — same invariant the sibling test above
        // proves, restated here so this test alone is a complete regression
        // check for the three-way split (this member's own / another
        // member's own / nobody's) without depending on test execution order.
        seed_subscription(&tenant, account_id, "sub-legacy-mail", "me/mailFolders('sentitems')/messages", "")
            .await;

        let outcome = purge_microsoft_connection(
            &reqwest::Client::new(),
            &tenant,
            account_id,
            TokenOwner::Member(member),
            None,
        )
        .await;

        assert_eq!(
            outcome.subscriptions_deleted, 1,
            "exactly this member's own mail subscription — not the other member's, not the legacy \
             account-wide one"
        );
        assert!(!outcome.purge_had_a_failure());
        assert!(
            !subscription_row_exists(&tenant, "sub-member-mail").await,
            "the disconnecting member's own mail subscription must be gone"
        );
        assert!(
            subscription_row_exists(&tenant, "sub-other-member-mail").await,
            "a DIFFERENT member's own mail subscription must survive untouched"
        );
        assert!(
            subscription_row_exists(&tenant, "sub-legacy-mail").await,
            "a legacy account-wide mail subscription must still survive a Member purge"
        );

        tenant.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// CS-452: a delegated (mail) subscription's upstream Graph delete is now
    /// ATTEMPTED when the caller supplies the disconnecting owner's own token
    /// (`delegated_access_token`) — pre-CS-452 this was unconditionally
    /// skipped for every delegated (`tenant_id = ""`) row, the exact
    /// "delegated subscriptions are never deleted upstream" gap CS-452's own
    /// brief names as item 3. Exercises the REAL upstream call against the
    /// real Graph endpoint with a deliberately bogus token (fails fast with
    /// `401`) rather than mocking — no Graph-mocking harness exists for this
    /// connector (see `app_token`'s own fast-fail-on-missing-client-id shape,
    /// which the pre-existing app-only tests in this module already lean on
    /// the same way: neither path in this file's test suite has ever reached
    /// a REAL successful upstream call).
    #[tokio::test]
    async fn member_scoped_purge_attempts_the_upstream_delete_for_a_delegated_subscription_when_given_a_token_pg(
    ) {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_scoped_purge_attempts_the_upstream_delete_for_a_delegated_subscription_when_given_a_token_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t452_delegated_upstream_attempted";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let tenant = build_tenant_pool(&base, db, "org_t452_delegated_upstream_attempted").await;

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();

        seed_subscription_owned(
            &tenant,
            account_id,
            "sub-member-mail-upstream",
            "me/mailFolders('inbox')/messages",
            member,
        )
        .await;

        let outcome = purge_microsoft_connection(
            &reqwest::Client::new(),
            &tenant,
            account_id,
            TokenOwner::Member(member),
            Some("cs-452-test-bogus-delegated-token"),
        )
        .await;

        assert_eq!(
            outcome.subscriptions_deleted, 1,
            "the local row must be removed regardless of the upstream outcome"
        );
        assert_eq!(
            outcome.subscriptions_upstream_attempted, 1,
            "CS-452: a delegated subscription must now be ATTEMPTED upstream when the caller \
             supplies a token — pre-CS-452 this counter stayed 0 for every delegated \
             (empty-tenant_id) row, unconditionally"
        );
        // The bogus token is rejected by the real Graph endpoint (401) —
        // expected, and must never fail the purge itself: `purge_had_a_
        // failure` excludes upstream-only failures by design, same as the
        // pre-existing app-only coverage above.
        assert!(
            !outcome.purge_had_a_failure(),
            "an upstream-only failure (a rejected/expired token, Graph already having forgotten \
             the subscription, ...) must never fail the purge — the local row is already gone \
             either way"
        );
        assert!(!subscription_row_exists(&tenant, "sub-member-mail-upstream").await);

        tenant.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// CS-452 sibling regression: with NO token supplied (`None` — the
    /// pre-CS-452 shape, and the real degrade path for a caller whose
    /// post-commit decrypt failed or who has no token to give at all), a
    /// delegated subscription's upstream delete is correctly SKIPPED rather
    /// than attempted with no credential: `subscriptions_upstream_attempted`
    /// stays `0` for it, exactly as before this PR.
    #[tokio::test]
    async fn member_scoped_purge_skips_the_upstream_delete_for_a_delegated_subscription_with_no_token_pg(
    ) {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_scoped_purge_skips_the_upstream_delete_for_a_delegated_subscription_with_no_token_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t452_delegated_upstream_skipped";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let tenant = build_tenant_pool(&base, db, "org_t452_delegated_upstream_skipped").await;

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();

        seed_subscription_owned(
            &tenant,
            account_id,
            "sub-member-mail-no-token",
            "me/mailFolders('inbox')/messages",
            member,
        )
        .await;

        let outcome = purge_microsoft_connection(
            &reqwest::Client::new(),
            &tenant,
            account_id,
            TokenOwner::Member(member),
            None,
        )
        .await;

        assert_eq!(outcome.subscriptions_deleted, 1);
        assert_eq!(
            outcome.subscriptions_upstream_attempted, 0,
            "no token to attempt with — a delegated row must be skipped, not attempted with an \
             empty/missing credential"
        );
        assert!(!outcome.purge_had_a_failure());
        assert!(!subscription_row_exists(&tenant, "sub-member-mail-no-token").await);

        tenant.close().await;
        drop_db(&mut admin_conn, db).await;
    }
}

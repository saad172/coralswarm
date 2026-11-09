//! Credential-death durable state (CS-453, Phase G wave 1, D21 —
//! `docs/plans/org-onboarding-from-the-invite.md` §3).
//!
//! [`ConnectorError::CredentialRevoked`] is the classification half — raised
//! ONLY from a definite dead-credential signal (see `connectors::microsoft::
//! classify_microsoft_token_error` / `connectors::slack::
//! classify_slack_api_error`). This module is the durable half: it reads and
//! writes `oauth_tokens.credential_invalid_at` / `.credential_invalid_reason`
//! (migration `0251`, tenant twin `0103`) for the EXACT `(account_id,
//! provider, TokenOwner)` row a `CredentialRevoked` was raised for.
//!
//! `oauth_tokens` is control-plane-only for every account, personal or org
//! (see `routes::oauth::store_token_bundle_tx`'s own doc comment) — every
//! function here takes whatever pool the caller already resolved as
//! `state.control_db()`/`creds_pool`, never the content/tenant pool `sync`/
//! `fetch_items` receive. Every read/write below runs inside
//! [`crate::tenancy::scope::begin_account_scoped`]: the production
//! `coralswarm_app` role is non-`BYPASSRLS` and the `oauth_tokens` policy
//! keys on `app.account_id`.
//!
//! **Owner-exactness.** Every query below branches on [`TokenOwner`] in
//! Rust — `AccountWide` binds `owner_user_id IS NULL`, `Member(uid)` binds
//! `owner_user_id = uid` — as two genuinely different statements, never a
//! single `owner_user_id IS NULL OR owner_user_id = $n` predicate. That
//! shape is the one [`TokenOwner`]'s own doc comment forbids: it would let a
//! `CredentialRevoked` raised for one member silently clear (or set) the
//! account-wide row, or vice versa, precisely the cross-credential mixup
//! this whole story exists to prevent.
//!
//! Callers this PR wires: `routes::connectors::trigger_sync`/`ingest_selected`
//! (SET, on an observed `CredentialRevoked`), `routes::oauth::
//! store_token_bundle_tx` (CLEAR, in its `ON CONFLICT ... DO UPDATE`).
//! `routes::slack_events`' `tokens_revoked` arm uses the provider-user-id
//! variant below directly, since Slack's push event names a provider
//! identity, not a `TokenOwner` the caller already resolved.
//!
//! **Read side.** `routes::oauth::list_connections` reports both columns on
//! every connection it lists, and the connectors page renders a marked row
//! as its reconnect notice (`web/components/connectors/ReconsentNotice.tsx`)
//! — the one user-facing surface for a dead credential, for every provider.
//! The scheduled sweeps (`workers::google_calendar_sync`,
//! `workers::microsoft_calendar_sync`) exclude a marked row, so the mark is
//! written when the death is first observed and not again every tick.
//! **Deliberately not built here**: re-election of anything keyed off a dead
//! credential, or a reconnect email — later Phase G stories.
//! [`is_credential_live`] is the read-side hook those stories attach to.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::{ConnectorError, CredentialDeathReason, TokenOwner};

/// Open the account-scoped transaction every helper in this module needs
/// before touching `oauth_tokens`. Maps [`crate::store::StoreError`] into
/// this module's existing [`sqlx::Error`] return type — same
/// `sqlx::Error::Protocol` bridge `connectors::slack::slack_watched_destination`
/// uses for its scoped setup.
async fn begin_credential_tx(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("credential_health scoped tx: {e}")))
}

/// Convenience wrapper for the common call-site shape: "if this
/// `ConnectorError` is a `CredentialRevoked`, durably record it for the
/// exact `(account_id, provider, owner)` row; otherwise do nothing." Used by
/// every real `sync`/`fetch_items`/`refresh_if_needed` call site this PR
/// wires (`routes::connectors::trigger_sync`/`ingest_selected`,
/// `routes::teams_ingest::microsoft_token`) so each doesn't hand-roll the
/// same `if let` — see [`mark_credential_invalid`] for the actual write.
/// Best-effort: a DB write failure here is logged and never propagated —
/// the caller still reports `e` to ITS OWN caller regardless of whether
/// this succeeded. Unguarded: the row is marked whatever has been written
/// to it since the caller loaded it — the request-path shape, where the
/// load and the failure are milliseconds apart. A background pass that
/// held its credentials across a Google/Graph round trip uses
/// [`record_if_revoked_since`].
pub(crate) async fn record_if_revoked(
    pool: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
    e: &ConnectorError,
) {
    record_if_revoked_since(pool, account_id, owner, e, None).await;
}

/// [`record_if_revoked`] guarded by the pass's witness of the row:
/// `row_updated_at` is `ConnectorCredentials::row_updated_at` — the
/// `oauth_tokens.updated_at` the pass loaded, or the one its own refresh
/// persist returned — and the mark lands only if the row has not been
/// written since. A reconnect (`routes::oauth::store_token_bundle_tx`, which
/// also clears the mark) that landed while this pass was still failing on
/// the OLD token therefore stays live; without the guard the stale pass's
/// mark arrived after the clear and the fresh connection sat revoked until
/// the user reconnected a second time. `None` is the unguarded write.
pub(crate) async fn record_if_revoked_since(
    pool: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
    e: &ConnectorError,
    row_updated_at: Option<DateTime<Utc>>,
) {
    if let ConnectorError::CredentialRevoked { provider, reason } = e {
        match mark_credential_invalid(pool, account_id, owner, provider, *reason, row_updated_at).await {
            Ok(0) if row_updated_at.is_some() => tracing::info!(
                %provider,
                reason = reason.code(),
                %account_id,
                "credential row was rewritten after this pass loaded it — not marking (a reconnect, or no row)"
            ),
            Ok(_) => {}
            Err(db_err) => tracing::warn!(
                error = %db_err,
                %provider,
                reason = reason.code(),
                %account_id,
                "failed to record credential_invalid_at for a revoked credential"
            ),
        }
    }
}

/// Mark the credential at `(account_id, provider, owner)` invalid as of now,
/// for `reason`. Idempotent — a second call while already invalid just
/// advances `credential_invalid_at` and overwrites the reason (the latest
/// definite signal wins; there is no "first observed" to preserve, unlike
/// `connector_health`'s open/resolve episode pair, since this is a single
/// live/dead bit, not a recurring condition).
///
/// `not_after`: when `Some`, the mark lands only if the row's `updated_at`
/// is at or before it — the caller's last observation of the row, in the
/// database's clock (see [`record_if_revoked_since`]). Token-bundle reconnect
/// and refresh writes through `routes::oauth::store_token_bundle_tx` advance
/// `updated_at` at write time, so a reconnect fails the guard even if its
/// transaction began before the observation.
///
/// Returns the number of rows updated — `0` if no row exists at that exact
/// address (the credential was already disconnected/deleted; nothing to
/// mark, and not an error), or if the guard refused.
pub async fn mark_credential_invalid(
    pool: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
    provider: &str,
    reason: CredentialDeathReason,
    not_after: Option<DateTime<Utc>>,
) -> Result<u64, sqlx::Error> {
    let mut tx = begin_credential_tx(pool, account_id).await?;
    let result = match owner {
        TokenOwner::AccountWide => {
            sqlx::query(
                "UPDATE oauth_tokens \
                 SET credential_invalid_at = now(), credential_invalid_reason = $3 \
                 WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NULL \
                   AND ($4::timestamptz IS NULL OR updated_at <= $4)",
            )
            .bind(account_id)
            .bind(provider)
            .bind(reason.code())
            .bind(not_after)
            .execute(&mut *tx)
            .await?
        }
        TokenOwner::Member(owner_user_id) => {
            sqlx::query(
                "UPDATE oauth_tokens \
                 SET credential_invalid_at = now(), credential_invalid_reason = $4 \
                 WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3 \
                   AND ($5::timestamptz IS NULL OR updated_at <= $5)",
            )
            .bind(account_id)
            .bind(provider)
            .bind(owner_user_id)
            .bind(reason.code())
            .bind(not_after)
            .execute(&mut *tx)
            .await?
        }
    };
    tx.commit().await?;
    let n = result.rows_affected();
    if n > 0 {
        notify_backer_lost_after_invalid(pool, account_id, owner, provider).await;
    }
    Ok(n)
}

/// After a successful mark, notify the capability-backing registry (S6) when
/// the dead credential may have been the last eligible backer of a *shared*
/// capability. Best-effort: never fails the mark itself.
///
/// This is the SHARED-capability surface only (an org's directory, Chat
/// readers, the Teams container owner, the Slack bot). The connection
/// itself — any provider, either plane — surfaces its dead credential
/// through `routes::oauth::list_connections`, which reports the row's
/// `credential_invalid_at`/`credential_invalid_reason` and the connectors
/// page renders as the reconnect notice; the sweeps exclude a marked row
/// until a reconnect clears it, so the mark is written once, not per tick.
async fn notify_backer_lost_after_invalid(
    pool: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
    provider: &str,
) {
    let user_id = match owner {
        TokenOwner::Member(uid) => Some(uid),
        TokenOwner::AccountWide => {
            // Account-wide Slack bot death is the cold-start-bot kind;
            // other AccountWide providers back no shared capability.
            // `backing_user_id` is NULL — Uuid::nil() is not a real user and
            // fails the connector_health FK.
            if provider != "slack" {
                return;
            }
            None
        }
    };
    if let Err(e) = crate::services::connector_health::on_backer_lost(
        pool,
        None, // content pool unavailable here; Chat election skipped
        account_id,
        user_id,
        provider,
        crate::services::connector_health::BackerLostCause::CredentialDeath,
        None,
        None,
    )
    .await
    {
        tracing::warn!(
            error = %e,
            %account_id,
            %provider,
            "credential_health: on_backer_lost after credential death failed — mark itself succeeded"
        );
    }
}

/// Clear a previously-set `credential_invalid_at`/`credential_invalid_reason`
/// at `(account_id, provider, owner)` — a no-op (0 rows, not an error) when
/// the row was already live or doesn't exist. Exists as its own function
/// (rather than folding into every `store_token_bundle_tx` INSERT/DO UPDATE
/// by hand) so a future reconnect/refresh call site outside that one write
/// path can reach the identical clear without duplicating the two-branch
/// owner predicate above.
#[allow(dead_code)] // hook for later Phase G stories — see module doc.
pub async fn clear_credential_invalid(
    pool: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
    provider: &str,
) -> Result<u64, sqlx::Error> {
    let mut tx = begin_credential_tx(pool, account_id).await?;
    let result = match owner {
        TokenOwner::AccountWide => {
            sqlx::query(
                "UPDATE oauth_tokens \
                 SET credential_invalid_at = NULL, credential_invalid_reason = NULL \
                 WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NULL",
            )
            .bind(account_id)
            .bind(provider)
            .execute(&mut *tx)
            .await?
        }
        TokenOwner::Member(owner_user_id) => {
            sqlx::query(
                "UPDATE oauth_tokens \
                 SET credential_invalid_at = NULL, credential_invalid_reason = NULL \
                 WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3",
            )
            .bind(account_id)
            .bind(provider)
            .bind(owner_user_id)
            .execute(&mut *tx)
            .await?
        }
    };
    tx.commit().await?;
    Ok(result.rows_affected())
}

/// Health of the newest row for this account, provider, and owner, matching
/// `routes::connectors::load_creds_bg`'s credential selection. `Some(true)`
/// means live, `Some(false)` means marked revoked, and `None` means absent.
pub async fn is_credential_live(
    pool: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
    provider: &str,
) -> Result<Option<bool>, sqlx::Error> {
    let mut tx = begin_credential_tx(pool, account_id).await?;
    let row: Option<(Option<chrono::DateTime<chrono::Utc>>,)> = match owner {
        TokenOwner::AccountWide => {
            sqlx::query_as(
                "SELECT credential_invalid_at FROM oauth_tokens \
                 WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NULL ORDER BY updated_at DESC LIMIT 1",
            )
            .bind(account_id)
            .bind(provider)
            .fetch_optional(&mut *tx)
            .await?
        }
        TokenOwner::Member(owner_user_id) => {
            sqlx::query_as(
                "SELECT credential_invalid_at FROM oauth_tokens \
                 WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3 ORDER BY updated_at DESC LIMIT 1",
            )
            .bind(account_id)
            .bind(provider)
            .bind(owner_user_id)
            .fetch_optional(&mut *tx)
            .await?
        }
    };
    tx.commit().await?;
    Ok(row.map(|(invalid_at,)| invalid_at.is_none()))
}

/// Slack's `tokens_revoked` Events API push (`routes::slack_events`) names a
/// PROVIDER identity (`event.tokens.oauth[]`, Slack user ids), not a
/// `TokenOwner` the caller already resolved. CoralSwarm has no member Slack
/// rows to resolve one from yet (CS-454 adds them, under the provider
/// literal `"slack_user"`). This matches ONLY `provider = 'slack_user'`:
/// the account-wide `"slack"` row stores the installing HUMAN as
/// `provider_user_id` (`SlackConnector::exchange_code` /
/// `routes::slack_events`' `auth.test` comments), so matching `"slack"`
/// here would mark the live workspace bot-token row dead when the installer
/// revokes only their user grant. Scoped to the ONE account this workspace
/// resolved to and the ONE workspace the event named. Every row this can
/// match already carries the identity the event is about, so this is not
/// the `owner_user_id IS NULL OR = x` fallback `TokenOwner`'s doc forbids —
/// that shape is about erasing the OWNER dimension; this narrows on
/// `provider_user_id`+`provider_workspace_id` instead.
pub async fn mark_slack_provider_user_revoked(
    pool: &PgPool,
    account_id: Uuid,
    team_id: &str,
    slack_user_id: &str,
) -> Result<u64, sqlx::Error> {
    let mut tx = begin_credential_tx(pool, account_id).await?;
    let result = sqlx::query(
        "UPDATE oauth_tokens \
         SET credential_invalid_at = now(), credential_invalid_reason = $4 \
         WHERE account_id = $1 \
           AND provider = 'slack_user' \
           AND provider_workspace_id = $2 \
           AND provider_user_id = $3",
    )
    .bind(account_id)
    .bind(team_id)
    .bind(slack_user_id)
    .bind(CredentialDeathReason::SlackTokensRevokedEvent.code())
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `CredentialDeathReason::code()` values are the DB/wire contract —
    /// pin them so a refactor of the enum's `Debug`/variant order can never
    /// silently change what's persisted to `credential_invalid_reason` or
    /// matched against by a future reader.
    #[test]
    fn reason_codes_are_stable() {
        assert_eq!(
            CredentialDeathReason::MicrosoftPasswordChangedOrAdminReset.code(),
            "ms_password_changed_or_admin_reset"
        );
        assert_eq!(
            CredentialDeathReason::MicrosoftAccountDisabled.code(),
            "ms_account_disabled"
        );
        assert_eq!(
            CredentialDeathReason::MicrosoftAccountDeleted.code(),
            "ms_account_deleted"
        );
        assert_eq!(
            CredentialDeathReason::MicrosoftConsentRevoked.code(),
            "ms_consent_revoked"
        );
        assert_eq!(CredentialDeathReason::MicrosoftGrantExpired.code(), "ms_grant_expired");
        assert_eq!(
            CredentialDeathReason::MicrosoftRefreshTokenInactive.code(),
            "ms_refresh_token_inactive"
        );
        assert_eq!(CredentialDeathReason::SlackTokenRevoked.code(), "slack_token_revoked");
        assert_eq!(CredentialDeathReason::SlackInvalidAuth.code(), "slack_invalid_auth");
        assert_eq!(
            CredentialDeathReason::SlackAccountInactive.code(),
            "slack_account_inactive"
        );
        assert_eq!(
            CredentialDeathReason::SlackTokensRevokedEvent.code(),
            "slack_tokens_revoked_event"
        );
        assert_eq!(CredentialDeathReason::GoogleInvalidGrant.code(), "google_invalid_grant");
        assert_eq!(
            CredentialDeathReason::GoogleUnauthorizedAfterRefresh.code(),
            "google_unauthorized_after_refresh"
        );
    }
}

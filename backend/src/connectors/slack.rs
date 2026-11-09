//! Slack connector — migrated onto the [`ConnectorProvider`] trait.
//!
//! This is a thin strategy over the existing token-exchange + parsing in
//! `crate::oauth::slack`. The only Slack-specific persistence (the
//! `slack_integrations` side-table the message pipeline reads) lives in
//! [`post_store`](SlackConnector::post_store) so it never leaks into the generic
//! OAuth path. Behavior is identical to the old `handle_slack_callback`.
//!
//! # Per-channel destination (`slack_watched_channels`, migration
//! `0101_slack_watched_channels` / tenant `0013`)
//!
//! docs/connectors/slack.md §2/§4 and docs/connectors/CONNECTOR_CONTRACT.md
//! §4: adding CoralSwarm to a channel is the authorising act.
//! [`slack_watched_destination`] is the explicit-row read side (mirrors
//! `connectors::microsoft::teams_watched_by_container`);
//! [`ensure_slack_watched_channel`] is the write side the interactive
//! destination picker (`routes::slack_events::handle_member_joined`'s
//! prompt, answered in `routes::slack_interactivity`) calls.
//! [`resolve_channel_destinations`] is the combined entry point message
//! ingest and huddle ingest (`workers::huddle::capture_huddle`) both call:
//! explicit row first, then the honest account-level default.
//!
//! ## `mode` — representing a pick with no single graph to store
//!
//! The org picker's first option is "everyone's individual oceans" — a
//! fan-out pick with NO single destination ocean to persist. A table with
//! only a non-null `graph_id` cannot represent that pick at all: a channel
//! with no row and a channel whose human explicitly chose fan-out would
//! both read back as "nobody has picked yet", which stops being harmless
//! the moment the no-row account-level default ever changes (see migration
//! `0101_slack_watched_channels`'s "AMENDED BEFORE SHIPPING" header). So the
//! table carries a `mode IN ('single', 'fanout')` discriminator, tied to
//! `graph_id`'s nullability by a table CHECK: `mode='single'` rows always
//! name their ocean, `mode='fanout'` rows never do. [`PickedDestination`] is
//! the write side's small enum over the two (what a human can persist);
//! [`ChannelDestination`] is the richer read-side type both an explicit
//! `mode='fanout'` row AND the no-row org default resolve into, since both
//! need the SAME live membership computation
//! ([`resolve_org_fanout_destinations`]) — an explicit fan-out pick does not
//! freeze a stale member list, it re-resolves on every read exactly like the
//! default does.

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    credential_health, ConnectorError, ConnectorProvider, CredentialDeathReason, TokenBundle,
    TokenOwner,
};
use crate::oauth::slack as slack_api;

// ── Credential-death classification (CS-453) ──────────────────────────────────

/// Classify a Slack API `error` string (the value of a `{"ok": false,
/// "error": "..."}` response body) into a [`CredentialDeathReason`] — but
/// ONLY from a DEFINITE dead-credential signal, per Slack's own error
/// reference (https://api.slack.com/authentication/oauth-v2#errors,
/// https://api.slack.com/methods/auth.test):
///
///   * **`token_revoked`** — the token was explicitly revoked (by the user,
///     or a workspace admin) since it was issued.
///   * **`invalid_auth`** — "Some aspect of authentication cannot be
///     validated. Either the provided token is invalid or the request
///     originates from an IP address disallowed from making the requested
///     method call." In practice the dominant real-world cause for an
///     established, previously-working token is that it no longer
///     authenticates at all (revoked/deleted app, or a token that outlived
///     the app's own install) — Slack's docs list it as one of the fixed
///     small set of authentication-failure errors alongside `token_revoked`.
///   * **`account_inactive`** — "Authentication token is for a deleted user
///     or workspace."
///
/// Deliberately left OUT:
///   * `not_authed` (no token provided at all — a caller bug, not a dead
///     credential; we always send a token).
///   * `no_permission`/`missing_scope` — a real problem, but a SCOPE gap,
///     not a dead credential; `connectors::slack::slack_missing_scopes` /
///     the existing `ReconsentNotice` machinery already own that case (see
///     `services::connector_health`'s own doc comment for why the two are
///     kept apart).
///   * `ratelimited`/`internal_error`/`fatal_error`/`service_unavailable` —
///     transient, must retry, never "dead". Never classified as revoked, per
///     this story's own D16 rule.
pub(crate) fn classify_slack_api_error(error: &str) -> Option<CredentialDeathReason> {
    match error {
        "token_revoked" => Some(CredentialDeathReason::SlackTokenRevoked),
        "invalid_auth" => Some(CredentialDeathReason::SlackInvalidAuth),
        "account_inactive" => Some(CredentialDeathReason::SlackAccountInactive),
        _ => None,
    }
}

/// Classify a Slack Web API `error` from a call that used a STORED bot
/// token, and durably record `credential_invalid_at` on the account-wide
/// `"slack"` row when the code is a definite dead-credential signal.
/// Best-effort: a DB write failure is logged inside
/// [`credential_health::record_if_revoked`].
///
/// Callers pass the raw Slack `error` code (not a formatted
/// `"conversations.list Slack error: …"` wrapper). Not used from
/// `exchange_code` — that path has no stored credential yet.
pub(crate) async fn record_if_stored_slack_revoked(
    pool: &PgPool,
    account_id: Uuid,
    slack_error: &str,
) {
    if let Some(reason) = classify_slack_api_error(slack_error) {
        tracing::info!(
            error = %slack_error,
            reason = reason.code(),
            %account_id,
            "slack stored-token API: classified as a dead credential"
        );
        let e = ConnectorError::CredentialRevoked { provider: "slack", reason };
        credential_health::record_if_revoked(pool, account_id, TokenOwner::AccountWide, &e).await;
    }
}

/// Resolve the EXPLICIT destination for a watched Slack channel
/// (`account_id`, `workspace_id`, `channel_id`). `Ok(None)` means no human
/// has picked a destination for this channel yet — callers fall back to the
/// account-level default; this function itself never guesses one.
///
/// A plain read. It used to be `UPDATE ... SET last_resolved_at = now()
/// RETURNING`, which nothing reads (the column keeps its admin-action bumps
/// in `slack_channel_actions`). An UPDATE takes the row lock, and the ingest
/// door share-locks this row for the length of a capture
/// (`ingest::grant::OrgSharedGrant::verify_tx`), so every message, huddle
/// and consent lookup in a channel queued behind any open capture of it. A
/// SELECT takes no row lock: the share lock now holds off only an admin's
/// re-point or delete.
///
/// Returns [`ChannelDestination`], not a bare `Uuid`/`Option<Uuid>`: an
/// explicit `mode='fanout'` row (docs/connectors/slack.md §2's "everyone's
/// individual oceans" pick) has no single graph to return, and MUST be
/// distinguishable from "nobody has picked yet" — see this module's `mode`
/// doc section. Before `mode` existed this returned `Option<Uuid>`, which
/// had no way to represent that pick at all.
///
/// Scoped inside `begin_tenant_system_scoped` (org-slack step 2: this is
/// reached from the signature/webhook-authenticated ingest funnels — the
/// Events API handler, and now the tenant-looped `SlackMessageIngest` worker
/// — which carry no acting end user, so the transaction asserts the tenant
/// SYSTEM principal rather than a member identity; see that function's own
/// doc comment. `slack_watched_channels` also carries a consumer-plane
/// `account_isolation` RLS policy (migration `0101_slack_watched_channels`);
/// `begin_tenant_system_scoped` is a strict superset of `begin_account_scoped`
/// (same `app.account_id` GUC, plus `app.tenant_actor='system'`, inert on the
/// consumer plane since `0083`'s policy never reads it), so this is
/// byte-identical there.
///
/// `tenant_pool` is where `slack_watched_channels` itself lives (a
/// tenant-tree table, present on both trees). `control_pool` is threaded
/// through only for the `'fanout'` materialization below
/// ([`resolve_org_fanout_destinations`] needs it for `account_members`) — on
/// the consumer plane every caller passes the same pool for both.
pub(crate) async fn slack_watched_destination(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<Option<ChannelDestination>, sqlx::Error> {
    Ok(slack_watched_route(control_pool, tenant_pool, account_id, workspace_id, channel_id)
        .await?
        .map(|(_, destination)| destination))
}

/// A `slack_watched_channels` row's `(mode, graph_id)`, as the resolver read
/// it. The ingest grant records this and re-reads it in the capture
/// transaction, so the row the destination was materialized from and the
/// row the grant records are one read, never two.
pub(crate) type ChannelRoute = (String, Option<Uuid>);

/// [`slack_watched_destination`], also returning the row it resolved from.
pub(crate) async fn slack_watched_route(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<Option<(ChannelRoute, ChannelDestination)>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("slack_watched_destination scoped tx: {e}")))?;
    let row: Option<ChannelRoute> = sqlx::query_as(
        "SELECT mode, graph_id FROM slack_watched_channels \
         WHERE account_id = $1 AND workspace_id = $2 AND channel_id = $3",
    )
    .bind(account_id)
    .bind(workspace_id)
    .bind(channel_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();

    let Some((mode, graph_id)) = row else {
        return Ok(None);
    };
    let destination =
        materialize_channel_destination(control_pool, tenant_pool, account_id, workspace_id, &mode, graph_id).await?;
    Ok(Some(((mode, graph_id), destination)))
}

/// Turn a raw `(mode, graph_id)` pair — as stored in `slack_watched_channels`
/// — into the [`ChannelDestination`] callers actually want. Shared by
/// [`slack_watched_destination`] (an existing row) and
/// [`ensure_slack_watched_channel`] (a row this call just inserted, or the
/// pre-existing row a conflicting insert deferred to), so the `'fanout'` ->
/// live-membership-lookup step is written exactly once.
///
/// The `other` arm and the `mode='single'`-with-NULL-`graph_id` arm should
/// both be unreachable given the table's `slack_watched_channels_mode_graph_id_check`
/// CHECK constraint; they are handled as errors rather than `unreachable!()`
/// so a future migration/constraint slip degrades to a loud, catchable
/// `sqlx::Error` instead of a panic on this webhook-driven ingest path.
async fn materialize_channel_destination(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    mode: &str,
    graph_id: Option<Uuid>,
) -> Result<ChannelDestination, sqlx::Error> {
    match mode {
        "single" => graph_id.map(ChannelDestination::Single).ok_or_else(|| {
            sqlx::Error::Protocol(format!(
                "slack_watched_channels: mode='single' row for account {account_id} carries a \
                 NULL graph_id — the table's mode/graph_id CHECK constraint should make this \
                 unreachable"
            ))
        }),
        "fanout" => {
            // Two pools (org-tenancy plane split, step 0) —
            // `resolve_org_fanout_destinations` needs `account_members` on
            // the CONTROL plane and `connector_identities`/`graphs` on the
            // CONTENT (tenant) plane. On the consumer plane every caller
            // passes the same pool for both.
            Ok(resolve_org_fanout_destinations(control_pool, tenant_pool, account_id, workspace_id)
                .await?
                .into_destination())
        }
        other => Err(sqlx::Error::Protocol(format!(
            "slack_watched_channels: unknown mode {other:?} for account {account_id} — the \
             table's mode CHECK constraint should make this unreachable"
        ))),
    }
}

/// A destination pick to persist via [`ensure_slack_watched_channel`].
///
/// A single struct instead of a run of positional parameters: the old
/// signature took THREE adjacent `Uuid`s (`account_id`, `user_id`,
/// `graph_id`) followed by THREE adjacent `&str`s (`workspace_id`,
/// `channel_id`, `display_name`) — swapping `account_id`/`user_id` or
/// `workspace_id`/`channel_id` at a call site compiles clean (both are
/// `Uuid`/`&str` respectively) and, because the write below is a deliberate
/// `ON CONFLICT DO NOTHING`, the wrong row becomes permanent by contract I2
/// (a second, correct call would silently no-op against it). Named fields
/// remove the ordering hazard entirely.
pub(crate) struct SlackChannelPick {
    pub(crate) account_id: Uuid,
    /// Provenance only — see [`ensure_slack_watched_channel`]'s doc comment.
    pub(crate) user_id: Uuid,
    pub(crate) workspace_id: String,
    pub(crate) channel_id: String,
    pub(crate) display_name: String,
    pub(crate) destination: PickedDestination,
}

/// The two persistable destination shapes docs/connectors/slack.md §2's
/// picker can produce. Distinct from [`ChannelDestination`] (the READ side's
/// type, which additionally carries a freshly-computed fan-out membership
/// list) — this is only ever "what a human just picked."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PickedDestination {
    /// One destination ocean, named explicitly.
    Single(Uuid),
    /// The org picker's "everyone's individual oceans" pick — no single
    /// ocean to name (docs/connectors/slack.md §2; see this module's `mode`
    /// doc section for why this needs its own persisted representation).
    FanOut,
}

/// The result of [`ensure_slack_watched_channel`] — lets a caller (the
/// interactivity handler) say "saved" apart from "already routed to
/// <ocean>", which a bare `Result<(), _>` could not distinguish (contract
/// I2's no-re-point guarantee means a second pick is ALWAYS the latter, and
/// silently reporting it as success either way would leave a human thinking
/// their pick took effect when it did not).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnsureOutcome {
    /// The channel's destination after this call returns: the pick that was
    /// just persisted when `newly_inserted` is `true`, or the PRE-EXISTING
    /// destination the pick was refused in favour of when it is `false`.
    pub(crate) destination: ChannelDestination,
    /// `true` only when THIS call's pick is the one that got persisted.
    pub(crate) newly_inserted: bool,
}

/// Create the destination row for a Slack channel — the write side of the
/// interactive destination picker (`routes::slack_events::handle_member_joined`'s
/// prompt, answered in `routes::slack_interactivity`).
///
/// `ON CONFLICT ... DO NOTHING`: contract I2, one container one destination
/// FIXED AT AUTHORISATION TIME. A second pick for an already-authorised
/// channel must never silently re-point it — changing a destination later is
/// a data migration, not a setting. `display_name` also does not update on
/// conflict for the same reason. See [`EnsureOutcome`] for how a caller tells
/// the two outcomes apart.
///
/// **Ownership check (BLOCKER 1 fix, this table's RLS section):** a
/// `PickedDestination::Single` carries a caller-supplied `graph_id` with no
/// a-priori guarantee it belongs to `account_id` — `routes/
/// teams_ingest.rs:206-218`'s precedent is to derive the destination graph
/// SERVER-SIDE instead of trusting a caller-supplied id at all, which this
/// function cannot fully copy (its whole job is to persist a HUMAN's
/// explicit pick, not resolve a default) — so instead it verifies the
/// caller-supplied `graph_id` up front, in the SAME scoped transaction the
/// insert runs in, and refuses outright (returns `Err`, no row written) when
/// it does not belong to `account_id`. The `INSERT ... WHERE $4 IS NULL OR
/// EXISTS (...)` shape re-asserts the same ownership check inline as
/// defence in depth (redundant once the check above has passed, but cheap,
/// and it is what actually stops a write should the explicit check above
/// ever be bypassed or refactored around) — a belt-and-suspenders pairing,
/// not a replacement for either half. `PickedDestination::FanOut` carries no
/// `graph_id` at all, so there is nothing to own-check.
///
/// This is deliberately a HARD refusal, not the same "silent no-op" as the
/// `ON CONFLICT DO NOTHING` case: those are different failure shapes (an
/// already-authorised channel vs. an attempt to author a cross-account row)
/// and must not be conflated.
///
/// **Plane guard:** `PickedDestination::FanOut` is refused outright for a
/// PERSONAL account. docs/connectors/slack.md §2's picker only ever offers
/// "everyone's individual oceans" on the ORG plane — a personal account has
/// no "everyone" to fan out to — so a `FanOut` pick reaching this function
/// for a personal account can only be a caller bug, not a legitimate human
/// choice, and must not be allowed to persist an unreachable-in-practice row.
///
/// **Two pools + an explicit plane (org-slack step 3).** This used to derive
/// its own plane guard with `SELECT clerk_org_id IS NOT NULL FROM accounts
/// WHERE id = $1` against whatever single `pool` it was handed — a query
/// against `accounts`, a CONTROL-ONLY table that does not exist on an org
/// TENANT pool, exactly the bug `routes::slack::resolve_primary_graph`'s
/// step-0 conversion fixed for the ingest side (see [`AccountPlane`]'s doc
/// comment). The caller now supplies `plane` directly — every caller through
/// `routes::slack_interactivity` already resolved it via `slack::account_plane`
/// before this point — so this function never touches `accounts` at all.
///
/// `graphs`/`slack_watched_channels` are tenant-tree tables (present under
/// an org tenant, migrations `0101_slack_watched_channels`), so the
/// ownership check and the insert itself both run against `tenant_pool` —
/// `control_pool` for a personal/not-yet-tenant-migrated account, or the
/// org's own tenant pool once one is resolved. `control_pool` is threaded
/// through separately because a freshly-inserted `FanOut` pick's outcome
/// needs a LIVE fan-out membership computation
/// ([`resolve_org_fanout_destinations`]), which needs `account_members` on
/// the CONTROL plane — see the tail of this function. On the consumer plane
/// every caller passes the same pool for both, so behaviour there is
/// byte-identical to before this split.
pub(crate) async fn ensure_slack_watched_channel(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    plane: crate::routes::slack::AccountPlane,
    pick: SlackChannelPick,
) -> Result<EnsureOutcome, sqlx::Error> {
    let SlackChannelPick {
        account_id,
        user_id,
        workspace_id,
        channel_id,
        display_name,
        destination,
    } = pick;

    // `begin_tenant_system_scoped` — a strict superset of `begin_account_scoped`
    // (same `app.account_id` GUC, plus `app.tenant_actor='system'`), and the
    // right scoping helper here: this write reaches this function from a
    // Slack webhook-authenticated interaction, which carries no acting
    // `Tenant`/end-user session for `begin_member_scoped` to key off. Inert on
    // the consumer plane (0083's policy never reads `app.tenant_actor`), same
    // convention `slack_watched_destination`'s own read-side conversion uses.
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("ensure_slack_watched_channel scoped tx: {e}")))?;

    let (mode, graph_id_param): (&'static str, Option<Uuid>) = match destination {
        PickedDestination::Single(graph_id) => {
            // Explicit ownership refusal — see this function's doc comment.
            // Checked BEFORE the insert so a foreign `graph_id` is reported
            // as exactly what it is (an ownership refusal), never silently
            // folded into the `ON CONFLICT DO NOTHING` no-op path below.
            let owned: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM graphs WHERE id = $1 AND account_id = $2)",
            )
            .bind(graph_id)
            .bind(account_id)
            .fetch_one(&mut *tx)
            .await?;
            if !owned {
                tx.rollback().await.ok();
                return Err(sqlx::Error::Protocol(format!(
                    "ensure_slack_watched_channel: graph {graph_id} does not belong to account \
                     {account_id}; refusing to persist a cross-account Slack channel destination"
                )));
            }
            ("single", Some(graph_id))
        }
        PickedDestination::FanOut => {
            if plane != crate::routes::slack::AccountPlane::Org {
                tx.rollback().await.ok();
                return Err(sqlx::Error::Protocol(format!(
                    "ensure_slack_watched_channel: a 'fanout' pick is only valid for an ORG \
                     account; refusing for personal account {account_id}"
                )));
            }
            ("fanout", None)
        }
    };

    let inserted_row: Option<(String, Option<Uuid>)> = sqlx::query_as(
        r#"
        INSERT INTO slack_watched_channels
            (account_id, user_id, mode, graph_id, workspace_id, channel_id, display_name)
        SELECT $1, $2, $3, $4, $5, $6, $7
        WHERE  $4::uuid IS NULL
           OR  EXISTS (SELECT 1 FROM graphs g WHERE g.id = $4 AND g.account_id = $1)
        ON CONFLICT (account_id, workspace_id, channel_id) DO NOTHING
        RETURNING mode, graph_id
        "#,
    )
    .bind(account_id)
    .bind(user_id)
    .bind(mode)
    .bind(graph_id_param)
    .bind(&workspace_id)
    .bind(&channel_id)
    .bind(&display_name)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;

    match inserted_row {
        // This call's own pick is the one that landed. Routed through the
        // SAME shared `materialize_channel_destination` helper the read-side
        // ingest funnel uses (`slack_watched_destination` /
        // `resolve_channel_destinations`) rather than a local
        // reimplementation: org-slack step 2 converted that helper to the
        // identical two-pool shape this function already needed (`single`:
        // no query, `graph_id` is already known; `fanout`:
        // `resolve_org_fanout_destinations(control_pool, tenant_pool, ...)`
        // for the CONTROL-plane `account_members` read), so duplicating it
        // here would only be two implementations of the same thing drifting
        // apart — an earlier cut of this function did exactly that
        // (reimplemented the match on `mode` inline to avoid touching this
        // helper before it was two-pool) and was collapsed back into this
        // call once step 2 landed.
        Some((mode, graph_id)) => {
            let destination =
                materialize_channel_destination(control_pool, tenant_pool, account_id, &workspace_id, &mode, graph_id)
                    .await?;
            Ok(EnsureOutcome {
                destination,
                newly_inserted: true,
            })
        }
        // `ON CONFLICT DO NOTHING` fired: the channel was already authorised
        // before this call. The ownership/plane checks above already ruled
        // out the other possible cause of a rejected write (a foreign
        // `graph_id`, or `FanOut` under a personal account), so a `None`
        // here can only mean "already authorised" — re-read the winning row
        // so the caller can report what it actually is.
        //
        // Re-reads via the SAME two-pool `slack_watched_destination` the
        // ingest funnel uses — an ORG account whose PRE-EXISTING row happens
        // to be `mode='fanout'` now re-materializes its live membership
        // correctly (via `control_pool`'s `account_members`), not just the
        // `mode='single'` common case: the narrow gap an earlier cut of this
        // function had here (before step 2 converted this helper) is closed.
        None => {
            let existing =
                slack_watched_destination(control_pool, tenant_pool, account_id, &workspace_id, &channel_id)
                    .await?;
            let destination = existing.ok_or_else(|| {
                sqlx::Error::Protocol(format!(
                    "ensure_slack_watched_channel: INSERT ON CONFLICT DO NOTHING no-opped for \
                     account {account_id}, workspace {workspace_id}, channel {channel_id}, but \
                     no existing row was found on re-read — should be unreachable"
                ))
            })?;
            Ok(EnsureOutcome {
                destination,
                newly_inserted: false,
            })
        }
    }
}

/// Where a Slack channel's messages should land, per
/// [`resolve_channel_destinations`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChannelDestination {
    /// One destination: either an explicit `slack_watched_channels` row (a
    /// human's authorisation-time pick, contract I2), or — for a PERSONAL
    /// account with no row — that account's own primary ocean.
    Single(Uuid),
    /// No explicit row, ORG account: the honest default per
    /// docs/connectors/CONNECTOR_CONTRACT.md §4's C5 row — fan out to each
    /// account member's OWN ocean (never a shared/team ocean; this is not a
    /// promotion decision, contract I3), but ONLY the members an identity
    /// mapping can actually confirm. `unmapped_members` is how many account
    /// members could not be mapped and were therefore skipped — never
    /// guessed, never dropped silently. See
    /// [`resolve_org_fanout_destinations`].
    FanOut {
        graphs: Vec<Uuid>,
        unmapped_members: usize,
        /// The account's members as the fan-out read them, the set `graphs`
        /// was intersected with. The ingest grant's re-check on an org
        /// tenant (`ingest::grant::Roster`) keeps this set instead of
        /// reading the roster a second time.
        members: std::collections::HashSet<Uuid>,
    },
}

/// What a member fan-out resolved: the mapped members' own oceans, how many
/// members could not be mapped, and the roster the mapping was intersected
/// with. [`resolve_org_fanout_destinations`] and
/// `google_chat::resolve_chat_fanout_destinations` both return it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FanOut {
    pub(crate) graphs: Vec<Uuid>,
    pub(crate) unmapped_members: usize,
    pub(crate) members: std::collections::HashSet<Uuid>,
}

impl FanOut {
    pub(crate) fn into_destination(self) -> ChannelDestination {
        let Self { graphs, unmapped_members, members } = self;
        ChannelDestination::FanOut { graphs, unmapped_members, members }
    }
}

/// Given a Slack channel (`account_id`, `workspace_id`, `channel_id`),
/// resolve where its messages should land. This is the single entry point
/// both `routes::slack_events::ingest_slack_message` and
/// `workers::huddle::capture_huddle` call instead of
/// `routes::slack::resolve_primary_graph(.., IngestCaller::NonAttributable)`:
/// see that variant's doc comment for why it unconditionally refuses under an
/// org account, which is exactly why Slack ingest (messages AND huddle
/// capture alike) was fail-closed dead for org accounts before this table's
/// read path existed.
///
/// Resolution order:
/// 1. An explicit `slack_watched_channels` row
///    ([`slack_watched_destination`]) — a human's authorisation-time choice,
///    contract I2, always wins, and SHORT-CIRCUITS regardless of which
///    `mode` it carries. A `mode='single'` row returns
///    [`ChannelDestination::Single`] directly; a `mode='fanout'` row —
///    "everyone's individual oceans", explicitly chosen — returns
///    [`ChannelDestination::FanOut`] directly too, computed the same live
///    way step 3 below computes its default. Neither case falls through to
///    steps 2/3: an explicit fan-out pick must never be re-derived from the
///    account's personal-vs-org default, because that default can change
///    independently of what the human actually picked (see migration
///    `0101_slack_watched_channels`'s "AMENDED BEFORE SHIPPING" header).
/// 2. No row yet, PERSONAL account: [`ChannelDestination::Single`] wrapping
///    the account's own primary ocean
///    (`routes::slack::resolve_primary_graph`) — byte-identical to
///    pre-`slack_watched_channels` behaviour.
/// 3. No row yet, ORG account: [`ChannelDestination::FanOut`] — see
///    [`resolve_org_fanout_destinations`]. This is the deliberate departure
///    from "pick one arbitrary representative member" — the founder's call:
///    the default is everyone's own ocean, not one guessed member's.
/// **Plane-explicit (org-slack step 2).** `resolve_channel_destinations`
/// itself used to derive personal-vs-org with its own `SELECT clerk_org_id
/// IS NOT NULL FROM accounts` — a query against `accounts`, a CONTROL-ONLY
/// table, run against whatever single `pool` this function was handed. That
/// was harmless while Slack ingest was consumer-plane-only (`pool` was
/// always the control pool too), but breaks outright
/// (`relation "accounts" does not exist`) the moment this is called with a
/// genuine org TENANT pool. Callers now supply BOTH pools plus the
/// already-known [`crate::routes::slack::AccountPlane`] — mirroring
/// [`crate::routes::slack::resolve_primary_graph`]'s own step-0 conversion —
/// so this function never touches `accounts` at all:
///
/// * `control_pool` — where `accounts`/`account_members` live (org fan-out's
///   membership roster).
/// * `tenant_pool` — where `graphs`/`connector_identities`/
///   `slack_watched_channels` live: the consumer pool for a personal/
///   not-yet-tenant-migrated account, or the org's own tenant pool once one
///   is resolved. On the consumer plane every caller passes the same pool
///   for both, so behaviour is byte-identical to before this split.
pub(crate) async fn resolve_channel_destinations(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    plane: crate::routes::slack::AccountPlane,
    workspace_id: &str,
    channel_id: &str,
) -> Result<ChannelDestination, sqlx::Error> {
    resolve_channel_route(control_pool, tenant_pool, account_id, plane, workspace_id, channel_id)
        .await
        .map(|(_, destination)| destination)
}

/// [`resolve_channel_destinations`], also returning the row it resolved
/// from: `None` is the account default (steps 2 and 3).
pub(crate) async fn resolve_channel_route(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    plane: crate::routes::slack::AccountPlane,
    workspace_id: &str,
    channel_id: &str,
) -> Result<(Option<ChannelRoute>, ChannelDestination), sqlx::Error> {
    if let Some((route, destination)) =
        slack_watched_route(control_pool, tenant_pool, account_id, workspace_id, channel_id).await?
    {
        return Ok((Some(route), destination));
    }

    // No explicit row — fall back to the account-level default.
    let destination =
        resolve_account_default_destination(control_pool, tenant_pool, account_id, plane, workspace_id).await?;
    Ok((None, destination))
}

/// Steps 2 and 3 of [`resolve_channel_destinations`] — the account-level
/// default a channel with NO `slack_watched_channels` row ingests through:
/// a personal account's own primary ocean, or an org account's live
/// per-member fan-out.
///
/// **Extracted so the READ side can call the identical function, not a
/// paraphrase of it.** `connectors::slack_channel_management` renders a
/// "lands in" label for exactly this case (`DestinationKind::Default`), and
/// before this existed that label was a hardcoded string — "your account
/// default" — which on an ORG account was simply false: the content is
/// fanned out to each MAPPED member's private ocean and the shared org ocean
/// receives nothing. A second, display-side derivation of "where would this
/// land" is the drift docs/connectors/slack-onboarding.md §3's
/// one-derivation rule exists to prevent, so the page calls THIS, and any
/// future change to the default lands on both surfaces at once.
///
/// Deliberately does NOT take a `channel_id`: there is nothing
/// channel-specific in either branch (that is precisely what makes it the
/// ACCOUNT-level default), which is also why the read side can resolve it
/// ONCE per page rather than once per row.
///
/// **Not side-effect free, and the read side accepts that knowingly.** The
/// personal branch's `resolve_primary_graph` creates the account's primary
/// ocean if it somehow has none — the same row ingest would create on the
/// next message, under the same uniqueness index — so calling this from a
/// `GET` cannot produce a destination ingest would not itself have produced.
/// The org branch is read-only, but does emit `resolve_primary_graph`'s
/// `connector_health` warnings for members whose member ocean cannot be
/// resolved: the same warnings ingest already emits for the same people, not
/// new ones.
pub(crate) async fn resolve_account_default_destination(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    plane: crate::routes::slack::AccountPlane,
    workspace_id: &str,
) -> Result<ChannelDestination, sqlx::Error> {
    match plane {
        crate::routes::slack::AccountPlane::Personal => {
            let graph_id = crate::routes::slack::resolve_primary_graph(
                tenant_pool,
                account_id,
                crate::routes::slack::AccountPlane::Personal,
                crate::routes::slack::IngestCaller::NonAttributable,
            )
            .await?;
            Ok(ChannelDestination::Single(graph_id))
        }
        crate::routes::slack::AccountPlane::Org => {
            Ok(resolve_org_fanout_destinations(control_pool, tenant_pool, account_id, workspace_id)
                .await?
                .into_destination())
        }
    }
}

/// The ORG-account default when a channel has no explicit destination row:
/// docs/connectors/CONNECTOR_CONTRACT.md §4's C5 row — "Seating is manual.
/// Surface 'N members are unmapped' to an admin. Never silently drop
/// unmapped people, and never guess from display names." Fan out to each
/// account member's OWN ocean, but only members an identity mapping says
/// correspond to a Slack user in this workspace.
///
/// **Wired to the matcher (phase two, docs/connectors/slack.md §1's C5
/// row).** A member is "mapped" here when TWO things both hold:
///
/// 1. `connector_identities` already carries a CONFIRMED link
///    (`provider = 'slack'`, this `workspace_id`, `user_id IS NOT NULL`) for
///    a `users.id` that is a CURRENT member of `account_id` — i.e. the SAME
///    persisted state [`resolve_verified_slack_identity`] writes wherever it
///    has already run (a message this person authored, an interactivity
///    click, or auto-seat's channel-roster enumeration —
///    `routes::slack_interactivity::auto_seat_channel_members`). Deliberately
///    a DIRECTORY READ, not a fresh `users.list` roster call: the directory
///    IS the matcher's own persisted, confirmed output, so re-deriving it via
///    a live per-member Slack round-trip on every account-wide default
///    resolution would be pure overhead with no behavior change — the same
///    "directory-first, never re-hit the network for an already-confirmed
///    link" philosophy [`resolve_verified_slack_identity`]'s own doc comment
///    establishes. The cost of this: a member who has NEVER triggered one of
///    those three discovery events (no message sent, no click, never seen in
///    a routed channel's roster) stays unmapped here until one of them fires
///    — an accepted, honest degradation, not a silent guess.
/// 2. [`crate::routes::slack::resolve_primary_graph`] (the SAME resolver
///    every other ingest path uses for "this member's own ocean") succeeds
///    for that `user_id` under this `account_id`. A confirmed identity whose
///    primary member ocean cannot yet be resolved (e.g.
///    `MEMBER_OCEANS_ENABLED` off, or provisioning has not caught up — see
///    that function's own doc comment) is ALSO counted unmapped: an identity
///    match with nowhere to land is not a usable destination.
///
/// **`unmapped_members` is `total_members − mapped.len()`, never an
/// independent count.** This was the bug in the pre-wiring placeholder this
/// function replaces: computing `unmapped` from a SEPARATE `count(*)` query
/// while `graphs` is built from a DIFFERENT, live-resolved set risks exactly
/// the failure mode a fan-out warning exists to prevent — a `connector_health`
/// warning claiming N members are unmapped when they were just successfully
/// seated. `mapped` below is deduplicated by MEMBER (a `HashSet<Uuid>` of
/// `user_id`, not a count of `graphs`), so even if a future change ever let
/// one member map to more than one graph, `unmapped_members` still reflects
/// how many DISTINCT people were not reached, not how many destinations were.
///
/// Never falls back to matching a Slack display name against a CoralSwarm
/// user's name — the contract explicitly forbids that guess (§4, C5 row:
/// "never guess from display names").
///
/// **Two pools (org-tenancy plane split, step 0).** This used to run one
/// query JOINing tenant-side `connector_identities` to control-only
/// `account_members`, plus a separate `count(*) FROM account_members` —
/// both fail outright (`relation "account_members" does not exist`) the
/// moment `pool` is a genuine org TENANT pool, since `account_members` does
/// not exist there at all. Split into a two-pool, two-step resolution
/// instead:
///
/// 1. CONTROL (`control_pool`): the account's member `user_id`s (and their
///    count).
/// 2. TENANT (`tenant_pool`): `connector_identities` rows for this
///    workspace with `user_id IS NOT NULL` — no JOIN, since `account_id`
///    does not exist on that table at all (a Slack workspace is connected
///    by exactly one account, so filtering by `workspace_id` is already the
///    account scope).
/// 3. Intersect the two sets in Rust — the confirmed identity must belong
///    to a CURRENT member of this account, same as the old JOIN enforced.
/// 4. TENANT: the existing `graphs` batch read (unchanged predicate).
///
/// On the consumer plane, every caller today passes the SAME pool for both
/// parameters, so behaviour is byte-identical to before this split.
/// `pub(crate)` (was private) because
/// `connectors::slack_channel_management` resolves the SAME fan-out to label
/// an explicit `mode = 'fanout'` row — see
/// [`resolve_account_default_destination`]'s doc comment on why the read side
/// calls these functions rather than describing them.
pub(crate) async fn resolve_org_fanout_destinations(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<FanOut, sqlx::Error> {
    // Phase G item 48: after the 30-day connect-email grace, this fan-out
    // is unreachable. Empty graphs is the same "nothing to land" result an
    // unmapped org already returns, so callers do not copy the message
    // into other members' oceans. Delegated member capture never calls
    // this function.
    if !crate::services::org_install_grace::org_install_capture_open(control_pool).await {
        tracing::info!(
            %account_id,
            "slack org fan-out unreachable — org-install grace closed"
        );
        return Ok(FanOut { graphs: Vec::new(), unmapped_members: 0, members: Default::default() });
    }
    // ── READ ORDER IS LOAD-BEARING: identities (TENANT) BEFORE members
    //    (CONTROL). Do not "tidy" these back into declaration order. ───────
    //
    // The SQL JOIN this replaced evaluated membership and identity in ONE
    // snapshot. Two reads on two separate pools have no shared snapshot, so
    // there is now a real gap between them, and the direction of that gap
    // decides who can be exposed.
    //
    // Members LAST (this order): a member removed from `account_members`
    // during the gap is absent from the fresher member read, so the
    // intersection drops them. Correct — they lose access immediately.
    //
    // Members FIRST (the reverse): the member read still contains someone an
    // operator just removed, their identity still matches, and the message
    // lands in a REMOVED member's private ocean. That is an access-control
    // failure, not a stale-read annoyance.
    //
    // The residual race in this order is benign and deliberate: a member
    // ADDED during the gap is in the member set but was not in the identity
    // snapshot, so they miss THIS ONE message and pick up the next. Losing
    // one message beats leaking one.

    // TENANT: confirmed Slack identities in THIS workspace — see this
    // function's doc comment for why this reads the matcher's persisted
    // directory rather than issuing a fresh per-member Slack call, and for
    // why there is no JOIN to `account_members` here (that table does not
    // exist on this plane). `DISTINCT` guards against a pathological
    // double-row for the same `user_id` (two Slack accounts sharing one
    // verified email is not possible under `users.email`'s `UNIQUE`
    // constraint, but this costs nothing and removes the possibility
    // outright).
    let identity_user_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT user_id FROM connector_identities \
         WHERE provider = 'slack' AND workspace_id = $1 AND user_id IS NOT NULL",
    )
    .bind(workspace_id)
    .fetch_all(tenant_pool)
    .await?;

    // CONTROL: this account's current members, read AFTER the identities
    // above for the reason documented there. `total_members` is derived from
    // this SAME list (`.len()`), never a separate `count(*)` — see this
    // function's own doc comment on why `unmapped_members` must never be an
    // independent count.
    let member_user_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT user_id FROM account_members WHERE account_id = $1")
            .bind(account_id)
            .fetch_all(control_pool)
            .await?;
    let total_members = member_user_ids.len();
    let member_set: std::collections::HashSet<Uuid> = member_user_ids.into_iter().collect();

    // Intersect in Rust: only identities belonging to a CURRENT member of
    // THIS account — the same scoping the removed JOIN enforced in SQL.
    let confirmed_user_ids: Vec<Uuid> = identity_user_ids
        .into_iter()
        .filter(|user_id| member_set.contains(user_id))
        .collect();

    let mut mapped_graphs: Vec<Uuid> = Vec::new();
    let mut mapped_members: std::collections::HashSet<Uuid> = std::collections::HashSet::new();

    // ── Batch pass ────────────────────────────────────────────────────────
    // Every confirmed member used to go through `resolve_primary_graph`,
    // which opens its OWN `begin_account_scoped` transaction (set_config +
    // plane lookup + select). On a fan-out channel that is one full
    // round-trip per member on the ingest hot path, for a question that is a
    // single indexed `= ANY(...)` read.
    //
    // The predicate below is `routes::slack::primary_ocean::MEMBER`, the one
    // `resolve_org_member_primary_graph` binds — account, owner,
    // `kind = 'member'`, `archived_at IS NULL`, `is_primary` — because
    // widening ANY of those is the cross-member leak that function's own doc
    // comment exists to warn about. Members this misses (personal-plane
    // accounts, or provisioning that has not caught up) fall through to the
    // per-member call below, so the slow path still owns every refusal and
    // every `connector_health` warning exactly as before. `graphs` is a
    // TENANT-side table (present on both trees), so this reads
    // `tenant_pool`.
    let batched: Vec<(Uuid, Uuid)> = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(tenant_pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(format!("fan-out batch graph lookup scope: {e}")))?;
        let rows = sqlx::query_as::<_, (Uuid, Uuid)>(&format!(
            "SELECT owner_user_id, id FROM graphs WHERE account_id = $1 AND owner_user_id = ANY($2) AND {}",
            crate::routes::slack::primary_ocean::MEMBER
        ))
        .bind(account_id)
        .bind(&confirmed_user_ids)
        .fetch_all(&mut *tx)
        .await?;
        tx.rollback().await.ok();
        rows
    };
    let batched: std::collections::HashMap<Uuid, Uuid> = batched.into_iter().collect();

    for user_id in confirmed_user_ids {
        if let Some(&graph_id) = batched.get(&user_id) {
            if mapped_members.insert(user_id) {
                mapped_graphs.push(graph_id);
            }
            continue;
        }

        // This function only ever runs for an ORG account (`resolve_channel_
        // destinations` only calls it on the org branch), so the plane is
        // hardcoded `Org` rather than re-derived — org-tenancy plane split,
        // step 0, see `AccountPlane`'s doc comment.
        match crate::routes::slack::resolve_primary_graph(
            tenant_pool,
            account_id,
            crate::routes::slack::AccountPlane::Org,
            crate::routes::slack::IngestCaller::Attributable(user_id),
        )
        .await
        {
            Ok(graph_id) => {
                if mapped_members.insert(user_id) {
                    mapped_graphs.push(graph_id);
                }
            }
            Err(e) => {
                // Identity confirmed, but no destination ocean exists for
                // them yet — never fabricate one. `resolve_primary_graph`'s
                // own org branch already logs a `connector_health` warning
                // for this exact case, so this is a debug-level echo, not a
                // duplicate alarm.
                tracing::debug!(
                    error = %e,
                    %account_id,
                    %user_id,
                    "Slack org fan-out: identity confirmed but no primary member ocean could be \
                     resolved yet — counted as unmapped, not guessed"
                );
            }
        }
    }

    let unmapped_members = total_members.saturating_sub(mapped_members.len());
    Ok(FanOut { graphs: mapped_graphs, unmapped_members, members: member_set })
}

/// Enumerate a Slack channel's current members via `conversations.members`,
/// paginating on Slack's cursor until exhausted.
///
/// Shared by the "existing team ocean" auto-seat write
/// (`routes::slack_interactivity::auto_seat_channel_members`, docs/connectors/
/// slack.md's C5 row) and the destination prompt's seat-count disclosure
/// (`routes::slack_events::post_destination_prompt`) — both need the SAME
/// roster, one to seat it and one to preview it, so the pagination logic
/// lives here once rather than twice.
///
/// Uses `channels:read` (already granted). A private channel needs
/// `groups:read`, which is not in the default scope list — that call fails
/// with `missing_scope`, surfaced as `Err` rather than an empty roster (an
/// empty channel and an inaccessible one must never look identical).
/* Public-facing text for the two failure modes whose underlying error is not
   safe to hand back verbatim (CWE-209).

   Both paginators below return `Err(String)`, and that string travels all the
   way out: `connectors::slack_onboarding` puts it in `channels.error`, the
   JSON endpoints serialise it, and the web UI prints it inside "we couldn't
   check which channels the bot is in ({error})". A `reqwest::Error`'s Display
   can carry the request URL, resolver and proxy detail, and a serde error
   carries a byte offset into a response body we never chose to expose — none
   of which helps the person reading that sentence, and all of which is
   infrastructure detail leaking through a user-facing surface.

   What is deliberately NOT sanitised: Slack's own `error` codes
   (`channel_not_found`, `missing_scope`, …) and the rate-limit case. Those
   are a fixed, documented vocabulary from the upstream API, they are exactly
   what makes a failure actionable, and they describe the integration rather
   than our infrastructure. Sanitising them too would trade a real diagnostic
   for nothing. */
const PUBLIC_TRANSPORT_ERROR: &str = "could not reach Slack";
const PUBLIC_PARSE_ERROR: &str = "Slack returned a response we couldn't read";

/// Most bytes either paginator will buffer from ONE Slack response.
///
/// 8 MiB is far above anything Slack actually returns for the paged calls
/// below — `conversations.members` at `limit=1000` is a flat array of short
/// ids, `conversations.list` at `limit=200` a list of channel objects — so
/// this is a backstop against a pathological or truncated-but-endless body,
/// not a tuning knob. Without it `json()` buffers whatever arrives, so the
/// only bound on memory is Slack's good behaviour.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Read a Slack JSON response with a hard size cap, then decode.
///
/// Streams the body chunk-by-chunk rather than calling `Response::json()`,
/// for two reasons:
///
/// 1. **Bounded memory** — `json()` buffers the whole body first, so an
///    unexpectedly huge response is buffered in full before anything can
///    object. This aborts as soon as the cap is crossed.
/// 2. **The enclosing deadline can actually fire.** `json()` is
///    `bytes().await` followed by a SYNCHRONOUS `serde_json::from_slice`, and
///    `tokio::time::timeout*` cannot interrupt code that never yields — so a
///    long decode runs past the deadline and still returns `Ok`. Chunked
///    reads yield repeatedly, so the buffering half is genuinely
///    interruptible. The decode itself is still synchronous and still
///    uninterruptible; capping the input is what bounds how long it can be,
///    which is the proportionate fix for payloads this size (moving decode to
///    a spawned pool was considered and rejected as machinery out of scale
///    with a millisecond-level concern).
pub(crate) async fn read_capped_json(
    resp: reqwest::Response,
    method: &'static str,
) -> Result<serde_json::Value, String> {
    let mut resp = resp;
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let chunk = resp.chunk().await.map_err(|e| {
            tracing::warn!(error = %e, method, connector_health = true, "slack response body read error");
            PUBLIC_TRANSPORT_ERROR.to_string()
        })?;
        let Some(chunk) = chunk else { break };
        if buf.len() + chunk.len() > MAX_RESPONSE_BYTES {
            tracing::warn!(
                method,
                cap_bytes = MAX_RESPONSE_BYTES,
                connector_health = true,
                "slack response exceeded the response-size cap; refusing to buffer further"
            );
            return Err(PUBLIC_PARSE_ERROR.to_string());
        }
        buf.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&buf).map_err(|e| {
        tracing::warn!(error = %e, method, connector_health = true, "slack response was not valid JSON");
        PUBLIC_PARSE_ERROR.to_string()
    })
}

pub(crate) async fn fetch_conversation_members(
    http: &reqwest::Client,
    bot_token: &str,
    channel_id: &str,
) -> Result<Vec<String>, String> {
    // Same backstop rationale as `routes::slack_events::fetch_channel_member_
    // count`'s own `MAX_PAGES`: guards against a pathological/adversarial
    // cursor loop, not a tuning knob.
    const MAX_PAGES: usize = 50;

    // `MAX_PAGES` bounds the number of REQUESTS but not the time they take,
    // and the shared `http` client's default timeout is per-REQUEST — so 50
    // slow pages compose into ~50 minutes of a caller blocked on one roster.
    // This is the same defect fixed in `fetch_bot_member_channels` (PR #510);
    // it lived here too, and the two paginators are kept deliberately
    // symmetrical so the next reader does not have to work out whether the
    // difference was meaningful.
    //
    // 25s matches its sibling. Callers are already off Slack's ~3s
    // interactivity ACK deadline (`auto_seat_channel_members` replaces its
    // message via `response_url`; `post_destination_prompt` runs in a spawned
    // task), so this bounds a background stall rather than a user-visible one
    // — but an unbounded background stall still holds a connection and a task
    // slot for the better part of an hour.
    //
    // Like its sibling it does NOT return the members collected so far: a
    // partial roster is what "identified N of M" is computed from, and a
    // short M silently understates how many people a routing decision seats.
    // Failing is the honest outcome — `resolve_channel_members`' own contract
    // already says a failed roster fetch must surface as a failure and never
    // be rounded down to a smaller number.
    // ONE absolute deadline, fixed before the loop — not a relative duration
    // recomputed each iteration. Both express the same budget, but an
    // absolute instant cannot drift: there is no per-iteration subtraction to
    // get wrong, and every `timeout_at` below is measured against the same
    // fixed point rather than against whenever its own iteration happened to
    // start.
    const TOTAL_BUDGET: std::time::Duration = std::time::Duration::from_secs(25);
    let deadline = tokio::time::Instant::now() + TOTAL_BUDGET;

    let mut members: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;

    for _ in 0..MAX_PAGES {
        // `timeout_at` would fire immediately on an already-past deadline
        // anyway; checking first only buys a more precise message —
        // "we ran out between pages" reads differently from "a single request
        // overran", and the two are worth telling apart in a log.
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "conversations.members: exceeded its {}s total budget before the cursor was exhausted",
                TOTAL_BUDGET.as_secs()
            ));
        }

        let mut query = vec![
            ("channel", channel_id.to_string()),
            ("limit", "1000".to_string()),
        ];
        if let Some(c) = &cursor {
            query.push(("cursor", c.clone()));
        }

        // Every part of the round-trip lives inside the budget: `send()`
        // resolves the headers, but the body read can stall independently of
        // it, so a between-pages check alone would still let a single page
        // burn the client's full per-request timeout.
        let page = tokio::time::timeout_at(deadline, async {
            let resp = http
                .get(format!("{}/conversations.members", slack_api_base()))
                .query(&query)
                .bearer_auth(bot_token)
                .send()
                .await
                .map_err(|e| {
                    // Detail to the log, not to the caller — see
                    // `PUBLIC_TRANSPORT_ERROR`'s note.
                    tracing::warn!(error = %e, connector_health = true, "conversations.members transport error");
                    PUBLIC_TRANSPORT_ERROR.to_string()
                })?;

            if resp.status().as_u16() == 429 {
                return Err("conversations.members: rate-limited by Slack".to_string());
            }

            if !resp.status().is_success() {
                return Err("conversations.members: HTTP failure".into());
            }
            read_capped_json(resp, "conversations.members").await
        })
        .await
        .map_err(|_| {
            format!(
                "conversations.members: exceeded its {}s total budget mid-request",
                TOTAL_BUDGET.as_secs()
            )
        })??;

        let json = page;

        if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            let err = json.get("error").and_then(serde_json::Value::as_str).unwrap_or("unknown");
            return Err(format!("conversations.members Slack error: {err}"));
        }

        let page = json
            .get("members")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "conversations.members: response had no members array".to_string())?;
        for id in page {
            let s = id.as_str().filter(|s| !s.is_empty())
                .ok_or_else(|| "conversations.members: malformed roster entry".to_string())?;
            members.push(s.to_string());
        }

        let next_cursor = match json.get("response_metadata") {
            None => None,
            Some(metadata) => {
                let cursor = metadata.get("next_cursor").and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "conversations.members: malformed cursor metadata".to_string())?;
                if cursor.is_empty() { None } else { Some(cursor) }
            }
        };
        match next_cursor {
            Some(c) => cursor = Some(c.to_string()),
            None => return Ok(members),
        }
    }

    Err(format!(
        "conversations.members: did not exhaust its cursor within {MAX_PAGES} pages"
    ))
}

/// The result of a successful [`leave_conversation`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaveOutcome {
    /// `conversations.leave` returned `ok: true` — the bot really was a
    /// member and just left.
    Left,
    /// `conversations.leave` returned `ok: false, error: "not_in_channel"`.
    /// Treated as a SUCCESS, not a failure: "stop watching" is idempotent —
    /// if the bot is already not in the channel, the caller's goal (the bot
    /// is not reading it) is already true, and there is nothing left to do
    /// but clean up the management row.
    AlreadyLeft,
}

/// `conversations.leave` failed in a way [`stop_watching_channel`]'s caller
/// MUST NOT paper over — see [`leave_conversation`]'s own doc comment for the
/// ordering rule this exists to support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LeaveError {
    /// `channels:leave` was added to `SLACK_SCOPES` for this feature — every
    /// workspace installed before that line shipped has a bot token that
    /// will return this, permanently, until a human reinstalls. Same
    /// unavoidable Slack limitation as `users:read.email`/`im:write` (see
    /// `authorize_url`'s own scope comments): Slack never retroactively
    /// grants a newly added scope to an existing installation. This needs
    /// its own error variant (rather than folding into `Slack` below)
    /// because the caller-facing message must NAME the fix (reinstall), not
    /// just report a generic Slack error string a human has no way to act
    /// on.
    MissingScope,
    /// Any other Slack `ok: false` error (`channel_not_found`, a transient
    /// `ok: false` with no recognisable code, etc). Carries Slack's own
    /// `error` string for the log; the HTTP layer decides what (if anything)
    /// of it is safe to surface to a caller.
    Slack(String),
    /// Rate-limited, or a transport/parse failure — the same public-facing
    /// strings ([`PUBLIC_TRANSPORT_ERROR`]/[`PUBLIC_PARSE_ERROR`]) this
    /// file's other Slack calls already use.
    Transport(String),
}

/// Have the bot actually leave a Slack channel — the write half of "stop
/// watching a channel" (the founder decision this exists for: unlike a
/// destination re-point, stopping watch is not just a routing-table change,
/// the bot must actually stop being able to read the channel).
///
/// **Ordering contract every caller MUST honour: only delete the
/// `slack_watched_channels` row after this returns `Ok`.** A row deleted
/// while the bot is STILL in the channel is a WORSE state than doing
/// nothing: the channel keeps ingesting via the account-level default
/// (`resolve_channel_destinations`'s no-row branch — a personal account's
/// primary ocean, or an org account's live per-member fan-out) with no
/// management row left to show for it, i.e. content silently resumes
/// flowing with zero visibility into where it is landing. See [`LeaveError`]
/// for the failure shapes a caller must preserve the row for, and
/// [`crate::connectors::slack_channel_actions::stop_watching_channel_after_leave`]
/// for the one place that ordering is actually enforced (split out from the
/// HTTP call specifically so the ordering rule is unit-testable without a
/// live Slack call — this crate has no HTTP-mock harness).
pub(crate) async fn leave_conversation(
    http: &reqwest::Client,
    bot_token: &str,
    channel_id: &str,
) -> Result<LeaveOutcome, LeaveError> {
    let resp = http
        .post(format!("{}/conversations.leave", slack_api_base()))
        .bearer_auth(bot_token)
        .json(&serde_json::json!({ "channel": channel_id }))
        .send()
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, connector_health = true, "conversations.leave transport error");
            LeaveError::Transport(PUBLIC_TRANSPORT_ERROR.to_string())
        })?;

    if resp.status().as_u16() == 429 {
        return Err(LeaveError::Transport(
            "conversations.leave: rate-limited by Slack".to_string(),
        ));
    }

    let json = read_capped_json(resp, "conversations.leave")
        .await
        .map_err(LeaveError::Transport)?;

    if json.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        return Ok(LeaveOutcome::Left);
    }

    let err = json
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    match err.as_str() {
        // Idempotent success — see this function's own doc comment.
        "not_in_channel" => Ok(LeaveOutcome::AlreadyLeft),
        "missing_scope" => Err(LeaveError::MissingScope),
        other => Err(LeaveError::Slack(other.to_string())),
    }
}

/// Enumerate the channels the bot is CURRENTLY a member of — step 1 of
/// docs/connectors/slack-onboarding.md §3's status contract ("is the bot in
/// any channel?"). Deliberately a LIVE `conversations.list` call, never a
/// read of `slack_watched_channels`: that table is written only when a human
/// answers the destination picker (migration `0101`'s header), so a
/// correctly-invited-but-not-yet-routed channel has no row there at all — see
/// docs/connectors/slack-onboarding.md §4 for the full "why this is a live
/// Slack call" argument this function exists to satisfy.
///
/// `types=public_channel` ONLY, not `public_channel,private_channel` —
/// `groups:read` is not in the granted scope set (same gap
/// [`fetch_conversation_members`]'s own doc comment flags), and Slack fails
/// `conversations.list` for its WHOLE requested `types` list, not just the
/// ungranted portion, when any one of them is missing its scope. Requesting
/// only what `channels:read` actually covers means a workspace with zero
/// private channels (the common case) gets a real `state: "ok"` answer
/// instead of `state: "unavailable"` for a scope this endpoint was never
/// promised anyway — private-channel coverage is a known, documented gap
/// here, not a bug: a bot invited only to private channels currently reads
/// `state: "ok", count: 0` (an honest "no PUBLIC channels", which happens to
/// coincide with "we can't see any" — see this crate's C10/scope roster in
/// docs/connectors/slack.md for the broader pattern of granted-scope-shaped
/// gaps).
///
/// Returns `(channel_id, name)` pairs for channels where `is_member == true`.
/// `Err` — never an empty `Ok(vec![])` standing in for a failure — covers
/// every case a caller cannot stand behind the count it would otherwise
/// report: HTTP error, a non-2xx/rate-limited response, a Slack `ok: false`,
/// or malformed JSON. This is the exact "unavailable is not zero" distinction
/// docs/connectors/slack-onboarding.md §3's rules section names explicitly —
/// callers must render `Err` as `state: "unavailable"`, never fold it into a
/// zero count.
pub(crate) async fn fetch_bot_member_channels(
    http: &reqwest::Client,
    bot_token: &str,
) -> Result<Vec<(String, String)>, String> {
    // Same backstop rationale as `fetch_conversation_members`'s own
    // `MAX_PAGES`.
    const MAX_PAGES: usize = 50;

    // ── Total wall-clock budget across the WHOLE pagination loop ──────────
    //
    // `MAX_PAGES` bounds the number of REQUESTS but not the time they take,
    // and that is not the same bound. `state.http_client` is built with the
    // shared 60s default timeout, which is PER REQUEST — so a Slack side
    // that is slow rather than down (60s per page, a `next_cursor` every
    // time) let this loop run for ~50 minutes while a caller waited. Both
    // callers are latency-visible: `slack_onboarding::derive_status` serves
    // the `/connectors` checklist and the Slack App Home tab, and
    // `slack_channel_management::derive_channel_management` serves the
    // connector detail page. A user is not going to sit there for 50
    // minutes; the request is abandoned long before, and the task keeps
    // holding a connection and a Slack rate-limit slice for nothing.
    //
    // The budget is enforced with `tokio::time::timeout` around each page's
    // send+parse, sized to whatever is LEFT of the budget — not merely
    // checked between pages. A between-pages check alone would still let the
    // very first request hang for its full 60s per-request timeout, which is
    // the exact failure being fixed.
    //
    // On exhaustion this returns the SAME `Err(String)` shape every other
    // failure here returns, so callers keep rendering `state: "unavailable"`
    // (docs/connectors/slack-onboarding.md §3: "unavailable is not zero").
    // It deliberately does NOT return the channels collected so far: a
    // partial page set presented as a complete channel list is a confident
    // wrong answer — the same class of mistake as folding an error into a
    // zero count — and a silent truncation would make "in 3 channels" mean
    // "in at least 3 channels" with no way for the caller to tell.
    // One absolute deadline fixed before the loop — see
    // `fetch_conversation_members` for why an instant beats a per-iteration
    // remaining-duration. The two paginators are kept symmetrical.
    const TOTAL_BUDGET: std::time::Duration = std::time::Duration::from_secs(25);
    let deadline = tokio::time::Instant::now() + TOTAL_BUDGET;

    let mut channels: Vec<(String, String)> = Vec::new();
    let mut cursor: Option<String> = None;

    for _ in 0..MAX_PAGES {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "conversations.list: exceeded its {}s total budget before the cursor was exhausted",
                TOTAL_BUDGET.as_secs()
            ));
        }

        let mut query = vec![
            ("types", "public_channel".to_string()),
            ("exclude_archived", "true".to_string()),
            ("limit", "200".to_string()),
        ];
        if let Some(c) = &cursor {
            query.push(("cursor", c.clone()));
        }

        // Every part of the round-trip lives inside the budget: `send()`
        // resolves the headers, but the body read can stall independently.
        let page = tokio::time::timeout_at(deadline, async {
            let resp = http
                .get(format!("{}/conversations.list", slack_api_base()))
                .query(&query)
                .bearer_auth(bot_token)
                .send()
                .await
                .map_err(|e| {
                    tracing::warn!(error = %e, connector_health = true, "conversations.list transport error");
                    PUBLIC_TRANSPORT_ERROR.to_string()
                })?;

            if resp.status().as_u16() == 429 {
                return Err("conversations.list: rate-limited by Slack".to_string());
            }

            read_capped_json(resp, "conversations.list").await
        })
        .await;

        let json: serde_json::Value = match page {
            Ok(Ok(json)) => json,
            Ok(Err(e)) => return Err(e),
            Err(_elapsed) => {
                return Err(format!(
                    "conversations.list: exceeded its {}s total budget mid-request",
                    TOTAL_BUDGET.as_secs()
                ))
            }
        };

        if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            let err = json.get("error").and_then(serde_json::Value::as_str).unwrap_or("unknown");
            return Err(format!("conversations.list Slack error: {err}"));
        }

        let page = json
            .get("channels")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "conversations.list: response had no channels array".to_string())?;
        for c in page {
            let is_member = c
                .get("is_member")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if !is_member {
                continue;
            }
            let id = c.get("id").and_then(serde_json::Value::as_str);
            let name = c.get("name").and_then(serde_json::Value::as_str);
            if let (Some(id), Some(name)) = (id, name) {
                channels.push((id.to_string(), name.to_string()));
            }
        }

        let next_cursor = json
            .get("response_metadata")
            .and_then(|m| m.get("next_cursor"))
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty());
        match next_cursor {
            Some(c) => cursor = Some(c.to_string()),
            None => return Ok(channels),
        }
    }

    Err(format!(
        "conversations.list: did not exhaust its cursor within {MAX_PAGES} pages"
    ))
}

/// Enumerate the public channels ONE Slack member belongs to — the member
/// half of [`fetch_bot_member_channels`], used by
/// `slack_onboarding::derive_status`'s org-member branch to answer "which of
/// MY channels is the bot in" (docs/connectors/slack-onboarding.md §3's
/// `member.my_channels`).
///
/// **This is the user's channels, NOT the intersection with the bot's.**
/// `users.conversations` answers "what is `slack_user_id` in", full stop —
/// including channels the bot has never been invited to. The caller owns the
/// intersection against [`fetch_bot_member_channels`]'s set, which it already
/// has in hand for step 1, so the member branch costs exactly ONE extra Slack
/// call rather than a second full roster walk. Returning the raw set here
/// (rather than taking the bot's set as an argument and intersecting inside)
/// keeps this function a thin, honest mirror of the Slack endpoint it wraps.
///
/// `types=public_channel` ONLY, for the same granted-scope reason
/// [`fetch_bot_member_channels`] gives: `groups:read` is not in `SLACK_SCOPES`,
/// and Slack fails the call for its WHOLE requested `types` list when any one
/// of them is missing its scope. A member's private channels are therefore
/// invisible here — a documented gap, identical to the one step 1 already
/// carries, not a new one.
///
/// Uses the BOT token with an explicit `user=` argument: there is no user
/// token in this integration at all (the install grants a bot token only), so
/// "which channels is this person in" has to be asked on the bot's behalf.
///
/// `Err` — never an empty `Ok(vec![])` standing in for a failure — for every
/// case the caller cannot stand behind a count: HTTP error, non-2xx/rate
/// limit, Slack `ok: false`, malformed JSON, or the wall-clock budget below.
/// Callers must render `Err` as `state: "unavailable"`, never as zero
/// (docs/connectors/slack-onboarding.md §3).
pub(crate) async fn fetch_user_member_channel_ids(
    http: &reqwest::Client,
    bot_token: &str,
    slack_user_id: &str,
) -> Result<Vec<String>, String> {
    // Same backstops, same reasoning, same numbers as
    // `fetch_bot_member_channels` — the two paginators are kept symmetrical
    // on purpose (a divergence here would be a second, differently-behaved
    // definition of "we could not establish this list").
    const MAX_PAGES: usize = 50;
    const TOTAL_BUDGET: std::time::Duration = std::time::Duration::from_secs(25);
    let deadline = tokio::time::Instant::now() + TOTAL_BUDGET;

    let mut ids: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;

    for _ in 0..MAX_PAGES {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "users.conversations: exceeded its {}s total budget before the cursor was exhausted",
                TOTAL_BUDGET.as_secs()
            ));
        }

        let mut query = vec![
            ("user", slack_user_id.to_string()),
            ("types", "public_channel".to_string()),
            ("exclude_archived", "true".to_string()),
            ("limit", "200".to_string()),
        ];
        if let Some(c) = &cursor {
            query.push(("cursor", c.clone()));
        }

        let page = tokio::time::timeout_at(deadline, async {
            let resp = http
                .get(format!("{}/users.conversations", slack_api_base()))
                .query(&query)
                .bearer_auth(bot_token)
                .send()
                .await
                .map_err(|e| {
                    tracing::warn!(error = %e, connector_health = true, "users.conversations transport error");
                    PUBLIC_TRANSPORT_ERROR.to_string()
                })?;

            if resp.status().as_u16() == 429 {
                return Err("users.conversations: rate-limited by Slack".to_string());
            }

            read_capped_json(resp, "users.conversations").await
        })
        .await;

        let json: serde_json::Value = match page {
            Ok(Ok(json)) => json,
            Ok(Err(e)) => return Err(e),
            Err(_elapsed) => {
                return Err(format!(
                    "users.conversations: exceeded its {}s total budget mid-request",
                    TOTAL_BUDGET.as_secs()
                ))
            }
        };

        if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            let err = json.get("error").and_then(serde_json::Value::as_str).unwrap_or("unknown");
            return Err(format!("users.conversations Slack error: {err}"));
        }

        let page = json
            .get("channels")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "users.conversations: response had no channels array".to_string())?;
        // No `is_member` filter, unlike `conversations.list`: this endpoint
        // returns ONLY the conversations `user` belongs to, so every row is a
        // membership by construction and the field is not even sent.
        for c in page {
            if let Some(id) = c.get("id").and_then(serde_json::Value::as_str) {
                ids.push(id.to_string());
            }
        }

        let next_cursor = json
            .get("response_metadata")
            .and_then(|m| m.get("next_cursor"))
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty());
        match next_cursor {
            Some(c) => cursor = Some(c.to_string()),
            None => return Ok(ids),
        }
    }

    Err(format!(
        "users.conversations: did not exhaust its cursor within {MAX_PAGES} pages"
    ))
}

/// Resolve a Slack channel's roster down to the CoralSwarm `users.id`s that
/// are both (a) verified-email-matched to a member of `channel_id`'s
/// workspace and (b) a CURRENT member of `account_id` — the two-part "resolve
/// AND is-a-member" check every C5 routing path needs (mirrors
/// [`resolve_verified_slack_identity`]'s own doc comment: it answers "is
/// there ANY CoralSwarm user with this verified email", not "is that user a
/// member of the account routing this workspace" — this function adds the
/// second half).
///
/// Returns `(matched_user_ids, total_channel_humans)`. `matched_user_ids`
/// may contain fewer entries than `total_channel_humans` — the difference
/// is members who could not be identified, or who were identified but belong
/// to a different account; both are ordinary, expected outcomes, never
/// errors. Bots are excluded from the total outright: they are not people,
/// and the app's own bot user sits in every channel it was invited to.
///
/// `Err` covers every case where the roster cannot be established with
/// confidence — [`fetch_conversation_members`] failing, a `users.info`
/// lookup that could not be completed (rate limit, HTTP, bad JSON), or a
/// database error on the membership check. All of them share one property:
/// answering anyway would mean disclosing a number we know might be wrong.
/// docs/connectors/slack.md's C5 row states the rule this enforces — "a
/// disclosure with a missing count is better than a wrong one".
///
/// Shared by [`crate::routes::slack_interactivity::auto_seat_channel_
/// members`] (which needs the actual `user_id`s to seat),
/// [`crate::routes::slack_events::post_destination_prompt`]'s seat-count
/// disclosure (which needs only the counts), and
/// [`crate::workers::slack_seat_reconcile`]'s tenant-looped sweep — one
/// roster walk, one matcher pass, reused by all three.
///
/// **Two pools (org-slack step 4; step 5a threads `control_pool` one level
/// deeper into the matcher itself).** `identity_pool` resolves
/// `connector_identities` (present on both trees — the plane this
/// workspace's content actually lands on; see
/// [`resolve_slack_identity_detailed`]); `control_pool` runs the
/// `account_members` membership check below AND — since step 5a —
/// [`resolve_slack_identity_detailed`]'s own `users` lookup, both CONTROL-
/// ONLY (absent from every tenant database). The two consumer-plane callers
/// above still pass the SAME pool for both (control and content are one
/// physical database there, byte-identical to before this split);
/// `slack_seat_reconcile`'s tenant-looped call is the one that actually
/// needs them to differ.
pub(crate) async fn resolve_channel_members(
    control_pool: &PgPool,
    identity_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
    channel_id: &str,
) -> Result<(Vec<Uuid>, usize), String> {
    let external_ids = fetch_conversation_members(http, bot_token, channel_id).await?;

    // Counts PEOPLE, not Slack member rows. Bots are subtracted as they are
    // identified, because the app's own bot user is a member of every channel
    // it was invited to and can never match — leaving it in makes every
    // "identified N of M" disclosure permanently short by at least one.
    let mut total = external_ids.len();

    let mut matched: Vec<Uuid> = Vec::new();
    for external_id in external_ids {
        let user_id = match resolve_slack_identity_detailed(
            control_pool,
            identity_pool,
            http,
            account_id,
            workspace_id,
            &external_id,
            bot_token,
        )
        .await
        {
            SlackIdentity::Matched(user_id) => user_id,
            SlackIdentity::Unmatched => continue,
            SlackIdentity::Bot => {
                total = total.saturating_sub(1);
                continue;
            }
            // The whole roster is abandoned rather than reported short. Both
            // callers of this function disclose their counts to a human (the
            // seat preview and the auto-seat summary), and this function's
            // own contract above already says so: a channel we cannot fully
            // enumerate must surface as a failure, never as a confident
            // partial answer. Under-reporting here is how a single
            // rate-limited `users.info` burst tells an admin that nobody in
            // a 200-person channel has an account.
            SlackIdentity::Unavailable(why) => {
                return Err(format!(
                    "could not resolve Slack member {external_id} in {channel_id}: {why}"
                ));
            }
        };

        // Was `unwrap_or(false)`, which made a database failure look exactly
        // like "this user is not a member of the account" — silently seating
        // fewer people than the disclosure promised, with nothing logged.
        let is_member: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(control_pool)
        .await
        .map_err(|e| format!("account_members check for {user_id}: {e}"))?;

        if is_member {
            matched.push(user_id);
        }
    }

    Ok((matched, total))
}

/// Per-(account, channel) cooldown for [`notify_unmatched_content_not_saved`].
///
/// The huddle path is naturally rare and gates its notice on the job's first
/// attempt. The MESSAGE path is not rare: `graphs.is_empty()` is re-evaluated
/// for every message in an unmapped channel, and `backfill_channel` replays
/// history through the same funnel. Ungated, one busy unmapped channel turns
/// the bot into the loudest participant in it and walks straight into Slack's
/// per-channel `chat.postMessage` rate limit.
///
/// In-process, so N backend machines can each post once per window rather
/// than one notice globally. That is a deliberate trade against a schema
/// change: bounded and small (this PR carries no migrations), versus the
/// unbounded per-message flood it replaces. A persisted `last_notified_at`
/// is the correct long-term home if the connector ever runs wide.
fn unmatched_notice_cooldown()
-> &'static std::sync::Mutex<std::collections::HashMap<(Uuid, String), std::time::Instant>> {
    static C: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(Uuid, String), std::time::Instant>>,
    > = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// How long one channel stays quiet after being told its content isn't being
/// saved. Long enough that a busy channel hears it once a shift, short enough
/// that someone who links identities and comes back tomorrow isn't left
/// wondering whether it took.
const UNMATCHED_NOTICE_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(6 * 60 * 60);

/// `true` at most once per [`UNMATCHED_NOTICE_COOLDOWN`] per (account,
/// channel). Claims the slot as it reports, so concurrent ingest tasks for
/// the same channel cannot both pass.
pub(crate) fn should_send_unmatched_notice(account_id: Uuid, channel_id: &str) -> bool {
    let now = std::time::Instant::now();
    let mut guard = match unmatched_notice_cooldown().lock() {
        Ok(g) => g,
        // A poisoned mutex means some other task panicked mid-update. Staying
        // quiet is the safe direction for a courtesy message.
        Err(_) => return false,
    };
    // Opportunistic sweep — this map is keyed by channel and would otherwise
    // grow for the lifetime of the process.
    guard.retain(|_, sent| now.duration_since(*sent) < UNMATCHED_NOTICE_COOLDOWN);

    match guard.get(&(account_id, channel_id.to_string())) {
        Some(sent) if now.duration_since(*sent) < UNMATCHED_NOTICE_COOLDOWN => false,
        _ => {
            guard.insert((account_id, channel_id.to_string()), now);
            true
        }
    }
}

/// Post a short, non-alarming notice into a Slack channel when content could
/// not be saved because nobody involved could be identity-matched to a
/// CoralSwarm account — docs/connectors/slack.md's C5 row, "unmatched
/// content drops and notifies in-channel": a human who watched a message or
/// huddle vanish deserves to know it was never saved and why, in plain
/// language, rather than silently losing it with no trace on either side.
///
/// `what` names the content that wasn't saved ("This message" / "This
/// huddle") — the ONLY content-specific detail in the message. Deliberately
/// leaks NOTHING internal: no account id, graph id, workspace id, or
/// technical error — this is a normal, expected outcome (an org whose Slack
/// identities are not yet linked), not a fault to diagnose from the message
/// itself.
///
/// Best-effort and fire-and-forget in spirit: a failure to post is logged
/// and swallowed, never propagated — losing the courtesy notice is strictly
/// better than failing the ingest/capture path that is already correctly
/// dropping the content over a message ABOUT that drop.
pub(crate) async fn notify_unmatched_content_not_saved(
    http: &reqwest::Client,
    bot_token: &str,
    channel_id: &str,
    what: &str,
) {
    let text = format!(
        "{what} wasn't saved to CoralSwarm — nobody involved could be matched to a CoralSwarm \
         account yet. This is expected until Slack identities here are linked; nothing for you \
         to do."
    );
    let body = serde_json::json!({
        "channel": channel_id,
        "text": text,
    });

    let resp = match http
        .post(format!("{}/chat.postMessage", slack_api_base()))
        .bearer_auth(bot_token)
        .json(&body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                channel_id,
                "notify_unmatched_content_not_saved: HTTP error posting notice (non-fatal)"
            );
            return;
        }
    };

    match resp.json::<serde_json::Value>().await {
        Ok(json) => {
            if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
                let err = json.get("error").and_then(serde_json::Value::as_str).unwrap_or("unknown");
                tracing::warn!(
                    error = %err,
                    channel_id,
                    "notify_unmatched_content_not_saved: Slack API error posting notice \
                     (non-fatal)"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                channel_id,
                "notify_unmatched_content_not_saved: failed to parse chat.postMessage response \
                 (non-fatal)"
            );
        }
    }
}

// ── C5: verified-email identity matcher (wired into routing, phase two) ────
//
// docs/connectors/CONNECTOR_CONTRACT.md §3, C5: "Can an author be matched to
// a CoralSwarm user? What scope does it need?" — `users:read.email`.
// docs/connectors/slack.md §1's C5 row is the capability declaration this
// section fills in.
//
// **Org plane only.** Founder correction (2026-07-27) to this phase's
// original brief: an org's Slack workspace is provisioned with company
// addresses, and the org's CoralSwarm account is invited with the same ones
// — domains match by construction, so a single `users.email` comparison is
// sufficient. The personal plane never needed identity mapping at all: the
// connecting member's own Slack identity is already proven at OAuth time
// (`oauth_tokens.provider_user_id`), a first-party proof strictly stronger
// than an email inference. A `user_emails` secondary-address table was
// drafted for this phase and deliberately DROPPED — do not resurrect it
// without a fresh design reason; `users.email` (single, `UNIQUE NOT NULL`,
// and — because it is Clerk's primary address — implicitly the verified
// CoralSwarm side of the match) is the whole comparison surface.
//
// **Wired into routing (phase two, 2026-07-27).** [`resolve_org_fanout_
// destinations`] above reads this matcher's persisted directory (never a
// fresh network call — see that function's doc comment for why);
// `routes::slack_interactivity::resolve_clicking_user` and `auto_seat_
// channel_members` both call [`resolve_verified_slack_identity`] directly.
// One matcher, three routing call sites, no second identity path anywhere.

/// Lowercase + trim ONLY. Deliberately NOT provider canonicalisation: Gmail
/// treats a dot or a `+tag` as an alias of the base address, but this
/// matcher must not. `person@example.com` normalises to itself,
/// NOT to `person@example.com` — collapsing the two would let
/// anyone who controls a plus-alias mailbox claim to be a colleague and
/// receive that colleague's private content.
///
/// `pub(crate)` (chat-per-user-setup roster-matching PR, 2026-09-20):
/// `connectors::google_chat`'s domain-directory matcher reuses this exact
/// function rather than hand-rolling a second lowercase-trim, for the same
/// reason `auto_seat_space_members` reuses Slack's seating write path
/// instead of re-implementing it — two independent copies of "how do two
/// email strings compare" is how a Gmail dot/plus-alias quietly starts
/// matching on one connector and not the other.
pub(crate) fn normalize_email(raw: &str) -> String {
    raw.trim().to_lowercase()
}

/// Pure, DB-and-network-free extraction from a raw Slack `users.info`
/// response. Applies every "treat this as unverified/absent" rule that does
/// NOT require a database lookup, so those rules are unit-testable without a
/// live Slack API call (this crate has no HTTP-mock harness):
///
///   - `ok` missing or `false` — a Slack API error (including, in the common
///     case, a token that lacks the `users:read.email` scope: Slack simply
///     omits the email field rather than erroring, so this rule alone
///     doesn't catch that — the missing-email check below does).
///   - `user.deleted == true` — a deactivated Slack account. Defensive: never
///     resolve identity for a member who has left the workspace, even if a
///     stale/cached email is still present on their profile.
///   - `profile.email` absent or empty — no email at all (no scope granted,
///     or Slack genuinely has none on file), or present but blank.
///
/// Returns `(email, canonical_name_hint)` on success — the name hint prefers
/// `display_name` then `real_name` then the email itself, mirroring
/// `routes::slack::resolve_user_name`'s own pick order, so a brand-new
/// `connector_identities` row created by [`match_and_persist_verified_email`]
/// still gets a sensible `canonical_name`.
fn extract_verified_slack_profile(user_info_json: &serde_json::Value) -> Option<(String, String)> {
    if user_info_json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return None;
    }
    let user = user_info_json.get("user")?;
    if user.get("deleted").and_then(serde_json::Value::as_bool) == Some(true) {
        return None;
    }
    let profile = user.get("profile")?;
    let email = profile.get("email").and_then(serde_json::Value::as_str)?.trim();
    if email.is_empty() {
        return None;
    }
    let pick = |k: &str| {
        profile
            .get(k)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
    };
    let name = pick("display_name").or_else(|| pick("real_name")).unwrap_or(email);
    Some((email.to_string(), name.to_string()))
}

/// The database core of the matcher, factored out from the Slack-API-calling
/// wrapper below so the exact-match/no-canonicalisation behavior is
/// unit-testable against a real `users`/`connector_identities` schema without
/// a live Slack call. `raw_slack_email` is whatever
/// [`extract_verified_slack_profile`] returned — already known non-empty and
/// already known not to belong to a deactivated account.
///
/// Matches against `users.email` ONLY (see this section's header for why no
/// secondary-address table exists), using the SAME normalisation
/// ([`normalize_email`]) on both sides — exact match, no provider
/// canonicalisation. `deleted_at IS NULL` excludes soft-deleted CoralSwarm
/// accounts from matching.
///
/// **Two pools (org-slack step 5a — identity matcher plane split).** `users`
/// is a CONTROL-ONLY table — it does not exist on a tenant database at all
/// (`migrations_tenant/0001_tenant_baseline.sql`'s header: "users/accounts
/// live in the control DB"). `connector_identities` is present on BOTH trees
/// (shared reference data, no RLS on either — see migration `0083`'s header
/// and [`resolve_slack_identity_detailed`]'s own doc comment for why it must
/// stay that way). So the match half of this function runs on
/// `control_pool` and the persist half runs on `content_pool` — the same
/// database for a personal/consumer account, a genuine ORG TENANT database
/// for an org account whose Slack workspace has been migrated off the
/// consumer plane. Before this split, the single `pool` this function was
/// handed came from whatever plane its caller happened to be on; on a real
/// org tenant pool the `users` SELECT below failed outright (`relation
/// "users" does not exist`), which `resolve_slack_identity_detailed` mapped
/// to `Unavailable` and `resolve_verified_slack_identity` flattened to
/// `None` — fail-safe, but it meant an org Slack user who was not already in
/// `connector_identities` could never be linked at all.
///
/// On a match, persists the confirmed link back into `connector_identities`
/// (`user_id`, `link_method = 'verified_email'`, `link_verified_at = now()`)
/// — mirroring `workers::slack_name_backfill::resolve_user`'s upsert-on-
/// confirm pattern — so the NEXT lookup for this `(workspace_id,
/// external_user_id)` is a plain DB read via the directory check in
/// [`resolve_verified_slack_identity`], never a repeat Slack round-trip. A
/// fresh row's `canonical_name` is seeded from `canonical_name_hint`; an
/// EXISTING row's `canonical_name` is left untouched (not part of the
/// `DO UPDATE SET` clause) since ordinary ingest may already have written a
/// fresher name there. The persist is best-effort: a failure to write the
/// link is logged and swallowed, not propagated — the caller already has its
/// answer (`Some(user_id)`), and losing the persistence optimization once is
/// far cheaper than failing the whole resolution over a non-essential write.
async fn match_and_persist_verified_email(
    control_pool: &PgPool,
    content_pool: &PgPool,
    workspace_id: &str,
    external_user_id: &str,
    raw_slack_email: &str,
    canonical_name_hint: &str,
) -> Result<Option<Uuid>, String> {
    let normalized = normalize_email(raw_slack_email);
    if normalized.is_empty() {
        return Ok(None);
    }

    // `Ok(None)` means "asked, and no CoralSwarm user has this address" —
    // the ordinary guest/contractor outcome. `Err` means the question could
    // not be asked at all. Collapsing the two (the previous
    // `.ok().flatten()?`) reported a database outage as a confident "this
    // person has no account", which is what every disclosure downstream then
    // repeated to a human. See [`SlackIdentity`]. CONTROL: `users` does not
    // exist on a tenant database at all.
    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM users WHERE lower(trim(email)) = $1 AND deleted_at IS NULL",
    )
    .bind(&normalized)
    .fetch_optional(control_pool)
    .await
    .map_err(|e| format!("users email lookup: {e}"))?
    .flatten();
    let Some(user_id) = user_id else {
        return Ok(None);
    };

    // CONTENT: `connector_identities` lives on the plane this Slack
    // workspace's content actually lands on (see this function's own doc
    // comment for why that table carries no RLS and needs none).
    if let Err(e) = sqlx::query(
        "INSERT INTO connector_identities \
             (provider, workspace_id, external_user_id, canonical_name, user_id, \
              link_method, link_verified_at) \
         VALUES ('slack', $1, $2, $3, $4, 'verified_email', now()) \
         ON CONFLICT (provider, workspace_id, external_user_id) DO UPDATE SET \
             user_id          = EXCLUDED.user_id, \
             link_method      = EXCLUDED.link_method, \
             link_verified_at = EXCLUDED.link_verified_at, \
             updated_at       = now()",
    )
    .bind(workspace_id)
    .bind(external_user_id)
    .bind(canonical_name_hint)
    .bind(user_id)
    .execute(content_pool)
    .await
    {
        tracing::warn!(
            error = %e,
            workspace_id,
            external_user_id,
            "match_and_persist_verified_email: failed to persist confirmed link (non-fatal)"
        );
    }

    Ok(Some(user_id))
}

/// Per-process cache of CONFIRMED matches only, keyed by `(workspace_id,
/// external_user_id)`. Deliberately never caches a negative: an operator can
/// grant `users:read.email` at any time while this process is running (a
/// reinstall/re-consent — see `SlackConnector::authorize_url`'s scope
/// comment), and a cached `None` would wrongly survive that, exactly the
/// staleness [`resolve_org_fanout_destinations`]'s own doc comment warns
/// against for the mirror-image case (a stale non-mapping).
fn verified_identity_cache() -> &'static std::sync::Mutex<std::collections::HashMap<(String, String), Uuid>>
{
    static C: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(String, String), Uuid>>,
    > = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Resolve a Slack `(workspace_id, external_user_id)` pair to the matching
/// CoralSwarm `users.id` via a two-way-verified email match — capability C5.
/// See this section's module header for the org-plane-only scope decision
/// and why there is no secondary-address table.
///
/// Returns `None` on ANY doubt — never a guess:
///   - the Slack API call itself fails (network error, non-2xx, bad JSON)
///   - `ok: false`, a deactivated account, or an absent/empty email (all via
///     [`extract_verified_slack_profile`] — including the common case of a
///     token still missing `users:read.email`, which makes Slack omit the
///     field rather than error)
///   - no CURRENT, non-deleted `users` row has the EXACT normalised address
///     (via [`match_and_persist_verified_email`] — lowercase + trim only, NO
///     provider canonicalisation: a Gmail dot- or plus-alias is a DIFFERENT
///     address and must not match)
///
/// Directory-first: a previously confirmed link is read straight out of
/// `connector_identities` (and the in-process cache above it), so a repeat
/// lookup for the same Slack member costs a DB read, never a fresh Slack API
/// round-trip — this is also why a directory hit never touches the network,
/// which the module's own tests rely on to exercise this function without a
/// live Slack call.
///
/// **Does not check `account_members`.** This function answers "is there ANY
/// CoralSwarm user with this verified email", not "is that user a member of
/// the account routing this workspace" — every routing caller adds that
/// check itself before using this to seat anyone:
/// `routes::slack_interactivity::resolve_clicking_user` and
/// [`resolve_channel_members`] both join `account_members` explicitly;
/// [`resolve_org_fanout_destinations`] gets the same guarantee for free by
/// reading from a directory query that is ITSELF already joined against
/// `account_members`.
///
/// **Two pools (org-slack step 5a).** `control_pool` and `content_pool` are
/// threaded straight through to [`resolve_slack_identity_detailed`] — see
/// that function's own doc comment. Every caller today except the tenant-
/// looped seat sweep passes the SAME pool for both (byte-identical to
/// before this split).
///
/// **`account_id` (PR 1, Slack OAuth scope reconciliation, 2026-09-19).**
/// Threaded straight through to [`resolve_slack_identity_detailed`] so it
/// can look up THIS workspace's own granted `oauth_tokens.scope` — see that
/// function's own doc comment for why `workspace_id` alone cannot address a
/// single scope row (migration `0121_connector_installs`'s header: the same
/// Slack workspace may legitimately be installed by two different
/// accounts). Every call site already had `account_id` in scope before this
/// parameter existed (it is needed for the `account_members` check every
/// routing caller performs around this function), so this is a pure
/// plumbing change, not a new dependency.
pub(crate) async fn resolve_verified_slack_identity(
    control_pool: &PgPool,
    content_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    external_user_id: &str,
    bot_token: &str,
) -> Option<Uuid> {
    match resolve_slack_identity_detailed(
        control_pool,
        content_pool,
        http,
        account_id,
        workspace_id,
        external_user_id,
        bot_token,
    )
    .await
    {
        SlackIdentity::Matched(user_id) => Some(user_id),
        SlackIdentity::Unmatched | SlackIdentity::Bot | SlackIdentity::Unavailable(_) => None,
    }
}

/// Test-only public alias for [`resolve_slack_identity_detailed`], which is
/// `pub(crate)` — `backend/tests/*.rs` link this crate as an ordinary
/// external dependency and cannot see it. Same idiom and same `test-support`
/// feature as [`crate::workers::huddle::test_support_slack_bot_token`]; never
/// present in a normal build.
///
/// **Why a `(String, Option<Uuid>)` rather than the enum.** [`SlackIdentity`]
/// is `pub(crate)` and should stay that way — making it `pub` to satisfy a
/// test would widen the real API surface for a test's convenience, which is
/// the thing this alias idiom exists to avoid. The discriminant string is the
/// whole point of the test: the end-to-end suite's central assertion is that a
/// workspace we cannot ask about reports `"unavailable"` and NOT
/// `"unmatched"`, because the invite-the-unmatched path emails exactly the
/// `Unmatched` and would otherwise email an entire scope-broken workspace.
///
/// Used by `backend/tests/chat_per_user_e2e_pg.rs`, which drives this against
/// a real mock Slack over real HTTP (via `SLACK_API_BASE`) and a real
/// Postgres — so it exercises the actual `users.info` request and response
/// handling, not a hand-fed struct.
#[cfg(feature = "test-support")]
pub async fn test_support_resolve_slack_identity(
    control_pool: &PgPool,
    content_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    external_user_id: &str,
    bot_token: &str,
) -> (String, Option<Uuid>) {
    match resolve_slack_identity_detailed(
        control_pool,
        content_pool,
        http,
        account_id,
        workspace_id,
        external_user_id,
        bot_token,
    )
    .await
    {
        SlackIdentity::Matched(user_id) => ("matched".to_string(), Some(user_id)),
        SlackIdentity::Unmatched => ("unmatched".to_string(), None),
        SlackIdentity::Bot => ("bot".to_string(), None),
        SlackIdentity::Unavailable(_) => ("unavailable".to_string(), None),
    }
}

/// The four genuinely different outcomes of a Slack-member lookup.
///
/// [`resolve_verified_slack_identity`] flattens all three non-match arms to
/// `None`, which is right for a caller that only wants "who is this, if
/// anyone". It is WRONG for any caller that reports a count to a human,
/// because it makes three very different situations indistinguishable:
///
///   - `Unmatched` — we asked, and this person genuinely has no CoralSwarm
///     account. Ordinary and expected; safe to disclose as "unidentified".
///   - `Bot` — not a person at all. Must be excluded from roster totals
///     entirely: the app's own bot user is a member of every channel it was
///     invited to and can never match, so counting it makes every "N of M
///     identified" disclosure permanently short by at least one.
///   - `Unavailable` — the lookup itself failed (rate limit, HTTP, bad JSON).
///     We do not know who this is. Reporting it as `Unmatched` is how a
///     single rate-limited burst silently turns a whole channel into "nobody
///     could be identified" — a confident, wrong disclosure. Callers that
///     disclose counts propagate this as an error instead, honouring
///     docs/connectors/slack.md's C5 rule: *a disclosure with a missing count
///     is better than a wrong one*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SlackIdentity {
    Matched(Uuid),
    Unmatched,
    Bot,
    Unavailable(String),
}

/// D2/D21 (CS-426, `docs/plans/org-onboarding-from-the-invite.md` §3 item
/// 1): which of Slack's three roster shapes an `Unmatched` member belongs
/// to, so the invite pass (`connectors::slack_unmatched_email`) can apply
/// D2's guest rule — "a full workspace member is invited as today; a guest
/// or Slack-Connect person is invited only if their email domain matches
/// the org's anchor domain" (D21).
///
/// Read from the SAME `users.info` response
/// [`resolve_slack_identity_detailed_impl`] already fetched for the
/// email/match lookup — see that function's own doc comment for why a
/// second round trip per member was rejected. Slack's `users.info` user
/// object carries all three fields this classification needs:
/// `is_restricted` (a multi-channel guest), `is_ultra_restricted` (a
/// single-channel guest), and `team_id` (the workspace THIS account
/// belongs to — which differs from the channel's own workspace id for a
/// Slack Connect shared-channel participant visiting from elsewhere).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MembershipClass {
    /// Neither a guest flag is set nor is this account's `team_id` foreign
    /// — an ordinary full member of this workspace. Invited unconditionally
    /// (same as today, pre-D2).
    FullMember,
    /// `is_restricted` (multi-channel) or `is_ultra_restricted`
    /// (single-channel) is `true`, and `team_id` matches this workspace.
    /// Invited only when their email domain matches the account's anchor
    /// domain (D2, D21).
    Guest,
    /// `team_id` does not match this workspace's id — a Slack Connect
    /// participant visiting from another workspace's shared channel.
    /// Invited only when their email domain matches the account's anchor
    /// domain (D2, D21) — the same rule as [`MembershipClass::Guest`], kept
    /// as a separate variant because the two are different facts about the
    /// member (guest STATUS vs. workspace MEMBERSHIP) and a future caller
    /// may want to disclose them separately (e.g. "N guests, M Slack
    /// Connect participants skipped").
    SlackConnect,
    /// One or more of `is_restricted`/`is_ultra_restricted`/`team_id` is
    /// absent from the response (a narrower `users:read` scope grant, or a
    /// Slack response-shape change) — NEVER read as `FullMember`. The whole
    /// point of this classification is a disclosure ("would we invite this
    /// person") that must fail closed on missing data, the identical
    /// discipline [`SlackIdentity::Unavailable`]'s own doc comment states
    /// for the email/match question. A member classified `Unavailable` is
    /// never invited, regardless of `SlackIdentity`.
    Unavailable,
}

impl MembershipClass {
    /// The stable wire label this classification travels under when it has
    /// to leave the process — today, the `cls` claim of a join-notice token
    /// (CS-447, D16 rung 2), which is how the workspace half of the
    /// assertion ladder reaches `routes::org_join_requests::create`.
    ///
    /// [`MembershipClass::Unavailable`] deliberately has NO label. It is
    /// the absence of a classification, and giving it a string would make
    /// "we could not tell" round-trip as though it were a verdict — the
    /// exact fail-open this enum exists to prevent. A caller with
    /// `Unavailable` simply omits the claim, and [`Self::from_label`] reads
    /// a missing claim as `None`.
    pub(crate) fn label(self) -> Option<&'static str> {
        match self {
            Self::FullMember => Some("full_member"),
            Self::Guest => Some("guest"),
            Self::SlackConnect => Some("slack_connect"),
            Self::Unavailable => None,
        }
    }

    /// Parse [`Self::label`] back. `None` for an absent, empty, or
    /// unrecognised label — never a guess, and in particular never
    /// `FullMember`.
    pub(crate) fn from_label(raw: &str) -> Option<Self> {
        match raw {
            "full_member" => Some(Self::FullMember),
            "guest" => Some(Self::Guest),
            "slack_connect" => Some(Self::SlackConnect),
            _ => None,
        }
    }
}

/// Pure, DB-and-network-free classification from a raw Slack `users.info`
/// response — see [`MembershipClass`]'s own doc comment for the three
/// fields and the fail-closed contract. `this_workspace_id` is the
/// CALLER'S workspace id (the channel's own `team_id`), compared against
/// the member's OWN `team_id` from the response to detect a Slack Connect
/// participant.
///
/// Unit-tested directly (no DB, no network) in this module's `tests`
/// submodule, mirroring [`extract_verified_slack_profile`]'s own "pure
/// extraction, DB-backed persistence is a separate function" split.
fn classify_membership(user_info_json: &serde_json::Value, this_workspace_id: &str) -> MembershipClass {
    let Some(user) = user_info_json.get("user") else {
        return MembershipClass::Unavailable;
    };
    let is_restricted = user.get("is_restricted").and_then(serde_json::Value::as_bool);
    let is_ultra_restricted = user.get("is_ultra_restricted").and_then(serde_json::Value::as_bool);
    let team_id = user.get("team_id").and_then(serde_json::Value::as_str);

    let (Some(is_restricted), Some(is_ultra_restricted), Some(team_id)) =
        (is_restricted, is_ultra_restricted, team_id)
    else {
        return MembershipClass::Unavailable;
    };

    if team_id != this_workspace_id {
        return MembershipClass::SlackConnect;
    }
    if is_restricted || is_ultra_restricted {
        return MembershipClass::Guest;
    }
    MembershipClass::FullMember
}

/// The `oauth_tokens.scope` string for the given Slack workspace's
/// account-wide bot-token row — PR 1 (Slack OAuth scope reconciliation,
/// 2026-09-19), built for [`resolve_slack_identity_detailed`]'s "ok: true,
/// no email" branch to tell "this member has no email" apart from "this
/// TOKEN can never see anyone's email".
///
/// **Why `account_id`, not `workspace_id` alone.** Migration
/// `0121_connector_installs`'s header documents, with evidence, that the
/// SAME Slack workspace can legitimately be installed by two different
/// CoralSwarm accounts ("two customers, or a customer plus someone's
/// personal account") — `oauth_tokens`' own unique indexes are keyed
/// `(account_id, provider, provider_workspace_id[, owner_user_id])`, never
/// `provider_workspace_id` alone. A query scoped only by `workspace_id`
/// could silently return (or average over) a DIFFERENT account's token.
/// Every caller of this function already has `account_id` in scope (see
/// [`resolve_verified_slack_identity`]'s own doc comment).
///
/// **Why `owner_user_id IS NULL`.** Slack has no per-member OAuth — a
/// workspace's bot token install is always account-wide (see
/// `connector_installs`' own migration header, §`owner_user_id`, and
/// `routes::slack_events::handle_app_home_opened`'s identical predicate).
/// There is exactly one row to find, never a set to disambiguate further.
///
/// **Returns `None` for "cannot tell", deliberately indistinguishable from
/// "confirmed missing" at the call site.** Three cases collapse to `None`:
///   - no matching row (should not normally happen for a workspace actively
///     making `users.info` calls, but this fails closed rather than
///     panicking);
///   - a DB error (`.ok()` swallows it — a database outage is not evidence
///     the scope is missing, any more than it would be evidence a *user* is
///     missing; see [`match_and_persist_verified_email`]'s identical
///     reasoning for the mirror-image case);
///   - the row's `scope` is EXACTLY
///     [`crate::slack_oauth_backfill::BACKFILLED_SCOPE_PLACEHOLDER`] — the
///     literal string written for a workspace migrated from the legacy
///     `slack_integrations`-only path before it captured a real scope
///     string at all (`slack_oauth_backfill.rs`'s own doc comment: "nothing
///     currently branches on that column" — this function is the FIRST
///     thing that does, so it must not misread the placeholder as either a
///     real grant or a real denial).
///
/// The call site treats `None` the same as a CONFIRMED-missing scope (both
/// mean this token cannot be trusted to answer "no such member"), which is
/// the conservative reading required by [`SlackIdentity`]'s own contract:
/// never guess.
async fn slack_granted_scope(control_pool: &PgPool, account_id: Uuid, workspace_id: &str) -> Option<String> {
    let scope: Option<String> = sqlx::query_scalar(
        "SELECT scope FROM oauth_tokens \
         WHERE provider = 'slack' AND account_id = $1 AND provider_workspace_id = $2 \
           AND owner_user_id IS NULL",
    )
    .bind(account_id)
    .bind(workspace_id)
    .fetch_optional(control_pool)
    .await
    .ok()
    .flatten();

    match scope {
        Some(s) if s == crate::slack_oauth_backfill::BACKFILLED_SCOPE_PLACEHOLDER => None,
        other => other,
    }
}

/// Whether a granted-scope string (Slack's OAuth v2 `scope` response field —
/// space-separated, matching `oauth_tokens.scope`'s own column comment and
/// `oauth::slack::exchange_code`'s test fixtures) includes
/// `users:read.email`. Exact-token match, not a substring check: a
/// substring match would also be correct here (no other real Slack scope
/// contains `users:read.email` as a substring) but exact-token comparison
/// is the same discipline this file already applies to email matching
/// (`normalize_email`'s own doc comment: no cleverness beyond what the
/// input format actually guarantees).
pub(crate) fn scope_grants_users_read_email(scope: &str) -> bool {
    scope.split_whitespace().any(|s| s == "users:read.email")
}

/// Which of [`slack_scopes`]'s currently-requested scopes are absent from a
/// stored `oauth_tokens.scope` grant — CS-431 (org-onboarding-from-the-
/// invite.md §3 item 34), the connectors-page re-consent prompt. Delegates
/// the actual set-difference to [`super::missing_scopes`]; this function's
/// own job is Slack's "unknown" pre-check, on top of that shared blank-grant
/// case: [`crate::slack_oauth_backfill::BACKFILLED_SCOPE_PLACEHOLDER`] is a
/// stored value, not a blank one, so the shared helper alone would read it
/// as "granted none of the scopes" and list every scope as missing — a
/// guess, not a fact, for a workspace this codebase never actually asked
/// Slack about (see that constant's own doc comment, and
/// [`slack_granted_scope`]'s identical reasoning for the read side this
/// mirrors).
///
/// Returns `None` — never a guessed `Vec` — for the placeholder or a blank
/// grant; `Some(vec![])` means every requested scope is already granted.
pub(crate) fn slack_missing_scopes(stored_scope: &str) -> Option<Vec<String>> {
    if stored_scope == crate::slack_oauth_backfill::BACKFILLED_SCOPE_PLACEHOLDER {
        return None;
    }
    let required = slack_scopes();
    super::missing_scopes(stored_scope, &required.split_whitespace().collect::<Vec<_>>())
}

/// [`resolve_verified_slack_identity`] with the outcome kept intact — see
/// [`SlackIdentity`] for why the difference between "no such user" and "the
/// lookup failed" has to survive.
///
/// **Two pools (org-slack step 5a — identity matcher plane split).**
/// `content_pool` resolves `connector_identities` — present on BOTH trees
/// (shared reference data, no RLS — migration `0083`'s header), so this is
/// the plane this Slack workspace's content actually lands on: the control/
/// consumer pool for a personal account, a genuine org TENANT pool for an
/// org account. `control_pool` is threaded through to
/// [`match_and_persist_verified_email`] for its `users` lookup — CONTROL-
/// ONLY, absent from every tenant database — which is the only place this
/// function's own network branch touches it. On the consumer plane every
/// caller passes the SAME pool for both (byte-identical to before this
/// split); [`resolve_channel_members`]'s tenant-looped seat-reconcile caller
/// is the one that actually needs them to differ.
///
/// **`account_id` (PR 1, Slack OAuth scope reconciliation, 2026-09-19).**
/// Used ONLY by the "ok: true, no email" branch below, to resolve THIS
/// workspace's own granted scope via [`slack_granted_scope`] — see that
/// function's own doc comment for why `account_id` (not `workspace_id`
/// alone) is required to address a single `oauth_tokens` row. Read from
/// `control_pool`, matching every other `oauth_tokens`/`users` read this
/// function already performs.
pub(crate) async fn resolve_slack_identity_detailed(
    control_pool: &PgPool,
    content_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    external_user_id: &str,
    bot_token: &str,
) -> SlackIdentity {
    resolve_slack_identity_detailed_impl(
        control_pool,
        content_pool,
        http,
        account_id,
        workspace_id,
        external_user_id,
        bot_token,
    )
    .await
    .0
}

/// PR 6 sibling of [`resolve_slack_identity_detailed`] (email-the-unmatched,
/// docs/plans/chat-per-user-ingest-and-in-chat-ask.md §11 correction 6) for
/// the one caller that needs the verified email address ITSELF, not just the
/// classification: `connectors::slack_unmatched_email`'s recipient-discovery
/// walk has to know WHO to email, and `extract_verified_slack_profile`
/// already pulled that address out of the exact same `users.info` response
/// this function's shared implementation calls — re-fetching it in a second
/// network round trip per member would be wasteful AND would risk drifting
/// from this function's own classification if the two ever raced against a
/// changed profile.
///
/// Deliberately the SAME shared implementation as
/// [`resolve_slack_identity_detailed`]
/// ([`resolve_slack_identity_detailed_impl`]) rather than a hand-rolled
/// second copy of the classification: PR 1's scope-broken-workspace fix (an
/// `Unmatched` verdict is reachable ONLY once `users:read.email` is
/// CONFIRMED granted on this workspace's bot token — see the impl's own doc
/// comment) is exactly the property this caller depends on, and a
/// hand-duplicated copy is how that property quietly drifts between the two
/// call sites over time.
///
/// The email is `Some` only on the branch that actually extracted one from
/// Slack's response — `Bot`/`Unavailable`/every early-return arm always
/// carries `None`. A `Matched` outcome also carries `Some(email)` (the
/// value was in hand either way); this caller has no use for it in that
/// case; a matched person already has a CoralSwarm account and is never a
/// nudge-email recipient (see [`SlackIdentity::Unmatched`]'s own contract).
///
/// **Third element (CS-426):** [`MembershipClass`] — see
/// [`resolve_slack_identity_detailed_impl`]'s own doc comment for exactly
/// when it is meaningful (only alongside `Unmatched` with `Some(email)`;
/// filler `Unavailable` everywhere else). `connectors::slack_unmatched_
/// email::discover_unmatched_recipients` is the one caller that reads it,
/// to apply D2's guest/Slack-Connect domain rule.
pub(crate) async fn resolve_slack_identity_with_email(
    control_pool: &PgPool,
    content_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    external_user_id: &str,
    bot_token: &str,
) -> (SlackIdentity, Option<String>, MembershipClass) {
    resolve_slack_identity_detailed_impl(
        control_pool,
        content_pool,
        http,
        account_id,
        workspace_id,
        external_user_id,
        bot_token,
    )
    .await
}

/// D21's anchor domain seam (item 37 of `docs/plans/org-onboarding-from-
/// the-invite.md` §3) — the org's VERIFIED domain, wired to `org_domains`
/// (migration 0242) by CS-443. [`resolve_anchor_domain`], its only caller,
/// needed no change at all when this stopped returning `None`.
///
/// **Only a VERIFIED row may be returned.**
/// [`crate::services::org_domains::verified_domains`] filters
/// `verified_at IS NOT NULL` and is the only read this function makes. A
/// claimed-but-unverified domain answering `Some(...)` here would silently
/// widen D2's guest rule: every guest and Slack-Connect participant on a
/// domain an admin merely TYPED into a settings box would start being
/// invited, on no evidence at all. That is the bug, and
/// `an_unverified_claim_does_not_satisfy_verified_org_domain` (in this
/// module's own `domain_seam_pg_tests`) is the test written against it.
///
/// **Either proving method, deliberately — do NOT narrow this to
/// `dns_txt`.** Founder ruling, 2026-09-22: an emailed confirmation proves
/// the org controls a mailbox at the domain, which is exactly what D2's
/// guest rule needs to decide whether a guest or Slack-Connect participant
/// is one of this org's own people. It is **auto-add** that asks the harder
/// question and accepts DNS only —
/// `services::assertion_ladder::domain_signal` applies that filter, on a
/// different read ([`crate::services::org_domains::
/// verified_domains_with_method`]), precisely so narrowing one gate can
/// never silently narrow the other. See
/// `services::org_domains`' "belt and braces" doc section for the full
/// ruling, and
/// `an_email_verified_domain_anchors_the_guest_rule_but_not_rung_two` (in
/// this module's own `domain_seam_pg_tests`) for the test that pins both
/// sides at once.
///
/// **Every verified domain, when an org has several.** D16's own text names
/// the post-acquisition org legitimately running on three domains. Item 30
/// widened D2's guest rule from the earliest verified domain to that full
/// set: a guest on any of them is admitted. The assertion ladder
/// (`services::assertion_ladder`, item 6) already reads the full list
/// through [`crate::services::org_domains::verified_domains_with_method`].
/// An empty `Ok` is "this org has verified nothing" — the bootstrap case.
///
/// **`Err` is not `Ok(empty)` — CodeRabbit round 1, #1031, and the very trap
/// this PR's own ladder is built around, committed here by its own author.**
/// An earlier cut of this function swallowed a database error into `None`.
/// That reads as "this org has verified no domain", which sends
/// [`resolve_anchor_domain`] on to D21's *bootstrap* fallback — the
/// connecting member's own email domain — and a guest whose address matches
/// THAT domain is then invited. A transient database failure would have
/// silently swapped a strong anchor for a weaker one and widened D2's guest
/// rule while doing it. **Could not ask is not "nothing is verified."**
/// `Err(reason)` now propagates and the caller resolves NO anchor at all,
/// which is the fail-closed reading (an empty anchor list matches nobody).
async fn verified_org_domain(control_pool: &PgPool, account_id: Uuid) -> Result<Vec<String>, String> {
    crate::services::org_domains::verified_domains(control_pool, account_id)
        .await
        .map_err(|e| e.to_string())
}

/// What [`verified_org_domain`]'s three outcomes mean for
/// [`resolve_anchor_domain`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AnchorDecision {
    /// The org has one or more verified domains; they ARE the anchors.
    /// The connecting member's domain is not consulted.
    Verified(Vec<String>),
    /// The org has genuinely verified none — fall through to D21's
    /// bootstrap, the connecting member's own email domain.
    Bootstrap,
    /// We could not ask. Resolve NO anchor at all.
    NoAnchor(String),
}

/// The decision behind [`resolve_anchor_domain`]'s first branch, factored out
/// so it is unit-testable without a database — the same "pure decision, DB
/// lookup is a separate function" split [`classify_membership`] and
/// [`extract_verified_slack_profile`] already use, and for the same reason:
/// the thing that must be right is the DECISION, and injecting a real
/// database failure to prove it would be far more machinery than the
/// property deserves.
///
/// **`Err` must never become [`AnchorDecision::Bootstrap`]** (CodeRabbit
/// round 1, #1031). Degrading a failed lookup to the connecting member's
/// domain would silently re-anchor D2's guest rule for an org that HAS a
/// verified domain, on nothing but a transient database error — the exact
/// "could not ask is not an answer" trap this PR's own ladder is built
/// around. See [`verified_org_domain`]'s own doc comment.
pub(crate) fn anchor_decision(lookup: Result<Vec<String>, String>) -> AnchorDecision {
    match lookup {
        Ok(domains) if !domains.is_empty() => AnchorDecision::Verified(domains),
        Ok(_) => AnchorDecision::Bootstrap,
        Err(reason) => AnchorDecision::NoAnchor(reason),
    }
}

/// D2's guest/Slack-Connect anchor domain for one Slack workspace connection
/// (CS-426, `docs/plans/org-onboarding-from-the-invite.md` §3 item 1, D21):
/// the org's VERIFIED domain when one exists ([`verified_org_domain`] —
/// item 37, BUILT by CS-443: the `org_domains` table, migration 0242),
/// else the domain of the CoralSwarm user who
/// authorised THIS workspace's account-wide Slack connection, normalised
/// (trim, lowercase — [`normalize_email`]'s own rule).
///
/// **Where "the authorising user" comes from.** Slack has no per-member
/// OAuth (`owner_user_id IS NULL` for every row today — see
/// [`slack_granted_scope`]'s own doc comment); the person who completed the
/// OAuth consent is instead recorded as `oauth_tokens.provider_user_id`,
/// written from Slack's own `authed_user.id` by BOTH writers of this row
/// (`routes::slack::oauth_callback`'s legacy path and
/// [`SlackConnector::exchange_code`]'s generic path) — see this section's
/// own header comment above [`match_and_persist_verified_email`]: "the
/// connecting member's own Slack identity is already proven at OAuth time
/// (`oauth_tokens.provider_user_id`), a first-party proof strictly stronger
/// than an email inference." Resolving that id's email costs one
/// `users.info` call through the SAME [`extract_verified_slack_profile`]
/// path every other lookup in this module uses — paid ONCE per channel
/// walk (the caller computes this before its per-member loop, not inside
/// it), not once per roster member, so it does not reintroduce the
/// per-member round trip [`classify_membership`]'s own doc comment avoids.
///
/// An empty list when the workspace has no account-wide `oauth_tokens` row,
/// the row's `provider_user_id` is empty (a connection that predates this
/// column being populated), the `users.info` call itself fails, or the
/// verified-domain lookup failed — the caller's domain comparison then
/// never matches (an empty list contains nothing), which is the fail-closed
/// reading D2 requires: an unknown anchor must never read as "everyone
/// matches." When verified domains exist, every one of them is returned and
/// the connecting member's domain is not looked up at all.
pub(crate) async fn resolve_anchor_domain(
    control_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
) -> Vec<String> {
    match anchor_decision(verified_org_domain(control_pool, account_id).await) {
        AnchorDecision::Verified(domains) => {
            crate::services::org_domains::guest_anchor_domains(Ok(domains), None)
        }
        AnchorDecision::NoAnchor(reason) => {
            tracing::warn!(
                reason = %reason, account_id = %account_id, workspace_id = %workspace_id,
                "slack: verified org-domain lookup failed — resolving NO anchor domain for this \
                 pass rather than falling back to the connecting member's domain (fail closed)"
            );
            Vec::new()
        }
        AnchorDecision::Bootstrap => {
            let bootstrap =
                slack_connecting_member_domain(control_pool, http, account_id, workspace_id, bot_token)
                    .await;
            crate::services::org_domains::guest_anchor_domains(Ok(Vec::new()), bootstrap)
        }
    }
}

/// The connecting member's email domain, used only as D21's bootstrap while
/// the org has verified no domain. `None` when that domain cannot be read.
async fn slack_connecting_member_domain(
    control_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
) -> Option<String> {
    let provider_user_id: Option<String> = sqlx::query_scalar(
        "SELECT provider_user_id FROM oauth_tokens \
         WHERE provider = 'slack' AND account_id = $1 AND provider_workspace_id = $2 \
           AND owner_user_id IS NULL",
    )
    .bind(account_id)
    .bind(workspace_id)
    .fetch_optional(control_pool)
    .await
    .ok()
    .flatten();

    let external_user_id = provider_user_id.filter(|s| !s.is_empty())?;

    let resp = http
        .get(format!("{}/users.info", slack_api_base()))
        .bearer_auth(bot_token)
        .query(&[("user", external_user_id.as_str())])
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let json: serde_json::Value = resp.json().await.ok()?;
    let (email, _name_hint) = extract_verified_slack_profile(&json)?;
    let normalized = normalize_email(&email);
    normalized.rsplit('@').next().map(str::to_string).filter(|d| !d.is_empty())
}

/// The shared classification body behind [`resolve_slack_identity_detailed`]
/// and [`resolve_slack_identity_with_email`] — see the former's own doc
/// comment (immediately above it) for the full per-branch reasoning (the
/// two-pool split, the cache, the directory hit, the network lookup, the
/// `ok: true`-no-email scope disambiguation). Not called by anything outside
/// this pair; every early return here pairs `None` with the email slot
/// except the one branch that extracted a real address from `users.info`.
///
/// **Third element (CS-426):** [`MembershipClass`] — computed ONLY on the
/// branch that reaches [`classify_membership`] (immediately after a
/// successful [`extract_verified_slack_profile`], the one place this
/// function still has the raw `users.info` JSON in scope); every other
/// branch — cache hit, directory hit, a Slack API error, `ok: true` with no
/// usable email — returns [`MembershipClass::Unavailable`] as filler,
/// because none of them ever reaches an `Unmatched` verdict WITH an email
/// attached (see [`SlackIdentity::Unmatched`]'s own contract for why that
/// pairing is the only one [`connectors::slack_unmatched_email`] acts on),
/// so the filler value is never read by that caller.
async fn resolve_slack_identity_detailed_impl(
    control_pool: &PgPool,
    content_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    external_user_id: &str,
    bot_token: &str,
) -> (SlackIdentity, Option<String>, MembershipClass) {
    if workspace_id.is_empty() || external_user_id.is_empty() {
        return (SlackIdentity::Unmatched, None, MembershipClass::Unavailable);
    }

    // Slackbot is a fixed, workspace-independent id and is a member of some
    // channels without ever appearing in `users.info` as a normal account.
    if external_user_id == "USLACKBOT" {
        return (SlackIdentity::Bot, None, MembershipClass::Unavailable);
    }

    let cache_key = (workspace_id.to_string(), external_user_id.to_string());
    if let Some(cached) = verified_identity_cache().lock().unwrap().get(&cache_key).copied() {
        return (SlackIdentity::Matched(cached), None, MembershipClass::Unavailable);
    }

    let directory_user_id: Option<Uuid> = match sqlx::query_scalar(
        "SELECT user_id FROM connector_identities \
         WHERE provider = 'slack' AND workspace_id = $1 AND external_user_id = $2 \
           AND user_id IS NOT NULL",
    )
    .bind(workspace_id)
    .bind(external_user_id)
    .fetch_optional(content_pool)
    .await
    {
        Ok(row) => row,
        // A DB failure is not evidence that this person has no account.
        Err(e) => {
            return (
                SlackIdentity::Unavailable(format!("connector_identities lookup: {e}")),
                None,
                MembershipClass::Unavailable,
            )
        }
    };

    if let Some(user_id) = directory_user_id {
        verified_identity_cache().lock().unwrap().insert(cache_key, user_id);
        return (SlackIdentity::Matched(user_id), None, MembershipClass::Unavailable);
    }

    // No confirmed link on file yet — this is the only branch that ever
    // touches the network.
    let resp = match http
        .get(format!("{}/users.info", slack_api_base()))
        .bearer_auth(bot_token)
        .query(&[("user", external_user_id)])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return (
                SlackIdentity::Unavailable(format!("users.info HTTP error: {e}")),
                None,
                MembershipClass::Unavailable,
            )
        }
    };

    // 429 in particular: Slack rate-limits `users.info` per workspace, and a
    // first-time roster walk of a large channel is exactly the burst that
    // trips it. Previously this fell through `.ok()?` into `None` and every
    // remaining member of the channel was reported as unidentified.
    let status = resp.status();
    if !status.is_success() {
        return (
            SlackIdentity::Unavailable(format!("users.info returned HTTP {status}")),
            None,
            MembershipClass::Unavailable,
        );
    }

    let json: serde_json::Value = match resp.json().await {
        Ok(j) => j,
        Err(e) => {
            return (
                SlackIdentity::Unavailable(format!("users.info JSON parse error: {e}")),
                None,
                MembershipClass::Unavailable,
            )
        }
    };

    // Checked before the profile extract: a bot has no email, so it would
    // otherwise fall out as `Unmatched` and inflate every unidentified count.
    if json
        .get("user")
        .and_then(|u| u.get("is_bot"))
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return (SlackIdentity::Bot, None, MembershipClass::Unavailable);
    }

    // Slack's own transport-level failures arrive as `ok: false` with HTTP
    // 200, so they have to be separated here rather than by status alone.
    // `ratelimited` is the one that matters most; the rest are treated as
    // "we could not ask", which is the conservative reading.
    if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let err = json.get("error").and_then(serde_json::Value::as_str).unwrap_or("unknown");
        // CORRECTED (PR 1, Slack OAuth scope reconciliation, 2026-09-19): a
        // PRIOR version of this comment claimed `missing_scope` was also "a
        // real answer about this member", matching `user_not_found`/
        // `users_not_found` into the SAME `Unmatched` arm — that was wrong,
        // and the code below it was always right despite the comment: only
        // `user_not_found`/`users_not_found` genuinely mean "no such Slack
        // member" (a fact about THIS member). `missing_scope` means Slack
        // refused the call outright because our bot token lacks a scope it
        // needs — it says nothing about whether this particular member
        // exists, and Slack would return the IDENTICAL error for every other
        // member of the workspace too. It falls through to the `other` arm
        // below as `Unavailable`, which is correct and unchanged; only the
        // comment was fixed to match what the code has always done. (See
        // the sibling bug this same PR fixes just below, in the `ok: true`
        // branch, for the case where a missing scope does NOT trip an
        // outright `ok: false` — Slack instead silently omits the field.)
        return match err {
            "user_not_found" | "users_not_found" => {
                (SlackIdentity::Unmatched, None, MembershipClass::Unavailable)
            }
            other => (
                SlackIdentity::Unavailable(format!("users.info Slack error: {other}")),
                None,
                MembershipClass::Unavailable,
            ),
        };
    }

    let Some((email, name_hint)) = extract_verified_slack_profile(&json) else {
        // `ok: true` with no usable email means one of two very different
        // things, and conflating them (as this branch used to, returning a
        // bare `Unmatched` either way) is the silent-degradation bug PR 1
        // fixes:
        //
        //   (a) THIS member's account is deactivated, or genuinely has no
        //       email Slack will show us — a fact about this one person.
        //       `Unmatched` is correct.
        //   (b) the bot token lacks `users:read.email` — Slack omits the
        //       field for EVERY member when this happens, rather than
        //       erroring the call the way `missing_scope` above does. This
        //       says NOTHING about this member; it says the workspace's
        //       token can never answer this question for anyone.
        //       `Unmatched` here is actively dangerous: PR 6 (email-the-
        //       unmatched, docs/plans/chat-per-user-ingest-and-in-chat-ask.md
        //       step 5) emails exactly the members this function reports as
        //       `Unmatched` — a scope-broken workspace read as "everyone
        //       here has no account" would email the entire workspace.
        //
        // Disambiguate using the scope actually GRANTED to this workspace's
        // bot token (`oauth_tokens.scope`, written from Slack's own OAuth
        // response — not a guess). A workspace whose granted scope is
        // unknown (no row, a DB error, or the legacy-backfill placeholder —
        // see `slack_granted_scope`'s own doc comment) is treated the SAME
        // as a CONFIRMED-missing scope: both mean this token cannot be
        // trusted to tell us "no such member", so `Unmatched` is only ever
        // reached when `users:read.email` is CONFIRMED present and Slack
        // still had nothing to show for this specific person.
        return match slack_granted_scope(control_pool, account_id, workspace_id).await {
            Some(scope) if scope_grants_users_read_email(&scope) => {
                (SlackIdentity::Unmatched, None, MembershipClass::Unavailable)
            }
            Some(_) => {
                tracing::warn!(
                    connector_health = true,
                    workspace_id,
                    external_user_id,
                    "users.info returned ok=true with no email, and this workspace's bot token \
                     is CONFIRMED missing users:read.email — every member of this workspace will \
                     read as unavailable (never unmatched) until an admin reinstalls the Slack \
                     app with the current scope set"
                );
                (
                    SlackIdentity::Unavailable(
                        "workspace bot token lacks users:read.email — cannot verify any member's \
                         email"
                            .to_string(),
                    ),
                    None,
                    MembershipClass::Unavailable,
                )
            }
            None => {
                tracing::warn!(
                    connector_health = true,
                    workspace_id,
                    external_user_id,
                    "users.info returned ok=true with no email, and this workspace's granted \
                     Slack scope could not be confirmed (no oauth_tokens row, a DB error, or a \
                     legacy-backfill placeholder scope) — treating as unavailable rather than \
                     risking a false unmatched"
                );
                (
                    SlackIdentity::Unavailable(
                        "could not confirm this workspace's granted Slack scopes".to_string(),
                    ),
                    None,
                    MembershipClass::Unavailable,
                )
            }
        };
    };

    // CS-426: computed here, once, from the SAME `json` the email extract
    // above just read — see `MembershipClass`'s own doc comment for the
    // three fields and the fail-closed contract. Threaded through every
    // branch below (including `Matched`, which never reads it — see this
    // function's own doc comment on the third element) so there is exactly
    // one call site for this classification, not one per branch.
    let membership = classify_membership(&json, workspace_id);

    match match_and_persist_verified_email(
        control_pool,
        content_pool,
        workspace_id,
        external_user_id,
        &email,
        &name_hint,
    )
    .await
    {
        Ok(Some(user_id)) => {
            verified_identity_cache().lock().unwrap().insert(cache_key, user_id);
            (SlackIdentity::Matched(user_id), Some(email), membership)
        }
        Ok(None) => (SlackIdentity::Unmatched, Some(email), membership),
        Err(e) => (SlackIdentity::Unavailable(e), None, MembershipClass::Unavailable),
    }
}

/// The single source of truth for the Slack bot-token scope list (PR 1,
/// Slack OAuth scope reconciliation, 2026-09-19).
///
/// Before this PR there were TWO independent Slack authorize-URL builders in
/// this codebase, each with its own hardcoded scope string: this one (used
/// by [`SlackConnector::authorize_url`], the modern
/// `GET /v1/oauth/slack/authorize` flow) and a second, under-scoped,
/// hand-maintained copy inside `routes::slack::oauth_start` (the legacy
/// `GET /v1/integrations/slack/oauth/start` flow —
/// `"channels:history,channels:read,groups:history,im:history,mpim:history"`,
/// comma-separated, no `users:read`/`users:read.email`). A workspace
/// installed through THAT URL could never pass
/// [`resolve_verified_slack_identity`]'s C5 match — its bot token would
/// never carry `users:read.email` — so it could never fan out into any
/// linked member's private ocean, which is the entire point of the
/// container-fan-out structure the connector exists to serve. `oauth_start`
/// now delegates to [`SlackConnector::authorize_url`] (see that route's own
/// doc comment) instead of maintaining a second copy, so there is exactly
/// one scope list left in the codebase to ever update.
///
/// `SLACK_SCOPES` env override is preserved byte-identical to the pre-PR
/// default (below) — an operator who has already set this env var keeps
/// getting exactly what they configured.
///
/// Per-scope rationale for why each entry in the default is there, and why
/// none of them can be silently dropped or added without an operational
/// consequence:
///
/// `users:read.email` (added for C5 — see [`resolve_verified_slack_identity`]
/// below) requires a manual app reinstall/re-consent in Slack for any
/// WORKSPACE that installed before this scope was added; Slack does not
/// retroactively grant a newly-added scope to an existing installation. The
/// operator must reinstall (or re-run OAuth) per workspace — this code
/// change cannot do that for them.
///
/// `im:write` (added for the slack-onboarding welcome DM —
/// docs/connectors/slack-onboarding.md §5.2, `post_store`'s
/// `send_welcome_dm` call) needs `conversations.open` to find/create the
/// installing user's DM channel. Same unavoidable limitation as
/// `users:read.email` above: Slack does not retro-grant a newly added scope,
/// so every workspace installed before this line shipped has a bot token
/// that will permanently lack it until a human reinstalls. Unlike
/// `users:read.email` (whose absence only degrades a background matcher), a
/// missing `im:write` is on the OAuth completion path itself, so
/// `send_welcome_dm` MUST fail soft — see its own doc comment for the
/// `missing_scope` handling this requires.
///
/// `channels:leave` (added for the "stop watching a channel" action —
/// `leave_conversation`/`stop_watching_channel`) needs
/// `conversations.leave`. Same unavoidable Slack limitation as the two
/// scopes above: Slack does NOT retroactively grant a newly added scope to
/// an already-installed workspace, so every workspace installed before this
/// line shipped has a bot token that will permanently return
/// `missing_scope` from `conversations.leave` until a human
/// reinstalls/re-consents. Unlike `users:read.email` (a background matcher
/// degrading quietly) this scope backs a user-INITIATED action, so
/// `leave_conversation` surfaces `LeaveError::MissingScope` as an actionable
/// error naming the reinstall, rather than failing soft — see that
/// function's own doc comment.
///
/// **Deliberately NOT widened to add `im:history`/`mpim:history`/
/// `groups:history`** (the DM/MPIM/private-channel history scopes the
/// legacy list carried): production code never branches on Slack
/// `channel_type` today, so DM/MPIM are not a live ingest surface, and
/// adding scopes forces every already-installed workspace to reinstall for
/// zero current gain. If a DM/MPIM ingest surface is ever built, add the
/// scope here — this function is the only place that needs to change.
pub(crate) fn slack_scopes() -> String {
    std::env::var("SLACK_SCOPES").unwrap_or_else(|_| {
        "channels:history channels:read users:read users:read.email reactions:read \
         files:read canvases:read chat:write im:write channels:leave"
            .into()
    })
}

// ---------------------------------------------------------------------------
// Member (user-scope-only) connect — CS-454, item 46a
// ---------------------------------------------------------------------------

/// `user_scope` requested for a MEMBER Slack connect (D21,
/// `docs/plans/org-onboarding-from-the-invite.md` §3 item 46) — an OAuth
/// request that asks Slack for ONLY these, no bot `scope` at all (see
/// [`authorize_url_for_member`]). This is a *different* scope set from
/// [`slack_scopes`]'s bot scopes, requested on the member's OWN `xoxp-…`
/// token, and stored under the distinct `oauth_tokens.provider` literal
/// `"slack_user"` (`routes::oauth::resolve_slack_token_target`) — never
/// merged with, or substituted for, the bot scopes above.
///
/// Per-scope rationale:
///
/// - `channels:history` / `channels:read` — the same public-channel pair
///   [`slack_scopes`] already carries for the bot, because a member's own
///   token lists and backfills public channels visible to that member.
/// - `groups:history` / `groups:read` — private channels. [`slack_scopes`]'s
///   own doc comment deliberately excludes these for the BOT ("production
///   code never branches on Slack `channel_type` today ... If a DM/MPIM
///   ingest surface is ever built, add the scope here") — that surface is
///   exactly what per-member capture is, and a private channel is only ever
///   visible through a MEMBER's own token, never the bot's (the bot is not
///   a member of a private channel it wasn't invited to). Requesting this
///   now, at first consent, avoids forcing every connected member through a
///   second re-consent the day per-member capture ships (Slack does not
///   retroactively grant a newly-added scope — see [`slack_scopes`]'s own
///   `users:read.email`/`im:write`/`channels:leave` entries for the exact
///   unavoidable cost of adding a scope AFTER workspaces have already
///   installed).
/// - `im:history` / `im:read` — the member's own direct messages. Same
///   "only visible through their own token, request it once" reasoning.
/// - `mpim:history` / `mpim:read` — the member's own group DMs. Same
///   reasoning again.
/// - `users:read` / `users:read.email` — identity: resolving Slack user ids
///   to CoralSwarm accounts (the same pair [`slack_scopes`] carries for the
///   bot, for the same reason — C5's `resolve_verified_slack_identity`).
///
/// Connect indexes public channels and backfills from the member's own
/// token (`slack_member_capture::refresh_and_backfill_on_connect`), without
/// requiring a shared bot grant. Private-channel and DM scopes are reserved
/// for future capture support. Overridable by `SLACK_MEMBER_USER_SCOPES`
/// for the same reason [`slack_scopes`] is overridable by `SLACK_SCOPES` —
/// an operator who has already shipped a workspace against a narrower set
/// can pin it without a code change.
pub(crate) fn slack_member_user_scopes() -> String {
    std::env::var("SLACK_MEMBER_USER_SCOPES").unwrap_or_else(|_| {
        "channels:history channels:read groups:history groups:read im:history im:read \
         mpim:history mpim:read users:read users:read.email"
            .into()
    })
}

/// Build the member-connect Slack authorize URL — `user_scope` ONLY, no
/// `scope` (bot) parameter at all. This is what lifts the admin gate: a
/// bare `scope=`-less authorize request cannot, by construction, request
/// any BOT permission, so [`routes::oauth::callback`]'s admin-only check
/// for an org Slack connect is skipped specifically for this flow (see that
/// function's own comment) — there is no shared workspace asset being
/// authorized, only the caller's own user token.
///
/// Slack's `oauth/v2/authorize` accepts `user_scope` with no `scope` at
/// all — this is the documented shape for a user-token-only app (e.g.
/// "Sign in with Slack"), not a malformed request.
pub(crate) fn authorize_url_for_member(base_url: &str, state: &str) -> Result<String, ConnectorError> {
    let client_id = std::env::var("SLACK_CLIENT_ID")
        .map_err(|_| ConnectorError::NotConfigured("SLACK_CLIENT_ID not configured".into()))?;

    let redirect_uri = format!("{base_url}/v1/oauth/slack/callback");
    let user_scopes = slack_member_user_scopes();

    Ok(format!(
        "https://slack.com/oauth/v2/authorize?client_id={}&user_scope={}&redirect_uri={}&state={}",
        urlencoding::encode(&client_id),
        urlencoding::encode(&user_scopes),
        urlencoding::encode(&redirect_uri),
        urlencoding::encode(state),
    ))
}

/// Build the install-and-sign-in Slack authorize URL (connectors redesign,
/// decision 4): the bot `scope` the app always asks for AND the member's own
/// `user_scope`, in one consent. Whoever connects first installs the app for the
/// workspace and gets their own user token in the same exchange. If the
/// workspace requires admin approval for apps, Slack asks for it on its side.
pub(crate) fn authorize_url_for_install(base_url: &str, state: &str) -> Result<String, ConnectorError> {
    let client_id = std::env::var("SLACK_CLIENT_ID")
        .map_err(|_| ConnectorError::NotConfigured("SLACK_CLIENT_ID not configured".into()))?;

    let redirect_uri = format!("{base_url}/v1/oauth/slack/callback");

    Ok(format!(
        "https://slack.com/oauth/v2/authorize?client_id={}&scope={}&user_scope={}&redirect_uri={}&state={}",
        urlencoding::encode(&client_id),
        urlencoding::encode(&slack_scopes()),
        urlencoding::encode(&slack_member_user_scopes()),
        urlencoding::encode(&redirect_uri),
        urlencoding::encode(state),
    ))
}

/// Exchange an install-and-sign-in authorization code: the workspace's bot token
/// and the member's own user token from the one `oauth.v2.access` call, as two
/// bundles ready for `store_token_bundle_tx` (`slack` account-wide, `slack_user`
/// for the member). The user token is deliberately NOT left on the bot bundle:
/// the bot row is the workspace's, and a member's `xoxp-` token must not sit on a
/// row every member of the account can read.
///
/// A response without a user token is an error (the member declined the user
/// scopes), not a half-install.
pub(crate) async fn exchange_code_install(
    http: &reqwest::Client,
    base_url: &str,
    code: &str,
) -> Result<(TokenBundle, TokenBundle), ConnectorError> {
    let client_id = std::env::var("SLACK_CLIENT_ID")
        .map_err(|_| ConnectorError::NotConfigured("SLACK_CLIENT_ID not configured".into()))?;
    let client_secret = std::env::var("SLACK_CLIENT_SECRET")
        .map_err(|_| ConnectorError::NotConfigured("SLACK_CLIENT_SECRET not configured".into()))?;
    let redirect_uri = format!("{base_url}/v1/oauth/slack/callback");

    let r = slack_api::exchange_code(http, code, &client_id, &client_secret, &redirect_uri)
        .await
        .map_err(|e| ConnectorError::Exchange(e.to_string()))?;
    let (Some(user_token), Some(user_scope)) = (r.user_token, r.user_scope) else {
        return Err(ConnectorError::Exchange(
            "slack returned no user token for the install".into(),
        ));
    };

    let bot = TokenBundle {
        access_token: r.access_token,
        refresh_token: r.refresh_token,
        user_token: None,
        scope: r.scope,
        // Slack bot and user tokens carry no expiry in this flow.
        expires_at: None,
        provider_workspace_id: r.team_id.clone(),
        provider_workspace_name: Some(r.team_name.clone()),
        provider_user_id: r.authed_user_id.clone(),
    };
    let member = TokenBundle {
        access_token: user_token,
        refresh_token: None,
        user_token: None,
        scope: user_scope,
        expires_at: None,
        provider_workspace_id: r.team_id,
        provider_workspace_name: Some(r.team_name),
        provider_user_id: r.authed_user_id,
    };
    Ok((bot, member))
}

/// Exchange a member-connect authorization code — the [`slack_api::
/// exchange_code_member`] counterpart to [`ConnectorProvider::exchange_code`]
/// above, mapped into the SAME [`TokenBundle`] shape every other connector
/// uses so `routes::oauth::callback`'s generic `store_token_bundle_tx`/
/// `record_connector_install` write path needs no member-specific branch of
/// its own past choosing which exchange function to call. `bundle.user_token`
/// is always `None` here — unlike the bot flow's `TokenBundle`, where
/// `user_token` carries a SECOND, optional user-scoped token riding alongside
/// the primary bot one, a member connect's `access_token` field IS the
/// member's user token; there is no second slot to fill.
pub(crate) async fn exchange_code_member(
    http: &reqwest::Client,
    base_url: &str,
    code: &str,
) -> Result<TokenBundle, ConnectorError> {
    let client_id = std::env::var("SLACK_CLIENT_ID")
        .map_err(|_| ConnectorError::NotConfigured("SLACK_CLIENT_ID not configured".into()))?;
    let client_secret = std::env::var("SLACK_CLIENT_SECRET")
        .map_err(|_| ConnectorError::NotConfigured("SLACK_CLIENT_SECRET not configured".into()))?;
    let redirect_uri = format!("{base_url}/v1/oauth/slack/callback");

    let r = slack_api::exchange_code_member(http, code, &client_id, &client_secret, &redirect_uri)
        .await
        .map_err(|e| ConnectorError::Exchange(e.to_string()))?;

    Ok(TokenBundle {
        access_token: r.user_access_token,
        refresh_token: None,
        user_token: None,
        scope: r.user_scope,
        // Slack user tokens carry no expiry in this flow, same as the bot
        // token (token rotation is opt-in and not enabled here).
        expires_at: None,
        provider_workspace_id: r.team_id,
        provider_workspace_name: Some(r.team_name),
        provider_user_id: r.authed_user_id,
    })
}

/// Revoke a member's own Slack user token at Slack (`auth.revoke`) — thin
/// wrapper over [`slack_api::revoke_token`] so `routes::oauth::
/// delete_slack_member_connection` doesn't need to reach into `oauth::slack`
/// directly, matching every other Slack call site in this crate.
pub(crate) async fn revoke_member_token(
    http: &reqwest::Client,
    token: &str,
) -> Result<(), ConnectorError> {
    slack_api::revoke_token(http, token)
        .await
        .map_err(|e| ConnectorError::Exchange(e.to_string()))
}

/// Base URL for Slack's Web API, overridable by `SLACK_API_BASE`.
///
/// **This exists as a test seam, and it is the only reason it exists.** Every
/// Slack call in this crate previously hardcoded `https://slack.com/api`, at
/// 21 call sites across 6 files, so nothing could intercept Slack HTTP. That
/// made the whole chat-per-user chain — gate event → roster walk → identity
/// match → fan-out → backfill — untestable above the database layer: a test
/// could assert what the functions did with data it handed them, and could
/// never assert that the connector asks Slack the right questions or handles
/// the answers it actually gets back.
///
/// The precedent is `routes::org_invites::resend_base_url`, which is
/// `RESEND_API_BASE_URL` for exactly this reason. Same shape, same default-to-
/// production behaviour, same "unset in every real environment" expectation.
///
/// **Never set this in production.** It is read per call rather than cached in
/// a `OnceLock` so a test can point one scenario at a mock without leaking the
/// override into the rest of the process; the cost is an env read per Slack
/// call, which is noise next to the network round-trip that follows it.
pub(crate) fn slack_api_base() -> String {
    std::env::var("SLACK_API_BASE").unwrap_or_else(|_| "https://slack.com/api".into())
}

/// Serializes every in-process (`--lib`) test in this crate that mutates the
/// process-global `SLACK_API_BASE` env var — CodeRabbit round 1 (#1005,
/// Thread 24). Before this lock, `history_backfill.rs`'s three tests and
/// `slack_unmatched_email.rs`'s two both set/unset this SAME var with no
/// guard at all, relying only on a doc-comment telling whoever runs them to
/// remember `--test-threads=1` by hand. All five are compiled into the same
/// `--lib` test binary, which the Rust test harness runs concurrently by
/// default, so a plain `cargo test --lib` (no filter, no `--test-threads=1`)
/// could genuinely interleave them — one test's mock-server URL leaking into
/// another's request, or a request escaping to the real `slack.com` API.
///
/// This is a DIFFERENT var from every existing `*_ENV_LOCK` in this crate
/// (`MASTER_KEK_HEX_ENV_LOCK`, `RETRY_ENV_LOCK`, `ASK_MODEL_ENV_LOCK`, …) —
/// none of them cover `SLACK_API_BASE`, and this one covers nothing but it.
///
/// `backend/tests/chat_per_user_e2e_pg.rs` is a SEPARATE integration-test
/// binary (its own process) and keeps its own `--test-threads=1` requirement
/// unchanged (its own module doc, `:56`) — this lock only serializes tests
/// inside the `--lib` binary, which is the scope this finding is about.
#[cfg(test)]
mod slack_api_base_default_tests {
    use super::*;

    /// Unset, the base is Slack's real API: the e2e stack sets it, nothing else does.
    #[test]
    fn slack_api_base_defaults_to_slack() {
        let _lock = SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var("SLACK_API_BASE").ok();
        std::env::remove_var("SLACK_API_BASE");
        let got = slack_api_base();
        if let Some(v) = previous {
            std::env::set_var("SLACK_API_BASE", v);
        }
        assert_eq!(got, "https://slack.com/api");
    }
}

#[cfg(test)]
pub(crate) static SLACK_API_BASE_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// RAII guard pairing [`SLACK_API_BASE_ENV_LOCK`]: sets `SLACK_API_BASE` to
/// `url` and restores whatever the var held before (or clears it, if it was
/// unset) on drop — including on a panicking `assert!` inside the test body,
/// so a failed assertion can never leave a stale mock-server URL behind for
/// the next lock holder to inherit. Construct it only while holding
/// `SLACK_API_BASE_ENV_LOCK` for the test's whole body.
///
/// `#[cfg(test)]`, like the lock above: this is test-only infrastructure,
/// never referenced by production code, so it must not exist in a real
/// build — `cfg(test)` is active crate-wide for every compilation unit
/// during `cargo test`, so `history_backfill.rs`'s and
/// `slack_unmatched_email.rs`'s own `#[cfg(test)]` modules can still reach
/// it from here.
#[cfg(test)]
pub(crate) struct SlackApiBaseTestGuard {
    previous: Option<String>,
}

#[cfg(test)]
impl SlackApiBaseTestGuard {
    pub(crate) fn set(url: &str) -> Self {
        let previous = std::env::var("SLACK_API_BASE").ok();
        std::env::set_var("SLACK_API_BASE", url);
        Self { previous }
    }
}

#[cfg(test)]
impl Drop for SlackApiBaseTestGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(v) => std::env::set_var("SLACK_API_BASE", v),
            None => std::env::remove_var("SLACK_API_BASE"),
        }
    }
}

/// The Slack OAuth connector (bot + optional user token).
pub struct SlackConnector;

#[async_trait]
impl ConnectorProvider for SlackConnector {
    fn id(&self) -> &'static str {
        "slack"
    }

    fn display_name(&self) -> &'static str {
        "Slack"
    }

    fn source_types(&self) -> &'static [&'static str] {
        &["slack"]
    }

    fn authorize_url(&self, base_url: &str, state: &str) -> Result<String, ConnectorError> {
        let client_id = std::env::var("SLACK_CLIENT_ID")
            .map_err(|_| ConnectorError::NotConfigured("SLACK_CLIENT_ID not configured".into()))?;

        let redirect_uri = format!("{base_url}/v1/oauth/slack/callback");
        // See `slack_scopes()`'s own doc comment for the full per-scope
        // rationale and why this is now the ONLY place the scope string is
        // computed.
        let scopes = slack_scopes();

        let mut url = format!(
            "https://slack.com/oauth/v2/authorize?client_id={}&scope={}&redirect_uri={}&state={}",
            urlencoding::encode(&client_id),
            urlencoding::encode(&scopes),
            urlencoding::encode(&redirect_uri),
            urlencoding::encode(state),
        );

        // Append user_scope when SLACK_USER_SCOPES is set and non-empty, so Slack
        // also returns a user token in authed_user.access_token.
        let user_scopes = std::env::var("SLACK_USER_SCOPES").unwrap_or_default();
        if !user_scopes.is_empty() {
            url.push_str("&user_scope=");
            url.push_str(&urlencoding::encode(&user_scopes));
        }

        Ok(url)
    }

    async fn exchange_code(
        &self,
        http: &reqwest::Client,
        base_url: &str,
        code: &str,
    ) -> Result<TokenBundle, ConnectorError> {
        let client_id = std::env::var("SLACK_CLIENT_ID")
            .map_err(|_| ConnectorError::NotConfigured("SLACK_CLIENT_ID not configured".into()))?;
        let client_secret = std::env::var("SLACK_CLIENT_SECRET").map_err(|_| {
            ConnectorError::NotConfigured("SLACK_CLIENT_SECRET not configured".into())
        })?;
        let redirect_uri = format!("{base_url}/v1/oauth/slack/callback");

        let r = slack_api::exchange_code(http, code, &client_id, &client_secret, &redirect_uri)
            .await
            .map_err(|e| ConnectorError::Exchange(e.to_string()))?;

        Ok(TokenBundle {
            access_token: r.access_token,
            refresh_token: r.refresh_token,
            user_token: r.user_token,
            scope: r.scope,
            // Slack v2 bot tokens do not carry an expiry in this flow (token
            // rotation is opt-in and not enabled here), matching prior behavior.
            expires_at: None,
            provider_workspace_id: r.team_id,
            provider_workspace_name: Some(r.team_name),
            provider_user_id: r.authed_user_id,
        })
    }

    async fn post_store(
        &self,
        http: &reqwest::Client,
        pool: &sqlx::PgPool,
        account_id: Uuid,
        bundle: &TokenBundle,
        provider_key: &str,
    ) {
        // CS-454 CodeRabbit: a member callback stores under `slack_user`
        // and carries an `xoxp-…` user token as `bundle.access_token`. The
        // upsert below writes that value into `slack_integrations.bot_token`,
        // which would replace the workspace bot token ingest reads. Skip
        // every side effect on that key (callback also skips the call).
        if provider_key == "slack_user" {
            return;
        }
        // Slack has no further per-surface scope split — for a bot install
        // `provider_key` is `id()` (`"slack"`), so the side effects below
        // stay unconditional exactly as before; see the trait method's own
        // doc for why the parameter exists at all.
        //
        // Two independent best-effort side effects. Neither may short-circuit
        // the other: the slack-onboarding welcome DM below must still be
        // attempted even if the `slack_integrations` mirror fails (a scoped-tx
        // failure or a write error there says nothing about whether
        // `conversations.open`/`chat.postMessage` will work), so this used to
        // be one function with an early `return` on the mirror's failure path
        // — split into two calls instead of nesting the DM inside the mirror's
        // success arm.
        upsert_slack_integrations_mirror(pool, account_id, bundle).await;

        // docs/connectors/slack-onboarding.md §5.2 — welcome DM to the
        // installing user, once, on OAuth completion. See
        // `send_welcome_dm`'s own doc comment for the `im:write`
        // missing-scope fail-soft contract.
        send_welcome_dm(http, &bundle.access_token, &bundle.provider_user_id).await;
    }
}

/// Mirror the legacy slack_integrations upsert (single source of truth for
/// the message-ingestion pipeline, which resolves team → account here).
/// bot_token is stored as the plaintext xoxb- access_token, matching the
/// legacy routes/slack.rs oauth_callback convention. Non-fatal: oauth_tokens
/// is already persisted; a failure here only means the message pipeline may
/// not pick up this workspace, so we log and continue (never affects the
/// OAuth redirect).
///
/// RLS scoping (org-tenancy conversion): `slack_integrations` carries an
/// `account_id` policy (migration 0083), so the upsert runs inside a
/// `begin_account_scoped` transaction — inert pre-OPS-4, load-bearing after.
async fn upsert_slack_integrations_mirror(pool: &sqlx::PgPool, account_id: Uuid, bundle: &TokenBundle) {
    let mut tx = match crate::tenancy::scope::begin_account_scoped(pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                team_id = %bundle.provider_workspace_id,
                "failed to open scoped tx for slack_integrations upsert (non-fatal)"
            );
            return;
        }
    };
    let res = sqlx::query(
        r#"
        INSERT INTO slack_integrations
            (account_id, workspace_id, workspace_name, bot_token, created_at)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (account_id, workspace_id)
        DO UPDATE SET
            workspace_name = EXCLUDED.workspace_name,
            bot_token      = EXCLUDED.bot_token
        "#,
    )
    .bind(account_id)
    .bind(&bundle.provider_workspace_id)
    .bind(bundle.provider_workspace_name.as_deref().unwrap_or(""))
    .bind(&bundle.access_token)
    .execute(&mut *tx)
    .await;
    let res = match res {
        Ok(r) => tx.commit().await.map(|_| r),
        Err(e) => Err(e),
    };

    match res {
        Ok(_) => tracing::debug!(
            account_id = %account_id,
            team_id = %bundle.provider_workspace_id,
            "slack_integrations upserted for Connectors-page OAuth flow"
        ),
        Err(e) => tracing::warn!(
            error = %e,
            account_id = %account_id,
            team_id = %bundle.provider_workspace_id,
            "failed to upsert slack_integrations (non-fatal — message pipeline may not work for this workspace)"
        ),
    }
}

/// Welcome DM to the installing user, sent once on successful OAuth
/// completion (docs/connectors/slack-onboarding.md §5.2). States the same
/// three steps and huddle prerequisites the channel-level destination prompt
/// carries (`routes::slack_events::post_destination_prompt`), plus the one
/// instruction the web redirect page cannot usefully give: `/invite
/// @CoralSwarm`, the exact command, in the place the installer would type it.
///
/// **`im:write` fail-soft contract — READ THIS BEFORE TOUCHING THIS
/// FUNCTION.** `im:write` (needed for `conversations.open`, which opens/finds
/// the DM channel) was added to `SLACK_SCOPES` in the SAME change that
/// introduced this function. Slack does **not** retroactively grant a newly
/// added scope to an already-installed workspace — every workspace installed
/// before this change has a bot token that will always return `missing_scope` here,
/// permanently, until an admin manually reinstalls/re-consents (same
/// unavoidable limitation `users:read.email` hit — see `authorize_url`'s own
/// comment on that scope, right above the line this scope was added next to).
/// So `missing_scope` from `conversations.open` is not a bug to chase, it is
/// the EXPECTED steady state for every pre-existing install: logged at `warn!`
/// with `connector_health = true` (so it is visible/aggregable, matching this
/// file's other `connector_health` warnings) but never escalated, and NEVER
/// allowed to affect the OAuth redirect — this whole function runs from
/// `post_store`, which is best-effort by contract. Any other failure
/// (`conversations.open`/`chat.postMessage` HTTP error, a non-`missing_scope`
/// Slack API error, malformed JSON) is handled the identical way: logged and
/// swallowed. There is no retry — a missed welcome DM is a worse UX than a
/// failed install, never the other way around, and the App Home tab
/// (`routes::slack_events`'s `app_home_opened` handler) is the durable
/// version of the same status this DM only previews once.
async fn send_welcome_dm(http: &reqwest::Client, bot_token: &str, slack_user_id: &str) {
    let open_resp = match http
        .post(format!("{}/conversations.open", slack_api_base()))
        .bearer_auth(bot_token)
        .json(&serde_json::json!({ "users": slack_user_id }))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                connector_health = true,
                "Slack welcome DM: conversations.open HTTP error (non-fatal, install unaffected)"
            );
            return;
        }
    };

    let open_json: serde_json::Value = match open_resp.json().await {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!(
                error = %e,
                connector_health = true,
                "Slack welcome DM: conversations.open JSON parse error (non-fatal, install unaffected)"
            );
            return;
        }
    };

    if open_json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let err = open_json
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        if err == "missing_scope" {
            // The expected steady state for every pre-`im:write` install —
            // see this function's own doc comment. `warn!` + `connector_health`
            // so it is visible/countable, not `debug!`: an operator deciding
            // whether "ask everyone to reinstall" is worth doing needs to see
            // how many installs are actually missing this scope.
            tracing::warn!(
                connector_health = true,
                "Slack welcome DM: conversations.open returned missing_scope — this workspace was \
                 installed before im:write was added and needs a manual reinstall to receive \
                 welcome DMs; the install itself is unaffected"
            );
        } else {
            tracing::warn!(
                error = %err,
                connector_health = true,
                "Slack welcome DM: conversations.open Slack error (non-fatal, install unaffected)"
            );
        }
        return;
    }

    let Some(dm_channel_id) = open_json
        .pointer("/channel/id")
        .and_then(serde_json::Value::as_str)
    else {
        tracing::warn!(
            connector_health = true,
            "Slack welcome DM: conversations.open ok=true but missing channel.id (non-fatal)"
        );
        return;
    };

    let text = "👋 *Welcome to CoralSwarm!* Slack is connected, but nothing is being read yet — \
                that only starts once the bot is invited into a channel.\n\n\
                *1. Invite the bot* — in any channel you want CoralSwarm to read, type:\n\
                `/invite @CoralSwarm`\n\
                *2. Choose a destination* — CoralSwarm will ask, right in that channel, which \
                ocean its content should feed.\n\
                *3. First capture* — new messages start flowing in immediately after; past \
                history is optional and asked about separately.\n\n\
                A couple of things worth knowing up front:\n\
                • Turn on Slack's *automatic huddle notes* for this workspace, or huddles here \
                  will never be saved — we can't flip that setting for you.\n\
                • Huddle notes need *Slack AI* (a paid add-on). Without it there are no notes at \
                  all, and that looks identical to \"notes are off.\"\n\
                • Deleting a message or huddle in Slack does *not* remove the copy already saved \
                  here.";

    let post_resp = match http
        .post(format!("{}/chat.postMessage", slack_api_base()))
        .bearer_auth(bot_token)
        .json(&serde_json::json!({ "channel": dm_channel_id, "text": text }))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                connector_health = true,
                "Slack welcome DM: chat.postMessage HTTP error (non-fatal, install unaffected)"
            );
            return;
        }
    };

    match post_resp.json::<serde_json::Value>().await {
        Ok(json) => {
            if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
                let err = json.get("error").and_then(serde_json::Value::as_str).unwrap_or("unknown");
                tracing::warn!(
                    error = %err,
                    connector_health = true,
                    "Slack welcome DM: chat.postMessage Slack error (non-fatal, install unaffected)"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                connector_health = true,
                "Slack welcome DM: chat.postMessage JSON parse error (non-fatal, install unaffected)"
            );
        }
    }
}

#[cfg(test)]
mod credential_death_classification_tests {
    //! Pure, DB-and-network-free tests for [`classify_slack_api_error`]
    //! (CS-453). Same posture as `verified_email_profile_extraction_tests`
    //! just below: no Postgres, no HTTP mock, plain `cargo test -p
    //! coralswarm-backend --lib`.

    use super::classify_slack_api_error;
    use crate::connectors::CredentialDeathReason;

    #[test]
    fn each_definite_slack_error_classifies_as_revoked() {
        assert_eq!(
            classify_slack_api_error("token_revoked"),
            Some(CredentialDeathReason::SlackTokenRevoked)
        );
        assert_eq!(
            classify_slack_api_error("invalid_auth"),
            Some(CredentialDeathReason::SlackInvalidAuth)
        );
        assert_eq!(
            classify_slack_api_error("account_inactive"),
            Some(CredentialDeathReason::SlackAccountInactive)
        );
    }

    #[test]
    fn transient_and_scope_errors_never_classify() {
        // Rate limiting, upstream 5xx-shaped errors, and missing-scope
        // errors are never "dead" — retried or scope-remediated, not
        // revoked. See `classify_slack_api_error`'s own doc comment.
        assert_eq!(classify_slack_api_error("ratelimited"), None);
        assert_eq!(classify_slack_api_error("internal_error"), None);
        assert_eq!(classify_slack_api_error("fatal_error"), None);
        assert_eq!(classify_slack_api_error("service_unavailable"), None);
        assert_eq!(classify_slack_api_error("missing_scope"), None);
        assert_eq!(classify_slack_api_error("no_permission"), None);
        assert_eq!(classify_slack_api_error("not_authed"), None);
    }

    #[test]
    fn unrecognized_or_empty_error_never_classifies() {
        assert_eq!(classify_slack_api_error(""), None);
        assert_eq!(classify_slack_api_error("some_future_slack_error_code"), None);
    }
}

#[cfg(test)]
mod verified_email_profile_extraction_tests {
    //! Pure, DB-and-network-free tests for [`extract_verified_slack_profile`]
    //! and [`normalize_email`]. No Postgres, no HTTP mock (this crate has
    //! none) — these run under a plain `cargo test -p coralswarm-backend --lib`
    //! with no environment setup at all.

    use serde_json::json;

    use super::{classify_membership, extract_verified_slack_profile, normalize_email, MembershipClass};

    // ── normalize_email: lowercase + trim ONLY, no provider canonicalisation ──

    #[test]
    fn normalize_lowercases_and_trims() {
        assert_eq!(normalize_email("  person@example.com  "), "person@example.com");
    }

    /// The single most important assertion in this phase: a Gmail plus-alias
    /// must NOT normalise to the base address. Without this, a plus-alias
    /// mailbox would let its owner match — and read — a colleague's content.
    /// FAILS WITHOUT THE FIX: a naive normaliser that stripped `+...@` (a
    /// real Gmail canonicalisation rule) would make this assertion fail,
    /// since the two sides would then be equal.
    #[test]
    fn normalize_does_not_strip_plus_alias() {
        let base = normalize_email("person@example.com");
        let alias = normalize_email("person@example.com");
        assert_ne!(base, alias, "a plus-alias must remain a DISTINCT address after normalisation");
    }

    /// The second most important assertion: Gmail also treats dots in the
    /// local part as insignificant, and this matcher must not. FAILS WITHOUT
    /// THE FIX: a naive normaliser that stripped dots from the local part
    /// would make this assertion fail.
    #[test]
    fn normalize_does_not_strip_dots() {
        let with_dots = normalize_email("person@example.com");
        let without_dots = normalize_email("person@example.com");
        assert_ne!(with_dots, without_dots, "dots in the local part must remain significant");
    }

    // ── extract_verified_slack_profile ────────────────────────────────────────

    #[test]
    fn extracts_email_and_prefers_display_name() {
        let resp = json!({
            "ok": true,
            "user": {
                "deleted": false,
                "profile": {"email": " Person@Example.com ", "display_name": "Casey", "real_name": "Casey Example"}
            }
        });
        let (email, name) = extract_verified_slack_profile(&resp).expect("should extract");
        assert_eq!(email, "Person@Example.com"); // trimmed, NOT case-normalised here — that's normalize_email's job
        assert_eq!(name, "Casey");
    }

    #[test]
    fn falls_back_to_real_name_when_display_name_blank() {
        let resp = json!({
            "ok": true,
            "user": {"profile": {"email": "p@example.com", "display_name": "", "real_name": "Real Name"}}
        });
        let (_, name) = extract_verified_slack_profile(&resp).expect("should extract");
        assert_eq!(name, "Real Name");
    }

    /// FAILS WITHOUT THE FIX: before this extractor existed there was no
    /// function forbidding a match when Slack reports `ok: false` (e.g. a
    /// transient API error) — a naive caller might otherwise try to read
    /// `/user/profile/email` anyway and get lucky on a stale cached value.
    #[test]
    fn ok_false_returns_none() {
        let resp = json!({"ok": false, "error": "rate_limited"});
        assert!(extract_verified_slack_profile(&resp).is_none());
    }

    /// FAILS WITHOUT THE FIX: without the explicit `deleted` check this would
    /// extract `former@example.com` and let a deactivated Slack account's
    /// stale profile resolve to a live CoralSwarm user.
    #[test]
    fn deactivated_account_returns_none() {
        let resp = json!({
            "ok": true,
            "user": {"deleted": true, "profile": {"email": "former@example.com"}}
        });
        assert!(extract_verified_slack_profile(&resp).is_none());
    }

    /// Absent email — e.g. `users:read.email` not yet granted, so Slack omits
    /// the field entirely rather than erroring. FAILS WITHOUT THE FIX: a
    /// caller reading `/user/profile/email` with `unwrap_or_default()` (a
    /// tempting shortcut) would treat a missing field as an empty string and
    /// this test would still pass for the wrong reason if `is_empty()` were
    /// the only guard — the real assertion that matters is that the whole
    /// `Option` chain (`?` on `.as_str()`) is what makes this `None`, which
    /// this test pins.
    #[test]
    fn absent_email_returns_none() {
        let resp = json!({"ok": true, "user": {"profile": {"display_name": "No Email"}}});
        assert!(extract_verified_slack_profile(&resp).is_none());
    }

    #[test]
    fn empty_email_returns_none() {
        let resp = json!({"ok": true, "user": {"profile": {"email": "   "}}});
        assert!(extract_verified_slack_profile(&resp).is_none());
    }

    // ── classify_membership (CS-426) ────────────────────────────────────────

    #[test]
    fn full_member_when_no_guest_flag_and_team_matches() {
        let resp = json!({
            "user": {"is_restricted": false, "is_ultra_restricted": false, "team_id": "T1"}
        });
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::FullMember);
    }

    /// `is_ultra_restricted` — a single-channel guest.
    #[test]
    fn single_channel_guest_is_classified_guest() {
        let resp = json!({
            "user": {"is_restricted": false, "is_ultra_restricted": true, "team_id": "T1"}
        });
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::Guest);
    }

    /// `is_restricted` — a multi-channel guest.
    #[test]
    fn multi_channel_guest_is_classified_guest() {
        let resp = json!({
            "user": {"is_restricted": true, "is_ultra_restricted": false, "team_id": "T1"}
        });
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::Guest);
    }

    /// `team_id` differs from the channel's own workspace — a Slack Connect
    /// participant visiting from another workspace, even when the guest
    /// flags themselves are both `false` on THEIR home team.
    #[test]
    fn foreign_team_id_is_classified_slack_connect() {
        let resp = json!({
            "user": {"is_restricted": false, "is_ultra_restricted": false, "team_id": "T_OTHER"}
        });
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::SlackConnect);
    }

    /// A foreign `team_id` takes priority over a guest flag also being set —
    /// still exactly one class, never both.
    #[test]
    fn foreign_team_id_wins_over_guest_flags() {
        let resp = json!({
            "user": {"is_restricted": true, "is_ultra_restricted": false, "team_id": "T_OTHER"}
        });
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::SlackConnect);
    }

    /// FAILS WITHOUT THE FIX: absence must never read as `FullMember` — a
    /// `users:read` scope narrower than expected (or a Slack response-shape
    /// change) that omits any of the three fields must fail closed.
    #[test]
    fn missing_is_restricted_is_unavailable() {
        let resp = json!({"user": {"is_ultra_restricted": false, "team_id": "T1"}});
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::Unavailable);
    }

    #[test]
    fn missing_is_ultra_restricted_is_unavailable() {
        let resp = json!({"user": {"is_restricted": false, "team_id": "T1"}});
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::Unavailable);
    }

    #[test]
    fn missing_team_id_is_unavailable() {
        let resp = json!({"user": {"is_restricted": false, "is_ultra_restricted": false}});
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::Unavailable);
    }

    #[test]
    fn missing_user_object_is_unavailable() {
        let resp = json!({"ok": true});
        assert_eq!(classify_membership(&resp, "T1"), MembershipClass::Unavailable);
    }

    // ── MembershipClass label round-trip (CS-447) ───────────────────────────

    /// **M3 (security review, 2026-09-22).** Before this test,
    /// [`MembershipClass::from_label`] had NO test call site anywhere in
    /// the tree — flipping its `_ => None` arm to
    /// `_ => Some(Self::FullMember)` left every existing test green while
    /// making every unrecognised `cls` claim auto-add-eligible.
    ///
    /// (`assertion_ladder::an_unavailable_membership_class_is_unknown_and_
    /// never_admits` does not cover this: it exercises a different function
    /// on an already-parsed enum, i.e. the step AFTER this one.)
    ///
    /// The three real labels must round-trip, and everything else — an
    /// unknown value, the empty string, wrong case, and a near-miss — must
    /// answer `None`, never `FullMember`.
    #[test]
    fn membership_class_labels_round_trip_and_unknown_never_becomes_full_member() {
        for class in [MembershipClass::FullMember, MembershipClass::Guest, MembershipClass::SlackConnect] {
            let label = class.label().expect("a real class has a label");
            assert_eq!(MembershipClass::from_label(label), Some(class), "{label} must round-trip");
        }
        assert_eq!(MembershipClass::Unavailable.label(), None, "'we could not tell' has no wire label");

        for raw in ["", " ", "admin", "owner", "member", "Full_Member", "FULL_MEMBER", "full member", "full_member ", "unavailable"] {
            assert_eq!(
                MembershipClass::from_label(raw),
                None,
                "{raw:?} must not parse — and in particular must never parse as FullMember"
            );
        }
    }

    /// The same property stated as the thing that would actually happen: a
    /// token whose `cls` nobody recognises must not be readable as the one
    /// class that arms auto-add.
    #[test]
    fn no_unrecognised_label_parses_as_the_auto_add_arming_class() {
        for raw in ["guest", "slack_connect", "", "nonsense"] {
            assert_ne!(
                MembershipClass::from_label(raw),
                Some(MembershipClass::FullMember),
                "{raw:?} must never reach rung 2's workspace half"
            );
        }
    }

    // ── slack_missing_scopes (CS-431) ───────────────────────────────────────

    use super::slack_missing_scopes;

    /// The exact shape D17 point 5 (org-onboarding-from-the-invite.md)
    /// describes: a workspace installed before `users:read.email` shipped
    /// carries a real, but narrower, grant string that lacks it.
    #[test]
    fn slack_missing_scopes_lists_users_read_email_when_absent() {
        let stored = "channels:history channels:read users:read reactions:read \
                       files:read canvases:read chat:write im:write channels:leave";
        let missing = slack_missing_scopes(stored).expect("a real grant string is never unknown");
        assert_eq!(missing, vec!["users:read.email".to_string()], "{missing:?}");
    }

    #[test]
    fn slack_missing_scopes_empty_when_fully_granted() {
        let missing = slack_missing_scopes(&super::slack_scopes()).expect("known grant");
        assert!(missing.is_empty(), "a grant matching slack_scopes() exactly must have nothing missing: {missing:?}");
    }

    /// The legacy-backfill placeholder is a stored value, not a blank one —
    /// the shared `connectors::missing_scopes` set-difference alone would
    /// read it as "granted none", which would list every requested scope as
    /// missing for a workspace this codebase never actually queried Slack
    /// about.
    #[test]
    fn slack_missing_scopes_unknown_for_the_backfill_placeholder() {
        assert_eq!(
            slack_missing_scopes(crate::slack_oauth_backfill::BACKFILLED_SCOPE_PLACEHOLDER),
            None,
            "a backfilled row's placeholder scope must never be read as a real, empty grant"
        );
    }

    #[test]
    fn slack_missing_scopes_unknown_for_a_blank_grant() {
        assert_eq!(slack_missing_scopes(""), None);
        assert_eq!(slack_missing_scopes("   "), None);
    }
}

#[cfg(test)]
mod verified_identity_matcher_pg_tests {
    //! DB-backed tests for [`match_and_persist_verified_email`] and
    //! [`resolve_verified_slack_identity`] — the exact-match-only matching
    //! behavior against a real `users` table, and the persist-then-read-back
    //! directory path.
    //!
    //! In-crate (not `backend/tests/*_pg.rs`): both functions under test are
    //! private/`pub(crate)`, so an external test crate cannot drive them —
    //! same reasoning as `routes::slack_events::slack_watched_channel_
    //! destination_tests`.
    //!
    //! Gated on `ORGT_TEST_BASE_URL`, migrating the CONTROL tree (where
    //! `users` and `connector_identities` both live for a personal/consumer
    //! account — the schema every real caller of these functions runs
    //! against, mirroring the org-fanout destination tests' own reasoning).
    //! Requires an EXPLICIT `cargo test --lib <module>` invocation (the
    //! workspace `cargo test` job has no Postgres service and silently skips
    //! every test here):
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib verified_identity_matcher_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::{
        match_and_persist_verified_email, resolve_slack_identity_detailed,
        resolve_verified_slack_identity, SlackIdentity,
    };

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
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }

    async fn seed_user(pool: &PgPool, email: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }

    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The plus-alias case — the single highest-severity test in this phase.
    /// FAILS WITHOUT THE FIX: a matcher that applied Gmail-style provider
    /// canonicalisation (or simply used `ILIKE`/case-fold-only comparison
    /// without also treating `+tag` as significant) would match this and
    /// hand `person@example.com`'s controller `Alex.Alex172@
    /// gmail.com`'s private content.
    #[tokio::test]
    async fn plus_alias_does_not_match_pg() {
        let db = "orgt_c5_plus_alias";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping plus_alias_does_not_match_pg: set ORGT_TEST_BASE_URL");
            return;
        };

        seed_user(&pool, "person@example.com").await;

        let result = match_and_persist_verified_email(
            &pool,
            &pool,
            "T1",
            "U-ALIAS",
            "person@example.com",
            "Alias Account",
        )
        .await;
        assert_eq!(result, Ok(None), "a plus-alias address must NOT match the base address");

        teardown(admin_conn, pool, db).await;
    }

    /// The dot-alias case. FAILS WITHOUT THE FIX: a matcher that stripped
    /// dots from the local part (another real Gmail canonicalisation rule)
    /// would match this.
    #[tokio::test]
    async fn dot_alias_does_not_match_pg() {
        let db = "orgt_c5_dot_alias";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping dot_alias_does_not_match_pg: set ORGT_TEST_BASE_URL");
            return;
        };

        seed_user(&pool, "person@example.com").await;

        let result = match_and_persist_verified_email(
            &pool,
            &pool,
            "T1",
            "U-DOTS",
            "person@example.com",
            "Dotted Account",
        )
        .await;
        assert_eq!(result, Ok(None), "a dotted-local-part address must NOT match the undotted address");

        teardown(admin_conn, pool, db).await;
    }

    /// The exact-match POSITIVE case, case/whitespace-insensitively — proves
    /// the matcher isn't simply refusing everything. FAILS WITHOUT THE FIX
    /// (before `normalize_email`/the `lower(trim(...))` comparison existed,
    /// or if a naive implementation used a case-sensitive `=`): a
    /// differently-cased or padded address would wrongly fail to match.
    #[tokio::test]
    async fn exact_match_succeeds_case_and_whitespace_insensitively_pg() {
        let db = "orgt_c5_exact_match";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping exact_match_succeeds_case_and_whitespace_insensitively_pg: set ORGT_TEST_BASE_URL");
            return;
        };

        // Stored mixed-case/padded on the CoralSwarm side too (Clerk does not
        // force `users.email` to lowercase), so this pins BOTH sides of the
        // comparison needing normalisation, not just the Slack-derived one.
        let user_id = seed_user(&pool, " Person@Company.EXAMPLE ").await;

        let result = match_and_persist_verified_email(
            &pool,
            &pool,
            "T1",
            "U-EXACT",
            "  person@company.example  ",
            "Person",
        )
        .await;
        assert_eq!(result, Ok(Some(user_id)));

        teardown(admin_conn, pool, db).await;
    }

    /// No CoralSwarm user at all with that email — the common, legitimate
    /// "guest/contractor" case, not an error. FAILS WITHOUT THE FIX: without
    /// the `fetch_optional` + `?` short-circuit this would either panic or
    /// (worse) fall through to matching some unrelated row.
    #[tokio::test]
    async fn absent_coralswarm_account_returns_none_pg() {
        let db = "orgt_c5_absent_account";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping absent_coralswarm_account_returns_none_pg: set ORGT_TEST_BASE_URL");
            return;
        };

        let result = match_and_persist_verified_email(
            &pool,
            &pool,
            "T1",
            "U-NOBODY",
            "nobody@example.com",
            "Nobody",
        )
        .await;
        assert_eq!(result, Ok(None));

        teardown(admin_conn, pool, db).await;
    }

    /// A soft-deleted CoralSwarm user must not match — `deleted_at IS NULL`
    /// is part of the query, not decoration. FAILS WITHOUT THE FIX: dropping
    /// that predicate would match this row.
    #[tokio::test]
    async fn soft_deleted_user_does_not_match_pg() {
        let db = "orgt_c5_soft_deleted";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping soft_deleted_user_does_not_match_pg: set ORGT_TEST_BASE_URL");
            return;
        };

        let user_id = seed_user(&pool, "gone@example.com").await;
        sqlx::query("UPDATE users SET deleted_at = now() WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .unwrap();

        let result =
            match_and_persist_verified_email(&pool, &pool, "T1", "U-GONE", "gone@example.com", "Gone")
                .await;
        assert_eq!(result, Ok(None));

        teardown(admin_conn, pool, db).await;
    }

    /// The persist-then-read-back path, exercised through the REAL production
    /// entry point (`resolve_verified_slack_identity`), not just the DB core:
    /// a confirmed match writes `user_id`/`link_method`/`link_verified_at`
    /// into `connector_identities`, and a SECOND call for the same
    /// `(workspace_id, external_user_id)` resolves via the directory alone.
    /// Proven with a deliberately garbage bot token and a short client
    /// timeout: with `SLACK_API_BASE` unset (as it is in every environment
    /// except a test that deliberately points it at a mock — see
    /// [`slack_api_base`]), `resolve_verified_slack_identity`'s network
    /// target is the real Slack API,
    /// so if the directory short-circuit did NOT fire, this call would
    /// either reach Slack and get `ok: false` for the bad token, or time out
    /// in a network-isolated test environment — either way `None`, never
    /// `Some(user_id)`. A `Some(user_id)` result therefore specifically
    /// proves the directory-read branch resolved it, not a lucky network
    /// response. FAILS WITHOUT THE FIX: without the persist step (or without
    /// the directory-first check in `resolve_verified_slack_identity`), the
    /// second call has no confirmed row to read and falls through to that
    /// network branch, returning `None` instead of the cached match.
    #[tokio::test]
    async fn confirmed_match_persists_and_is_read_back_on_second_call_pg() {
        let db = "orgt_c5_persist_readback";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping confirmed_match_persists_and_is_read_back_on_second_call_pg: set ORGT_TEST_BASE_URL");
            return;
        };

        let user_id = seed_user(&pool, "linked@company.example").await;

        let first = match_and_persist_verified_email(
            &pool,
            &pool,
            "T1",
            "U-LINKED",
            "linked@company.example",
            "Linked Person",
        )
        .await;
        assert_eq!(first, Ok(Some(user_id)));

        let row: (Option<Uuid>, Option<String>, bool, String) = sqlx::query_as(
            "SELECT user_id, link_method, link_verified_at IS NOT NULL, canonical_name \
             FROM connector_identities \
             WHERE provider = 'slack' AND workspace_id = 'T1' AND external_user_id = 'U-LINKED'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0, Some(user_id));
        assert_eq!(row.1.as_deref(), Some("verified_email"));
        assert!(row.2, "link_verified_at must be set on a confirmed link");
        assert_eq!(row.3, "Linked Person");

        // Second call: a garbage bot token and a short-timeout client — see
        // this test's doc comment for why any outcome OTHER than the
        // directory short-circuit firing must produce `None`, not
        // `Some(user_id)`.
        let short_timeout_http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        // `Uuid::nil()` for `account_id`: this test's whole point is that the
        // directory short-circuit fires FIRST, before anything reads
        // `oauth_tokens.scope`, so the account never has to exist. If the
        // short-circuit regressed, the nil account would find no scope row and
        // the outcome would be `Unavailable` — still not `Some(user_id)`, so
        // the assertion below stays honest either way.
        let second = resolve_verified_slack_identity(
            &pool,
            &pool,
            &short_timeout_http,
            Uuid::nil(),
            "T1",
            "U-LINKED",
            "xoxb-garbage",
        )
        .await;
        assert_eq!(second, Some(user_id), "the second lookup must resolve from the DB directory alone");

        teardown(admin_conn, pool, db).await;
    }

    /// Slackbot is a bot, not an unidentified human. It must never land in a
    /// roster total, because the app's own bot user sits in every channel it
    /// was invited to and can never match — counting it makes every
    /// "identified N of M" disclosure permanently short by at least one.
    ///
    /// No network and no database: the classification happens before either,
    /// which is itself part of the contract.
    #[tokio::test]
    async fn slackbot_classifies_as_bot_not_unmatched() {
        // A pool is required by the signature but must never be consulted —
        // a lazily-connected handle to a database that does not exist proves
        // it. If this arm ever started touching the DB, this test would fail
        // with a connection error rather than pass.
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/nonexistent")
            .unwrap();
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap();

        let outcome =
            // `Uuid::nil()`: the Slackbot classification is made from the
            // member id alone, before any scope lookup.
            resolve_slack_identity_detailed(
                &pool,
                &pool,
                &http,
                Uuid::nil(),
                "T1",
                "USLACKBOT",
                "xoxb-garbage",
            )
            .await;

        assert_eq!(
            outcome,
            SlackIdentity::Bot,
            "Slackbot must classify as Bot — as Unmatched it would inflate every unidentified \
             count by one forever"
        );
    }

    /// A `users.info` lookup that cannot be completed is `Unavailable`, NOT
    /// `Unmatched`. This is the distinction that stops one rate-limited
    /// burst from reporting a whole channel as nobody-has-an-account.
    ///
    /// Driven by an unroutable address with a 50ms timeout, which exercises
    /// the same arm a 429 or a 5xx takes.
    ///
    /// FAILS WITHOUT THE FIX: every network/parse failure used to run through
    /// `.ok()?` into a bare `None`, indistinguishable from a member who
    /// genuinely has no CoralSwarm account.
    #[tokio::test]
    async fn a_failed_lookup_is_unavailable_not_unmatched_pg() {
        let db = "orgt_c5_lookup_unavailable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_failed_lookup_is_unavailable_not_unmatched_pg: set ORGT_TEST_BASE_URL");
            return;
        };

        // No `connector_identities` row for this member, so the lookup falls
        // through to the network branch — where it cannot succeed.
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap();

        let outcome =
            // `Uuid::nil()`: the network failure this test drives happens
            // before the no-email branch that consults the granted scope, so
            // the outcome is `Unavailable` for the reason the test names (the
            // lookup could not be completed), not for a missing scope row.
            resolve_slack_identity_detailed(
                &pool,
                &pool,
                &http,
                Uuid::nil(),
                "T1",
                "U-UNKNOWN",
                "xoxb-garbage",
            )
            .await;

        assert!(
            matches!(outcome, SlackIdentity::Unavailable(_)),
            "a lookup that could not be completed must be Unavailable, not Unmatched — got \
             {outcome:?}; reporting it as Unmatched is how a rate-limited burst silently turns a \
             whole channel into 'nobody could be identified'"
        );

        // The thin `Option` wrapper still flattens it, which is correct for
        // callers that only want "who is this" — the point is that the
        // information survives for callers that disclose counts.
        assert_eq!(
            resolve_verified_slack_identity(
                &pool,
                &pool,
                &http,
                Uuid::nil(),
                "T1",
                "U-UNKNOWN",
                "xoxb-garbage",
            )
            .await,
            None
        );

        teardown(admin_conn, pool, db).await;
    }

    // ── Org-slack step 5a: the matcher's OWN control/content plane split ─────
    //
    // Every test above this point migrates ONE database and passes it as
    // both `control_pool` and `content_pool` — proving the matching LOGIC,
    // not the plane split. This is the decisive proof: `users` lives ONLY on
    // a control-tree database, `connector_identities` lives on BOTH, and a
    // confirmed match must read the former from `control_pool` and persist
    // the latter to `content_pool` even when they are two genuinely
    // separate physical databases — the exact shape a real org Slack
    // workspace's identity match runs against.
    //
    // What this DOES and does NOT prove (corrected after review — the
    // original comment here overclaimed): the fresh-match leg below calls
    // [`match_and_persist_verified_email`] DIRECTLY, not through
    // `resolve_verified_slack_identity` -> `resolve_slack_identity_detailed`.
    // That is deliberate, not a shortcut of convenience: `resolve_slack_
    // identity_detailed` only ever reaches `match_and_persist_verified_
    // email` after a LIVE `users.info` call to Slack's real, hardcoded API
    // URL succeeds (see that function's own doc comment on why this crate
    // has no HTTP-mock harness), so there is no way to exercise a FRESH
    // (no-directory-row) match through the outer wrapper without a real
    // network call — the exact same constraint `match_and_persist_verified_
    // email`'s own doc comment states as the reason it is factored out at
    // all. So this test proves the DB core — the piece every production
    // caller ultimately funnels into, and the piece that actually had the
    // bug (a single `pool` reused for both the `users` SELECT and the
    // `connector_identities` INSERT) — correctly splits `control_pool`
    // from `content_pool`. It does NOT, on its own, prove `resolve_slack_
    // identity_detailed` forwards both pools into that call; the read-back
    // assertion further below closes part of that gap by going through the
    // REAL `resolve_verified_slack_identity` entry point for the directory-
    // hit branch (proving the OUTER wrapper's read path also honors
    // `content_pool`) — but the outer wrapper's NETWORK-match branch
    // specifically remains untested end-to-end here for the reason above.
    //
    /// FAILS WITHOUT THE FIX: before `match_and_persist_verified_email` took
    /// two pools, this ran the `users` SELECT on whatever single `pool` it
    /// was handed. Handed the TENANT pool here (as
    /// `resolve_channel_members`'s tenant-looped seat-reconcile caller
    /// genuinely does), that query would fail outright — `migrations_tenant`
    /// never creates a `users` table at all — and
    /// `resolve_slack_identity_detailed` mapped that failure to
    /// `Unavailable`, `resolve_verified_slack_identity` flattened it to
    /// `None`: fail-safe, but an org Slack user who was not already in
    /// `connector_identities` could never be linked, no matter how many
    /// times they were seen.
    #[tokio::test]
    async fn matches_and_persists_across_two_genuinely_separate_pools_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping matches_and_persists_across_two_genuinely_separate_pools_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "orgt_c5_matcher_two_pool_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let content_db = "orgt_c5_matcher_two_pool_content";
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        let mut control_admin = recreate(&base, control_db).await;
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, content_db);
        let mut content_admin = recreate(&base, content_db).await;
        let control_pool: PgPool =
            PgPoolOptions::new().connect(&with_db(&base, control_db)).await.unwrap();
        let content_pool: PgPool =
            PgPoolOptions::new().connect(&with_db(&base, content_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control_pool).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&content_pool).await.unwrap();

        // CONTROL: the CoralSwarm user with the verified email — `users`
        // does not exist on `content_pool` at all.
        let user_id = seed_user(&control_pool, "org-member@company.example").await;

        // The DB core, called DIRECTLY — not the `resolve_verified_slack_
        // identity` -> `resolve_slack_identity_detailed` chain (see this
        // module section's header comment for exactly why a FRESH match
        // cannot be driven through that chain here without a live Slack
        // call). This proves `match_and_persist_verified_email` itself
        // splits `control_pool`/`content_pool` correctly — the function
        // every production caller's fresh-match path funnels into.
        let matched = match_and_persist_verified_email(
            &control_pool,
            &content_pool,
            "T-two-pool",
            "U-ORG-MEMBER",
            "org-member@company.example",
            "Org Member",
        )
        .await;
        assert_eq!(
            matched,
            Ok(Some(user_id)),
            "the match must succeed reading `users` from CONTROL even though the caller's other \
             pool is a genuine org TENANT database"
        );

        // CONTENT: the confirmed link must land on the TENANT pool — never
        // on control (where this org's Slack workspace has no
        // `connector_identities` row of its own once it is a genuine
        // tenant).
        let content_row: (Uuid, String) = sqlx::query_as(
            "SELECT user_id, link_method FROM connector_identities \
             WHERE provider = 'slack' AND workspace_id = 'T-two-pool' AND external_user_id = \
             'U-ORG-MEMBER'",
        )
        .fetch_one(&content_pool)
        .await
        .expect("the confirmed link must be persisted on the CONTENT (tenant) pool");
        assert_eq!(content_row.0, user_id);
        assert_eq!(content_row.1, "verified_email");

        let control_row_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_identities \
             WHERE provider = 'slack' AND workspace_id = 'T-two-pool' AND external_user_id = \
             'U-ORG-MEMBER'",
        )
        .fetch_one(&control_pool)
        .await
        .unwrap();
        assert_eq!(
            control_row_count, 0,
            "the confirmed link must NEVER also land on control — this org's content lives on \
             its own tenant plane"
        );

        // Directory-first re-read, through `resolve_verified_slack_identity`,
        // proves the SAME two-pool split is honored on the read-back path
        // too — a garbage bot token and a short-timeout client, exactly like
        // `confirmed_match_persists_and_is_read_back_on_second_call_pg`
        // above, so any outcome other than the directory short-circuit
        // firing produces `None`, never `Some(user_id)`.
        let short_timeout_http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        // `Uuid::nil()` for the same reason as the single-pool twin above: the
        // content-pool directory read this test is about happens before any
        // control-pool scope lookup.
        let second = resolve_verified_slack_identity(
            &control_pool,
            &content_pool,
            &short_timeout_http,
            Uuid::nil(),
            "T-two-pool",
            "U-ORG-MEMBER",
            "xoxb-garbage",
        )
        .await;
        assert_eq!(
            second,
            Some(user_id),
            "the second lookup must resolve from the CONTENT-pool directory alone, never \
             touching the network or `control_pool` again"
        );

        control_pool.close().await;
        content_pool.close().await;
        control_admin
            .execute(format!("DROP DATABASE IF EXISTS {control_db} WITH (FORCE)").as_str())
            .await
            .ok();
        content_admin
            .execute(format!("DROP DATABASE IF EXISTS {content_db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod org_fanout_destinations_pg_tests {
    //! C5 phase two — DB-backed tests for [`resolve_org_fanout_destinations`]:
    //! the account-wide default that fans a Slack channel with no explicit
    //! destination row out to each MAPPED account member's own ocean.
    //!
    //! In-crate: the function under test is private, so an external test
    //! crate cannot drive it — same reasoning as `verified_identity_matcher_
    //! pg_tests` above.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib org_fanout_destinations_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::{resolve_org_fanout_destinations, FanOut};

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
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn seed_org_account(pool: &PgPool, owner: Uuid) -> Uuid {
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(format!("org-{}", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
        account_id
    }

    async fn add_member(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Seeds a `kind='member'` primary ocean for `user_id` — the shape
    /// `crate::routes::slack::resolve_primary_graph`'s org branch reads.
    async fn seed_member_ocean(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'member ocean', 'member', $2, true) RETURNING id",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Seeds a CONFIRMED `connector_identities` link — the persisted state
    /// [`super::match_and_persist_verified_email`] writes on a real match,
    /// reproduced directly here so these tests don't need a live Slack call.
    async fn seed_confirmed_identity(
        pool: &PgPool,
        workspace_id: &str,
        external_user_id: &str,
        user_id: Uuid,
    ) {
        sqlx::query(
            "INSERT INTO connector_identities \
                (provider, workspace_id, external_user_id, canonical_name, user_id, \
                 link_method, link_verified_at) \
             VALUES ('slack', $1, $2, 'Test Person', $3, 'verified_email', now())",
        )
        .bind(workspace_id)
        .bind(external_user_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// The core fix this phase makes: TWO confirmed, mapped members must
    /// both appear in `graphs`, and `unmapped_members` must be derived as
    /// `total − mapped.len()` — NOT a second, independent `count(*)` over
    /// `account_members` (the pre-fix placeholder's bug). FAILS WITHOUT THE
    /// FIX: the old placeholder returned `(vec![], total_members)`
    /// unconditionally, so this would assert `graphs.len() == 2` against an
    /// empty vec and fail immediately.
    #[tokio::test]
    async fn two_confirmed_members_are_both_mapped_and_unmapped_is_total_minus_mapped_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping two_confirmed_members_are_both_mapped_and_unmapped_is_total_minus_mapped_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_c5_fanout_two_mapped";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;

        let member_a = seed_user(&pool).await;
        add_member(&pool, account, member_a).await;
        let graph_a = seed_member_ocean(&pool, account, member_a).await;
        seed_confirmed_identity(&pool, "T1", "U-A", member_a).await;

        let member_b = seed_user(&pool).await;
        add_member(&pool, account, member_b).await;
        let graph_b = seed_member_ocean(&pool, account, member_b).await;
        seed_confirmed_identity(&pool, "T1", "U-B", member_b).await;

        // total_members = owner + member_a + member_b = 3; owner has no
        // confirmed identity, so unmapped must be 1, never 3.
        // Two-pool signature (org-tenancy plane split, step 0) — on the
        // consumer plane every caller passes the same pool for both, so this
        // stays the byte-identical-on-consumer-plane case.
        let FanOut { mut graphs, unmapped_members: unmapped, .. } = resolve_org_fanout_destinations(&pool, &pool, account, "T1")
            .await
            .unwrap();
        graphs.sort();
        let mut expected = vec![graph_a, graph_b];
        expected.sort();
        assert_eq!(graphs, expected, "both confirmed, ocean-resolvable members must be mapped");
        assert_eq!(
            unmapped, 1,
            "unmapped_members must be total_members (3) minus mapped.len() (2), not an \
             independent count"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A confirmed identity whose primary member ocean cannot be resolved
    /// (no `kind='member'` graph provisioned yet) must be counted UNMAPPED,
    /// not crash and not appear in `graphs` — an identity match with nowhere
    /// to land is not a usable destination.
    #[tokio::test]
    async fn confirmed_identity_without_a_provisioned_ocean_counts_as_unmapped_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping confirmed_identity_without_a_provisioned_ocean_counts_as_unmapped_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_c5_fanout_unprovisioned";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;

        let member = seed_user(&pool).await;
        add_member(&pool, account, member).await;
        seed_confirmed_identity(&pool, "T1", "U-NOOCEAN", member).await;
        // Deliberately no `seed_member_ocean` call for `member`.

        let FanOut { graphs, unmapped_members: unmapped, .. } = resolve_org_fanout_destinations(&pool, &pool, account, "T1").await.unwrap();
        assert!(graphs.is_empty(), "a confirmed member with no resolvable ocean must not appear in graphs");
        assert_eq!(unmapped, 2, "both the owner (never confirmed) and the ocean-less member are unmapped");

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A confirmed identity for a DIFFERENT workspace, or for a user who is
    /// NOT a current member of this account, must never be mapped — the
    /// directory join's `workspace_id` and `account_members` predicates are
    /// both load-bearing, not decoration.
    #[tokio::test]
    async fn confirmed_identity_for_a_different_workspace_or_non_member_is_ignored_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping confirmed_identity_for_a_different_workspace_or_non_member_is_ignored_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_c5_fanout_cross_scope";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;

        // Confirmed for a DIFFERENT workspace ("T-other", not "T1").
        let member_wrong_workspace = seed_user(&pool).await;
        add_member(&pool, account, member_wrong_workspace).await;
        seed_member_ocean(&pool, account, member_wrong_workspace).await;
        seed_confirmed_identity(&pool, "T-other", "U-WRONG-WS", member_wrong_workspace).await;

        // Confirmed for "T1", but never added to THIS account's members.
        let non_member = seed_user(&pool).await;
        seed_confirmed_identity(&pool, "T1", "U-NON-MEMBER", non_member).await;

        let FanOut { graphs, unmapped_members: unmapped, .. } = resolve_org_fanout_destinations(&pool, &pool, account, "T1").await.unwrap();
        assert!(
            graphs.is_empty(),
            "neither a different-workspace confirmation nor a non-member's confirmation may map"
        );
        // total_members = owner + member_wrong_workspace = 2 (non_member was
        // never added to `account_members`, so it isn't counted at all).
        assert_eq!(unmapped, 2);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Genuine two-pool proof (org-tenancy plane split, step 0):
    /// `control_pool` and `tenant_pool` are TWO SEPARATE physical databases —
    /// `control_pool` migrated from `migrations_control` (where
    /// `account_members` lives) and `tenant_pool` migrated from
    /// `migrations_tenant` (which never creates `accounts`/`account_members`
    /// at all — only `connector_identities`/`graphs`). Before this split,
    /// `resolve_org_fanout_destinations` JOINed `connector_identities` to
    /// `account_members` in ONE query, which would fail outright
    /// (`relation "account_members" does not exist`) the moment it ran
    /// against a real tenant pool. This test proves the split actually works
    /// end to end against two real, independently-migrated databases, not
    /// just against one DB playing both roles (the other tests in this
    /// module, and `resolve_channel_destinations`'s own callers today).
    #[tokio::test]
    async fn resolves_across_two_genuinely_separate_pools_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolves_across_two_genuinely_separate_pools_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "orgt_c5_fanout_two_pool_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "orgt_c5_fanout_two_pool_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        let _database_cleanup_2 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        let mut control_admin = recreate(&base, control_db).await;
        let _database_cleanup_3 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        let mut tenant_admin = recreate(&base, tenant_db).await;
        let control_pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, control_db))
            .await
            .unwrap();
        let tenant_pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, tenant_db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control").run(&control_pool).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();

        // CONTROL: the account and its three members.
        let owner = seed_user(&control_pool).await;
        let account = seed_org_account(&control_pool, owner).await;
        let member_mapped = seed_user(&control_pool).await;
        add_member(&control_pool, account, member_mapped).await;
        let member_unmapped = seed_user(&control_pool).await;
        add_member(&control_pool, account, member_unmapped).await;

        // TENANT: a confirmed identity + primary member ocean for ONLY the
        // mapped member. `account_members` does not exist on this pool at
        // all (`migrations_tenant` never creates it) — if this function
        // still tried to read it here (directly, or via a JOIN), this test
        // would fail with a Postgres "relation does not exist" error, not a
        // wrong assertion.
        let graph_mapped: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'member ocean', 'member', $2, true) RETURNING id",
        )
        .bind(account)
        .bind(member_mapped)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO connector_identities \
                (provider, workspace_id, external_user_id, canonical_name, user_id, \
                 link_method, link_verified_at) \
             VALUES ('slack', $1, $2, 'Test Person', $3, 'verified_email', now())",
        )
        .bind("T-two-pool")
        .bind("U-MAPPED")
        .bind(member_mapped)
        .execute(&tenant_pool)
        .await
        .unwrap();

        let FanOut { graphs, unmapped_members: unmapped, .. } = resolve_org_fanout_destinations(
            &control_pool,
            &tenant_pool,
            account,
            "T-two-pool",
        )
        .await
        .unwrap();
        assert_eq!(
            graphs,
            vec![graph_mapped],
            "the mapped member's TENANT-side primary ocean must resolve"
        );
        assert_eq!(
            unmapped, 2,
            "total_members (3, from CONTROL) minus mapped.len() (1) — the owner (never \
             confirmed) and member_unmapped (no identity) must both count as unmapped"
        );

        control_pool.close().await;
        tenant_pool.close().await;
        control_admin
            .execute(format!("DROP DATABASE IF EXISTS {control_db} WITH (FORCE)").as_str())
            .await
            .ok();
        tenant_admin
            .execute(format!("DROP DATABASE IF EXISTS {tenant_db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    async fn set_connect_emails_began(pool: &PgPool, days_ago: i32) {
        sqlx::query(
            "UPDATE org_install_capture_grace SET connect_emails_began_at = now() - make_interval(days => $1)",
        )
        .bind(days_ago)
        .execute(pool)
        .await
        .unwrap();
    }

    /// One mapped member, so a non-empty graph list means the fan-out path
    /// actually ran. The migration seed is pinned open in tests; these two
    /// write a different instant so the comparison is the real 30-day rule.
    async fn one_mapped_member(pool: &PgPool) -> (Uuid, Uuid) {
        let owner = seed_user(pool).await;
        let account = seed_org_account(pool, owner).await;
        let member = seed_user(pool).await;
        add_member(pool, account, member).await;
        let graph = seed_member_ocean(pool, account, member).await;
        seed_confirmed_identity(pool, "T-grace", "U-GRACE", member).await;
        (account, graph)
    }

    #[tokio::test]
    async fn org_install_grace_slack_org_fanout_still_resolves_before_the_window_closes_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_install_grace_slack_org_fanout_still_resolves_before_the_window_\
                 closes_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_c5_fanout_grace_open";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        // No member_connect notice exists in this empty database, so the
        // migration must record item 50's merge and not "now".
        let began: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT connect_emails_began_at FROM org_install_capture_grace",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            began.timestamp(),
            crate::services::org_install_grace::item_50_merge_instant().timestamp(),
            "with no member_connect row the grace starts at the item 50 merge"
        );
        set_connect_emails_began(&pool, 1).await;
        let (account, graph) = one_mapped_member(&pool).await;
        let FanOut { graphs, .. } = resolve_org_fanout_destinations(&pool, &pool, account, "T-grace")
            .await
            .unwrap();
        assert_eq!(
            graphs,
            vec![graph],
            "inside the 30-day grace the org fan-out still lands in the mapped member's ocean"
        );
        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn org_install_grace_slack_org_fanout_is_unreachable_after_the_window_closes_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_install_grace_slack_org_fanout_is_unreachable_after_the_window_\
                 closes_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_c5_fanout_grace_closed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        set_connect_emails_began(&pool, 31).await;
        let (account, graph) = one_mapped_member(&pool).await;
        let FanOut { graphs, unmapped_members: unmapped, .. } =
            resolve_org_fanout_destinations(&pool, &pool, account, "T-grace")
                .await
                .unwrap();
        assert!(
            graphs.is_empty(),
            "after the grace the mapped ocean {graph} must not be reachable, got {graphs:?}"
        );
        assert_eq!(unmapped, 0, "a closed window returns before the membership read");
        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}


#[cfg(test)]
mod anchor_decision_tests {
    //! Pure — no DB, no network. [`super::anchor_decision`] is the whole of
    //! what [`super::resolve_anchor_domain`]'s first branch decides, so these
    //! three cases are the decision itself rather than a proxy for it.

    use super::{anchor_decision, AnchorDecision};

    /// **CodeRabbit round 1, #1031.** A FAILED verified-domain lookup must
    /// not degrade to D21's bootstrap (the connecting member's own domain).
    /// An org that HAS a verified domain would otherwise have D2's guest
    /// rule silently re-anchored on the installer's domain by a transient
    /// database error — admitting guests it never agreed to admit. "Could
    /// not ask" is not "nothing is verified", which is the same discipline
    /// this PR's ladder is built around, applied to its own seam.
    #[test]
    fn a_failed_lookup_resolves_no_anchor_rather_than_falling_back() {
        let decision = anchor_decision(Err("connection reset".to_string()));
        assert!(
            matches!(decision, AnchorDecision::NoAnchor(_)),
            "a failed lookup must resolve NO anchor, got {decision:?}"
        );
        assert_ne!(
            decision,
            AnchorDecision::Bootstrap,
            "could not ask is NOT 'this org has verified no domain'"
        );
    }

    #[test]
    fn a_verified_domain_is_the_anchor_and_no_verified_domain_bootstraps() {
        assert_eq!(
            anchor_decision(Ok(vec!["acme.com".to_string(), "acquired.com".to_string()])),
            AnchorDecision::Verified(vec!["acme.com".to_string(), "acquired.com".to_string()]),
            "every verified domain is an anchor, not only the earliest"
        );
        assert_eq!(
            anchor_decision(Ok(vec![])),
            AnchorDecision::Bootstrap,
            "a genuine 'nothing verified' is what D21's bootstrap exists for"
        );
    }
}

/// CS-443 (org-onboarding plan §3 item 37, D11/D2) — the verified-org-domain
/// seam, against a live control database.
///
/// In-crate: [`verified_org_domain`] is deliberately PRIVATE (it is "the ONE
/// place" the lookup is wired), so an external test crate cannot drive it —
/// same reasoning `org_fanout_destinations_pg_tests` above gives for its own
/// private subject.
///
/// `RELINFER_TEST_DATABASE_URL`-gated against a PERSISTENT, pre-migrated
/// database, the convention `routes::org_join_requests::pg_tests` and
/// `clerk_provision::pg_tests` already use — not the createdb-per-run
/// pattern the fan-out suite above uses, because nothing here needs a
/// disposable tenant database. Every seeded row carries a fresh
/// `Uuid::new_v4()`, so this suite is safe alongside every other suite
/// sharing that database.
///
///   RELINFER_TEST_DATABASE_URL=postgres://alex@localhost:5432/cs443 \
///     cargo test -p coralswarm-backend --lib domain_seam_pg_tests -- --test-threads=1 --nocapture
#[cfg(test)]
mod domain_seam_pg_tests {
    use super::{resolve_anchor_domain, verified_org_domain};
    use sqlx::PgPool;
    use uuid::Uuid;

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    async fn seed_org(pool: &PgPool) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users (id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
        )
        .bind(format!("cs443-seam-{}@example.test", Uuid::new_v4().simple()))
        .bind(format!("clerk_cs443_seam_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(format!("org_cs443_seam_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert account");
        (account_id, user_id)
    }

    /// **NEGATIVE TEST 1 (CS-443).** A claimed-but-UNVERIFIED domain must not
    /// satisfy [`verified_org_domain`], and therefore must not widen D2's
    /// guest rule through [`resolve_anchor_domain`].
    ///
    /// This is the bug the seam exists to prevent: if a bare "is there a row"
    /// lookup were wired here, anyone who can type a domain into an admin
    /// settings box would start having every guest and Slack-Connect
    /// participant on that domain invited — on no evidence at all. The test
    /// asserts the unverified state FIRST (so it cannot pass merely because
    /// verification works), then flips the same row to verified and asserts
    /// both functions change their answer, which is what proves the `None`
    /// above came from the `verified_at IS NOT NULL` filter rather than from
    /// a lookup that never worked.
    ///
    /// No Slack call is made in either half: the unverified half returns
    /// before the HTTP lookup (this account has no `oauth_tokens` row, so
    /// `provider_user_id` is `None`), and the verified half returns on the
    /// very first line of [`resolve_anchor_domain`].
    #[tokio::test]
    async fn an_unverified_claim_does_not_satisfy_verified_org_domain() {
        let Some(pool) = pg_test_pool("an_unverified_claim_does_not_satisfy_verified_org_domain").await
        else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = format!("cs443-seam-{}.example", Uuid::new_v4().simple());
        let http = reqwest::Client::new();
        let workspace_id = format!("T{}", Uuid::new_v4().simple().to_string().to_uppercase());

        let domain_id: Uuid = sqlx::query_scalar(
            "INSERT INTO org_domains (account_id, domain, verification_token, claimed_by) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(&domain)
        .bind(Uuid::new_v4().simple().to_string())
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("claim an unverified domain");

        assert_eq!(
            verified_org_domain(&pool, account_id).await,
            Ok(Vec::<String>::new()),
            "a CLAIMED but unverified domain must never be returned as the org's verified domain"
        );
        assert!(
            resolve_anchor_domain(&pool, &http, account_id, &workspace_id, "xoxb-never-used")
                .await
                .is_empty(),
            "an unverified claim must not become D2's guest-rule anchor domain"
        );

        // Same row, now verified — the answer flips, which is what proves
        // the `None` above was the verified filter and not a dead lookup.
        sqlx::query("UPDATE org_domains SET verified_at = now(), method = 'dns_txt' WHERE id = $1")
            .bind(domain_id)
            .execute(&pool)
            .await
            .expect("verify the domain");

        assert_eq!(verified_org_domain(&pool, account_id).await, Ok(vec![domain.clone()]));
        assert_eq!(
            resolve_anchor_domain(&pool, &http, account_id, &workspace_id, "xoxb-never-used").await,
            vec![domain],
            "a VERIFIED domain is the anchor, with no Slack round trip at all"
        );
    }

    /// **NEGATIVE TEST 5 (CS-443), the live-database half of the founder's
    /// 2026-09-22 ruling.** The two gates that read `org_domains` do NOT
    /// accept the same proofs, and this test is the whole point of the
    /// ruling: without it the two methods are indistinguishable in the code
    /// the moment someone refactors.
    ///
    /// ```text
    /// verified by DNS   -> D2 guest rule + D16 rung 2 / auto-add
    /// verified by email -> D2 guest rule only
    /// ```
    ///
    /// One domain, verified BY EMAIL, asserted on both sides at once:
    /// [`verified_org_domain`]/[`resolve_anchor_domain`] DO return it (an
    /// emailed confirmation proves control, so D2's guest rule is armed —
    /// this seam is deliberately NOT narrowed), while the ladder reports
    /// `MatchEmailOnly` and refuses to admit even when paired with a full
    /// workspace member. The SAME row, flipped to `dns_txt`, then admits —
    /// which is what proves the refusal came from the method filter rather
    /// than from anything else about the fixture.
    ///
    /// **Not redundant with
    /// `services::assertion_ladder::tests::an_email_verified_domain_never_satisfies_rung_two`,
    /// and must not be deleted as such.** That test is database-free and now
    /// drives the ruling through the real `classify_domain_match` mapping,
    /// so a mutation of `may_arm_auto_add` kills both — but it cannot reach
    /// the OTHER half of the ruling, which is that the SEAM keeps returning
    /// an email-verified domain so D2's guest rule stays armed. Only a live
    /// row read by both `verified_org_domain` and `domain_signal` proves the
    /// two gates genuinely disagree about the same row. Deleting this test
    /// would leave "email still anchors the guest rule" pinned by nothing.
    #[tokio::test]
    async fn an_email_verified_domain_anchors_the_guest_rule_but_not_rung_two() {
        let Some(pool) = pg_test_pool("an_email_verified_domain_anchors_the_guest_rule_but_not_rung_two").await
        else {
            return;
        };
        use crate::services::assertion_ladder::{
            evaluate, DirectorySignal, DomainSignal, LadderSignals, Rung, Verdict, WorkspaceSignal,
            NO_DIRECTORY_AVAILABLE,
        };

        let (account_id, user_id) = seed_org(&pool).await;
        let domain = format!("cs443-email-verified-{}.example", Uuid::new_v4().simple());
        let address = format!("dana@{domain}");
        let http = reqwest::Client::new();
        let workspace_id = format!("T{}", Uuid::new_v4().simple().to_string().to_uppercase());

        let domain_id: Uuid = sqlx::query_scalar(
            "INSERT INTO org_domains (account_id, domain, verification_token, claimed_by, verified_at, method) \
             VALUES ($1, $2, $3, $4, now(), 'email') RETURNING id",
        )
        .bind(account_id)
        .bind(&domain)
        .bind(Uuid::new_v4().simple().to_string())
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("verify a domain BY EMAIL");

        // ── D2's guest rule: armed. The seam is deliberately method-blind.
        assert_eq!(
            verified_org_domain(&pool, account_id).await,
            Ok(vec![domain.clone()]),
            "an email-confirmed domain still proves control and still anchors D2's guest rule"
        );
        assert_eq!(
            resolve_anchor_domain(&pool, &http, account_id, &workspace_id, "xoxb-never-used").await,
            vec![domain.clone()]
        );

        // ── D16 rung 2: refused.
        let signal = crate::services::assertion_ladder::domain_signal(&pool, account_id, &address).await;
        assert_eq!(
            signal,
            DomainSignal::MatchEmailOnly(domain.clone()),
            "the ladder must see this as a match it may NOT act on, never a plain match"
        );
        let full_member_on_that_domain = LadderSignals {
            directory: DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            workspace: WorkspaceSignal::FullMember,
            domain: signal,
        };
        let outcome = evaluate(full_member_on_that_domain);
        assert_eq!(
            outcome.verdict,
            Verdict::ApprovalOnly,
            "full workspace member + an EMAIL-verified domain is rung 3, never rung 2"
        );
        assert!(!outcome.verdict.may_auto_add());
        assert_eq!(outcome.evidence()["domain"], serde_json::json!("match_email_verified_only"));

        // ── The same row, DNS-verified: both gates now say yes.
        sqlx::query("UPDATE org_domains SET method = 'dns_txt' WHERE id = $1")
            .bind(domain_id)
            .execute(&pool)
            .await
            .expect("flip the proving method to DNS");

        assert_eq!(verified_org_domain(&pool, account_id).await, Ok(vec![domain.clone()]));
        let signal = crate::services::assertion_ladder::domain_signal(&pool, account_id, &address).await;
        assert_eq!(signal, DomainSignal::Match(domain.clone()));
        let outcome = evaluate(LadderSignals {
            directory: DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            workspace: WorkspaceSignal::FullMember,
            domain: signal,
        });
        assert_eq!(outcome.verdict, Verdict::AutoAddEligible);
        assert_eq!(outcome.rung, Rung::MemberAndDomain);
    }

    /// A verified domain belonging to a DIFFERENT account is invisible here —
    /// the `account_id` predicate (and `org_domains`' own
    /// `account_isolation` RLS policy behind it) is per-org, so one org
    /// verifying `acme.com` never arms another org's guest rule.
    #[tokio::test]
    async fn another_orgs_verified_domain_is_not_this_orgs_anchor() {
        let Some(pool) = pg_test_pool("another_orgs_verified_domain_is_not_this_orgs_anchor").await else {
            return;
        };
        let (account_a, user_a) = seed_org(&pool).await;
        let (account_b, _user_b) = seed_org(&pool).await;
        let domain = format!("cs443-otherorg-{}.example", Uuid::new_v4().simple());

        sqlx::query(
            "INSERT INTO org_domains (account_id, domain, verification_token, claimed_by, verified_at, method) \
             VALUES ($1, $2, $3, $4, now(), 'email')",
        )
        .bind(account_a)
        .bind(&domain)
        .bind(Uuid::new_v4().simple().to_string())
        .bind(user_a)
        .execute(&pool)
        .await
        .expect("verify A's domain");

        assert_eq!(verified_org_domain(&pool, account_a).await, Ok(vec![domain]));
        assert_eq!(
            verified_org_domain(&pool, account_b).await,
            Ok(Vec::<String>::new()),
            "another org's verified domain must never answer for this one"
        );
    }
}

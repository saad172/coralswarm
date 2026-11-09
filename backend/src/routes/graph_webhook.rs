//! Microsoft Graph change-notification webhook (CS-117).
//!
//! Public endpoint (`/v1/webhooks/graph`) that backs Outlook reply auto-capture.
//! Graph cannot subscribe to a single conversation, so we keep one *inbox-wide*
//! subscription per account (created lazily on the first thread ingest — see
//! `connectors::microsoft::ensure_inbox_subscription`) and filter here: a
//! notification is acted on only when its message's `conversationId` belongs to
//! a thread the user explicitly ingested. Everything else is discarded without
//! ever being fetched or stored. See ADR-0008 for the privacy reasoning.
//!
//! Two request shapes arrive on this URL:
//!
//! * **Validation** — when a subscription is created/renewed, Graph POSTs with a
//!   `?validationToken=…` query and expects it echoed back as `text/plain`
//!   within 10s. No auth; this just proves we own the endpoint.
//! * **Notifications** — a JSON `{ "value": [ … ] }` batch. Each entry carries
//!   the `subscriptionId`, our `clientState` secret and the changed message
//!   id. The route checks each entry against the control plane before it
//!   holds anything ([`route_secrets`] then [`route_verdict`]: the
//!   subscription is ours and the `clientState` matches, constant-time,
//!   verified per unique subscription id for the whole batch), writes one durable
//!   `JobKind::IngestReceive` job per verified notification and returns
//!   `202` at once, so Graph never times out and retries (ADR-0016's receive
//!   stage). An unknown subscription or a wrong `clientState` is dropped
//!   without a job, so an unauthenticated sender can neither grow `jobs` nor
//!   occupy a genuine notification's dedupe key. `workers::ingest_receive`
//!   then verifies again on the row it acts on, fetches, matches and ingests
//!   through [`receive_notification`]; a transient failure retries on the
//!   queue's backoff. If the control plane cannot be read or the jobs cannot
//!   be written the batch answers `503` and Graph redelivers it.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use subtle::ConstantTimeEq;

use crate::{
    connectors::{microsoft, ConnectorError, MicrosoftConnector, NormalizedItem, TokenOwner},
    ingest::atom::IngestAtomRequest,
    jobs,
    middleware::auth::UserContext,
    routes::{
        connectors::load_creds_bg,
        oauth::store_token_bundle,
        workflows,
    },
    workers::ingest_receive::ReceiveError,
    AppState,
};

#[derive(Debug, Deserialize)]
pub struct ValidationParams {
    #[serde(rename = "validationToken")]
    validation_token: Option<String>,
}

/// A notification batch. Each entry is kept raw, so the receive job holds
/// exactly what Graph sent; [`parse_notification`] reads it in place.
#[derive(Debug, Deserialize)]
struct NotificationBatch {
    #[serde(default = "Vec::new")]
    value: Vec<serde_json::Value>,
}

/// One batch entry, read from the raw value without copying it.
fn parse_notification(raw: &serde_json::Value) -> Result<Notification, serde_json::Error> {
    Notification::deserialize(raw)
}

#[derive(Debug, Clone, Deserialize)]
struct Notification {
    #[serde(rename = "subscriptionId")]
    subscription_id: Option<String>,
    #[serde(rename = "clientState")]
    client_state: Option<String>,
    /// The OData path of the changed resource (e.g. a Teams message). Used to
    /// fetch Teams messages, which aren't addressable by a flat id like mail.
    resource: Option<String>,
    #[serde(rename = "resourceData")]
    resource_data: Option<ResourceData>,
    /// Present only on lifecycle notifications: `reauthorizationRequired`,
    /// `subscriptionRemoved`, or `missed` (ADR-0008). Absent on ordinary
    /// resource-change notifications.
    #[serde(rename = "lifecycleEvent")]
    lifecycle_event: Option<String>,
    /// `"created"`, `"updated"`, or `"deleted"` (m365 Train 4 PR 4.4 —
    /// subscriptions now request all three, see `connectors::microsoft::
    /// subscription_change_types`). Absent on a lifecycle notification;
    /// treated as `"created"` when absent on an ordinary one, matching every
    /// notification this backend ever received before this PR (subscriptions
    /// used to request `created` only, and Graph never sent this property's
    /// value when there was only one possible changeType to report).
    #[serde(rename = "changeType")]
    change_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ResourceData {
    /// Graph message id of the changed message.
    id: Option<String>,
}

/// Constant-time comparison of the notification's `clientState` against the
/// secret we stored at subscription creation. Constant-time so a timing oracle
/// can't be used to forge a valid `clientState`.
fn client_state_matches(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1
}

/// `POST /v1/webhooks/graph` — Graph subscription validation + notifications.
pub async fn graph_notifications(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ValidationParams>,
    body: Bytes,
) -> Response {
    // Validation handshake: echo the token verbatim as text/plain.
    if let Some(token) = params.validation_token {
        return (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )],
            token,
        )
            .into_response();
    }

    // Notification batch — parse, verify each entry against the control
    // plane, then hand the verified ones to the receive stage so we can ACK
    // fast (Graph retries on slow/failed responses). A dropped `deleted`
    // notification is unrecoverable (no missed-deletion backstop sweep
    // exists, see `workers::graph_subscriptions`'s module doc), which is why
    // the batch is written durably before the 202.
    let batch = match serde_json::from_slice::<NotificationBatch>(&body) {
        Ok(batch) => batch,
        Err(e) => {
            // Malformed body: ACK anyway so Graph stops retrying; nothing to do.
            tracing::warn!(error = %e, "unparseable graph notification body");
            return StatusCode::ACCEPTED.into_response();
        }
    };
    if batch.value.is_empty() {
        return StatusCode::ACCEPTED.into_response();
    }
    let Some(control) = state.db.as_ref() else {
        tracing::error!("graph notifications: no database — cannot verify or hold the batch");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // Parse every entry first, then verify per UNIQUE subscription id with a
    // bounded number of control-plane queries (`route_secrets`): a batch of
    // a hundred notifications on one subscription costs the same as one,
    // and Graph's response budget is not spent on serial lookups.
    let mut entries = Vec::with_capacity(batch.value.len());
    let mut subscription_ids: Vec<String> = Vec::new();
    for raw in batch.value {
        let n = match parse_notification(&raw) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "unparseable graph notification entry — skipping it");
                continue;
            }
        };
        // No subscription id: nothing could ever verify it.
        let Some(subscription_id) = n.subscription_id.as_deref() else {
            continue;
        };
        if !subscription_ids.iter().any(|id| id == subscription_id) {
            subscription_ids.push(subscription_id.to_string());
        }
        entries.push((n, raw));
    }
    let secrets = match route_secrets(control, &subscription_ids).await {
        Ok(secrets) => secrets,
        Err(e) => {
            // The control plane could not answer: hold nothing and let
            // Graph redeliver, rather than guess.
            tracing::error!(error = %e, subscriptions = subscription_ids.len(), "graph notifications: subscription lookup failed");
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    let mut received = Vec::with_capacity(entries.len());
    for (n, raw) in entries {
        let subscription_id = n.subscription_id.as_deref().unwrap_or_default();
        let provided = n.client_state.as_deref().unwrap_or_default();
        match route_verdict(secrets.get(subscription_id), provided) {
            RouteVerdict::Verified => {
                received.push((jobs::ReceivedEvent::GraphNotification { notification: raw }, dedupe_key(&n)));
            }
            RouteVerdict::Unverifiable => {
                received.push((jobs::ReceivedEvent::GraphNotification { notification: raw }, None));
            }
            RouteVerdict::Unknown => {
                tracing::debug!(%subscription_id, "graph notification for unknown subscription — not held");
            }
            RouteVerdict::Mismatch => {
                tracing::warn!(%subscription_id, "graph notification clientState mismatch — not held");
            }
        }
    }
    if !crate::workers::ingest_receive::enqueue(&state, received).await {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    StatusCode::ACCEPTED.into_response()
}

/// What the control plane says about a notification before the route holds
/// it ([`route_verdict`] on what [`route_secrets`] read).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteVerdict {
    /// The subscription is ours and the `clientState` is its own.
    Verified,
    /// No subscription of ours has this id. Not held.
    Unknown,
    /// The subscription is ours but the `clientState` is not its own. Not held.
    Mismatch,
    /// An org mail route written before migration `0403` carries no
    /// `clientState` hash yet (the next `sync_mail_routes` fills it in).
    /// Held without a dedupe key; the worker verifies it against the tenant
    /// row, as every notification was before the route checked.
    Unverifiable,
}

/// What the control plane holds for one subscription id, for the route's
/// verification ([`route_secrets`], [`route_verdict`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RouteSecret {
    /// The subscription's own `clientState`: a Teams doorbell
    /// (`teams_container_subscriptions`) or a personal subscription
    /// (`graph_subscriptions`), both on the control plane.
    ClientState(String),
    /// An org mail subscription: its row lives on the org's tenant
    /// database, so the control route carries the SHA-256 of its
    /// `clientState` (`microsoft::client_state_sha256`, migration `0403`).
    ClientStateHash(String),
    /// An org mail route from before migration `0403`, with no hash yet
    /// (the next `sync_mail_routes` fills it in).
    Unhashed,
}

/// The cheap authenticity material for a batch of subscription ids, read
/// from the control plane in a bounded number of queries whatever the
/// batch size: one over `graph_mail_routes` and one over
/// `teams_container_subscriptions` (`WHERE subscription_id = ANY($1)`), one
/// resolving the personal subscriptions to their accounts, and one scoped
/// read per distinct personal account in the batch. No tenant hop. An id no
/// table carries is absent from the map. Where the same id is somehow in
/// more than one table, a doorbell wins over a personal subscription, which
/// wins over a mail route, as the one-id check always did. `Err` when the
/// control plane could not be read.
pub(crate) async fn route_secrets(
    control: &sqlx::PgPool,
    subscription_ids: &[String],
) -> Result<HashMap<String, RouteSecret>, ConnectorError> {
    let mut secrets = HashMap::with_capacity(subscription_ids.len());
    if subscription_ids.is_empty() {
        return Ok(secrets);
    }
    for (id, hash) in microsoft::mail_route_client_state_hashes(control, subscription_ids).await? {
        secrets.insert(id, hash.map_or(RouteSecret::Unhashed, RouteSecret::ClientStateHash));
    }
    let mut by_account: HashMap<uuid::Uuid, Vec<String>> = HashMap::new();
    for (id, account_id) in microsoft::subscription_accounts(control, subscription_ids).await? {
        by_account.entry(account_id).or_default().push(id);
    }
    for (account_id, ids) in by_account {
        for (id, client_state) in microsoft::subscription_client_states_for_account(control, account_id, &ids).await? {
            secrets.insert(id, RouteSecret::ClientState(client_state));
        }
    }
    for (id, client_state) in crate::connectors::teams_doorbell::client_states_for(control, subscription_ids).await? {
        secrets.insert(id, RouteSecret::ClientState(client_state));
    }
    Ok(secrets)
}

/// Is `provided` the `clientState` of the subscription whose material is
/// `secret`? Both comparisons are constant-time. No I/O.
pub(crate) fn route_verdict(secret: Option<&RouteSecret>, provided: &str) -> RouteVerdict {
    let verdict = |matches: bool| if matches { RouteVerdict::Verified } else { RouteVerdict::Mismatch };
    match secret {
        None => RouteVerdict::Unknown,
        Some(RouteSecret::Unhashed) => RouteVerdict::Unverifiable,
        Some(RouteSecret::ClientState(expected)) => verdict(client_state_matches(provided, expected)),
        Some(RouteSecret::ClientStateHash(hash)) => verdict(client_state_matches(&microsoft::client_state_sha256(provided), hash)),
    }
}

/// The receive job's dedupe key: a redelivery of the same change before the
/// first job is claimed adds nothing. Only a verified notification is keyed,
/// so no forged one can hold a genuine one's key. `None` when the
/// notification names no resource (neither `resource` nor
/// `resourceData.id`): keyed on the subscription and change alone,
/// different notifications would collapse into one, so such a notification
/// is held without a key.
fn dedupe_key(n: &Notification) -> Option<String> {
    let subscription_id = n.subscription_id.as_deref().unwrap_or_default();
    let change = n.lifecycle_event.as_deref().or(n.change_type.as_deref()).unwrap_or("created");
    let resource = n
        .resource
        .as_deref()
        .or(n.resource_data.as_ref().and_then(|r| r.id.as_deref()))
        .map(str::trim)
        .filter(|r| !r.is_empty())?;
    Some(format!("ingest_receive:graph:{subscription_id}:{change}:{resource}"))
}

/// Verify and act on one received notification — `workers::ingest_receive`'s
/// entry for [`jobs::ReceivedEvent::GraphNotification`]: lifecycle events
/// renew or recreate the subscription, everything else is fetched, matched
/// and ingested. A payload that no longer parses is
/// [`ReceiveError::Malformed`] (never retried); a provider or storage
/// failure is classified by the worker.
pub(crate) async fn receive_notification(
    state: Arc<AppState>,
    notification: serde_json::Value,
) -> Result<(), ReceiveError> {
    let n = parse_notification(&notification)
        .map_err(|e| ReceiveError::Malformed(format!("graph notification: {e}")))?;
    if n.lifecycle_event.is_some() {
        process_lifecycle(state, n).await?;
    } else {
        process_notification(state, n).await?;
    }
    Ok(())
}

type SubscriptionRow = (uuid::Uuid, String, String, String, Option<uuid::Uuid>);

/// Resolve a notification's `subscriptionId` to its row AND the pool that
/// holds the account's content-plane state (`graph_subscriptions`,
/// `outlook_watched_conversations`, workflows).
///
/// A Personal account's rows live on the control/consumer pool, found by the
/// `SECURITY DEFINER` resolver (the original, unchanged path; content pool ==
/// control pool). An ORG account's rows live on the org's own tenant
/// database, unreachable from a bare subscription id: the control-plane
/// `graph_mail_routes` pointer names the account, and the same decision
/// table the ingest hand-off uses names its tenant pool. Before this hop
/// existed every org mail notification (per-thread "Keep saving new
/// replies") resolved to "unknown subscription" and was silently dropped.
///
/// An org whose tenant plane cannot be resolved right now (a lookup failed
/// or timed out, the pool could not be built) is an error, so the receive
/// job retries; one that must never receive content (revoked) resolves to
/// `None`, as an unknown subscription does.
async fn resolve_subscription(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    subscription_id: &str,
) -> Result<Option<(SubscriptionRow, sqlx::PgPool)>, ConnectorError> {
    resolve_subscription_via(control_pool, subscription_id, |account_id| async move {
        match resolve_graph_ingest_target(state, account_id).await {
            GraphIngestTarget::Sync => Ok(Some(control_pool.clone())),
            GraphIngestTarget::Enqueue { pool, .. } => Ok(Some(pool)),
            GraphIngestTarget::Drop => {
                tracing::info!(%account_id, "routed graph notification: org refused (revoked) — dropping");
                Ok(None)
            }
            GraphIngestTarget::Unavailable(why) => Err(ConnectorError::Internal(format!(
                "routed graph notification: tenant plane unavailable: {why}"
            ))),
        }
    })
    .await
}

/// [`resolve_subscription`] with the account -> content-pool decision injected,
/// so the control-lookup -> route -> tenant-row composition is testable against
/// two real databases without a full tenant runtime. The decision answers
/// `Ok(None)` for an account whose content must not land and `Err` for one
/// whose plane cannot be resolved right now.
pub(crate) async fn resolve_subscription_via<F, Fut>(
    control_pool: &sqlx::PgPool,
    subscription_id: &str,
    content_pool_for: F,
) -> Result<Option<(SubscriptionRow, sqlx::PgPool)>, ConnectorError>
where
    F: FnOnce(uuid::Uuid) -> Fut,
    Fut: std::future::Future<Output = Result<Option<sqlx::PgPool>, ConnectorError>>,
{
    if let Some(row) = microsoft::subscription_by_id(control_pool, subscription_id).await? {
        return Ok(Some((row, control_pool.clone())));
    }
    let Some(account_id) = microsoft::mail_route_account(control_pool, subscription_id).await?
    else {
        return Ok(None);
    };
    let Some(content_pool) = content_pool_for(account_id).await? else {
        return Ok(None);
    };
    let row = microsoft::subscription_by_id_for_account(&content_pool, account_id, subscription_id)
        .await?;
    Ok(row.map(|r| (r, content_pool)))
}

/// Verify one notification's `clientState`, then fetch the changed message and —
/// only if its conversation is watched — ingest it as the opted-in user.
async fn process_notification(state: Arc<AppState>, n: Notification) -> Result<(), ConnectorError> {
    let Some(subscription_id) = n.subscription_id.clone() else {
        return Ok(());
    };
    let pool = state
        .db
        .clone()
        .ok_or_else(|| ConnectorError::Internal("no database".into()))?;

    // Teams channel/chat doorbell (Phase G wave 3, item 56). One control-plane
    // subscription rings every watcher; each fetch uses that watcher's token.
    // Mail and app-mode subscriptions still resolve through `graph_subscriptions`
    // below.
    if let Some(door) =
        crate::connectors::teams_doorbell::lookup_subscription(&pool, &subscription_id).await?
    {
        let provided = n.client_state.clone().unwrap_or_default();
        if !client_state_matches(&provided, &door.client_state) {
            tracing::warn!(%subscription_id, "teams doorbell clientState mismatch — dropping");
            return Ok(());
        }
        return deliver_teams_doorbell(state, door, n).await;
    }

    // Resolve the subscription → account + expected secret + watched resource +
    // tenant + owning member (m365 enterprise train PR 3.3 adds the last one —
    // `graph_subscriptions.owner_user_id`, migration `0190`/`0068`).
    let Some(((account_id, expected_state, resource, tenant_id, owner_user_id), content_pool)) =
        resolve_subscription(&state, &pool, &subscription_id).await?
    else {
        tracing::debug!(%subscription_id, "notification for unknown subscription — ignoring");
        return Ok(());
    };
    // The `TokenOwner` this subscription's mailbox belongs to — `AccountWide`
    // for Personal-plane mail (and every Teams/transcript resource, which
    // never carries an owner), `Member(uid)` for an org member's own Outlook
    // connection. Used below for the delegated (mail) fetch; Teams/transcript
    // dispatch (further down) still resolves its own owner independently via
    // `teams_watched_by_container`, unaffected by this.
    let mail_owner = owner_user_id
        .map(TokenOwner::Member)
        .unwrap_or(TokenOwner::AccountWide);

    // Constant-time verify the shared secret before doing anything else.
    let provided = n.client_state.clone().unwrap_or_default();
    if !client_state_matches(&provided, &expected_state) {
        tracing::warn!(%subscription_id, "graph notification clientState mismatch — dropping");
        return Ok(());
    }

    // A member-owned subscription (an org member's own Outlook mail, or a
    // delegated Teams/transcript resource) whose owner has LEFT the account
    // is dropped before any fetch with their token (settings launch PR 2.8),
    // and their capture is offboarded so the subscription stops for good.
    // Account-wide subscriptions (no owner) are the org's and pass through.
    if let Some(owner) = owner_user_id {
        let plane = crate::crypto::DekPlane::resolve_for_account(&state, &pool, account_id).await.ok();
        if !crate::connectors::member_offboarding::admit_capture_or_offboard(
            &state.http_client,
            &pool,
            plane.as_ref(),
            account_id,
            owner,
            "graph_notification",
        )
        .await
        {
            return Ok(());
        }
    }

    // Every Graph call from here on (this notification's own fetch, plus any
    // nested backfill it triggers) runs under one rate-limit scope — m365
    // Train 2 PR 2.6. Keyed via `rate_key` (CodeRabbit review, PR #673): the
    // AAD `tenant_id` for an app-only/RSC (Teams) notification, `account_id`
    // for a delegated (mail) one — `tenant_id` here is straight off the
    // `graph_subscriptions` row `subscription_by_id` just read, and is empty
    // exactly for delegated resources (see `ensure_subscription_for`'s doc),
    // so this dispatches on the same signal that already distinguishes the
    // two auth modes. See `graph_rate_limit`'s module doc.
    crate::graph_rate_limit::scoped(crate::graph_rate_limit::rate_key(account_id, &tenant_id), async move {
        // m365 Train 4 PR 4.4: subscriptions now request `created,updated,
        // deleted` (see `connectors::microsoft::subscription_change_types`),
        // so dispatch on `changeType` before any resource-kind branching
        // below — `deleted`/`updated` need entirely different handling than
        // an ordinary create/reply, which every branch past this match was
        // written exclusively for.
        match n.change_type.as_deref().unwrap_or("created") {
            "deleted" => {
                return process_deleted_notification(state, account_id, resource, n, owner_user_id).await;
            }
            "updated" => {
                // Accepting `updated` is a consequence of subscribing to it
                // at all (Graph delivers it once requested) — it is NOT a
                // request to build a correction pipeline here. `ingest_atom`/
                // `GraphMessageIngest`'s dedup is insert-only (`ON CONFLICT
                // DO NOTHING` on `(graph_id, source_type, source_id)`), so
                // simply re-running the create path for an edited message
                // would silently no-op rather than refresh its content —
                // worse than an explicit, documented skip, since it would
                // look like nothing happened rather than say so. Follow-up
                // (not this PR): a real update path needs either an UPDATE
                // of the existing atom's `content_blob_key`/embedding, or
                // `delete_atom_cascade` + re-ingest (safe to build now that
                // deletion propagation exists) — both are genuine new scope.
                tracing::info!(
                    %subscription_id, %resource,
                    "graph update notification received — not yet handled (see module doc), \
                     skipping"
                );
                return Ok(());
            }
            _ => {}
        }
        // Meeting transcript available (CS-121). RSC uses the app token.
        // The delegated organiser resource (item 51) fetches on the
        // subscription owner's own token.
        if resource.contains("getAllTranscripts") {
            return process_transcript_notification(
                state,
                pool,
                account_id,
                tenant_id,
                owner_user_id,
                &resource,
                n,
            )
            .await;
        }
        // Teams channel/chat messages take a different fetch path than mail (CS-119).
        if microsoft::is_teams_resource(&resource) {
            return process_teams_notification(state, pool, account_id, resource, owner_user_id, n)
                .await;
        }

        let Some(message_id) = n.resource_data.and_then(|r| r.id) else {
            return Ok(());
        };

        // We need a delegated token to read the message. Load + refresh as
        // needed. `mail_owner` (m365 enterprise train PR 3.3) — the exact
        // owner `graph_subscriptions` recorded when this subscription was
        // created (`AccountWide` on Personal, `Member(uid)` for an org
        // member's own mailbox); no fallback, same discipline `routes::
        // connectors::load_creds`'s own doc comment states for a credential
        // read.
        // Signature(clientState)-authenticated webhook: no `TenancyDecision`
        // exists here, so the DEK plane is resolved from the account itself
        // through THE decision table (`crypto::DekPlane::resolve_for_org`).
        // One indexed `accounts` lookup by primary key, once per
        // notification — and the reason an ORG account's mailbox can now be
        // read at all: `crypto::resolve_dek`'s hardcoded `TenantId::Consumer`
        // could only ever miss a key that lives in the org's tenant DB.
        let dek_plane = crate::crypto::DekPlane::resolve_for_account(&state, &pool, account_id)
            .await
            .map_err(crate::workers::ingest_receive::plane_error)?;
        let Some(mut creds) = load_creds_bg(&pool, &dek_plane, account_id, "microsoft", mail_owner).await?
        else {
            tracing::warn!(%account_id, "no microsoft connection for notified account");
            return Ok(());
        };
        let connector = MicrosoftConnector;
        connector
            .refresh_if_needed(&state.http_client, &mut creds)
            .await?;
        if let Some(refreshed) = creds.refreshed.as_ref() {
            if let Err(_resp) =
                store_token_bundle(&pool, &dek_plane, account_id, "microsoft", mail_owner, refreshed).await
            {
                tracing::warn!(%account_id, "failed to persist refreshed microsoft token from webhook");
            }
        }

        // Fetch the changed message (with its conversationId) and decide.
        let msg =
            microsoft::fetch_message(&state.http_client, &creds.access_token, &message_id).await?;
        let Some(conversation_id) = msg.conversation_id().map(str::to_string) else {
            return Ok(());
        };

        // (1) Already a watched thread → ingest just this new message (the reply).
        if let Some((graph_id, user_id, follow_since)) =
            microsoft::watched_conversation_graph(&content_pool, account_id, &conversation_id).await?
        {
            if !msg.is_future_reply(follow_since) { return Ok(()); }
            let item = microsoft::message_to_item(msg);
            // A failure surfaces: a transient one fails the receive job so
            // it retries, a permanent one completes it.
            if ingest_message(&state, account_id, user_id, graph_id, item).await? {
                tracing::info!(%account_id, %conversation_id, "auto-captured reply into watched thread");
            } else {
                tracing::info!(%account_id, %conversation_id, "auto-capture withheld the reply");
            }
            return Ok(());
        }

        // Existing unfollowed membership is an explicit opt-out. It is
        // not a missing watch eligible for workflow registration; otherwise
        // the next matching notification would silently undo PUT/DELETE.
        if microsoft::watched_status(&content_pool, account_id, &conversation_id).await?.is_some() {
            return Ok(());
        }

        // (2) Not yet watched → does an enabled workflow match this message? If so,
        // start watching the conversation and ingest the whole thread (CS-118).
        let folder = microsoft::folder_name_from_resource(&resource);
        let match_input = msg.match_input(folder);
        let workflows = workflows::enabled_for_account(&content_pool, account_id).await?;
        let Some(wf) = workflows
            .iter()
            .find(|w| workflows::matches(&w.filter, &match_input))
        else {
            tracing::debug!(%account_id, "message matches no watched thread or workflow — discarding");
            return Ok(());
        };

        // `true` (gmail-options backend train, PR 1 of 4): a workflow-rule
        // auto-watch, not the UI's opt-in follow toggle — always follow,
        // byte-identical to this call site's pre-PR behavior.
        microsoft::upsert_watched_conversation(
            &content_pool,
            account_id,
            wf.user_id,
            wf.graph_id,
            &conversation_id,
            true,
        )
        .await?;
        match microsoft::fetch_conversation(
            &state.http_client,
            &creds.access_token,
            &conversation_id,
            None,
        )
        .await
        {
            Ok(page) => {
                let complete = page.complete;
                // The thread's messages follow the fan-out rule: a transient
                // failure for any of them retries the notification (the
                // retry finds the conversation watched and dedupes on the
                // atom key), a permanent one is logged.
                let mut fan_out = crate::workers::ingest_receive::FanOut::new();
                for m in page.items {
                    let item = microsoft::message_to_item(m);
                    if let Err(e) = ingest_message(&state, account_id, wf.user_id, wf.graph_id, item).await {
                        fan_out.failed(e, |e, retry| {
                            tracing::warn!(error = %e, %account_id, %conversation_id, retry, "workflow match: message ingest failed")
                        });
                    }
                }
                if let Err(e) = microsoft::set_conversation_backfill_state(
                    &content_pool,
                    account_id,
                    &conversation_id,
                    page.next_cursor.as_deref(),
                    complete,
                )
                .await
                {
                    tracing::warn!(error = %e, %conversation_id, "workflow match: cursor persist failed");
                }
                tracing::info!(%account_id, %conversation_id, complete, "workflow auto-captured a new thread");
                fan_out.finish(())?;
            }
            Err(e) => tracing::warn!(error = %e, %conversation_id, "workflow match: thread fetch failed"),
        }
        Ok(())
    })
    .await
}

/// Fan one doorbell notification out to every watcher of that container.
/// Each watcher fetches on their own token. A 403/404 drops only that watcher.
/// Message bodies are not reused across watchers.
async fn deliver_teams_doorbell(
    state: Arc<AppState>,
    door: crate::connectors::teams_doorbell::DoorbellSubscription,
    n: Notification,
) -> Result<(), ConnectorError> {
    let pool = state
        .db
        .clone()
        .ok_or_else(|| ConnectorError::Internal("no database".into()))?;
    match n.change_type.as_deref().unwrap_or("created") {
        "deleted" => {
            let watchers: Vec<(uuid::Uuid, uuid::Uuid)> = sqlx::query_as(
                "SELECT account_id, user_id FROM teams_container_watchers \
                 WHERE ms_tenant_id = $1 AND resource = $2",
            )
            .bind(&door.ms_tenant_id)
            .bind(&door.resource)
            .fetch_all(&pool)
            .await
            .map_err(|e| ConnectorError::Internal(e.to_string()))?;
            // The fan-out rule: one watcher's transient failure retries the
            // whole notification (a deletion that already went through for
            // another watcher is a no-op the second time), a permanent one
            // is logged.
            let mut fan_out = crate::workers::ingest_receive::FanOut::new();
            for (account_id, watcher_user_id) in watchers {
                if let Err(e) = process_deleted_notification(
                    state.clone(),
                    account_id,
                    door.resource.clone(),
                    n.clone(),
                    Some(watcher_user_id),
                )
                .await
                {
                    fan_out.failed(e, |e, retry| {
                        tracing::warn!(error = %e, %account_id, retry, "teams doorbell delete propagation failed")
                    });
                }
            }
            return fan_out.finish(());
        }
        "updated" => {
            tracing::info!(
                resource = %door.resource,
                "teams doorbell update notification — not yet handled, skipping"
            );
            return Ok(());
        }
        _ => {}
    }

    let Some((source_type, container_id)) =
        microsoft::parse_subscribed_teams_resource(&door.resource)
    else {
        return Ok(());
    };
    let fetch_path = n.resource.clone().or_else(|| {
        n.resource_data
            .as_ref()
            .and_then(|r| r.id.clone())
            .map(|id| format!("{}/{}", door.resource, id))
    });
    let Some(path) = fetch_path else {
        return Ok(());
    };
    let ms_tenant_id = door.ms_tenant_id.clone();
    let resource = door.resource.clone();
    // The fan-out rule across the watchers: a transient failure for any of
    // them retries the notification as a whole (a watcher already fetched
    // dedupes on the atom key); `Gone` and `Skip` are not failures.
    let fan_out = Arc::new(std::sync::Mutex::new(crate::workers::ingest_receive::FanOut::new()));
    crate::connectors::teams_doorbell::for_each_watcher(
        &pool,
        &ms_tenant_id,
        &resource,
        |watcher| {
            let state = state.clone();
            let pool = pool.clone();
            let path = path.clone();
            let source_type = source_type.clone();
            let container_id = container_id.clone();
            let ms_tenant_id = ms_tenant_id.clone();
            let resource = resource.clone();
            let fan_out = fan_out.clone();
            async move {
                match fetch_doorbell_watcher(
                    &state,
                    &pool,
                    &ms_tenant_id,
                    &resource,
                    watcher,
                    &path,
                    &source_type,
                    &container_id,
                )
                .await
                {
                    Ok(()) => crate::connectors::teams_doorbell::WatcherOp::Keep,
                    Err(DoorbellFetch::Gone) => crate::connectors::teams_doorbell::WatcherOp::Drop,
                    Err(DoorbellFetch::Skip) => crate::connectors::teams_doorbell::WatcherOp::Keep,
                    Err(DoorbellFetch::Transient(e)) => {
                        fan_out.lock().unwrap_or_else(|p| p.into_inner()).failed(e, |e, retry| {
                            tracing::warn!(error = %e, retry, "teams doorbell: watcher fetch failed")
                        });
                        crate::connectors::teams_doorbell::WatcherOp::Keep
                    }
                }
            }
        },
    )
    .await?;
    let fan_out = std::mem::replace(
        &mut *fan_out.lock().unwrap_or_else(|p| p.into_inner()),
        crate::workers::ingest_receive::FanOut::new(),
    );
    fan_out.finish(())
}

/// Why one doorbell watcher's fetch did not store a message.
enum DoorbellFetch {
    /// Graph answered 403/404 for this watcher: they can no longer read the
    /// container, so the watcher row goes.
    Gone,
    /// Nothing to do for this watcher now (no watch, no token, offboarded,
    /// a permanent provider answer). The watcher stays.
    Skip,
    /// A failure a retry can change (`ingest_receive::transient`): storage,
    /// a timeout, a 5xx. The notification retries as a whole.
    Transient(ConnectorError),
}

impl DoorbellFetch {
    /// [`Transient`](Self::Transient) or [`Skip`](Self::Skip), by the
    /// failure's class.
    fn from_error(e: ConnectorError) -> Self {
        if crate::workers::ingest_receive::transient(&e) {
            DoorbellFetch::Transient(e)
        } else {
            DoorbellFetch::Skip
        }
    }

    fn storage(e: impl std::fmt::Display) -> Self {
        DoorbellFetch::Transient(ConnectorError::Internal(e.to_string()))
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_doorbell_watcher(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    ms_tenant_id: &str,
    resource: &str,
    watcher: crate::connectors::teams_doorbell::ContainerWatcher,
    path: &str,
    source_type: &str,
    container_id: &str,
) -> Result<(), DoorbellFetch> {
    let account_id = watcher.account_id;
    let user_id = watcher.user_id;
    let plane = crate::routes::slack::account_plane(pool, account_id)
        .await
        .map_err(DoorbellFetch::storage)?;
    let clerk_org_id: Option<String> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .map_err(DoorbellFetch::storage)?
            .flatten();
    let content_pool = match plane {
        crate::routes::slack::AccountPlane::Personal => pool.clone(),
        crate::routes::slack::AccountPlane::Org => {
            let Some(org) = clerk_org_id.as_deref() else {
                return Err(DoorbellFetch::Skip);
            };
            match resolve_org_tenant_pool_outcome(state, pool, org).await {
                TenantPoolOutcome::Pool(pool) => pool,
                TenantPoolOutcome::Refused => return Err(DoorbellFetch::Skip),
                TenantPoolOutcome::Unavailable(why) => return Err(DoorbellFetch::storage(why)),
            }
        }
    };
    let graph_id: Option<uuid::Uuid> = {
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&content_pool, account_id)
            .await
            .map_err(DoorbellFetch::storage)?;
        let id = sqlx::query_scalar(
            "SELECT graph_id FROM teams_watched_resources \
             WHERE account_id = $1 AND user_id = $2 AND auth_mode = 'delegated' \
               AND (channel_id = $3 OR chat_id = $3) \
             LIMIT 1",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(container_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(DoorbellFetch::storage)?;
        tx.rollback().await.ok();
        id
    };
    let Some(graph_id) = graph_id else {
        return Err(DoorbellFetch::Skip);
    };

    let owner = match plane {
        crate::routes::slack::AccountPlane::Org => TokenOwner::Member(user_id),
        crate::routes::slack::AccountPlane::Personal => TokenOwner::AccountWide,
    };
    // Classified like every plane failure: a lookup or pool failure is
    // transient, a gone account or frozen tenant is permanent (`Skip`).
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, pool, account_id)
        .await
        .map_err(|e| DoorbellFetch::from_error(crate::workers::ingest_receive::plane_error(e)))?;
    // A doorbell watcher who has left the org is never fetched as (settings
    // launch PR 2.8); their watcher rows and token are offboarded instead.
    if plane == crate::routes::slack::AccountPlane::Org
        && !crate::connectors::member_offboarding::admit_capture_or_offboard(
            &state.http_client,
            pool,
            Some(&dek_plane),
            account_id,
            user_id,
            "teams_doorbell",
        )
        .await
    {
        return Err(DoorbellFetch::Skip);
    }
    let mut creds = match load_creds_bg(pool, &dek_plane, account_id, "microsoft", owner).await {
        Ok(Some(c)) => c,
        Ok(None) => return Err(DoorbellFetch::Skip),
        Err(e) => return Err(DoorbellFetch::from_error(e)),
    };
    let connector = MicrosoftConnector;
    if let Err(e) = connector
        .refresh_if_needed(&state.http_client, &mut creds)
        .await
    {
        return Err(DoorbellFetch::from_error(e));
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        let _ =
            store_token_bundle(pool, &dek_plane, account_id, "microsoft", owner, refreshed).await;
    }
    let msg = match microsoft::fetch_teams_message(&state.http_client, &creds.access_token, path)
        .await
    {
        Ok(m) => m,
        Err(e) => {
            let status = e.upstream_status();
            if status == Some(403) || status == Some(404) {
                tracing::info!(
                    %account_id, %user_id, status,
                    "teams doorbell: watcher cannot read this resource — dropping that watcher only"
                );
                return Err(DoorbellFetch::Gone);
            }
            tracing::warn!(error = %e, %account_id, %user_id, "teams doorbell: fetch failed");
            return Err(DoorbellFetch::from_error(e));
        }
    };
    if let Some(item) = microsoft::teams_message_to_item(&msg, source_type, container_id) {
        match ingest_message(state, account_id, user_id, graph_id, item).await {
            Ok(true) => {
                let _ = crate::connectors::teams_doorbell::note_successful_fetch(
                    pool,
                    ms_tenant_id,
                    resource,
                    account_id,
                    user_id,
                )
                .await;
            }
            Ok(false) => {}
            Err(e) => return Err(DoorbellFetch::from_error(e)),
        }
    }
    Ok(())
}

/// Handle a Teams channel/chat message notification (CS-119): if the resource is
/// a watched channel/chat, fetch the changed message and ingest it as the opting-
/// in user. Discards messages on non-watched resources.
async fn process_teams_notification(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    account_id: uuid::Uuid,
    subscribed_resource: String,
    owner_user_id: Option<uuid::Uuid>,
    n: Notification,
) -> Result<(), ConnectorError> {
    let Some((source_type, container_id)) =
        microsoft::parse_subscribed_teams_resource(&subscribed_resource)
    else {
        return Ok(());
    };
    let watchers = microsoft::teams_watched_by_container(&pool, account_id, &container_id).await?;
    if watchers.is_empty() {
        tracing::debug!(%account_id, %container_id, "teams notification for non-watched resource — discarding");
        return Ok(());
    };

    // Prefer the notification's own resource path (handles replies); fall back to
    // the subscribed resource + message id. Resolved once; each watcher fetches
    // with its own token (S4).
    let fetch_path = n.resource.clone().or_else(|| {
        n.resource_data
            .as_ref()
            .and_then(|r| r.id.clone())
            .map(|id| format!("{subscribed_resource}/{id}"))
    });
    let Some(path) = fetch_path else {
        return Ok(());
    };

    let plane_pool = state.control_db().unwrap_or(&pool);
    let plane = crate::routes::slack::account_plane(plane_pool, account_id)
        .await
        .unwrap_or(crate::routes::slack::AccountPlane::Personal);
    // Phase G item 48: an RSC app-mode watcher fetches with the tenant app
    // token and lands that content in the watcher's graph. After the grace
    // that watcher is skipped. Delegated watchers still fetch on their own
    // token — content is not copied across credentials.
    let capture_open =
        crate::services::org_install_grace::org_install_capture_open(plane_pool).await;
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(&state, &pool, account_id)
        .await
        .map_err(crate::workers::ingest_receive::plane_error)?;
    let creds_pool = state.control_db().unwrap_or(&pool);

    // The fan-out rule across the watchers: a transient ingest failure for
    // any of them retries the notification, a permanent one is logged.
    let mut fan_out = crate::workers::ingest_receive::FanOut::new();
    for (graph_id, user_id, tenant_id, auth_mode) in watchers {
        if crate::services::org_install_grace::rsc_app_capture_blocked(capture_open, &auth_mode) {
            continue;
        }
        // Per-watcher subscriptions (S4) notify once per delegated owner.
        // When this notification carries a Member owner, ingest only that
        // watcher's row — otherwise N watchers × N notifications is N² Graph
        // fetches. Legacy account-wide / app subscriptions (`None`) keep
        // the fan-out across every watcher.
        if let Some(owner) = owner_user_id {
            if auth_mode != "delegated" || user_id != owner {
                continue;
            }
        }
        // App-mode (RSC) resources read with an app-only token for the tenant;
        // delegated resources use the watching member's own token (S4):
        // Member on the org plane, AccountWide on personal.
        let token = if auth_mode == "app" && !tenant_id.is_empty() {
            match microsoft::app_token(&state.http_client, &tenant_id).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, %user_id, "teams notification: app token failed");
                    continue;
                }
            }
        } else {
            let owner = match plane {
                crate::routes::slack::AccountPlane::Org => TokenOwner::Member(user_id),
                crate::routes::slack::AccountPlane::Personal => TokenOwner::AccountWide,
            };
            let mut creds =
                match load_creds_bg(creds_pool, &dek_plane, account_id, "microsoft", owner).await {
                    Ok(Some(c)) => c,
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!(
                            error = %e, %account_id, %user_id,
                            "teams notification: load creds failed"
                        );
                        continue;
                    }
                };
            let connector = MicrosoftConnector;
            if let Err(e) = connector
                .refresh_if_needed(&state.http_client, &mut creds)
                .await
            {
                tracing::warn!(error = %e, %account_id, %user_id, "teams notification: token refresh failed");
                continue;
            }
            if let Some(refreshed) = creds.refreshed.as_ref() {
                let _ = store_token_bundle(
                    creds_pool,
                    &dek_plane,
                    account_id,
                    "microsoft",
                    owner,
                    refreshed,
                )
                .await;
            }
            creds.access_token
        };

        let msg = match microsoft::fetch_teams_message(&state.http_client, &token, &path).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, %user_id, "teams notification: fetch failed");
                continue;
            }
        };
        if let Some(item) = microsoft::teams_message_to_item(&msg, &source_type, &container_id) {
            match ingest_message(&state, account_id, user_id, graph_id, item).await {
                Ok(true) => tracing::info!(%account_id, %container_id, %user_id, "auto-captured teams message"),
                Ok(false) => {}
                Err(e) => fan_out.failed(e, |e, retry| {
                    tracing::warn!(error = %e, %account_id, %container_id, %user_id, retry, "teams message ingest failed")
                }),
            }
        }
    }
    fan_out.finish(())
}

/// Handle a `deleted` change notification (m365 Train 4 PR 4.4 — upstream
/// deletion propagation): propagate the deletion via `services::deletion::
/// delete_source`, which cascades to every promoted copy within the SAME
/// account (see that module's own "Propagation" doc section).
///
/// **Known limit — same-account-only.** A coral promoted from this account
/// into a DIFFERENT account's ocean (cross-DB org-tenant promotion —
/// `services::promotion`'s cross-DB path) is NOT reached from here: no
/// single `PgPool` can see both databases at once, so an upstream deletion
/// only ever removes the copies living in the SAME physical database this
/// notification's account is on. This is `delete_source`'s own pre-existing,
/// documented gap (`services::deletion`'s module doc, "Known, documented
/// gap"), inherited unchanged by every caller including this one — a
/// promoted copy in another account's tenant database needs its own
/// target-side retraction (`services::retraction::unpublish_atom`, run from
/// within that target database) or a future cross-pool orchestrator.
///
/// Both Outlook mail and Teams channel/chat messages reach here (their
/// subscriptions widened to `created,updated,deleted` — see `connectors::
/// microsoft::subscription_change_types`). The meeting-transcript stream
/// never will: it stays subscribed `created`-only, so Graph never sends this
/// changeType for it (defensive early-return below regardless).
///
/// # Identity, not content
/// A deleted item can no longer be fetched from Graph — `resourceData.id` is
/// the only thing this notification carries. That is exactly the natural key
/// (`source_type`, `source_id`) atoms are keyed on, so no fetch is needed:
/// for mail, `source_id` IS the Graph message id (`message_to_item`'s own
/// `source_id: m.id`); for Teams, it is reconstructed with the SAME
/// `"{container_id}:{message_id}"` shape `teams_message_to_item` stamps on
/// ingest.
///
/// # Finding the right graph, without a conversation/container lookup
/// A deleted mail notification carries no `conversationId` (unlike a live
/// reply), so `outlook_watched_conversations` (keyed on conversationId) can't
/// answer "which graph" the way [`process_notification`]'s create/reply path
/// does. This looks the atom up by its own natural key directly
/// (`microsoft::atom_graph_for_source`) instead — exact regardless of
/// resource kind, and it doubles as the "was this ever ingested at all"
/// check: `None` means it wasn't (never opted into a watched thread/
/// channel), so there is nothing to delete and no `connector_audit_log` row
/// worth writing — mirrors `google_gmail::propagate_deletions`'/
/// `google_calendar::propagate_deletions`'s identical "skip audit noise for
/// a source that was never ingested" precedent (m365 enterprise train PR
/// 4.2's sibling provider-push deletion paths).
///
/// # Plane
/// Resolved via [`resolve_graph_ingest_target`] — the SAME function the live
/// ingest path uses (m365 Train 2 PR 2.3) — so this always operates on
/// whichever pool (consumer, or the org's own tenant pool) actually holds
/// this account's `atoms`/`graphs` rows. `Drop` (a revoked org) discards
/// the deletion: that org never lands content again. `Unavailable` (a
/// lookup failed or timed out, the tenant pool could not be built) is an
/// error, so the receive job retries: Graph already got its 202 for this
/// notification and will not redeliver it, and no backstop sweep finds a
/// missed deletion, so dropping it here would lose it for good.
///
/// # Audit
/// `Actor::Webhook` (m365 enterprise train PR 4.2's third `connector_audit_
/// log.actor_kind` value — a verified Microsoft Graph push, no CoralSwarm
/// user session). `delete_source` ITSELF writes the `deletion_propagated`
/// audit row (via `record_or_queue` for a non-`User` actor — see that
/// function's own doc) — this function does not emit a second one.
async fn process_deleted_notification(
    state: Arc<AppState>,
    account_id: uuid::Uuid,
    resource: String,
    n: Notification,
    owner_user_id: Option<uuid::Uuid>,
) -> Result<(), ConnectorError> {
    // Defensive only — the transcript stream is subscribed created-only (see
    // `microsoft::subscription_change_types`), so Graph never sends a
    // `deleted` notification for it in practice.
    if resource.contains("getAllTranscripts") || resource.contains("onlineMeetings") {
        return Ok(());
    }
    let Some(message_id) = n.resource_data.and_then(|r| r.id) else {
        return Ok(());
    };

    let (source_type, source_id): (String, String) =
        match microsoft::parse_subscribed_teams_resource(&resource) {
            Some((source_type, container_id)) => {
                (source_type, format!("{container_id}:{message_id}"))
            }
            None => ("outlook".to_string(), message_id),
        };

    let target = resolve_graph_ingest_target(&state, account_id).await;
    let Some(pool) = target.content_pool(state.db.as_ref()).cloned() else {
        return match target {
            GraphIngestTarget::Unavailable(why) => {
                tracing::warn!(
                    %account_id, %source_type, %source_id, %why,
                    "graph delete notification: ingest target unavailable — retrying"
                );
                Err(ConnectorError::Internal(format!("graph delete: ingest target unavailable: {why}")))
            }
            _ => {
                tracing::info!(
                    %account_id, %source_type, %source_id,
                    "graph delete notification: ingest target refused (revoked org) — dropping"
                );
                Ok(())
            }
        };
    };

    // Subscription ownership is server-stored. Never infer a mailbox from
    // a native message ID or choose the first matching retrieval chunk.
    let owner=match owner_user_id {
        Some(owner)=>owner,
        None=>match microsoft::account_owner_user(state.db.as_ref().ok_or_else(||ConnectorError::Internal("no control pool".into()))?,account_id).await? {
            Some(owner)=>owner,
            None=>return Err(ConnectorError::Internal("deleted notification has no resolved connector owner".into())),
        },
    };
    let namespace=format!("microsoft:connection:{account_id}:{owner}");
    let mut tx=crate::tenancy::scope::begin_tenant_system_scoped(&pool,account_id).await
        .map_err(|e|ConnectorError::Internal(e.to_string()))?;
    let sources:Vec<(uuid::Uuid,String)>=sqlx::query_as(
        "SELECT DISTINCT s.graph_id,s.provider_namespace FROM source_items s JOIN graphs g ON g.id=s.graph_id \
         WHERE g.account_id=$1 AND s.source_type=$2 AND s.source_id=$3 AND s.provider_namespace=ANY($4) \
         UNION SELECT DISTINCT a.graph_id,$6::text FROM atoms a JOIN graphs g ON g.id=a.graph_id \
         WHERE g.account_id=$1 AND a.source_type=$2 AND a.ingested_by=$5 \
         AND (a.source_id=$3 OR (left(a.source_id,char_length($3)+7)=$3 || ':chunk:' AND substring(a.source_id FROM char_length($3)+8) ~ '^[0-9]+$')) \
         AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id) \
         AND NOT EXISTS(SELECT 1 FROM source_items s JOIN graphs sg ON sg.id=s.graph_id WHERE sg.account_id=$1 AND s.source_type=$2 AND s.source_id=$3 AND s.provider_namespace=ANY($4)) \
         ORDER BY 1,2",
    ).bind(account_id).bind(&source_type).bind(&source_id).bind(vec![namespace.clone()])
        .bind(owner).bind(&namespace).fetch_all(&mut *tx).await.map_err(|e|ConnectorError::Internal(e.to_string()))?;
    tx.rollback().await.map_err(|e|ConnectorError::Internal(e.to_string()))?;
    let blobs=state.r2.as_deref().map(|r|r as &dyn crate::services::blob_store::PromotionBlobStore);
    // Source items include snapshots with zero retrieval chunks. Preserve all
    // same-ID items belonging to another mailbox or watcher.
    let mut seen=std::collections::HashSet::new();
    for (graph_id,namespace) in sources {
        if !seen.insert(namespace.clone()) {continue;}
        crate::services::deletion::delete_chunked_source_in_namespace(
            &pool,blobs,crate::store::CLOUD_DEVICE_ID,graph_id,&source_type,&source_id,
            &namespace,crate::services::connector_audit::Actor::Webhook,None,
        ).await.map_err(|e|ConnectorError::Internal(format!("deletion propagation failed: {e}")))?;
    }
    Ok(())

}

/// Handle a meeting-transcript-available notification (CS-121): fetch the VTT
/// with the tenant's app token and ingest it as a `teams_transcript` coral into
/// the account's primary ocean.
async fn process_transcript_notification(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    account_id: uuid::Uuid,
    tenant_id: String,
    owner_user_id: Option<uuid::Uuid>,
    subscribed_resource: &str,
    n: Notification,
) -> Result<(), ConnectorError> {
    if crate::connectors::teams_transcripts::is_delegated_organiser_transcript_resource(
        subscribed_resource,
    ) {
        return process_delegated_transcript_notification(
            state,
            pool,
            account_id,
            owner_user_id,
            n,
        )
        .await;
    }
    // Legacy app-catalog subscriptions have no authenticated member/meeting
    // consent binding. Never read their transcript bodies or infer an owner.
    let _ = (state, pool, account_id, tenant_id, owner_user_id, n);
    Ok(())
}

/// Delegated organiser notification. Fetch with the subscription owner's
/// token only — org plane [`TokenOwner::Member`], personal plane
/// [`TokenOwner::AccountWide`]. Never another member's grant, and never the
/// tenant app token.
async fn process_delegated_transcript_notification(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    account_id: uuid::Uuid,
    owner_user_id: Option<uuid::Uuid>,
    n: Notification,
) -> Result<(), ConnectorError> {
    let resource = match n
        .resource
        .clone()
        .or_else(|| n.resource_data.as_ref().and_then(|r| r.id.clone()))
    {
        Some(r) => r,
        None => return Ok(()),
    };
    let plane = crate::routes::slack::account_plane(&pool, account_id).await.map_err(|e| {
        ConnectorError::Internal(format!("delegated transcript: account plane lookup failed: {e}"))
    })?;
    let org_plane = matches!(plane, crate::routes::slack::AccountPlane::Org);
    // An org subscription names its member. A personal account-wide subscription
    // carries no owner; it belongs to the account's real owner and nobody else.
    let user_id = match owner_user_id {
        Some(user_id) => user_id,
        None if !org_plane => {
            match sqlx::query_scalar::<_, uuid::Uuid>(
                "SELECT owner_user_id FROM accounts WHERE id = $1 AND clerk_org_id IS NULL",
            )
            .bind(account_id)
            .fetch_optional(&pool)
            .await
            {
                Ok(Some(owner)) => owner,
                _ => return Ok(()),
            }
        }
        None => return Ok(()),
    };
    let skip_user = if org_plane { Some(user_id) } else { None };
    if crate::services::connector_health::transcript_access_disabled(&pool, account_id, skip_user)
        .await
    {
        return Ok(());
    }
    let token_owner =
        crate::connectors::teams_transcripts::transcript_token_owner(org_plane, user_id);
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(&state, &pool, account_id)
        .await
        .map_err(crate::workers::ingest_receive::plane_error)?;
    let Some(mut creds) =
        load_creds_bg(&pool, &dek_plane, account_id, "microsoft", token_owner).await?
    else {
        tracing::warn!(%account_id, %user_id, "delegated transcript: no token for owner");
        return Ok(());
    };
    let connector = MicrosoftConnector;
    if let Err(e) = connector
        .refresh_if_needed(&state.http_client, &mut creds)
        .await
    {
        if microsoft::graph_access_to_transcripts_disabled(&e) {
            let _ = crate::services::connector_health::note_transcript_access_disabled(
                &pool, account_id, skip_user,
            )
            .await;
            return Ok(());
        }
        // Classified by the receive worker: a 5xx or timeout retries, a
        // dead credential completes.
        return Err(e);
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        let _ = store_token_bundle(
            &pool,
            &dek_plane,
            account_id,
            "microsoft",
            token_owner,
            refreshed,
        )
        .await;
    }
    let content_pool = if org_plane {
        let clerk_org_id: Option<String> =
            sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
                .bind(account_id)
                .fetch_optional(&pool)
                .await
                .map_err(|e| ConnectorError::Internal(format!("delegated transcript org: {e}")))?
                .flatten();
        let Some(org) = clerk_org_id else {
            return Ok(());
        };
        match resolve_org_tenant_pool_outcome(&state, &pool, &org).await {
            TenantPoolOutcome::Pool(content) => content,
            TenantPoolOutcome::Refused => return Ok(()),
            TenantPoolOutcome::Unavailable(why) => {
                return Err(ConnectorError::Internal(format!(
                    "delegated transcript: tenant plane unavailable: {why}"
                )))
            }
        }
    } else {
        pool.clone()
    };
    let Some(meeting_id) = microsoft::meeting_id_from_resource(&resource) else {
        return Ok(());
    };
    let Some(graph_id) = consented_transcript_graph(
        &state,
        &content_pool,
        account_id,
        user_id,
        &meeting_id,
        None,
    )
    .await
    else {
        return Ok(());
    };
    let Some(created_at) =
        microsoft::transcript_created_at(&state.http_client, &creds.access_token, &resource)
            .await?
    else {
        return Ok(());
    };
    let control = state.control_db().unwrap_or(&pool);
    let (tenant, aad) = match crate::routes::teams_onboarding::capture_identity(
        control,
        account_id,
        user_id,
        crate::routes::teams_onboarding::ExpectedPlane::Any,
    )
    .await
    {
        Ok(identity) => identity.pair(),
        Err(crate::routes::teams_onboarding::CaptureIdentityError::Unavailable(e)) => {
            return Err(ConnectorError::Internal(e.to_string()))
        }
        Err(_) => return Ok(()),
    };
    // The token read above must be the grant the verified identity names.
    if !org_plane && !creds.provider_workspace_id.eq_ignore_ascii_case(&tenant) {
        return Ok(());
    }
    if crate::services::teams_meeting_capture::active_graph_at(
        &content_pool,
        account_id,
        user_id,
        &tenant,
        &aad,
        &meeting_id,
        created_at,
    )
    .await
    .map_err(|e| ConnectorError::Internal(e.to_string()))?
        != Some(graph_id)
    {
        return Ok(());
    }
    let verified = crate::connectors::teams_targeted_transcripts::resolve(
        &state.http_client,
        &creds.access_token,
        crate::connectors::teams_targeted_transcripts::Target::MeetingId(meeting_id.clone()),
    )
    .await
    .map_err(microsoft::targeted_transcript_error)?;
    let (Some(start), Some(end)) = (
        verified
            .start
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&chrono::Utc)),
        verified
            .end
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&chrono::Utc)),
    ) else {
        return Ok(());
    };
    if crate::services::teams_meeting_capture::matches_occurrence(
        &content_pool,
        account_id,
        user_id,
        &tenant,
        &aad,
        &meeting_id,
        start,
        end,
    )
    .await
    .map_err(|e| ConnectorError::Internal(e.to_string()))?
        != Some(graph_id)
    {
        return Ok(());
    }
    let text =
        match microsoft::fetch_transcript_text(&state.http_client, &creds.access_token, &resource)
            .await
        {
            Ok(text) => text,
            Err(e) if microsoft::graph_access_to_transcripts_disabled(&e) => {
                let _ = crate::services::connector_health::note_transcript_access_disabled(
                    &pool, account_id, skip_user,
                )
                .await;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
    if text.trim().is_empty() {
        return Ok(());
    }

    let transcript_id =
        microsoft::transcript_id_from_resource(&resource).unwrap_or_else(|| resource.clone());
    // Keep the verified original provider time through the write gate. Never
    // manufacture consent-relevant time from the worker's wall clock.
    let timestamp = created_at;
    let item = NormalizedItem {
        source_type: "teams_transcript".into(),
        source_id: format!("{meeting_id}:{transcript_id}"),
        content: format!("Teams meeting transcript\n\n{text}"),
        participants: Vec::new(),
        timestamp,
        scope: None,
        thread_id: Some(meeting_id),
        channel_id: None,
        author_id: None,
        author_name: None,
    };
    if ingest_message(&state, account_id, user_id, graph_id, item).await? {
        tracing::info!(%account_id, %user_id, "auto-captured delegated teams meeting transcript");
    }
    Ok(())
}

/// Pick the token for a subscription: an app-only token for the tenant when the
/// subscription is RSC (app-mode, `tenant_id` set), else the account's delegated
/// token. `Ok(None)` when no delegated connection exists for a delegated sub.
///
/// `owner` (m365 enterprise train PR 3.3): the [`TokenOwner`] this
/// subscription's mailbox belongs to (`graph_subscriptions.owner_user_id`,
/// via the caller's own [`microsoft::subscription_by_id`] read) —
/// `AccountWide` for Personal-plane mail and every Teams/transcript resource
/// (which never carries an owner and ignores this parameter's non-app-only
/// branch entirely — that arm returns before `owner` is ever consulted),
/// `Member(uid)` for an org member's own delegated mail connection.
pub(crate) async fn subscription_token(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    tenant_id: &str,
    owner: TokenOwner,
) -> Result<Option<String>, ConnectorError> {
    if !tenant_id.is_empty() {
        return Ok(Some(
            microsoft::app_token(&state.http_client, tenant_id).await?,
        ));
    }
    // Same plane resolution as `process_notification` — see its comment.
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, pool, account_id)
        .await
        .map_err(crate::workers::ingest_receive::plane_error)?;
    let Some(mut creds) = load_creds_bg(pool, &dek_plane, account_id, "microsoft", owner).await?
    else {
        return Ok(None);
    };
    let connector = MicrosoftConnector;
    connector
        .refresh_if_needed(&state.http_client, &mut creds)
        .await?;
    if let Some(refreshed) = creds.refreshed.as_ref() {
        let _ =
            store_token_bundle(pool, &dek_plane, account_id, "microsoft", owner, refreshed).await;
    }
    Ok(Some(creds.access_token))
}

/// Re-list on each doorbell watcher's own token. This is the existing
/// per-account backstop (`resync_account`), not a copy of one watcher's
/// message body onto another watcher.
async fn resync_doorbell_watchers(
    state: Arc<AppState>,
    control: &sqlx::PgPool,
    ms_tenant_id: &str,
    resource: &str,
) {
    let watchers: Vec<(uuid::Uuid, uuid::Uuid)> = match sqlx::query_as(
        "SELECT account_id, user_id FROM teams_container_watchers \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_all(control)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, "teams doorbell resync: watcher list failed");
            return;
        }
    };
    let mut seen: Vec<uuid::Uuid> = Vec::new();
    for (account_id, watcher_user_id) in watchers {
        if seen.contains(&account_id) {
            continue;
        }
        seen.push(account_id);
        let plane = crate::routes::slack::account_plane(control, account_id)
            .await
            .unwrap_or(crate::routes::slack::AccountPlane::Personal);
        let content = match plane {
            crate::routes::slack::AccountPlane::Personal => control.clone(),
            crate::routes::slack::AccountPlane::Org => {
                let org: Option<String> =
                    sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
                        .bind(account_id)
                        .fetch_optional(control)
                        .await
                        .ok()
                        .flatten()
                        .flatten();
                let Some(org) = org else { continue };
                match resolve_org_tenant_pool_for_graph_ingest(&state, control, &org).await {
                    Some(pool) => pool,
                    None => continue,
                }
            }
        };
        let dek_plane = match crate::crypto::DekPlane::resolve_for_account(
            &state, control, account_id,
        )
        .await
        {
            Ok(plane) => plane,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "teams doorbell resync: dek plane failed");
                continue;
            }
        };
        let source = dek_plane.as_source();
        crate::workers::graph_subscriptions::resync_account(
            &state, &content, control, &source, account_id,
        )
        .await;
    }
}

/// Handle a Graph lifecycle notification — keep the subscription alive or
/// recover from a delivery gap (ADR-0008). `reauthorizationRequired` → extend
/// the subscription; `subscriptionRemoved` → recreate it and resync; `missed`
/// → resync the account's watched threads (replies dropped during the gap are
/// recovered by re-listing each conversation).
async fn process_lifecycle(state: Arc<AppState>, n: Notification) -> Result<(), ConnectorError> {
    let (Some(subscription_id), Some(event)) =
        (n.subscription_id.clone(), n.lifecycle_event.clone())
    else {
        return Ok(());
    };
    let pool = state
        .db
        .clone()
        .ok_or_else(|| ConnectorError::Internal("no database".into()))?;

    // Teams channel/chat doorbell (S5b). The id lives on the control-plane
    // registry, not `graph_subscriptions`. Handle it here and do not fall
    // through to mail or RSC.
    if let Some(door) =
        crate::connectors::teams_doorbell::lookup_subscription(&pool, &subscription_id).await?
    {
        let provided = n.client_state.clone().unwrap_or_default();
        if !client_state_matches(&provided, &door.client_state) {
            tracing::warn!(%subscription_id, %event, "teams doorbell lifecycle clientState mismatch — dropping");
            return Ok(());
        }
        let graph = crate::connectors::teams_doorbell::HttpDoorbell {
            http: &state.http_client,
        };
        let tokens = crate::connectors::teams_doorbell::ResolvedWatcherTokens {
            state: &state,
            control: &pool,
            http: &state.http_client,
        };
        let outcome = crate::connectors::teams_doorbell::handle_doorbell_lifecycle(
            &pool,
            &graph,
            &tokens,
            &door.ms_tenant_id,
            &door.resource,
            &event,
        )
        .await?;
        if outcome.resync_watchers {
            resync_doorbell_watchers(state, &pool, &door.ms_tenant_id, &door.resource).await;
        }
        return Ok(());
    }

    let Some(((account_id, expected_state, resource, tenant_id, owner_user_id), content_pool)) =
        resolve_subscription(&state, &pool, &subscription_id).await?
    else {
        tracing::debug!(%subscription_id, %event, "lifecycle for unknown subscription — ignoring");
        return Ok(());
    };
    if !client_state_matches(&n.client_state.unwrap_or_default(), &expected_state) {
        tracing::warn!(%subscription_id, %event, "lifecycle clientState mismatch — dropping");
        return Ok(());
    }
    // App-only lifecycle (non-empty tenant id) renews or recreates an RSC
    // subscription, including the tenant-wide transcript stream. After the
    // grace those are not recreated. Delegated lifecycle has an empty
    // tenant id and continues, which keeps item 51's organiser subscription
    // alive.
    if !tenant_id.is_empty()
        && !crate::services::org_install_grace::org_install_capture_open(
            state.control_db().unwrap_or(&pool),
        )
        .await
    {
        tracing::info!(
            %subscription_id,
            "org-install grace closed; ignoring app-only graph lifecycle event"
        );
        return Ok(());
    }
    // See `process_notification`'s identical `mail_owner` derivation.
    let owner = owner_user_id
        .map(TokenOwner::Member)
        .unwrap_or(TokenOwner::AccountWide);

    // Rate-limit scope for every Graph call this lifecycle event triggers —
    // m365 Train 2 PR 2.6 (see `graph_rate_limit`'s module doc). `resync_
    // account` ALSO opens its own nested scope with the same key (it's
    // called directly by the tenant-loop tick too, with no outer scope to
    // rely on) — nesting the same key is harmless (shadows, doesn't stack).
    // Resolved once for the whole lifecycle event, then reused by the two
    // `resync_account` arms below — a signature-authenticated webhook has no
    // `TenancyDecision` to read a plane off, so THE decision table
    // (`crypto::DekPlane::resolve_for_org`) names it from the account.
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(&state, &pool, account_id)
        .await
        .map_err(crate::workers::ingest_receive::plane_error)?;

    crate::graph_rate_limit::scoped(account_id.to_string(), async move {
        // App-mode (RSC) subscriptions renew with an app token for the tenant;
        // delegated ones with the connecting user's token.
        let Some(token) = subscription_token(&state, &pool, account_id, &tenant_id, owner).await?
        else {
            tracing::warn!(%account_id, "no token available for lifecycle event");
            return Ok(());
        };

        match event.as_str() {
            "reauthorizationRequired" => {
                let new_expiry = microsoft::renew_subscription(
                    &state.http_client,
                    &token,
                    &subscription_id,
                    &resource,
                )
                .await?;
                microsoft::set_subscription_expiry_for_account(
                    &content_pool,
                    account_id,
                    &subscription_id,
                    new_expiry,
                )
                .await?;
                tracing::info!(%subscription_id, "reauthorized graph subscription");
            }
            "subscriptionRemoved" => {
                // Graph dropped its side — recreate the *same* resource (mail folder,
                // Teams channel/chat, or transcript stream), then resync to fill the gap.
                microsoft::delete_subscription_row_for_account(
                    &content_pool,
                    account_id,
                    &subscription_id,
                )
                .await?;
                microsoft::ensure_subscription_for(
                    &state.http_client,
                    &content_pool,
                    account_id,
                    &token,
                    &resource,
                    &tenant_id,
                    owner,
                )
                .await?;
                // `resync_account` is pool-explicit (org-tenancy PR 2.2), but this
                // funnel is still consumer-plane-only (see that function's own doc
                // comment) — `pool` doubles as both the content pool and the
                // control pool here, exactly as it did implicitly before this
                // call site's `pool` parameter existed. The DEK plane is NOT
                // assumed consumer, though: it is the one this handler already
                // resolved for `account_id` above, widened to the
                // account-independent form the shared worker helper takes
                // (`DekPlane::as_source` — sound because `resync_account` is
                // single-account).
                if matches!(
                    crate::routes::slack::account_plane(&pool, account_id).await,
                    Ok(crate::routes::slack::AccountPlane::Org)
                ) {
                    if let Err(e) =
                        microsoft::sync_mail_routes(&content_pool, &pool, account_id).await
                    {
                        tracing::warn!(error = %e, %account_id, "lifecycle: mail route re-sync failed");
                    }
                }
                crate::workers::graph_subscriptions::resync_account(
                    &state,
                    &content_pool,
                    &pool,
                    &dek_plane.as_source(),
                    account_id,
                )
                .await;
                tracing::info!(%account_id, "recreated removed graph subscription and resynced");
            }
            "missed" => {
                crate::workers::graph_subscriptions::resync_account(
                    &state,
                    &content_pool,
                    &pool,
                    &dek_plane.as_source(),
                    account_id,
                )
                .await;
                tracing::info!(%account_id, "resynced watched threads after missed notifications");
            }
            other => tracing::warn!(%other, "unknown graph lifecycle event — ignoring"),
        }
        Ok(())
    })
    .await
}

/// Ingest one Outlook message into a watched thread's ocean, as the user who
/// opted the thread in, through the canonical ingest path (dedup makes
/// already-seen messages a no-op). Shared by the webhook (a single reply) and
/// the backstop resync (a whole conversation).
///
/// `Ok(true)` when the message was stored (for an org-plane account:
/// durably handed off, see [`GraphIngestTarget`]); `Ok(false)` when a rule
/// withheld it (no consent, a watch cutoff, a revoked org), which no retry
/// changes and is not a failure. `Err` when the write itself failed, so the
/// receive worker can classify it (`ingest_receive::transient`): a
/// transient `submit` refusal, a storage failure, a plane that cannot be
/// resolved right now and a failed job enqueue retry
/// ([`ConnectorError::Internal`]); a permanent `submit` refusal
/// (`IngestError::is_permanent`: the item can never be stored as sent)
/// completes ([`ConnectorError::NotConfigured`], the permanent class).
/// Before this returned a bare `bool`, so a receive job whose write failed
/// completed and the delivery was lost.
pub(crate) async fn ingest_message(
    state: &Arc<AppState>,
    account_id: uuid::Uuid,
    user_id: uuid::Uuid,
    graph_id: uuid::Uuid,
    item: NormalizedItem,
) -> Result<bool, ConnectorError> {
    let target = resolve_graph_ingest_target(state, account_id).await;
    // Single-message path (the live webhook): resolve the channel/chat's human
    // label from the watched resource on demand. Batch callers should use
    // `ingest_container_messages`, which resolves the label once for the whole
    // container instead of once per message.
    let channel_name = match (&item.channel_id, target.content_pool(state.db.as_ref())) {
        (Some(container), Some(pool)) => {
            microsoft::teams_channel_name_by_container(pool, account_id, container).await
        }
        _ => None,
    };
    ingest_message_with(
        state,
        account_id,
        user_id,
        graph_id,
        item,
        channel_name,
        &target,
    )
    .await
}

/// [`ingest_message`] as a bare "stored or not", for the callers that count
/// stored messages and hold their cursor on anything else (the backstop
/// sweeps, the workflow backfill, the interactive meeting ingest): a
/// failure is logged with its class and counts as not stored, as it always
/// did for them. The receive worker never uses this; it classifies the
/// error itself.
pub(crate) async fn ingest_message_stored(
    state: &Arc<AppState>,
    account_id: uuid::Uuid,
    user_id: uuid::Uuid,
    graph_id: uuid::Uuid,
    item: NormalizedItem,
) -> bool {
    match ingest_message(state, account_id, user_id, graph_id, item).await {
        Ok(stored) => stored,
        Err(e) => {
            let retryable = crate::workers::ingest_receive::transient(&e);
            tracing::warn!(error = %e, %account_id, %graph_id, retryable, "graph message ingest failed");
            false
        }
    }
}

/// Ingest every message pulled for ONE Teams container, resolving the
/// container's human label AND the account's ingest target exactly once (a
/// backfill can carry hundreds, and both are constant per call). Returns how
/// many were newly ingested/handed off.
pub(crate) async fn ingest_container_messages(
    state: &Arc<AppState>,
    account_id: uuid::Uuid,
    user_id: uuid::Uuid,
    graph_id: uuid::Uuid,
    container_id: &str,
    items: Vec<NormalizedItem>,
) -> usize {
    let target = resolve_graph_ingest_target(state, account_id).await;
    let channel_name = match target.content_pool(state.db.as_ref()) {
        Some(pool) => {
            microsoft::teams_channel_name_by_container(pool, account_id, container_id).await
        }
        None => None,
    };
    let mut ingested = 0usize;
    for item in items {
        match ingest_message_with(
            state,
            account_id,
            user_id,
            graph_id,
            item,
            channel_name.clone(),
            &target,
        )
        .await
        {
            Ok(true) => ingested += 1,
            Ok(false) => {}
            // A batch caller counts stored messages and holds its cursor on
            // the rest; the failure's class is in the log.
            Err(e) => {
                let retryable = crate::workers::ingest_receive::transient(&e);
                tracing::warn!(error = %e, %account_id, %graph_id, retryable, "teams container message ingest failed");
            }
        }
    }
    ingested
}

/// Where one account's connector content should land — resolved ONCE per
/// [`ingest_message`]/[`ingest_container_messages`] call (m365 Train 2 PR
/// 2.3: killing the `Tenant::legacy` hardcode that used to run this funnel
/// unconditionally against the personal plane, regardless of which plane the
/// notifying account was actually on).
enum GraphIngestTarget {
    /// `AccountPlane::Personal` — byte-identical to the pre-PR-2.3 behavior:
    /// ingest synchronously, in-process, through `Tenant::legacy(state)`.
    Sync,
    /// `AccountPlane::Org`, and [`resolve_org_tenant_pool_for_graph_ingest`]
    /// resolved a pool to hand this account's ingest off to — either the
    /// org's own tenant pool (`Active` routing row) or the shared consumer
    /// pool (the org has never been tenant-migrated; content genuinely still
    /// lives there, same reasoning `routes::slack_events::resolve_org_plane`
    /// documents for Slack). `org_id` is threaded through purely to wake this
    /// tenant's worker harness (`AppState::worker_activity::note_enqueue`) —
    /// a no-op for an org with no active tenant-loop entry yet.
    Enqueue { pool: sqlx::PgPool, org_id: String },
    /// `AccountPlane::Org`, and the org must never receive content again
    /// (revoked, [`TenantPoolOutcome::Refused`]): drop the notification.
    Drop,
    /// The plane could not be resolved right now (a lookup failed or timed
    /// out, the routing row is mid-migration, the tenant pool could not be
    /// built): drop nothing on the strength of it. A receive worker turns
    /// this into a retryable error; a sweep skips the account for this pass
    /// (the next one re-lists). Carries the reason for the error text.
    Unavailable(String),
}

impl GraphIngestTarget {
    /// The content-plane pool this target's Teams state
    /// (`teams_watched_resources`, read by `microsoft::teams_channel_name_by_
    /// container`) should be looked up on for THIS target — `state_db` (the
    /// control/consumer pool) for `Sync`, the resolved tenant pool for
    /// `Enqueue`, and `None` for `Drop` (the notification is being discarded
    /// outright, so there is nothing worth a lookup for).
    ///
    /// m365 Train 2 PR 2.5 cross-plane read sweep: `ingest_message`/
    /// `ingest_container_messages` used to call `teams_channel_name_by_
    /// container` against `state.db` UNCONDITIONALLY, regardless of which
    /// plane THIS target actually resolved to. That was invisible pre-2.5
    /// (every reachable account was Personal, where `state.db` genuinely IS
    /// the right pool — `Sync`'s case below), but once the org gate lifts an
    /// `Enqueue` target's `teams_watched_resources` row lives on the org's
    /// OWN tenant pool, absent from `state.db` entirely, so the old code
    /// silently resolved every org channel name to `None` — same bug class
    /// as Slack's step-5a sweep (#492) found in `ingest_slack_message`'s
    /// `slack_integrations` read (a control-only table read against whatever
    /// pool the caller happened to be holding, rather than the pool that
    /// table actually lives on).
    fn content_pool<'a>(&'a self, state_db: Option<&'a sqlx::PgPool>) -> Option<&'a sqlx::PgPool> {
        match self {
            GraphIngestTarget::Sync => state_db,
            GraphIngestTarget::Enqueue { pool, .. } => Some(pool),
            GraphIngestTarget::Drop | GraphIngestTarget::Unavailable(_) => None,
        }
    }
}

/// Resolve which plane `account_id`'s connector content should land on.
/// `Personal` keeps the pre-PR-2.3 synchronous path; `Org` mirrors
/// `routes::slack_events::resolve_org_plane`'s three-way decision table
/// (revoked/mid-migration ⇒ drop, never-tenant-migrated ⇒ the consumer pool,
/// `Active` ⇒ the org's own tenant pool) — generalized off `clerk_org_id`
/// rather than a second, Graph-specific copy of that routing logic.
async fn resolve_graph_ingest_target(
    state: &Arc<AppState>,
    account_id: uuid::Uuid,
) -> GraphIngestTarget {
    let Some(control_pool) = state.db.clone() else {
        return GraphIngestTarget::Unavailable("no control pool configured".into());
    };
    let plane = match crate::routes::slack::account_plane(&control_pool, account_id).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id,
                "graph ingest: account plane lookup failed — not guessing a plane"
            );
            return GraphIngestTarget::Unavailable(format!("account plane lookup failed: {e}"));
        }
    };
    if plane == crate::routes::slack::AccountPlane::Personal {
        return GraphIngestTarget::Sync;
    }

    // Org plane: `account_plane` only returns whether `clerk_org_id IS NOT
    // NULL`, not the id itself — one more read for the value the routing
    // lookup below needs. `fetch_one` (not `_optional`): `account_plane`
    // just confirmed this row exists a moment ago.
    let clerk_org_id: Option<String> =
        match sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_one(&control_pool)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(
                    error = %e, %account_id,
                    "graph ingest: clerk_org_id re-read failed — not guessing a plane"
                );
                return GraphIngestTarget::Unavailable(format!("clerk_org_id read failed: {e}"));
            }
        };
    let Some(clerk_org_id) = clerk_org_id else {
        // Defensive only: `account_plane` already confirmed `clerk_org_id IS
        // NOT NULL` off the same row. Unreachable barring a concurrent write
        // clearing it between the two reads.
        tracing::warn!(
            %account_id,
            "graph ingest: account_plane said Org but clerk_org_id re-read NULL — dropping"
        );
        return GraphIngestTarget::Drop;
    };

    match resolve_org_tenant_pool_outcome(state, &control_pool, &clerk_org_id).await {
        TenantPoolOutcome::Pool(pool) => GraphIngestTarget::Enqueue {
            pool,
            org_id: clerk_org_id,
        },
        TenantPoolOutcome::Refused => GraphIngestTarget::Drop,
        TenantPoolOutcome::Unavailable(why) => GraphIngestTarget::Unavailable(why),
    }
}

/// How an org's content pool resolved — [`resolve_org_tenant_pool_outcome`].
pub(crate) enum TenantPoolOutcome {
    /// The pool this org's content lands on.
    Pool(sqlx::PgPool),
    /// This org must never receive content again (revoked).
    Refused,
    /// Not resolvable now: a lookup failed or timed out, the routing row is
    /// mid-migration or torn down, no tenant runtime is configured, or the
    /// pool could not be built. The reason, for a retry's error text.
    Unavailable(String),
}

/// How long any one control-plane lookup or tenant-pool build may take before
/// this hand-off gives up on it — mirrors `routes::slack_events::
/// SLACK_TENANT_ENQUEUE_TIMEOUT` exactly (this call runs in a receive or
/// sweep worker, never a caller's own request, but an unbounded await here
/// would still let one stuck org tie up a worker slot indefinitely).
const GRAPH_TENANT_ENQUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The org branch of [`resolve_graph_ingest_target`], as a bare
/// `Option<PgPool>` for callers that only need "a pool, or not this pass"
/// (`routes::teams_bot`'s `resolve_teams_content_pool`, the calendar push,
/// Slack member capture): both a refusal and an unavailable plane are
/// `None`. A receive worker, which must retry rather than drop, reads
/// [`resolve_org_tenant_pool_outcome`] instead.
pub(crate) async fn resolve_org_tenant_pool_for_graph_ingest(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    clerk_org_id: &str,
) -> Option<sqlx::PgPool> {
    match resolve_org_tenant_pool_outcome(state, control_pool, clerk_org_id).await {
        TenantPoolOutcome::Pool(pool) => Some(pool),
        TenantPoolOutcome::Refused | TenantPoolOutcome::Unavailable(_) => None,
    }
}

/// Which pool an org's connector content lands on. Mirrors `routes::
/// slack_events::resolve_org_plane` step for step:
/// * `org_is_revoked` ⇒ [`TenantPoolOutcome::Refused`] (never land new
///   content for a revoked org).
/// * No `tenant_databases` row at all (never tenant-migrated) ⇒ the control
///   pool — this org's content genuinely still lives there.
/// * A row exists but is not `is_servable()` (mid-migration/torn down) ⇒
///   [`TenantPoolOutcome::Unavailable`] — guessing a plane here risks a
///   split-brain write; a migration ends, and a torn-down org is revoked.
/// * `Active` ⇒ `registry.get_or_build` the tenant pool.
///
/// Every lookup failure, timeout and pool-build failure is `Unavailable`
/// too: a receive worker retries on it (the delivery was already
/// acknowledged to the provider, so a drop here would lose it), a sweep
/// skips the account for this pass.
pub(crate) async fn resolve_org_tenant_pool_outcome(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    clerk_org_id: &str,
) -> TenantPoolOutcome {
    match tokio::time::timeout(
        GRAPH_TENANT_ENQUEUE_TIMEOUT,
        crate::tenancy::routing::org_is_revoked(control_pool, clerk_org_id),
    )
    .await
    {
        Ok(Ok(true)) => {
            tracing::info!(
                org = %clerk_org_id,
                "graph message hand-off: org is revoked — refusing (never landing new content \
                 for a revoked org)"
            );
            return TenantPoolOutcome::Refused;
        }
        Ok(Ok(false)) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                org = %clerk_org_id, error = %e,
                "graph message hand-off: org_revocations lookup failed — not guessing a plane"
            );
            return TenantPoolOutcome::Unavailable(format!("org_revocations lookup failed: {e}"));
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_secs = GRAPH_TENANT_ENQUEUE_TIMEOUT.as_secs(),
                "graph message hand-off: org_revocations lookup timed out"
            );
            return TenantPoolOutcome::Unavailable("org_revocations lookup timed out".into());
        }
    }

    let row = match tokio::time::timeout(
        GRAPH_TENANT_ENQUEUE_TIMEOUT,
        crate::tenancy::routing::lookup_by_org(control_pool, clerk_org_id),
    )
    .await
    {
        Ok(Ok(row)) => row,
        Ok(Err(e)) => {
            tracing::warn!(
                org = %clerk_org_id, error = %e,
                "graph message hand-off: routing lookup failed — not guessing a plane"
            );
            return TenantPoolOutcome::Unavailable(format!("tenant routing lookup failed: {e}"));
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_secs = GRAPH_TENANT_ENQUEUE_TIMEOUT.as_secs(),
                "graph message hand-off: routing lookup timed out"
            );
            return TenantPoolOutcome::Unavailable("tenant routing lookup timed out".into());
        }
    };

    let Some(row) = row else {
        // Never onboarded onto a tenant database — its content genuinely
        // still lives on the shared consumer plane.
        return TenantPoolOutcome::Pool(control_pool.clone());
    };

    if !row.is_servable() {
        tracing::warn!(
            org = %clerk_org_id, status = %row.status, connector_health = true,
            "graph message hand-off: tenant routing row exists but is not active (mid-migration \
             or torn down) — not risking a split-brain write"
        );
        return TenantPoolOutcome::Unavailable(format!("tenant routing row is {}", row.status));
    }

    let Some(rt) = state.tenant_runtime.as_ref() else {
        tracing::warn!(
            org = %clerk_org_id,
            "graph message hand-off: active tenant routing row but no TenantRuntime configured"
        );
        return TenantPoolOutcome::Unavailable("no TenantRuntime configured".into());
    };

    match tokio::time::timeout(
        GRAPH_TENANT_ENQUEUE_TIMEOUT,
        rt.registry
            .get_or_build(clerk_org_id, row.account_id, &row.db_secret_ref),
    )
    .await
    {
        Ok(Ok(pool)) => TenantPoolOutcome::Pool(pool),
        Ok(Err(e)) => {
            tracing::warn!(
                org = %clerk_org_id, error = %e,
                "graph message hand-off: tenant pool build failed"
            );
            TenantPoolOutcome::Unavailable(format!("tenant pool build failed: {e}"))
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_secs = GRAPH_TENANT_ENQUEUE_TIMEOUT.as_secs(),
                "graph message hand-off: tenant pool build timed out"
            );
            TenantPoolOutcome::Unavailable("tenant pool build timed out".into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn teams_watch_allows_message(
    pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    user_id: uuid::Uuid,
    graph_id: uuid::Uuid,
    source_type: &str,
    container_id: &str,
    created_at: chrono::DateTime<chrono::Utc>,
    allow_personal_app: bool,
) -> Result<bool, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let allowed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM teams_watched_resources \
         WHERE account_id=$1 AND user_id=$2 AND graph_id=$3 \
         AND (($4='teams_channel' AND kind='channel' AND channel_id=$5) \
              OR ($4='teams_chat' AND kind='chat' AND chat_id=$5)) \
         AND (auth_mode = 'delegated' OR ($7 AND auth_mode = 'app')) \
         AND (auth_mode <> 'delegated' OR history_choice IS DISTINCT FROM 'none' \
              OR (tenant_id <> '' AND created_at <= $6)))",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(graph_id)
    .bind(source_type)
    .bind(container_id)
    .bind(created_at)
    .bind(allow_personal_app)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(allowed)
}

/// Build the canonical ingest request for one normalized item, stamping the
/// already-resolved `channel_name`, and either run it synchronously (personal
/// plane, byte-identical to the pre-PR-2.3 behavior) or hand it off through
/// [`jobs::JobKind::GraphMessageIngest`] (org plane) — per `target`, resolved
/// once by the caller. Shared by the single-message and batch paths so the
/// channel-label lookup and plane resolution each happen once per caller, not
/// per message.
/// Live member grant, organization membership and explicit private meeting
/// consent must all remain valid both before a body read and before a write.
async fn consented_transcript_graph(
    state: &Arc<AppState>,
    content: &sqlx::PgPool,
    account: uuid::Uuid,
    member: uuid::Uuid,
    meeting: &str,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Option<uuid::Uuid> {
    let control = state.control_db().or(state.db.as_ref())?;
    // Plane-aware: org keeps the member grant; a personal account resolves only to
    // its real owner's current work/school account-wide grant.
    let (tenant, aad) = crate::routes::teams_onboarding::capture_identity(
        control,
        account,
        member,
        crate::routes::teams_onboarding::ExpectedPlane::Any,
    )
    .await
    .ok()?
    .pair();
    if tenant.is_empty() || meeting.is_empty() {
        return None;
    }
    let org: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
        .bind(account)
        .fetch_optional(control)
        .await
        .ok()?
        .flatten();
    if let Some(org) = org {
        if !matches!(crate::middleware::auth::live_org_membership_for_capture(control, &org, member).await,
            Ok(Some((id, _))) if id == account)
            || !crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, &org).await
        {
            return None;
        }
    }
    match timestamp {
        Some(at) => crate::services::teams_meeting_capture::active_graph_at(
            content, account, member, &tenant, &aad, meeting, at,
        )
        .await
        .ok()
        .flatten(),
        None => crate::services::teams_meeting_capture::active_graph(
            content, account, member, &tenant, &aad, meeting,
        )
        .await
        .ok()
        .flatten(),
    }
}

/// Keep pause/disable acknowledgment behind any already admitted durable write
/// or enqueue. The control transaction intentionally spans the content-plane
/// transaction; the latter never updates connector_sources.
async fn teams_source_admission<'a>(
    pool: &'a sqlx::PgPool, account: uuid::Uuid, member: uuid::Uuid, source: &str,
) -> Result<Option<sqlx::Transaction<'a, sqlx::Postgres>>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await?;
    // Missing catalog rows preserve legacy enabled-by-default consent. The
    // uncommitted row also serializes a concurrent pause upsert; it rolls back
    // with the admission guard, without changing wizard/setup state.
    sqlx::query("INSERT INTO connector_sources(account_id, owner_user_id, provider, source, enabled) VALUES ($1,$2,'microsoft',$3,true) ON CONFLICT DO NOTHING")
        .bind(account).bind(member).bind(source).execute(&mut *tx).await?;
    let allowed: bool = sqlx::query_scalar("SELECT enabled AND paused_at IS NULL FROM connector_sources WHERE account_id=$1 AND owner_user_id=$2 AND provider='microsoft' AND source=$3 FOR SHARE")
        .bind(account).bind(member).bind(source).fetch_one(&mut *tx).await?;
    Ok(allowed.then_some(tx))
}

async fn ingest_message_with(
    state: &Arc<AppState>,
    account_id: uuid::Uuid,
    user_id: uuid::Uuid,
    graph_id: uuid::Uuid,
    item: NormalizedItem,
    channel_name: Option<String>,
    target: &GraphIngestTarget,
) -> Result<bool, ConnectorError> {
    // A plane that cannot be resolved right now is a failure a retry can
    // change; a revoked org withholds the message for good.
    if let GraphIngestTarget::Unavailable(why) = target {
        return Err(ConnectorError::Internal(format!("graph ingest: target unavailable: {why}")));
    }
    // The pause gate, at the one write boundary of every Microsoft message path
    // (a mail notification, the Teams doorbell, the backstop resync, a meeting
    // sync, and the org-plane hand-off below): a member who paused or turned off
    // Outlook, Teams chats, channels or meetings gets nothing here, whichever
    // door the message came in by. `false` leaves the callers' cursors where
    // they are, so a resumed source reads on from them.
    if let Some(key) = crate::connectors::catalog::pause::SourceKey::microsoft_for_atom(&item.source_type) {
        if crate::connectors::catalog::pause::is_paused_in(state, account_id, user_id, key).await {
            tracing::info!(%account_id, source = key.source, "microsoft capture: source is paused or off -- not ingesting");
            return Ok(false);
        }
    }
    // Recheck at the common write boundary, including fetch/revoke races.
    if item.source_type == "teams_transcript" {
        let (Some(pool), Some(meeting)) = (
            target.content_pool(state.db.as_ref()),
            item.thread_id.as_deref(),
        ) else {
            return Ok(false);
        };
        if consented_transcript_graph(
            state,
            pool,
            account_id,
            user_id,
            meeting,
            Some(item.timestamp),
        )
        .await
            != Some(graph_id)
        {
            return Ok(false);
        }
    }
    let mut _source_admission = None;
    // All Teams fetch paths converge here, including periodic resync and
    // polling, which may list the entire container. Enforce the opting-in
    // member's durable cutoff before either writing content or enqueuing it.
    // last_ingested_at is a mutable fetch watermark, not consent time.
    if matches!(item.source_type.as_str(), "teams_channel" | "teams_chat") {
        let (Some(pool), Some(container)) = (
            target.content_pool(state.db.as_ref()),
            item.channel_id.as_deref(),
        ) else {
            return Ok(false);
        };
        let Some(control) = state.control_db().or(state.db.as_ref()) else { return Err(ConnectorError::Internal("Teams source admission: no control pool".into())); };
        let source = if item.source_type == "teams_chat" { "teams_chats" } else { "teams_channels" };
        match teams_source_admission(control, account_id, user_id, source).await {
            Ok(Some(guard)) => _source_admission = Some(guard),
            Ok(None) => return Ok(false),
            Err(error) => {
                tracing::warn!(%account_id, %user_id, %error, "Teams source admission failed");
                return Err(ConnectorError::Internal(format!("Teams source admission: {error}")));
            }
        }
        match teams_watch_allows_message(
            pool,
            account_id,
            user_id,
            graph_id,
            &item.source_type,
            container,
            item.timestamp,
            matches!(target, GraphIngestTarget::Sync),
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(error) => {
                // A storage failure, not a consent answer: retried.
                tracing::warn!(%account_id, %user_id, %graph_id, %error,
                    "Teams watch consent lookup failed — not storing the message");
                return Err(ConnectorError::Internal(format!("teams watch consent lookup: {error}")));
            }
        }
    }
    match target {
        GraphIngestTarget::Drop => Ok(false),
        GraphIngestTarget::Unavailable(why) => {
            Err(ConnectorError::Internal(format!("graph ingest: target unavailable: {why}")))
        }
        GraphIngestTarget::Sync => {
            let user_ctx = UserContext {
                user_id,
                account_id,
                role: "member".into(),
                device_id: String::new(),
            };
            let req = IngestAtomRequest {
                graph_id,
                source_type: item.source_type,
                source_id: item.source_id,
                content: item.content,
                participants: item.participants,
                timestamp: item.timestamp,
                scope: item.scope,
                fts_language: None,
                channel_id: item.channel_id,
                channel_name,
                author_id: item.author_id,
                author_name: item.author_name,
                project: None,
                mcp_client_name: None,
                raw_content: None,
                duration_ms: None,
                segments: None,
                session_id: None,
                session_kind: None,
                session_title: None,
                repo_path: None,
                remote: None,
                hostname: None,
                harness_version: None,
                model: None,
                branch: None,
                agent_name: None,
                platform: None,
                task: None,
                parent_session_id: None,
                trusted_user_agent: None,
                client_info: None,
                code_path: None,
            };
            // `Tenant::legacy` (org-tenancy PR 1.4): the personal-plane arm of
            // this webhook funnel — byte-identical to every commit before
            // PR 2.3, which only ever reached this line.
            let tenant = crate::tenancy::extractor::Tenant::legacy(state);
            let namespace=format!("microsoft:connection:{account_id}:{user_id}");
            let resp=crate::ingest::submit(crate::ingest::IngestRequest::private(crate::ingest::Member {
                provider_namespace: Some(&namespace),
                ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
            })).await;
            match resp {
                Ok(_) => Ok(true),
                // The refusal's own permanence decides the class: a
                // malformed or invalid item completes, everything else
                // (a dependency down, a mid-way failure, a grant or
                // authorization re-resolved on the next attempt) retries.
                Err(e) if e.is_permanent() => {
                    tracing::warn!(%account_id, %graph_id, error = ?e, "graph ingest refused for good");
                    Err(ConnectorError::NotConfigured(format!("ingest refused: {e:?}")))
                }
                Err(e) => {
                    tracing::warn!(%account_id, %graph_id, error = ?e, "graph ingest failed — retrying");
                    Err(ConnectorError::Internal(format!("ingest failed: {e:?}")))
                }
            }
        }
        GraphIngestTarget::Enqueue { pool, org_id } => {
            // Same `(graph_id, source_type, source_id)` atom-level dedup
            // `ingest_atom` already enforces makes a duplicate enqueue
            // harmless; this key just avoids piling up redundant `pending`
            // rows for a notification Graph redelivers before the first job
            // is claimed — same rationale as `routes::slack_events::
            // enqueue_slack_message_ingest`'s own idempotency key.
            let idempotency = format!(
                "graph_ingest:{account_id}:{graph_id}:{}:{}",
                item.source_type, item.source_id
            );
            let kind = jobs::JobKind::GraphMessageIngest {
                account_id,
                user_id,
                graph_id,
                channel_name,
                source_type: item.source_type,
                source_id: item.source_id,
                content: item.content,
                participants: item.participants,
                timestamp: item.timestamp,
                scope: item.scope,
                // `item.thread_id` deliberately dropped — see `JobKind::
                // GraphMessageIngest`'s own doc comment.
                channel_id: item.channel_id,
                author_id: item.author_id,
                author_name: item.author_name,
            };
            match jobs::queue::enqueue(pool, &kind, Some(account_id), Some(&idempotency), 0).await {
                Ok(Some(_)) => {
                    // Wake this tenant's harness — same "org ticks back off,
                    // the consumer plane never does" reasoning `enqueue_
                    // slack_message_ingest` documents for its own identical
                    // call.
                    state.worker_activity.note_enqueue(org_id);
                    Ok(true)
                }
                Ok(None) => {
                    tracing::debug!(
                        %account_id, %graph_id,
                        "GraphMessageIngest job already pending — deduped"
                    );
                    Ok(true)
                }
                // A failed write of the hand-off job: the tenant pool is
                // down or busy, which a retry can change.
                Err(e) => {
                    tracing::warn!(
                        error = %e, %account_id, %graph_id,
                        "failed to enqueue GraphMessageIngest job — retrying"
                    );
                    Err(ConnectorError::Internal(format!("enqueue GraphMessageIngest: {e}")))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::ingest::ingest_atom;

    /// m365 Train 2 PR 2.5 cross-plane read sweep: [`GraphIngestTarget::
    /// content_pool`] must resolve `Sync` against `state_db` (the
    /// control/consumer pool — byte-identical to the pre-2.5 unconditional
    /// `state.db` read), `Enqueue { pool, .. }` against ITS OWN `pool` (the
    /// resolved org tenant pool, NOT `state_db`), and `Drop` against neither.
    /// Regression coverage for the bug this PR fixed: `ingest_message`/
    /// `ingest_container_messages` used to call `teams_channel_name_by_
    /// container` against `state.db` UNCONDITIONALLY regardless of `target`,
    /// which silently resolved every Org-plane channel name to `None` once
    /// the org gate lifted (`teams_watched_resources` lives on the tenant
    /// pool, absent from `state.db` entirely) — same bug class as Slack's
    /// step-5a sweep (#492) found in `ingest_slack_message`'s
    /// `slack_integrations` read. Lazy-connect pools (never actually dial
    /// out, same trick `tenancy::extractor::Tenant`'s own test module uses)
    /// distinguished by their `PgConnectOptions::get_database()` name, so
    /// this needs no live Postgres at all.
    #[tokio::test]
    async fn content_pool_resolves_the_target_appropriate_pool_not_always_state_db() {
        let state_db = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/control_db")
            .expect("lazy pool construction never connects");
        let tenant_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/tenant_a_db")
            .expect("lazy pool construction never connects");

        // Sync (Personal / pre-2.5-reachable path): resolves `state_db`,
        // unchanged from before this PR.
        let sync = GraphIngestTarget::Sync;
        let resolved = sync
            .content_pool(Some(&state_db))
            .expect("Sync must resolve Some when state_db is configured");
        assert_eq!(
            resolved.connect_options().get_database(),
            Some("control_db"),
            "Sync must resolve state_db, not any other pool"
        );

        // Sync with no `state_db` configured (misconfigured cloud) —
        // `None`, matching the pre-2.5 `state.db.as_ref()` behavior exactly.
        assert!(sync.content_pool(None).is_none());

        // Enqueue (the Org path this PR makes genuinely reachable): must
        // resolve ITS OWN `pool` — the org's tenant pool — never `state_db`,
        // regardless of whether `state_db` is even configured.
        let enqueue = GraphIngestTarget::Enqueue {
            pool: tenant_pool.clone(),
            org_id: "org_a".to_string(),
        };
        let resolved = enqueue
            .content_pool(Some(&state_db))
            .expect("Enqueue must resolve Some regardless of state_db");
        assert_eq!(
            resolved.connect_options().get_database(),
            Some("tenant_a_db"),
            "Enqueue must resolve its OWN tenant pool, never state_db — this is the exact bug \
             this test guards against"
        );
        assert!(
            enqueue.content_pool(None).is_some(),
            "Enqueue's pool comes from the target itself, not state_db — must resolve Some even \
             when state_db is None"
        );

        // Drop: the notification is being discarded outright — no lookup is
        // worth making, regardless of what state_db is.
        let drop = GraphIngestTarget::Drop;
        assert!(drop.content_pool(Some(&state_db)).is_none());
        assert!(drop.content_pool(None).is_none());
        let unavailable = GraphIngestTarget::Unavailable("lookup timed out".into());
        assert!(unavailable.content_pool(Some(&state_db)).is_none());
    }

    #[test]
    fn client_state_matches_exact_only() {
        assert!(client_state_matches("s3cret-abc", "s3cret-abc"));
        assert!(!client_state_matches("s3cret-abc", "s3cret-abd"));
        assert!(!client_state_matches("", "s3cret-abc"));
        assert!(!client_state_matches("s3cret-abc", ""));
        // Length mismatch must fail (ct_eq returns 0 for differing lengths).
        assert!(!client_state_matches("short", "longer-value"));
    }

    #[test]
    fn parses_graph_notification_batch() {
        let body = serde_json::json!({
            "value": [{
                "subscriptionId": "sub-1",
                "changeType": "created",
                "clientState": "secret-xyz",
                "resource": "Users/u/Messages/m",
                "resourceData": { "@odata.type": "#Microsoft.Graph.Message", "id": "MSG-1" }
            }]
        });
        let batch: NotificationBatch = serde_json::from_value(body).unwrap();
        assert_eq!(batch.value.len(), 1);
        let n = parse_notification(&batch.value[0]).unwrap();
        assert_eq!(n.subscription_id.as_deref(), Some("sub-1"));
        assert_eq!(n.client_state.as_deref(), Some("secret-xyz"));
        assert_eq!(
            n.resource_data.as_ref().and_then(|r| r.id.as_deref()),
            Some("MSG-1")
        );
    }

    #[test]
    fn parses_lifecycle_notification() {
        let body = serde_json::json!({
            "value": [{
                "lifecycleEvent": "reauthorizationRequired",
                "subscriptionId": "sub-9",
                "clientState": "secret-xyz"
            }]
        });
        let batch: NotificationBatch = serde_json::from_value(body).unwrap();
        let n = parse_notification(&batch.value[0]).unwrap();
        assert_eq!(
            n.lifecycle_event.as_deref(),
            Some("reauthorizationRequired")
        );
        assert_eq!(n.subscription_id.as_deref(), Some("sub-9"));
        // A resource-change notification has no lifecycleEvent.
        assert!(n.resource_data.is_none());
    }

    #[test]
    fn resource_notification_has_no_lifecycle_event() {
        let body = serde_json::json!({
            "value": [{ "subscriptionId": "s", "clientState": "c",
                        "resourceData": { "id": "m" } }]
        });
        let batch: NotificationBatch = serde_json::from_value(body).unwrap();
        assert!(parse_notification(&batch.value[0]).unwrap().lifecycle_event.is_none());
    }

    #[test]
    fn empty_batch_parses_to_no_notifications() {
        let batch: NotificationBatch = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(batch.value.is_empty());
    }

    #[test]
    fn validation_token_uses_graph_field_name() {
        let params: ValidationParams =
            serde_json::from_value(serde_json::json!({ "validationToken": "echo-me" })).unwrap();
        assert_eq!(params.validation_token.as_deref(), Some("echo-me"));
        // Absent token (a real notification request) → None.
        let none: ValidationParams = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(none.validation_token.is_none());
    }

    // ── m365 Train 2 PR 2.3: `resolve_graph_ingest_target`'s decision table ──
    //
    // Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every other
    // `*_pg` suite in this crate:
    //
    //   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //     cargo test -p coralswarm-backend --lib routes::graph_webhook::tests
    //
    // Mirrors `routes::slack_events::enqueue_slack_message_ingest_fans_out_
    // to_the_correct_plane_per_account_pg`'s fixture shape (same helpers,
    // trimmed to what this module's own decision function needs) — that test
    // is the closest precedent for exercising this exact "personal vs
    // never-migrated-org vs active-org vs revoked-org" routing table.

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};

    use crate::store::PostgresStore;
    use crate::tenancy::registry::TenantRegistry;
    use crate::tenancy::routing::TenantStatus;
    use crate::tenancy::runtime::TenantRuntime;
    use crate::tenancy::secrets::StaticMapResolver;

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
    #[tokio::test]
    async fn teams_from_now_cutoff_is_member_and_container_scoped_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping teams_from_now_cutoff_is_member_and_container_scoped_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let guard = crate::test_db::TestDatabase::adopt(&base.parse().unwrap(), "orgt_teams_from_now_cutoff");
        let db: &str = &guard;
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.unwrap();
        let admin = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        let role = guard.role("cutoff");
        admin
            .execute(format!("CREATE ROLE {role} NOLOGIN").as_str())
            .await
            .unwrap();
        admin
            .execute(
                "CREATE TABLE teams_watched_resources (
            account_id uuid NOT NULL, user_id uuid NOT NULL, graph_id uuid NOT NULL,
            kind text NOT NULL, channel_id text NOT NULL, chat_id text NOT NULL,
            tenant_id text NOT NULL, auth_mode text NOT NULL, history_choice text,
            created_at timestamptz NOT NULL, last_ingested_at timestamptz NOT NULL);
            ALTER TABLE teams_watched_resources ENABLE ROW LEVEL SECURITY;
            CREATE POLICY account_boundary ON teams_watched_resources
              USING (account_id::text = current_setting('app.account_id', true));",
            )
            .await
            .unwrap();
        admin
            .execute(format!("GRANT SELECT ON teams_watched_resources TO {role}").as_str())
            .await
            .unwrap();
        let account = uuid::Uuid::new_v4();
        let member = uuid::Uuid::new_v4();
        let graph = uuid::Uuid::new_v4();
        let cutoff = chrono::DateTime::parse_from_rfc3339("2026-09-30T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        sqlx::query(
            "INSERT INTO teams_watched_resources VALUES ($1,$2,$3,'channel','C','',
            'tenant-A','delegated','none',$4,$4 + interval '1 day')",
        )
        .bind(account)
        .bind(member)
        .bind(graph)
        .bind(cutoff)
        .execute(&admin)
        .await
        .unwrap();
        let scoped_role = role.clone();
        let app = PgPoolOptions::new()
            .after_connect(move |conn, _| {
                let statement = format!("SET ROLE {scoped_role}");
                Box::pin(async move { conn.execute(statement.as_str()).await.map(|_| ()) })
            })
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        let before = cutoff - chrono::Duration::milliseconds(1);
        let after = cutoff + chrono::Duration::milliseconds(1);
        assert!(!teams_watch_allows_message(
            &app,
            account,
            member,
            graph,
            "teams_channel",
            "C",
            before,
            false
        )
        .await
        .unwrap());
        assert!(teams_watch_allows_message(
            &app,
            account,
            member,
            graph,
            "teams_channel",
            "C",
            cutoff,
            false
        )
        .await
        .unwrap());
        assert!(
            teams_watch_allows_message(
                &app,
                account,
                member,
                graph,
                "teams_channel",
                "C",
                after,
                false
            )
            .await
            .unwrap(),
            "mutable last_ingested_at must not become the cutoff"
        );
        assert!(!teams_watch_allows_message(
            &app,
            uuid::Uuid::new_v4(),
            member,
            graph,
            "teams_channel",
            "C",
            after,
            false
        )
        .await
        .unwrap());
        assert!(!teams_watch_allows_message(
            &app,
            account,
            uuid::Uuid::new_v4(),
            graph,
            "teams_channel",
            "C",
            after,
            false
        )
        .await
        .unwrap());
        assert!(!teams_watch_allows_message(
            &app,
            account,
            member,
            uuid::Uuid::new_v4(),
            "teams_channel",
            "C",
            after,
            false
        )
        .await
        .unwrap());
        assert!(!teams_watch_allows_message(
            &app,
            account,
            member,
            graph,
            "teams_channel",
            "other",
            after,
            false
        )
        .await
        .unwrap());
        assert!(!teams_watch_allows_message(
            &app,
            account,
            member,
            graph,
            "teams_chat",
            "C",
            after,
            false
        )
        .await
        .unwrap());
        admin
            .execute("UPDATE teams_watched_resources SET kind='chat', chat_id='C', channel_id=''")
            .await
            .unwrap();
        assert!(!teams_watch_allows_message(
            &app,
            account,
            member,
            graph,
            "teams_chat",
            "C",
            before,
            false
        )
        .await
        .unwrap());
        assert!(teams_watch_allows_message(
            &app,
            account,
            member,
            graph,
            "teams_chat",
            "C",
            after,
            false
        )
        .await
        .unwrap());
        admin
            .execute(
                "UPDATE teams_watched_resources SET kind='channel', chat_id='', channel_id='C'",
            )
            .await
            .unwrap();
        admin
            .execute("UPDATE teams_watched_resources SET tenant_id=''")
            .await
            .unwrap();
        assert!(
            !teams_watch_allows_message(
                &app,
                account,
                member,
                graph,
                "teams_channel",
                "C",
                after,
                false
            )
            .await
            .unwrap(),
            "new delegated From-now enrollment requires a tenant binding"
        );
        admin
            .execute("UPDATE teams_watched_resources SET history_choice=NULL")
            .await
            .unwrap();
        assert!(
            teams_watch_allows_message(
                &app,
                account,
                member,
                graph,
                "teams_channel",
                "C",
                before,
                false
            )
            .await
            .unwrap(),
            "legacy watches preserve historical capture"
        );
        admin
            .execute("UPDATE teams_watched_resources SET history_choice='none', auth_mode='app'")
            .await
            .unwrap();
        assert!(
            teams_watch_allows_message(
                &app,
                account,
                member,
                graph,
                "teams_channel",
                "C",
                before,
                true
            )
            .await
            .unwrap(),
            "legacy app-mode behavior stays unchanged"
        );
        assert!(
            !teams_watch_allows_message(
                &app,
                account,
                member,
                graph,
                "teams_channel",
                "C",
                before,
                false
            )
            .await
            .unwrap(),
            "org capture cannot use an app-mode row to bypass delegated consent"
        );
        app.close().await;
        admin.close().await;
        let mut cleanup = PgConnection::connect(&base).await.unwrap();
        cleanup
            .execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        cleanup
            .execute(format!("DROP ROLE {role}").as_str())
            .await
            .unwrap();
    }
    async fn seed_user(pool: &PgPool) -> uuid::Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", uuid::Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }
    async fn seed_account(
        pool: &PgPool,
        owner: uuid::Uuid,
        clerk_org_id: Option<&str>,
    ) -> uuid::Uuid {
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    async fn mint_tenant_databases_row(
        control_pool: &PgPool,
        org_id: &str,
        slug: &str,
        account_id: uuid::Uuid,
        secret_ref: &str,
        status: TenantStatus,
    ) {
        crate::billing::renewal::execute_financial_fixture(
            control_pool,
            account_id,
            sqlx::query(
                "INSERT INTO tenant_databases \
                    (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
                 VALUES ($1, $2, $3, $4, 'neon', 1, $5)",
            )
            .bind(org_id)
            .bind(account_id)
            .bind(slug)
            .bind(secret_ref)
            .bind(status.as_str()),
        )
        .await
        .unwrap();
    }
    async fn build_tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }
    fn state_with_control(
        control: PgPool,
        tenant_runtime: Option<Arc<TenantRuntime>>,
    ) -> Arc<AppState> {
        let mut state = crate::test_app_state_owned(tenant_runtime);
        state.db = Some(control.clone());
        state.store = Some(Arc::new(PostgresStore::new(control)));
        Arc::new(state)
    }

    /// A personal (non-org) account must resolve to [`GraphIngestTarget::
    /// Sync`] — the byte-identical, pre-PR-2.3 in-process path. This is the
    /// PR's own "personal-plane regression" proof: `ingest_message`/
    /// `ingest_message_with`'s `Sync` arm is untouched code (still `Tenant::
    /// legacy(state)` → `ingest_atom`), so what actually changed — and what
    /// must be proven not to have regressed — is that a personal account
    /// still reaches that arm at all, never the new `Enqueue` branch.
    #[tokio::test]
    async fn resolve_graph_ingest_target_personal_account_is_sync_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_graph_ingest_target_personal_account_is_sync_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t23_target_personal";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, None).await;
        let state = state_with_control(control.clone(), None);

        let target = resolve_graph_ingest_target(&state, account_id).await;
        assert!(
            matches!(target, GraphIngestTarget::Sync),
            "a personal account must resolve to the synchronous in-process path"
        );

        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// An org account with NO `tenant_databases` row (never onboarded onto
    /// its own tenant database) must resolve to `Enqueue { pool: <the
    /// control/consumer pool> }` — its content genuinely still lives there,
    /// same reasoning `routes::slack_events::resolve_org_plane`'s "Missing"
    /// arm documents for Slack.
    #[tokio::test]
    async fn resolve_graph_ingest_target_org_never_tenant_migrated_falls_back_to_consumer_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_graph_ingest_target_org_never_tenant_migrated_falls_back_to_consumer_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t23_target_missing";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_2 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let org_id = format!("org_t23_missing_{}", uuid::Uuid::new_v4());
        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, Some(&org_id)).await;
        let state = state_with_control(control.clone(), None);

        let target = resolve_graph_ingest_target(&state, account_id).await;
        match target {
            GraphIngestTarget::Enqueue {
                pool,
                org_id: got_org_id,
            } => {
                assert_eq!(got_org_id, org_id);
                // Same physical pool as `control` — this org's content lives
                // on the shared consumer plane, not a tenant DB (there isn't
                // one).
                let marker: i64 = sqlx::query_scalar("SELECT 1::bigint")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                assert_eq!(marker, 1);
                let same_db: String = sqlx::query_scalar("SELECT current_database()")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                assert_eq!(same_db, db, "must be the consumer plane's own database");
            }
            GraphIngestTarget::Sync => panic!(
                "expected Enqueue onto the consumer pool for a never-tenant-migrated org, got Sync \
                 — an org account must never take the personal-plane path"
            ),
            GraphIngestTarget::Drop => panic!(
                "expected Enqueue onto the consumer pool for a never-tenant-migrated org, got Drop \
                 — a missing tenant_databases row must fall back to the consumer plane, not refuse"
            ),
            GraphIngestTarget::Unavailable(why) => panic!(
                "expected Enqueue onto the consumer pool for a never-tenant-migrated org, got \
                 Unavailable: {why}"
            ),
        }

        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A revoked org must resolve to [`GraphIngestTarget::Drop`] — never land
    /// new content for a revoked org, on any plane.
    #[tokio::test]
    async fn resolve_graph_ingest_target_org_revoked_drops_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_graph_ingest_target_org_revoked_drops_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t23_target_revoked";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_3 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let org_id = format!("org_t23_revoked_{}", uuid::Uuid::new_v4());
        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, Some(&org_id)).await;
        sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)")
            .bind(&org_id)
            .execute(&control)
            .await
            .unwrap();
        let state = state_with_control(control.clone(), None);

        let target = resolve_graph_ingest_target(&state, account_id).await;
        assert!(
            matches!(target, GraphIngestTarget::Drop),
            "a revoked org must never resolve to a plane to enqueue onto"
        );

        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// An org with an `Active` `tenant_databases` row must resolve to
    /// `Enqueue { pool: <that org's own tenant pool> }` — the durable
    /// hand-off's actual destination, proven against a REAL second Postgres
    /// database via the real `TenantRegistry`, not a stand-in. This is the
    /// PR's "job enqueued onto the tenant pool" proof; `workers::
    /// graph_ingest`'s own pg suite (`graph_ingest_worker_tenant_loop_pg.rs`)
    /// covers the consumer side (the worker dequeuing and processing that
    /// job).
    #[tokio::test]
    async fn resolve_graph_ingest_target_org_active_tenant_resolves_the_tenant_pool_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_graph_ingest_target_org_active_tenant_resolves_the_tenant_pool_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = ("orgt_t23_target_active_ctl", "orgt_t23_target_active_tnt");
        let _database_cleanup_4 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let conn = recreate(&base, ctl_db).await;
        conn.close().await.ok();
        let _database_cleanup_5 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
        let tconn = recreate(&base, tnt_db).await;
        tconn.close().await.ok();

        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, ctl_db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let org_id = format!("org_t23_active_{}", uuid::Uuid::new_v4());
        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, Some(&org_id)).await;
        mint_tenant_databases_row(
            &control,
            &org_id,
            "t23-active",
            account_id,
            "SECRET_T23_ACTIVE",
            TenantStatus::Active,
        )
        .await;
        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;

        let resolver =
            StaticMapResolver::from_pairs([("SECRET_T23_ACTIVE", with_db(&base, tnt_db))]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let state = state_with_control(
            control.clone(),
            Some(TenantRuntime::new(true, "coralswarm.test", registry)),
        );

        let target = resolve_graph_ingest_target(&state, account_id).await;
        match target {
            GraphIngestTarget::Enqueue {
                pool,
                org_id: got_org_id,
            } => {
                assert_eq!(got_org_id, org_id);
                // Prove it's genuinely the TENANT database, not the control
                // one — `tenant_meta` only exists on the tenant tree.
                let seen_org: String =
                    sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
                        .fetch_one(&pool)
                        .await
                        .expect("tenant_meta must be reachable on the resolved pool");
                assert_eq!(seen_org, org_id);
            }
            GraphIngestTarget::Sync => {
                panic!("expected Enqueue onto the org's own tenant pool, got Sync")
            }
            GraphIngestTarget::Drop => {
                panic!("expected Enqueue onto the org's own tenant pool, got Drop")
            }
            GraphIngestTarget::Unavailable(why) => {
                panic!("expected Enqueue onto the org's own tenant pool, got Unavailable: {why}")
            }
        }

        control.close().await;
        tenant.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        for db in [ctl_db, tnt_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    // ── m365 Train 2 PR 2.7: purged/unknown subscription must not ingest ─────
    //
    // `process_notification`'s FIRST act is `microsoft::subscription_by_id`
    // (see this file's module doc + that function's own comment). PR 3.7's
    // `microsoft_disconnect` pg suite (`account_wide_purge_removes_every_
    // row_across_all_four_tables_pg`) proves the `graph_subscriptions` ROW
    // disappears on purge, but its own comment says the lookup itself is
    // "not exercised directly here — that lookup goes through a
    // control-plane-only SECURITY DEFINER resolver with no tenant-tree
    // counterpart... so the row's ABSENCE, which this test asserts directly,
    // is what that lookup's `None` result depends on". These four tests are
    // that missing other half: they exercise the lookup itself AND the real
    // `process_notification` production entry point, end to end, for both a
    // purged/unknown subscription (must be a silent no-op) and a live one
    // (the lookup must succeed and the function must proceed past the
    // clientState gate). Control-pool only (`migrations_control`), same as
    // `resolve_graph_ingest_target_*_pg` above — `resolve_graph_subscription_
    // account_id` (migration 0086, §2) has no tenant-tree counterpart, per
    // that migration's own header and `connectors::microsoft`'s module-doc
    // RLS-scoping note.
    //
    // What this suite does NOT cover, and why: proving a live subscription's
    // notification actually FETCHES the changed message and INGESTS it would
    // need a mockable Graph HTTP endpoint — `connectors::microsoft::GRAPH` is
    // a hardcoded `https://graph.microsoft.com/v1.0` const, not swappable
    // like `StripeClient::with_base_url`, and widening it is real connector
    // surgery out of this fixtures-and-conformance PR's scope (and a conflict
    // hazard against `email_body.rs`/other in-flight Microsoft work). The
    // live-subscription test below goes exactly as far as this crate makes
    // possible without that: it proves the lookup resolves and
    // `process_notification` reaches the credential-load step (the next gate
    // downstream) rather than bailing at the subscription lookup, using a
    // deterministic no-oauth-token account so no network call is ever made.
    //
    //   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //     cargo test -p coralswarm-backend --lib \
    //       routes::graph_webhook::tests::subscription_by_id_ \
    //       routes::graph_webhook::tests::process_notification_for_ \
    //     -- --test-threads=1

    async fn seed_graph_subscription(
        control: &PgPool,
        account_id: uuid::Uuid,
        subscription_id: &str,
        resource: &str,
        client_state: &str,
        tenant_id: &str,
    ) {
        sqlx::query(
            "INSERT INTO graph_subscriptions \
                (account_id, subscription_id, resource, client_state, expires_at, tenant_id) \
             VALUES ($1, $2, $3, $4, now() + interval '1 hour', $5)",
        )
        .bind(account_id)
        .bind(subscription_id)
        .bind(resource)
        .bind(client_state)
        .bind(tenant_id)
        .execute(control)
        .await
        .unwrap();
    }

    /// A subscription id with no `graph_subscriptions` row at all — exactly
    /// the state left behind by `microsoft_disconnect::purge_microsoft_
    /// connection`'s step 4 DELETE — must resolve to `None`.
    #[tokio::test]
    async fn subscription_by_id_returns_none_for_a_purged_or_unknown_subscription_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping subscription_by_id_returns_none_for_a_purged_or_unknown_subscription_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t27_sub_purged";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_6 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let resolved = microsoft::subscription_by_id(&control, "sub-never-existed")
            .await
            .unwrap();
        assert!(
            resolved.is_none(),
            "an unknown/purged subscription id must resolve to None — this is what makes a \
             redelivered notification for it a silent no-op in process_notification"
        );

        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A live `graph_subscriptions` row must resolve to exactly the fields a
    /// genuine notification needs to act on — the symmetric positive case to
    /// the test above, proving the lookup discriminates correctly in both
    /// directions rather than just "returns something".
    #[tokio::test]
    async fn subscription_by_id_resolves_a_live_subscriptions_own_fields_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping subscription_by_id_resolves_a_live_subscriptions_own_fields_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t27_sub_live";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_7 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, None).await;
        seed_graph_subscription(
            &control,
            account_id,
            "sub-live-1",
            "me/mailFolders('inbox')/messages",
            "secret-live-xyz",
            "",
        )
        .await;

        let resolved = microsoft::subscription_by_id(&control, "sub-live-1")
            .await
            .unwrap();
        assert_eq!(
            resolved,
            Some((
                account_id,
                "secret-live-xyz".to_string(),
                "me/mailFolders('inbox')/messages".to_string(),
                "".to_string(),
                None,
            ))
        );

        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // Fixture-driven (backend/tests/fixtures/graph/ — see its README for
    // provenance: hand-authored against Microsoft's documented schema and
    // this crate's own parsing, not a live capture).
    const NOTIFICATION_UNKNOWN_SUBSCRIPTION: &str =
        include_str!("../../tests/fixtures/graph/notification_unknown_subscription.json");
    const OUTLOOK_NOTIFICATION_ENVELOPE: &str =
        include_str!("../../tests/fixtures/graph/outlook_notification_envelope.json");
    const NOTIFICATION_WRONG_CLIENT_STATE: &str =
        include_str!("../../tests/fixtures/graph/notification_wrong_client_state.json");
    const MALFORMED_NOTIFICATION_BODY: &str =
        include_str!("../../tests/fixtures/graph/malformed_notification_body.json");

    fn first_notification(batch_json: &str) -> Notification {
        let batch: NotificationBatch = serde_json::from_str(batch_json).unwrap();
        parse_notification(batch.value.first().expect("fixture batch has one entry")).unwrap()
    }

    /// `process_notification` itself (not just its lookup helper) for a
    /// purged/unknown subscription id — driven by the
    /// `notification_unknown_subscription.json` fixture, the exact shape
    /// Graph would redeliver for a subscription `microsoft_disconnect::
    /// purge_microsoft_connection` already purged: the actual production
    /// entry point the webhook route spawns per notification must complete
    /// with `Ok(())` and take the earliest-possible exit — no credential
    /// load, no Graph fetch, no ingest attempt — exactly the silent-no-op
    /// contract `microsoft_disconnect`'s own module doc promises.
    #[tokio::test]
    async fn process_notification_for_a_purged_subscription_is_a_silent_noop_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping process_notification_for_a_purged_subscription_is_a_silent_noop_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t27_procnotif_purged";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_8 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();
        let state = state_with_control(control.clone(), None);

        // No graph_subscriptions row seeded at all — the fixture's
        // subscriptionId (00000000-...) is deliberately absent, standing in
        // for "purged" just as well as "never existed".
        let n = first_notification(NOTIFICATION_UNKNOWN_SUBSCRIPTION);
        let result = process_notification(state, n).await;
        assert!(
            result.is_ok(),
            "a purged/unknown subscription must be a clean no-op, not an error"
        );

        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// `process_notification` for a LIVE, correctly-verified subscription on
    /// an account with no stored Microsoft credentials (disconnected some
    /// other way, or never finished connecting) — driven by the
    /// `outlook_notification_envelope.json` fixture, whose `subscriptionId`/
    /// `clientState`/`resource` are seeded verbatim into `graph_subscriptions`
    /// so the fixture and this assertion can never drift apart: the lookup +
    /// constant-time clientState gate must PASS — the mirror image of the
    /// purged case above — and the function must still complete cleanly by
    /// reaching the next gate downstream (`load_creds_bg` finding no
    /// `oauth_tokens` row), with no panic and no network call. This is as
    /// far into the live path as this suite can go without a mockable Graph
    /// endpoint — see this suite's own header comment.
    #[tokio::test]
    async fn process_notification_for_a_live_subscription_with_no_credentials_completes_cleanly_pg()
    {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping process_notification_for_a_live_subscription_with_no_credentials_completes_cleanly_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t27_procnotif_live";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_9 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, None).await;
        let n = first_notification(OUTLOOK_NOTIFICATION_ENVELOPE);
        seed_graph_subscription(
            &control,
            account_id,
            n.subscription_id.as_deref().unwrap(),
            "me/mailFolders('inbox')/messages",
            n.client_state.as_deref().unwrap(),
            "",
        )
        .await;
        let state = state_with_control(control.clone(), None);

        let result = process_notification(state, n).await;
        assert!(
            result.is_ok(),
            "a live subscription with no oauth_tokens row must still complete cleanly (the \
             established 'no microsoft connection for notified account' branch), not error"
        );

        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A forged/stale `clientState` on an otherwise-real subscription id
    /// (`notification_wrong_client_state.json` carries the SAME
    /// `subscriptionId` as `outlook_notification_envelope.json`, but a
    /// different secret) must fail `client_state_matches` — a realistic,
    /// fixture-shaped mismatch check, not just the short hand-typed literals
    /// `client_state_matches_exact_only` already covers. Pure, no DB.
    #[test]
    fn wrong_client_state_fixture_fails_the_constant_time_check() {
        let real = first_notification(OUTLOOK_NOTIFICATION_ENVELOPE);
        let forged = first_notification(NOTIFICATION_WRONG_CLIENT_STATE);
        assert_eq!(
            real.subscription_id, forged.subscription_id,
            "same subscription, forged secret"
        );
        assert!(!client_state_matches(
            &forged.client_state.unwrap(),
            &real.client_state.unwrap(),
        ));
    }

    /// A malformed (unparseable) notification body must still be ACKed
    /// (`202`) by the webhook route, not surfaced as an error — Graph
    /// retries on a non-2xx/timeout, and a body that can never parse would
    /// otherwise retry forever. Pure, no DB (the parse failure is handled
    /// before any pool access).
    #[tokio::test]
    async fn graph_notifications_acks_a_malformed_body_instead_of_retrying_forever() {
        let state = Arc::new(crate::test_app_state_owned(None));
        let resp = graph_notifications(
            axum::extract::State(state),
            axum::extract::Query(ValidationParams {
                validation_token: None,
            }),
            axum::body::Bytes::from(MALFORMED_NOTIFICATION_BODY.as_bytes().to_vec()),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::ACCEPTED,
            "a malformed body must still be ACKed (202) so Graph stops retrying a batch it can \
             never successfully redeliver"
        );
    }

    // ── m365 Train 4 PR 4.4: `deleted` change notification → delete_source ───

    /// A `deleted`-shaped notification body, built inline (not a checked-in
    /// fixture — `Notification`'s shape is simple enough that constructing it
    /// via `serde_json::json!` keeps this suite self-contained, matching how
    /// `deleted_notification`'s only caller-visible knobs — subscriptionId/
    /// clientState/the deleted item's own Graph id — vary per test).
    fn deleted_notification(
        subscription_id: &str,
        client_state: &str,
        resource_data_id: &str,
    ) -> Notification {
        let body = serde_json::json!({
            "subscriptionId": subscription_id,
            "clientState": client_state,
            "changeType": "deleted",
            "resourceData": { "id": resource_data_id },
        });
        serde_json::from_value(body).unwrap()
    }

    /// Exercise the public ingress handler, then create a same-account promoted
    /// copy and adversarial neighbors. No hand-written chunk identity stands in
    /// for the actual ingestion convention in the deletion regression.
    async fn ingest_deletion_family(
        state: &Arc<AppState>,
        pool: &PgPool,
        account: uuid::Uuid,
        owner: uuid::Uuid,
        graph: uuid::Uuid,
        source_type: &str,
        source_id: &str,
        org: Option<&str>,
    ) -> (Vec<uuid::Uuid>, Vec<uuid::Uuid>) {
        use crate::tenancy::{
            extractor::Tenant,
            runtime::{TenancyDecision, TenantContext},
            TenantId,
        };
        let decision = org.map(|org| {
            TenancyDecision::Org(TenantContext {
                org_id: org.to_owned(),
                account_id: account,
                user_id: owner,
                pool: pool.clone(),
                page_pool: None,
            })
        });
        let key = org
            .map(|org| TenantId::Org(org.to_owned()))
            .unwrap_or(TenantId::Consumer);
        state.dek_cache.insert(&key, account, [7; 32]);
        if org.is_none() {
            sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
                .bind(account).bind(owner).execute(pool).await.unwrap();
        }
        let tenant = Tenant {
            store: Some(Arc::new(PostgresStore::new(pool.clone()))),
            pool: Some(pool.clone()),
            decision,
            class: crate::route_class::RouteClass::Other,
        };
        let req: IngestAtomRequest = serde_json::from_value(serde_json::json!({
            "graph_id": graph, "source_type": source_type, "source_id": source_id,
            "content": if source_type == "outlook" {
                "Latest reply.\n> Prior response.\n>> Original message.\nLatest closing line.".to_owned()
            } else {
                "A detailed project discussion with enough content to produce multiple chunks. ".repeat(160)
            },
            "participants": [], "timestamp": chrono::Utc::now(),
        })).unwrap();
        let user=UserContext {user_id:owner,account_id:account,role:"owner".into(),device_id:String::new()};
        let namespace=format!("microsoft:connection:{account}:{owner}");
        let ingested=crate::ingest::submit(crate::ingest::IngestRequest::private(crate::ingest::Member {
            provider_namespace: Some(&namespace),
            ..crate::ingest::Member::new(state, &tenant, &user, req)
        })).await.unwrap_or_else(|refused| panic!("public ingest failed: {refused}"));
        let count = ingested.body.atom_ids.len();
        assert!(count > 1, "fixture must generate real multiple chunks");
        let exact = crate::services::deletion::delete_source(
            pool,
            None,
            crate::store::CLOUD_DEVICE_ID,
            graph,
            source_type,
            source_id,
            crate::services::connector_audit::Actor::System,
            None,
        )
        .await
        .unwrap();
        assert!(
            exact.atoms_deleted.is_empty(),
            "ordinary source deletion must remain exact"
        );
        let confined = crate::services::deletion::delete_source_in_callers_scope(
            pool,
            None,
            crate::store::CLOUD_DEVICE_ID,
            graph,
            source_type,
            source_id,
            crate::services::connector_audit::Actor::User(owner),
            None,
        )
        .await
        .unwrap();
        assert!(
            confined.atoms_deleted.is_empty(),
            "user-scoped retraction must remain exact"
        );
        let promoted: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id,name,kind) VALUES($1,'promoted','team') RETURNING id",
        )
        .bind(account)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,participants,\"timestamp\",scope,promoted_from_atom_id,ingested_by) SELECT gen_random_uuid(),$1,source_type,source_id,content_blob_key,content_inline,participants,\"timestamp\",scope,atom_id,ingested_by FROM atoms WHERE graph_id=$2")
            .bind(promoted).bind(graph).execute(pool).await.unwrap();
        let deleted: Vec<uuid::Uuid> =
            sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id = ANY($1)")
                .bind(vec![graph, promoted])
                .fetch_all(pool)
                .await
                .unwrap();
        assert_eq!(deleted.len(), count * 2);
        let other_account = if org.is_some() {
            uuid::Uuid::new_v4()
        } else {
            // A personal account is unique per owner. Use a distinct caller
            // so this survivor exercises a real account boundary.
            let other_owner = seed_user(pool).await;
            seed_account(pool, other_owner, None).await
        };
        let other_graph: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id,name) VALUES($1,'other account') RETURNING id",
        )
        .bind(other_account)
        .fetch_one(pool)
        .await
        .unwrap();
        let mut surviving = Vec::new();
        for (destination, ty, id) in [
            (
                graph,
                source_type.to_owned(),
                format!("{source_id}-neighbor:chunk:0"),
            ),
            (
                graph,
                source_type.to_owned(),
                format!("{source_id}:chunk:not-a-number"),
            ),
            (
                graph,
                source_type.to_owned(),
                format!("{source_id}:chunk:0:other"),
            ),
            (
                graph,
                source_type.to_owned(),
                source_id.replace('_', "X").replace('%', "Y") + ":chunk:0",
            ),
            (
                graph,
                "unrelated_provider".to_owned(),
                format!("{source_id}:chunk:0"),
            ),
            (
                other_graph,
                source_type.to_owned(),
                format!("{source_id}:chunk:0"),
            ),
        ] {
            let atom = uuid::Uuid::new_v4();
            sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,participants,\"timestamp\",scope) VALUES($1,$2,$3,$4,'fixture', '{}', now(),'private')")
                .bind(atom).bind(destination).bind(ty).bind(id).execute(pool).await.unwrap();
            surviving.push(atom);
        }
        (deleted, surviving)
    }

    async fn assert_deletion_family(
        pool: &PgPool,
        account: uuid::Uuid,
        deleted: &[uuid::Uuid],
        survivors: &[uuid::Uuid],
    ) {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=ANY($1)")
            .bind(survivors)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(
            count as usize,
            survivors.len(),
            "neighbors, other provider, other account must survive"
        );
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id=ANY($1)")
            .bind(deleted)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(
            remaining, 0,
            "every generated chunk and promoted copy must be absent"
        );
        let detail: serde_json::Value = sqlx::query_scalar("SELECT detail FROM connector_audit_log WHERE account_id=$1 AND action='deletion_propagated' AND actor_kind='webhook'")
            .bind(account).fetch_one(pool).await.unwrap();
        assert_eq!(
            detail["atoms_deleted"].as_u64().unwrap() as usize,
            deleted.len(),
            "all chunks and promoted copies must be deleted together"
        );
    }

    /// End-to-end through the REAL production entry point (`process_
    /// notification`, not `process_deleted_notification` directly): a live
    /// mail subscription's `deleted` notification must find the already-
    /// ingested atom by its own natural key (no `conversationId` is ever
    /// available for a deleted message — see `process_deleted_notification`'s
    /// own doc), delete it via `services::deletion::delete_source`, and write
    /// exactly one `deletion_propagated` audit row attributed to
    /// `Actor::Webhook`. Personal plane (`GraphIngestTarget::Sync`).
    #[tokio::test]
    async fn deleted_notification_mail_propagates_to_delete_source_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deleted_notification_mail_propagates_to_delete_source_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t44_del_mail";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_10 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, None).await;
        let graph_id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name) VALUES ($1, 'primary') RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&control)
        .await
        .unwrap();
        seed_graph_subscription(
            &control,
            account_id,
            "sub-del-1",
            "me/mailFolders('inbox')/messages",
            "secret-del-1",
            "",
        )
        .await;

        let n = deleted_notification("sub-del-1", "secret-del-1", "msg_del_%_1");
        let state = state_with_control(control.clone(), None);

        let (deleted_count, survivors) = ingest_deletion_family(
            &state, &control, account_id, owner, graph_id, "outlook", "msg_del_%_1", None,
        ).await;
        // Same native message ID in another mailbox has zero legacy chunks
        // because the old retrieval key is graph/type/native-ID only.
        let declared_req:IngestAtomRequest=serde_json::from_value(serde_json::json!({
            "graph_id":graph_id,"source_type":"outlook","source_id":"msg_del_%_1",
            "content":"A user-declared source is independent of the mailbox.","participants":[],"timestamp":chrono::Utc::now(),
        })).unwrap();
        let declared_response=ingest_atom(State(state.clone()),crate::tenancy::extractor::Tenant::legacy(&state),
            axum::Extension(UserContext {user_id:owner,account_id,role:"owner".into(),device_id:String::new()}),
            axum::http::HeaderMap::new(),axum::Json(declared_req)).await;
        assert!(declared_response.status().is_success());
        let other_owner=seed_user(&control).await;
        let other_namespace=format!("microsoft:connection:{account_id}:{other_owner}");
        let tenant=crate::tenancy::extractor::Tenant::legacy(&state);
        let req:IngestAtomRequest=serde_json::from_value(serde_json::json!({
            "graph_id":graph_id,"source_type":"outlook","source_id":"msg_del_%_1",
            "content":"Another mailbox's independent message.","participants":[],"timestamp":chrono::Utc::now(),
        })).unwrap();
        let user=UserContext {user_id:owner,account_id,role:"owner".into(),device_id:String::new()};
        let captured=crate::ingest::submit(crate::ingest::IngestRequest::private(crate::ingest::Member {
            provider_namespace: Some(&other_namespace),
            ..crate::ingest::Member::new(&state, &tenant, &user, req)
        })).await;
        assert!(captured.is_ok(), "{:?}", captured.err());
        let other_item:uuid::Uuid=sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND provider_namespace=$2")
            .bind(graph_id).bind(&other_namespace).fetch_one(&control).await.unwrap();
        let links:i64=sqlx::query_scalar("SELECT count(*) FROM source_version_chunks c JOIN source_versions v ON v.version_id=c.source_version_id WHERE v.source_item_id=$1")
            .bind(other_item).fetch_one(&control).await.unwrap();
        assert_eq!(links,0,"fixture must exercise a snapshot without retrieval chunks");
        let result = process_notification(state.clone(), n).await;
        assert!(
            result.is_ok(),
            "deleted-notification handling must not error: {result:?}"
        );

        let remaining: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_type = 'outlook' AND source_id = 'msg_del_%_1:chunk:0'",
        )
        .bind(graph_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(
            remaining, 0,
            "a deleted mail notification must propagate the atom's removal"
        );

        let audit_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_audit_log \
             WHERE account_id = $1 AND action = 'deletion_propagated' AND actor_kind = 'webhook'",
        )
        .bind(account_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(
            audit_count, 1,
            "delete_source must write exactly one deletion_propagated/webhook audit row — no \
             double-emit from this dispatch layer"
        );

        assert_deletion_family(&control, account_id, &deleted_count, &survivors).await;
        let declared_survives:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM source_items WHERE graph_id=$1 AND provider_namespace=$2 AND source_id='msg_del_%_1')")
            .bind(graph_id).bind(format!("declared:user:{owner}")).fetch_one(&control).await.unwrap();
        assert!(declared_survives,"provider events cannot delete public user-declared evidence");
        let survives:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM source_items WHERE source_item_id=$1)")
            .bind(other_item).fetch_one(&control).await.unwrap();
        assert!(survives,"mailbox A deletion must preserve mailbox B's same-ID zero-chunk source");
        sqlx::query("INSERT INTO graph_subscriptions(account_id,subscription_id,resource,client_state,expires_at,tenant_id,owner_user_id) VALUES($1,'sub-del-B','me/mailFolders(''inbox'')/messages','secret-B',now()+interval '1 hour','',$2)")
            .bind(account_id).bind(other_owner).execute(&control).await.unwrap();
        process_notification(state.clone(),deleted_notification("sub-del-B","secret-B","msg_del_%_1")).await.unwrap();
        let survives:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM source_items WHERE source_item_id=$1)")
            .bind(other_item).fetch_one(&control).await.unwrap();
        assert!(!survives,"mailbox B deletion must resolve its source even with zero chunks");
        let legacy=uuid::Uuid::new_v4();
        let other_legacy=uuid::Uuid::new_v4();
        for (id,actor,position) in [(legacy,owner,0),(other_legacy,other_owner,1)] {
            sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,participants,timestamp,ingested_by,scope) VALUES($1,$2,'outlook',$3,'legacy-inline','legacy mail','{}',now(),$4,'private')")
                .bind(id).bind(graph_id).bind(format!("legacy-mail:chunk:{position}")).bind(actor)
                .execute(&control).await.unwrap();
        }
        process_notification(state.clone(),deleted_notification("sub-del-1","secret-del-1","legacy-mail")).await.unwrap();
        let remaining:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT atom_id FROM atoms WHERE atom_id=ANY($1)")
            .bind(vec![legacy,other_legacy]).fetch_all(&control).await.unwrap();
        assert_eq!(remaining,vec![other_legacy],"legacy-only deletion must qualify each chunk by trusted mailbox owner");


        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A `deleted` notification for a message that was never ingested (never
    /// opted into a watched thread/channel) must be a clean no-op — no atom
    /// to delete, and no `connector_audit_log` row worth writing (mirrors
    /// `google_gmail::propagate_deletions`'s identical "skip audit noise for
    /// a source with nothing to delete" precedent). Called directly against
    /// `process_deleted_notification` — the subscription lookup/clientState
    /// gate this exercises when reached through `process_notification` is
    /// already covered by `process_notification_deleted_mail_propagates_
    /// to_delete_source_pg` and the pre-existing `subscription_by_id_*_pg`
    /// suite.
    #[tokio::test]
    async fn deleted_notification_never_ingested_source_is_a_silent_noop_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deleted_notification_never_ingested_source_is_a_silent_noop_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t44_del_noop";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_11 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, None).await;
        // Deliberately NO graph, NO atom seeded — this message was never
        // opted into anything.
        let state = state_with_control(control.clone(), None);

        let n = deleted_notification("sub-unused", "secret-unused", "msg-never-ingested");
        let result = process_deleted_notification(
            state,
            account_id,
            "me/mailFolders('inbox')/messages".to_string(),
            n,
            Some(owner),
        )
        .await;
        assert!(result.is_ok());

        let audit_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM connector_audit_log WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(
            audit_count, 0,
            "a source that was never ingested must not write an audit row — atom_graph_for_source \
             finding nothing must short-circuit BEFORE delete_source is ever called"
        );

        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Org plane (`GraphIngestTarget::Enqueue`, an `Active` tenant): a
    /// deleted Teams channel-message notification must delete the atom on
    /// the ORG'S OWN TENANT POOL, not the control pool — proving [`process_
    /// deleted_notification`] resolves plane through the SAME [`resolve_
    /// graph_ingest_target`] the live ingest path uses (m365 Train 2 PR 2.3),
    /// exactly like [`resolve_graph_ingest_target_org_active_tenant_resolves_
    /// the_tenant_pool_pg`] proves for ingest. Also proves the Teams
    /// `"{container_id}:{message_id}"` source_id reconstruction (`connectors::
    /// microsoft::parse_subscribed_teams_resource`) matches what `teams_
    /// message_to_item` stamps on ingest. Called directly against
    /// `process_deleted_notification` (subscription lookup is control-plane-
    /// only machinery, already covered elsewhere — see this module's own
    /// "what this suite does NOT cover" header comment).
    #[tokio::test]
    async fn deleted_notification_org_plane_teams_message_deletes_on_the_tenant_pool_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping deleted_notification_org_plane_teams_message_deletes_on_the_\
                 tenant_pool_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = ("orgt_t44_del_org_ctl", "orgt_t44_del_org_tnt");
        let _database_cleanup_12 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let conn = recreate(&base, ctl_db).await;
        conn.close().await.ok();
        let _database_cleanup_13 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
        let tconn = recreate(&base, tnt_db).await;
        tconn.close().await.ok();

        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, ctl_db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let org_id = format!("org_t44_del_{}", uuid::Uuid::new_v4());
        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, Some(&org_id)).await;
        // Capture admission reads CONTROL membership even when content lives on TENANT.
        // Bind the existing org owner and the explicit active tier required by 0176.
        let membership = sqlx::query(
            "INSERT INTO account_members(account_id,user_id,role,seat_tier_id) \
             SELECT a.id,a.owner_user_id,'owner',t.id FROM accounts a CROSS JOIN seat_tiers t \
             WHERE a.id=$1 AND a.owner_user_id=$2 AND a.clerk_org_id=$3 AND a.is_org_account \
               AND t.code='standard' AND t.is_active",
        )
        .bind(account_id)
        .bind(owner)
        .bind(&org_id)
        .execute(&control)
        .await
        .expect("seed the existing org owner's CONTROL membership with an explicit tier");
        assert_eq!(membership.rows_affected(), 1, "exactly the org account owner must be admitted");
        mint_tenant_databases_row(
            &control,
            &org_id,
            "t44-del",
            account_id,
            "SECRET_T44_DEL",
            TenantStatus::Active,
        )
        .await;
        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;

        let tenant_graph_id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id) VALUES ($1, 'member', 'member', $2) RETURNING id",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&tenant)
        .await
        .unwrap();
        let resolver = StaticMapResolver::from_pairs([("SECRET_T44_DEL", with_db(&base, tnt_db))]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let state = state_with_control(
            control.clone(),
            Some(TenantRuntime::new(true, "coralswarm.test", registry)),
        );

        let (deleted_count, survivors) = ingest_deletion_family(
            &state, &tenant, account_id, owner, tenant_graph_id,
            "teams_channel", "C_1:msg_%_9", Some(&org_id),
        ).await;
        let n = deleted_notification("sub-unused", "secret-unused", "msg_%_9");
        let result = process_deleted_notification(
            state,
            account_id,
            "teams/T1/channels/C_1/messages".to_string(),
            n,
            Some(owner),
        )
        .await;
        assert!(
            result.is_ok(),
            "org-plane deleted-notification handling must not error: {result:?}"
        );

        let remaining: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_type = 'teams_channel' AND source_id = 'C_1:msg_%_9:chunk:0'",
        )
        .bind(tenant_graph_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(
            remaining, 0,
            "a deleted Teams notification for an org account must delete the atom on the org's \
             OWN tenant pool"
        );
        let control_leftover: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms")
            .fetch_one(&control)
            .await
            .unwrap();
        assert_eq!(
            control_leftover, 0,
            "nothing should ever have touched the control pool's atoms table"
        );

        let audit_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_audit_log \
             WHERE account_id = $1 AND action = 'deletion_propagated' AND actor_kind = 'webhook'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(
            audit_count, 1,
            "the audit row must land on the tenant pool too, not the control pool"
        );

        assert_deletion_family(&tenant, account_id, &deleted_count, &survivors).await;
        control.close().await;
        tenant.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        for db in [ctl_db, tnt_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    // ── Webhook contract (ADR-0016 layer 3 pins) ─────────────────────────────

    /// Subscription validation: the token comes back verbatim, as
    /// `text/plain`, with a 200.
    #[tokio::test]
    async fn graph_validation_handshake_echoes_the_token_as_plain_text() {
        let state = Arc::new(crate::test_app_state_owned(None));
        let resp = graph_notifications(
            axum::extract::State(state),
            axum::extract::Query(ValidationParams { validation_token: Some("Validation: Token-1 &x".into()) }),
            axum::body::Bytes::new(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "text/plain; charset=utf-8");
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], b"Validation: Token-1 &x");
    }

    /// A `deleted` notification for an org account whose tenant plane cannot
    /// be resolved right now (an active routing row, but no tenant runtime
    /// to build the pool) is a transient error, so the receive job retries
    /// rather than dropping a deletion Graph will not redeliver. A revoked
    /// org is refused for good: `Ok`, nothing to retry.
    #[tokio::test]
    async fn a_deletion_whose_tenant_plane_is_unavailable_retries_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_deletion_whose_tenant_plane_is_unavailable_retries_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_graph_delete_unavailable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
        let owner = seed_user(&control).await;
        let org_id = format!("org_delete_unavailable_{}", uuid::Uuid::new_v4());
        let account_id = seed_account(&control, owner, Some(&org_id)).await;
        mint_tenant_databases_row(&control, &org_id, "del-unavail", account_id, "SECRET_DEL_UNAVAIL", TenantStatus::Active)
            .await;
        let state = state_with_control(control.clone(), None);
        let n = deleted_notification("sub-del-unavail", "secret", "msg-gone");

        let result =
            process_deleted_notification(state.clone(), account_id, "me/mailFolders('inbox')/messages".into(), n.clone(), Some(owner))
                .await;
        let err = result.expect_err("an unavailable tenant plane must not complete the deletion");
        assert!(crate::workers::ingest_receive::transient(&err), "{err}");
        assert!(matches!(
            resolve_graph_ingest_target(&state, account_id).await,
            GraphIngestTarget::Unavailable(_)
        ));

        sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)")
            .bind(&org_id)
            .execute(&control)
            .await
            .unwrap();
        assert!(matches!(resolve_graph_ingest_target(&state, account_id).await, GraphIngestTarget::Drop));
        process_deleted_notification(state.clone(), account_id, "me/mailFolders('inbox')/messages".into(), n, Some(owner))
            .await
            .expect("a revoked org's deletion is dropped, not retried");

        drop(state);
        control.close().await;
    }

    /// Run the receive jobs the route wrote; how many ran.
    async fn drain_received(state: &Arc<AppState>) -> usize {
        crate::workers::ingest_receive::drain(state, &crate::routes::google_push::gmail_pass).await
    }

    async fn pending_receive_jobs(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'IngestReceive' AND status = 'pending'")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn post_graph_batch(state: &Arc<AppState>, batch: serde_json::Value) -> StatusCode {
        graph_notifications(
            axum::extract::State(state.clone()),
            axum::extract::Query(ValidationParams { validation_token: None }),
            axum::body::Bytes::from(serde_json::to_vec(&batch).unwrap()),
        )
        .await
        .status()
    }

    /// A personal mail subscription's notification is held only when the
    /// route can verify it: a forged `clientState`, an unknown subscription
    /// id and a missing id all answer 202 and hold nothing, so a forged
    /// notification pending under the genuine one's dedupe key cannot
    /// suppress it. The verified one is held as one receive job that does
    /// nothing until the worker runs it (a redelivery while it is pending
    /// adds no second job), and the worker then removes the captured message.
    #[tokio::test]
    async fn a_graph_notification_is_held_only_when_the_route_verifies_it_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_graph_notification_acts_only_on_a_verified_client_state_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_graph_webhook_contract";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, None).await;
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
            .bind(account_id)
            .bind(owner)
            .execute(&control)
            .await
            .unwrap();
        let graph_id: uuid::Uuid =
            sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, 'primary') RETURNING id")
                .bind(account_id)
                .fetch_one(&control)
                .await
                .unwrap();
        seed_graph_subscription(&control, account_id, "sub-pin-1", "me/mailFolders('inbox')/messages", "secret-pin-1", "")
            .await;
        let state = state_with_control(control.clone(), None);
        state.dek_cache.insert(&crate::tenancy::TenantId::Consumer, account_id, [7; 32]);
        let req: IngestAtomRequest = serde_json::from_value(serde_json::json!({
            "graph_id": graph_id, "source_type": "outlook", "source_id": "msg-pin-1",
            "content": "Latest reply on the pinned thread.", "participants": [], "timestamp": chrono::Utc::now(),
        }))
        .unwrap();
        let user = UserContext { user_id: owner, account_id, role: "owner".into(), device_id: String::new() };
        let namespace = format!("microsoft:connection:{account_id}:{owner}");
        let tenant = crate::tenancy::extractor::Tenant::legacy(&state);
        let stored = crate::ingest::submit(crate::ingest::IngestRequest::private(crate::ingest::Member {
            provider_namespace: Some(&namespace),
            ..crate::ingest::Member::new(&state, &tenant, &user, req)
        }))
        .await;
        assert!(stored.is_ok(), "seed capture failed: {:?}", stored.err());
        let atoms = || async {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_type = 'outlook'")
                .bind(graph_id)
                .fetch_one(&control)
                .await
                .unwrap()
        };
        assert!(atoms().await > 0);
        let deleted = |client_state: &str| {
            serde_json::json!({"value": [{
                "subscriptionId": "sub-pin-1", "clientState": client_state, "changeType": "deleted",
                "resource": "me/mailFolders('inbox')/messages/msg-pin-1", "resourceData": {"id": "msg-pin-1"},
            }]})
        };

        let all_receive_jobs = || async {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE kind = 'IngestReceive'")
                .fetch_one(&control)
                .await
                .unwrap()
        };

        // Forged first, so it would be the pending holder of the dedupe key
        // if the route held it; then the genuine one must still be held.
        assert_eq!(post_graph_batch(&state, deleted("forged")).await, StatusCode::ACCEPTED);
        assert_eq!(post_graph_batch(&state, deleted("")).await, StatusCode::ACCEPTED);
        let unknown = serde_json::json!({"value": [{
            "subscriptionId": "sub-nobody", "clientState": "secret-pin-1", "changeType": "deleted",
            "resource": "me/mailFolders('inbox')/messages/msg-pin-1", "resourceData": {"id": "msg-pin-1"},
        }]});
        assert_eq!(post_graph_batch(&state, unknown).await, StatusCode::ACCEPTED);
        let no_subscription = serde_json::json!({"value": [{"clientState": "secret-pin-1", "changeType": "deleted"}]});
        assert_eq!(post_graph_batch(&state, no_subscription).await, StatusCode::ACCEPTED);
        assert_eq!(all_receive_jobs().await, 0, "nothing the route cannot verify is held");
        assert_eq!(drain_received(&state).await, 0);
        assert!(atoms().await > 0, "a forged clientState must change nothing");

        assert_eq!(post_graph_batch(&state, deleted("secret-pin-1")).await, StatusCode::ACCEPTED);
        assert_eq!(pending_receive_jobs(&control).await, 1, "the genuine notification is held despite the forged one before it");
        assert_eq!(post_graph_batch(&state, deleted("secret-pin-1")).await, StatusCode::ACCEPTED);
        assert_eq!(post_graph_batch(&state, deleted("forged")).await, StatusCode::ACCEPTED);
        assert_eq!(pending_receive_jobs(&control).await, 1, "a redelivery while pending is not held twice");
        assert_eq!(all_receive_jobs().await, 1);
        assert!(atoms().await > 0, "the webhook itself acts on nothing");
        assert_eq!(drain_received(&state).await, 1);
        assert_eq!(atoms().await, 0, "the verified notification removes the message");
        let payload: serde_json::Value =
            sqlx::query_scalar("SELECT payload FROM jobs WHERE kind = 'IngestReceive' AND status = 'done' ORDER BY finished_at DESC LIMIT 1")
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(payload["notification"]["clientState"], "[redacted on completion]");

        drop(state);
        control.close().await;
    }


    /// The personal (`Sync`) arm's write failure reaches the receive worker
    /// classified, instead of being folded into a `bool` and lost with the
    /// job: a `submit` refusal a retry can change (here the account's DEK is
    /// not available yet, `IngestError::Unavailable`; also a D7 refusal,
    /// which is re-resolved on the next attempt) is retryable, so the job
    /// stays pending; one that is permanent (`IngestError::Invalid`, empty
    /// content) completes; a stored message is `Ok(true)`. `ReceiveError::
    /// retryable` is the exact predicate `ingest_receive::process_job`
    /// settles on. The Graph fetch in front of this arm cannot be faked
    /// (`microsoft::GRAPH` is fixed), so the chain is pinned at the write.
    #[tokio::test]
    async fn a_personal_graph_ingest_failure_is_classified_for_the_receive_worker_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_personal_graph_ingest_failure_is_classified_for_the_receive_worker_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_graph_ingest_failure_class";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, None).await;
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
            .bind(account_id)
            .bind(owner)
            .execute(&control)
            .await
            .unwrap();
        let graph_id: uuid::Uuid =
            sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, 'primary') RETURNING id")
                .bind(account_id)
                .fetch_one(&control)
                .await
                .unwrap();
        let stranger = seed_user(&control).await;
        let strangers_account = seed_account(&control, stranger, None).await;
        let strangers_graph: uuid::Uuid =
            sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, 'theirs') RETURNING id")
                .bind(strangers_account)
                .fetch_one(&control)
                .await
                .unwrap();
        let state = state_with_control(control.clone(), None);
        assert!(matches!(resolve_graph_ingest_target(&state, account_id).await, GraphIngestTarget::Sync));
        let item = |source_id: &str, content: &str| NormalizedItem {
            source_type: "outlook".into(),
            source_id: source_id.into(),
            content: content.into(),
            participants: vec![],
            timestamp: chrono::Utc::now(),
            scope: None,
            thread_id: None,
            channel_id: None,
            author_id: None,
            author_name: None,
        };
        let retryable = |e: ConnectorError| crate::workers::ingest_receive::ReceiveError::from(e).retryable();

        // No DEK for the account yet: `submit` answers `Unavailable`, which
        // a retry can change. The worker keeps the job pending.
        let err = ingest_message(&state, account_id, owner, graph_id, item("msg-1", "A reply."))
            .await
            .expect_err("a transient submit failure must surface");
        assert!(matches!(&err, ConnectorError::Internal(m) if m.contains("ingest failed")), "{err}");
        assert!(crate::workers::ingest_receive::transient(&err), "{err}");
        assert!(retryable(err));
        // An object-store outage is transient too: this arm retries it.
        assert!(!crate::ingest::IngestError::StorageUnavailable { cleanup_status: "complete" }.is_permanent());

        state.dek_cache.insert(&crate::tenancy::TenantId::Consumer, account_id, [7; 32]);
        // Empty content can never be stored as sent: permanent, the job completes.
        let err = ingest_message(&state, account_id, owner, graph_id, item("msg-2", ""))
            .await
            .expect_err("a permanent submit refusal must surface");
        assert!(matches!(&err, ConnectorError::NotConfigured(m) if m.contains("ingest refused")), "{err}");
        assert!(!crate::workers::ingest_receive::transient(&err), "{err}");
        assert!(!retryable(err));
        // A D7 refusal is re-resolved on the next attempt: retryable.
        let err = ingest_message(&state, account_id, owner, strangers_graph, item("msg-3", "Not mine."))
            .await
            .expect_err("writing to another account's ocean is refused");
        assert!(retryable(err));
        // The stored case.
        assert!(ingest_message(&state, account_id, owner, graph_id, item("msg-4", "A reply.")).await.unwrap());
        let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_type = 'outlook'")
            .bind(graph_id)
            .fetch_one(&control)
            .await
            .unwrap();
        assert!(atoms > 0);
        let theirs: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
            .bind(strangers_graph)
            .fetch_one(&control)
            .await
            .unwrap();
        assert_eq!(theirs, 0);
        // The bool adapter the sweeps use: a failure counts as not stored.
        assert!(!ingest_message_stored(&state, account_id, owner, graph_id, item("msg-5", "")).await);

        drop(state);
        control.close().await;
    }

    /// A notification that names no resource gets no dedupe key: keyed on
    /// the subscription and change alone, different notifications would
    /// collapse into one.
    #[test]
    fn a_notification_without_a_resource_is_not_keyed() {
        let keyed = |v: serde_json::Value| dedupe_key(&parse_notification(&v).unwrap());
        assert_eq!(
            keyed(serde_json::json!({"subscriptionId": "s", "changeType": "deleted", "resource": "me/messages/m1"})).as_deref(),
            Some("ingest_receive:graph:s:deleted:me/messages/m1")
        );
        assert_eq!(
            keyed(serde_json::json!({"subscriptionId": "s", "changeType": "created", "resourceData": {"id": "m2"}})).as_deref(),
            Some("ingest_receive:graph:s:created:m2")
        );
        assert_eq!(
            keyed(serde_json::json!({"subscriptionId": "s", "lifecycleEvent": "reauthorizationRequired", "resource": "me/messages"})).as_deref(),
            Some("ingest_receive:graph:s:reauthorizationRequired:me/messages")
        );
        assert_eq!(keyed(serde_json::json!({"subscriptionId": "s", "changeType": "created"})), None);
        assert_eq!(keyed(serde_json::json!({"subscriptionId": "s", "changeType": "created", "resource": "  "})), None);
        assert_eq!(keyed(serde_json::json!({"subscriptionId": "s", "changeType": "created", "resourceData": {}})), None);
    }

    /// One batch carrying notifications for a personal subscription, a
    /// Teams doorbell, an org mail route (verified by hash), a route from
    /// before the hash existed, an unknown subscription and a forged
    /// `clientState` is verified per entry from one bounded set of
    /// control-plane reads (`route_secrets`), with the right verdict for
    /// each; an entry with no resource is held without a key.
    #[tokio::test]
    async fn a_mixed_batch_is_verified_per_entry_from_one_set_of_lookups_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_mixed_batch_is_verified_per_entry_from_one_set_of_lookups_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_graph_webhook_mixed_batch";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner, None).await;
        seed_graph_subscription(&control, account_id, "sub-mixed-personal", "me/mailFolders('inbox')/messages", "secret-personal", "").await;
        sqlx::query(
            "INSERT INTO teams_container_subscriptions \
                (ms_tenant_id, resource, subscription_id, client_state, state, owner_account_id, owner_user_id, expires_at) \
             VALUES ('tid-mixed', 'teams/t/channels/c/messages', 'sub-mixed-door', 'secret-door', 'live', $1, $2, now() + interval '2 days')",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&control)
        .await
        .unwrap();
        sqlx::query("INSERT INTO graph_mail_routes (subscription_id, account_id, client_state_sha256) VALUES ('sub-mixed-mail', $1, $2), ('sub-mixed-old', $1, NULL)")
            .bind(account_id)
            .bind(microsoft::client_state_sha256("secret-mail"))
            .execute(&control)
            .await
            .unwrap();

        let ids: Vec<String> = ["sub-mixed-personal", "sub-mixed-door", "sub-mixed-mail", "sub-mixed-old", "sub-mixed-nobody"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let secrets = route_secrets(&control, &ids).await.unwrap();
        assert_eq!(secrets.len(), 4, "the unknown id is absent");
        assert_eq!(secrets["sub-mixed-personal"], RouteSecret::ClientState("secret-personal".into()));
        assert_eq!(secrets["sub-mixed-door"], RouteSecret::ClientState("secret-door".into()));
        assert_eq!(secrets["sub-mixed-mail"], RouteSecret::ClientStateHash(microsoft::client_state_sha256("secret-mail")));
        assert_eq!(secrets["sub-mixed-old"], RouteSecret::Unhashed);
        assert_eq!(route_verdict(secrets.get("sub-mixed-mail"), "secret-mail"), RouteVerdict::Verified);
        assert_eq!(route_verdict(secrets.get("sub-mixed-mail"), "secret-door"), RouteVerdict::Mismatch);
        assert_eq!(route_verdict(secrets.get("sub-mixed-old"), "anything"), RouteVerdict::Unverifiable);
        assert_eq!(route_verdict(secrets.get("sub-mixed-nobody"), "secret-personal"), RouteVerdict::Unknown);
        assert!(route_secrets(&control, &[]).await.unwrap().is_empty());

        let state = state_with_control(control.clone(), None);
        let entry = |sub: &str, client_state: &str, resource: Option<&str>| {
            let mut n = serde_json::json!({"subscriptionId": sub, "clientState": client_state, "changeType": "created"});
            if let Some(r) = resource {
                n["resource"] = serde_json::json!(format!("{r}"));
                n["resourceData"] = serde_json::json!({"id": r.rsplit('/').next().unwrap()});
            }
            n
        };
        let batch = serde_json::json!({"value": [
            entry("sub-mixed-personal", "secret-personal", Some("me/mailFolders('inbox')/messages/m-1")),
            entry("sub-mixed-personal", "forged", Some("me/mailFolders('inbox')/messages/m-2")),
            entry("sub-mixed-personal", "secret-personal", Some("me/mailFolders('inbox')/messages/m-1")),
            entry("sub-mixed-door", "secret-door", Some("teams/t/channels/c/messages/t-1")),
            entry("sub-mixed-mail", "secret-mail", Some("users/u/mailFolders('inbox')/messages/o-1")),
            entry("sub-mixed-mail", "secret-door", Some("users/u/mailFolders('inbox')/messages/o-2")),
            entry("sub-mixed-old", "whatever", Some("users/u/mailFolders('inbox')/messages/o-3")),
            entry("sub-mixed-nobody", "secret-personal", Some("me/messages/x-1")),
            entry("sub-mixed-personal", "secret-personal", None),
        ]});
        assert_eq!(post_graph_batch(&state, batch.clone()).await, StatusCode::ACCEPTED);
        let held = || async {
            sqlx::query_as::<_, (i64, i64)>(
                "SELECT count(*), count(*) FILTER (WHERE idempotency_key IS NULL) FROM jobs WHERE kind = 'IngestReceive' AND status = 'pending'",
            )
            .fetch_one(&control)
            .await
            .unwrap()
        };
        assert_eq!(held().await, (5, 2), "personal, door, mail, the unhashed route and the resource-less entry are held; the last two unkeyed");
        let keys: Vec<String> =
            sqlx::query_scalar("SELECT idempotency_key FROM jobs WHERE kind = 'IngestReceive' AND idempotency_key IS NOT NULL ORDER BY 1")
                .fetch_all(&control)
                .await
                .unwrap();
        assert_eq!(
            keys,
            vec![
                "ingest_receive:graph:sub-mixed-door:created:teams/t/channels/c/messages/t-1",
                "ingest_receive:graph:sub-mixed-mail:created:users/u/mailFolders('inbox')/messages/o-1",
                "ingest_receive:graph:sub-mixed-personal:created:me/mailFolders('inbox')/messages/m-1",
            ]
        );
        // Redelivered: the keyed entries coalesce, the unkeyed ones are held again.
        assert_eq!(post_graph_batch(&state, batch).await, StatusCode::ACCEPTED);
        assert_eq!(held().await, (7, 4));

        drop(state);
        control.close().await;
    }
}

#[cfg(test)]
#[path = "teams_source_admission_pg_tests.rs"]
mod teams_source_admission_pg_tests;

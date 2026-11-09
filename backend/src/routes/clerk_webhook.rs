//! `POST /v1/webhooks/clerk` — Clerk Organizations webhook (ADR-0007 D3).
//!
//! Clerk is the source of truth for org signup, invites, roles, and removals.
//! This endpoint receives `organization*` events (delivered via Svix) and
//! eagerly provisions/syncs the mirroring org account + memberships — covering
//! members who were invited but have not yet signed in, plus role changes and
//! removals that the lazy on-JWT provisioning (`require_auth`) cannot see.
//!
//! Public route: authenticity is the Svix HMAC signature, not a Bearer token.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use serde_json::{json, Value};

use crate::clerk_provision as prov;
use crate::AppState;
use uuid::Uuid;

/// Verify a Svix webhook signature (the scheme Clerk uses).
///
/// `secret` is the endpoint signing secret (`whsec_<base64>`). The signed
/// content is `{svix-id}.{svix-timestamp}.{body}`; the `svix-signature` header
/// is a space-separated list of `v1,<base64sig>` entries — we accept if any
/// matches (constant-time).
fn verify_svix(secret: &str, headers: &HeaderMap, body: &[u8]) -> bool {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use hmac::{Hmac, Mac};
    use subtle::ConstantTimeEq;

    let h = |k: &str| headers.get(k).and_then(|v| v.to_str().ok());
    let (Some(id), Some(ts), Some(sig_header)) =
        (h("svix-id"), h("svix-timestamp"), h("svix-signature"))
    else {
        return false;
    };

    // Replay protection: reject timestamps outside a tolerance window (Svix
    // sends unix seconds and expects receivers to enforce freshness).
    const TOLERANCE_SECS: i64 = 300;
    match ts.parse::<i64>() {
        Ok(t) if (chrono::Utc::now().timestamp() - t).abs() <= TOLERANCE_SECS => {}
        _ => return false,
    }

    let key_b64 = secret.strip_prefix("whsec_").unwrap_or(secret);
    let Ok(key) = STANDARD.decode(key_b64) else {
        return false;
    };
    let Ok(body_str) = std::str::from_utf8(body) else {
        return false;
    };
    let signed = format!("{id}.{ts}.{body_str}");

    let Ok(mut mac) = Hmac::<sha2::Sha256>::new_from_slice(&key) else {
        return false;
    };
    mac.update(signed.as_bytes());
    let expected = STANDARD.encode(mac.finalize().into_bytes());

    sig_header.split(' ').any(|entry| {
        entry
            .split_once(',')
            .map(|(_ver, sig)| sig.as_bytes().ct_eq(expected.as_bytes()).into())
            .unwrap_or(false)
    })
}

pub async fn clerk_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let secret = match std::env::var("CLERK_WEBHOOK_SECRET") {
        Ok(s) if !s.is_empty() => s,
        _ => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "clerk webhook not configured"})),
            );
        }
    };
    if !verify_svix(&secret, &headers, &body) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid signature"})),
        );
    }

    let pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            );
        }
    };

    let event: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid payload"})),
            );
        }
    };
    let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let data = event.get("data").cloned().unwrap_or(Value::Null);

    let result = match event_type {
        "organization.created" => handle_org_created(&state, &pool, &data).await,
        "organization.updated" => handle_org_updated(&state, &pool, &data).await,
        "organizationMembership.created" => handle_membership_upsert(&state, &pool, &data, true).await,
        "organizationMembership.updated" => handle_membership_upsert(&state, &pool, &data, false).await,
        "organizationMembership.deleted" => handle_membership_deleted(&state, &pool, &data).await,
        "organization.deleted" => handle_org_deleted(&state, &pool, &data).await,
        // Settings launch 2.4: Clerk no longer has this user. Joins the
        // deletion we may already be running, or starts one for an external
        // delete (Clerk dashboard / API).
        "user.deleted" => handle_user_deleted(&pool, &data).await,
        // Auth-mail self-delivery (Cloudflare Email Service) — see
        // `handle_email_created`'s own doc comment for the full design
        // (kill switch, double-send guard, and why this can never turn into
        // a 500). Note this arm does NOT thread `pool` OR `state`: sending
        // an email has no control-plane row to write, unlike every arm
        // above it, and the outbound call uses its own dedicated HTTP
        // client (`cloudflare_email_http_client`) rather than
        // `state.http_client` — see that function's doc comment for why.
        "email.created" => {
            handle_email_created(&data).await;
            Ok(())
        }
        // Ack-and-ignore everything else (user.*, session.*, …).
        other => {
            tracing::debug!(event = %other, "clerk webhook: ignoring event");
            Ok(())
        }
    };

    match result {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))),
        // Org-creation gate (org-tenancy PR 3.3): a PERMANENT policy rejection,
        // never transient — acked as a no-op (200, not 500) so Svix never
        // retries an event that will reject identically every time. This is
        // the "no-op" half of `ensure_org_account`'s documented behavior for
        // an un-onboarded org id under `ORG_SELFSERVE_DISABLED`.
        Err(prov::ProvisionError::SelfServeDisabled) => {
            tracing::info!(
                event = %event_type,
                "clerk webhook: org account not provisioned (self-serve org creation disabled)"
            );
            (
                StatusCode::OK,
                Json(json!({"ok": true, "skipped": "self_serve_disabled"})),
            )
        }
        // Legacy-token fence (org-tenancy PR 4.2): a permanent rejection like
        // `SelfServeDisabled` just above — `org_revocations` is never cleared
        // (Clerk never reuses an org id), so retrying this event would reject
        // identically forever. In practice this arm is close to unreachable
        // via genuine Clerk delivery (an org's `organization.created` always
        // precedes any membership event, and revocation only ever follows
        // `organization.deleted`), but out-of-order/replayed delivery is
        // possible, so ack rather than 500-retry-forever.
        // A deleted user's late webhook (settings launch 2.4): permanent no-op.
        Err(prov::ProvisionError::AccountDeleted) => {
            tracing::info!(
                event = %event_type,
                "clerk webhook: ignored (the account has been deleted)"
            );
            (
                StatusCode::OK,
                Json(json!({"ok": true, "skipped": "account_deleted"})),
            )
        }
        Err(prov::ProvisionError::OrgRevoked) => {
            tracing::info!(
                event = %event_type,
                "clerk webhook: org account not provisioned (organization already revoked)"
            );
            (
                StatusCode::OK,
                Json(json!({"ok": true, "skipped": "org_revoked"})),
            )
        }
        Err(e) => {
            // 5xx so Clerk/Svix retries transient failures.
            tracing::error!(event = %event_type, error = %e, "clerk webhook handler failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
        }
    }
}

pub(crate) async fn handle_org_created(
    state: &AppState,
    pool: &sqlx::PgPool,
    data: &Value,
) -> Result<(), prov::ProvisionError> {
    let Some(org_id) = data.get("id").and_then(|v| v.as_str()) else {
        return Ok(());
    };
    let name = data.get("name").and_then(|v| v.as_str());
    // Self-serve signup A2 (security review N1): an org `POST /v1/orgs`
    // created carries the self-serve marker in its `private_metadata`, and
    // that route writes its `accounts` row (no `free` grant, no shared-plane
    // ocean) in the same transaction that records it. This event can land
    // before the route has recorded it; it must never mint the row itself,
    // whatever `ORG_SELFSERVE_DISABLED` says.
    // #1540 N2: Clerk's `private_metadata` is optional on a webhook's org
    // object; when it is absent the org is read from Clerk (fail closed).
    let clerk = crate::clerk_admin::ClerkAdminClient::from_env();
    if crate::routes::orgs_create::org_is_selfserve(org_id, data, clerk.as_ref()).await.map_err(prov::ProvisionError::Store)? {
        tracing::info!(org = org_id, "clerk webhook: organization.created for a self-serve org; the create route owns its account row");
        return Ok(());
    }
    // Seed the account owner with the org creator when present.
    let owner = match data.get("created_by").and_then(|v| v.as_str()) {
        Some(creator) => prov::ensure_user(state, pool, creator, None, None).await?,
        None => {
            // No creator in payload — create the account anyway with a deferred
            // owner via the first membership event; skip until then.
            return Ok(());
        }
    };
    prov::ensure_org_account(state, pool, org_id, owner, name).await?;
    Ok(())
}

/// `organization.updated` — reconcile a Clerk **slug rename** into
/// `tenant_databases.slug` (org-tenancy: the post-onboarding half of the slug
/// parity guarantee).
///
/// Why reconcile rather than reject: `tenant_databases.slug` must equal the
/// Clerk org slug, because the web app derives the tenant host from
/// `useOrganization().organization.slug` and has no fallback (`/v1/me` sits
/// behind the binding gate itself), while the gate resolves the routing row
/// from that Host. `orgt_verify_clerk_slug` asserts parity at ONBOARDING time;
/// a rename afterwards re-breaks the identical path on a LIVE tenant — every
/// org user 404s. A webhook is a notification, not a veto: the rename has
/// already happened in Clerk, so "reject" could only mean "leave the DB
/// authoritative and page a human", which keeps the tenant broken until
/// someone notices. Reconciling self-heals; the loud logs below are what a
/// "reject" design would have given us anyway.
///
/// Everything this cannot safely apply — a slug that is reserved or malformed,
/// one already owned by another tenant, an out-of-order/replayed event — leaves
/// routing exactly as it was and logs at ERROR/WARN. Those are PERMANENT
/// outcomes (a retry re-decides identically), so they are acked with 200 like
/// the other permanent rejections this endpoint already models, rather than
/// 500ing Svix into a retry loop that cannot converge.
async fn handle_org_updated(
    state: &AppState,
    pool: &sqlx::PgPool,
    data: &Value,
) -> Result<(), prov::ProvisionError> {
    use crate::tenancy::routing::{self, SlugReconcile};

    let Some(org_id) = data.get("id").and_then(|v| v.as_str()) else {
        return Ok(());
    };
    // `organization.updated` also fires for name/metadata/logo edits; only the
    // slug routes anything. A payload with no slug carries no routing decision.
    let Some(new_slug) = data
        .get("slug")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        tracing::warn!(
            org = %org_id,
            "clerk webhook: organization.updated carries no slug — tenant routing left unchanged"
        );
        return Ok(());
    };
    // Clerk's own clock for this event (unix ms). It is the ONLY ordering
    // authority available — Clerk does not guarantee delivery order — so
    // without it we must not write, or a delayed event could clobber a newer
    // slug. Fail closed and loudly rather than reconcile blind.
    let Some(clerk_updated_at) = data
        .get("updated_at")
        .and_then(|v| v.as_i64())
        .and_then(chrono::DateTime::from_timestamp_millis)
    else {
        tracing::error!(
            org = %org_id,
            slug = %new_slug,
            "clerk webhook: organization.updated has no usable `updated_at` — refusing to \
             reconcile tenant routing without an ordering key (payload contract changed?)"
        );
        return Ok(());
    };

    match routing::reconcile_slug(pool, org_id, new_slug, clerk_updated_at).await? {
        // The overwhelmingly common case: a consumer org with no tenant DB.
        SlugReconcile::NotOnboarded => {
            tracing::debug!(
                org = %org_id,
                "clerk webhook: organization.updated for an org with no tenant database — no-op"
            );
        }
        SlugReconcile::Unchanged { slug } => {
            tracing::debug!(
                org = %org_id, slug = %slug,
                "clerk webhook: organization.updated left the tenant slug unchanged"
            );
        }
        SlugReconcile::Renamed { old, new } => {
            // Deliberately WARN, not INFO: a tenant's routing identity changing
            // under a live deployment is an operationally significant event
            // (DNS/TLS for `<slug>.<apex>` and any bookmarked host follow the
            // NEW name), even though the reconciliation itself succeeded.
            tracing::warn!(
                org = %org_id, old_slug = %old, new_slug = %new,
                "clerk webhook: tenant slug RENAMED in Clerk — reconciled tenant_databases.slug \
                 and invalidated routing cache; ensure DNS/TLS covers the new subdomain"
            );
            // Both slugs AND the org-id key — see `invalidate_routing_rename`.
            if let Some(rt) = &state.tenant_runtime {
                rt.invalidate_routing_rename(org_id, &old, &new);
            }
        }
        SlugReconcile::Stale {
            current,
            applied_at,
        } => {
            tracing::warn!(
                org = %org_id, ignored_slug = %new_slug, current_slug = %current,
                event_updated_at = %clerk_updated_at, applied_updated_at = %applied_at,
                "clerk webhook: ignoring an out-of-order/replayed organization.updated — it is \
                 not newer than the last slug event applied to this tenant"
            );
        }
        SlugReconcile::SelfServe { current } => {
            tracing::debug!(
                org = %org_id, slug = %current, ignored_slug = %new_slug,
                "clerk webhook: organization.updated for a self-serve org — its routing key is \
                 derived, never the Clerk slug; left unchanged"
            );
        }
        SlugReconcile::ReservedSelfServeShape { slug } => {
            tracing::error!(
                org = %org_id, rejected_slug = %slug,
                "clerk webhook: refusing to route a tenant to a slug shaped like a self-serve \
                 routing key (o-<16 hex>); tenant routing left on its previous slug. Operator \
                 action required: give this org a different slug in Clerk"
            );
        }
        SlugReconcile::Invalid { slug } => {
            tracing::error!(
                org = %org_id, rejected_slug = %slug,
                "clerk webhook: refusing to route a tenant to an invalid or reserved slug — \
                 tenant routing left on its previous slug, which the web app will NO LONGER \
                 redirect to. Operator action required: give this org a valid slug in Clerk"
            );
        }
        SlugReconcile::Conflict { current, wanted } => {
            tracing::error!(
                org = %org_id, current_slug = %current, rejected_slug = %wanted,
                "clerk webhook: refusing a tenant slug rename that collides with another \
                 tenant's slug — tenant routing left on its previous slug, which the web app \
                 will NO LONGER redirect to. Operator action required: this org's users cannot \
                 reach it until the collision is resolved in Clerk"
            );
        }
    }
    Ok(())
}

pub(crate) async fn handle_membership_upsert(
    state: &AppState,
    pool: &sqlx::PgPool,
    data: &Value,
    // Billing self-serve PR F review, MAJOR finding: whether this call was
    // dispatched from `organizationMembership.created` (`true`) or
    // `.updated` (`false`) — threaded down to `ensure_org_membership`'s own
    // retry gate; see that function's own doc comment ("Retry gate") for why
    // `newly_inserted` alone left a failed seat acquisition permanently
    // unretriable by Clerk's own redelivery.
    is_created_event: bool,
) -> Result<(), prov::ProvisionError> {
    let org = data.get("organization");
    let Some(org_id) = org.and_then(|o| o.get("id")).and_then(|v| v.as_str()) else {
        return Ok(());
    };
    let org_name = org.and_then(|o| o.get("name")).and_then(|v| v.as_str());
    // N1: a self-serve org whose `accounts` row the create route has not
    // written yet. The route mirrors the creator's membership itself once
    // the row exists; this event must not create the row first.
    //
    // #1540 N2: the existence check runs first, and the marker is read from
    // Clerk when the payload's `organization` carries no `private_metadata`
    // (Clerk's membership events need not include it), so a missing field
    // can never read as "not self-serve". A Clerk failure is a 5xx (Svix
    // retries).
    let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM accounts WHERE clerk_org_id = $1)")
        .bind(org_id)
        .fetch_one(pool)
        .await?;
    if !exists {
        let clerk = crate::clerk_admin::ClerkAdminClient::from_env();
        let org = org.unwrap_or(&Value::Null);
        if crate::routes::orgs_create::org_is_selfserve(org_id, org, clerk.as_ref()).await.map_err(prov::ProvisionError::Store)? {
            tracing::info!(org = org_id, "clerk webhook: membership event for a self-serve org not yet recorded; skipped");
            return Ok(());
        }
    }
    let pud = data.get("public_user_data");
    let Some(clerk_user_id) = pud.and_then(|p| p.get("user_id")).and_then(|v| v.as_str()) else {
        return Ok(());
    };
    let email = pud.and_then(|p| p.get("identifier")).and_then(|v| v.as_str());
    // `public_user_data` also carries `first_name`/`last_name` on this event —
    // a free name hint (no extra Backend-API round-trip) for brand-new users,
    // same spirit as the `email` extraction just above.
    let first_name = pud.and_then(|p| p.get("first_name")).and_then(|v| v.as_str());
    let last_name = pud.and_then(|p| p.get("last_name")).and_then(|v| v.as_str());
    let name = match (first_name, last_name) {
        (Some(f), Some(l)) if !f.is_empty() && !l.is_empty() => Some(format!("{f} {l}")),
        (Some(f), _) if !f.is_empty() => Some(f.to_string()),
        (_, Some(l)) if !l.is_empty() => Some(l.to_string()),
        _ => None,
    };
    let payload_role = prov::map_org_role(data.get("role").and_then(|v| v.as_str()));

    // Out-of-order / redelivered membership events must not undo an admin
    // action. Svix retries and `.created`/`.updated` ordering are unordered,
    // and a payload can describe a membership (or a role) that no longer
    // exists. Ask Clerk for the CURRENT state:
    //   * no membership -> this delivery is stale (the member was removed;
    //     `.deleted` is, or already was, handled) -> skip WITHOUT clearing the
    //     removal tombstone or re-inserting `account_members`;
    //   * membership -> take the role from Clerk, not the payload.
    // A Clerk lookup failure is a 5xx so Svix retries: resurrecting a removed
    // member on a guess is the worse outcome.
    //
    // Removal fence (settings PR 2.9): capture the control DB's clock BEFORE
    // asking Clerk. An admin removal can land between Clerk's "member" answer
    // and our write; `ensure_org_membership` refuses (retryably) if a
    // tombstone was written at or after this instant. DB time, not app time,
    // so it is on the same clock as `org_membership_tombstones.removed_at`.
    let clerk_observed_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT now()").fetch_one(pool).await?;
    let role = match crate::clerk_admin::ClerkAdminClient::from_env() {
        // No Clerk key: only `APP_ENV=development` (exactly; fail-closed, see
        // `AppEnv::is_dev`) may trust the payload role. Anywhere else this is
        // a misconfiguration that would let a stale payload re-add a removed
        // member or restore an old role, so refuse with a 5xx and keep the
        // delivery in Svix's retry queue until the key is fixed.
        None if state.env.is_dev => payload_role,
        None => {
            tracing::error!(
                alert = true,
                org_id, clerk_user_id,
                "clerk_webhook: CLERK_SECRET_KEY is not configured outside development — \
                 refusing to mirror organizationMembership upsert from the payload alone \
                 (Svix will retry). Set CLERK_SECRET_KEY."
            );
            return Err(prov::ProvisionError::Store(
                "clerk secret key not configured; refusing to trust the webhook payload role"
                    .to_string(),
            ));
        }
        Some(clerk) => match tokio::time::timeout(
            MEMBER_OCEAN_WEBHOOK_TIMEOUT,
            clerk.get_user_org_membership(org_id, clerk_user_id),
        )
        .await
        {
            Ok(Ok(Some(m))) => prov::map_org_role(m.role.as_deref()),
            Ok(Ok(None)) => {
                tracing::info!(
                    org_id, clerk_user_id,
                    "clerk_webhook: organizationMembership upsert looks stale — Clerk reports no \
                     current membership (member removed) — skipping"
                );
                return Ok(());
            }
            Ok(Err(e)) => {
                return Err(prov::ProvisionError::Store(format!(
                    "clerk membership lookup failed: {e}"
                )));
            }
            Err(_) => {
                return Err(prov::ProvisionError::Store(
                    "clerk membership lookup timed out".to_string(),
                ));
            }
        },
    };

    let user_id = prov::ensure_user(state, pool, clerk_user_id, email, name.as_deref()).await?;
    // owner seed = this member (no-op if the account already exists).
    prov::ensure_org_account(state, pool, org_id, user_id, org_name).await?;
    // CS-442 (item 40, D8): a plain Clerk-driven membership event is always
    // `standard` — `None` resolves it internally, exactly the pre-CS-442
    // behaviour. The admin-chosen tier only ever reaches
    // `ensure_org_membership` through `routes::org_invites::
    // mirror_local_membership` (a join-request approval), never through
    // this webhook.
    prov::ensure_org_membership(
        state,
        pool,
        org_id,
        user_id,
        role,
        is_created_event,
        None,
        Some(clerk_observed_at),
    )
    .await?;

    // Member-ocean provisioning, trigger 1 of 3 (member-private-oceans PR 4,
    // `docs/plans/member-private-oceans.md` §2.4). Best-effort and NEVER
    // fatal to this webhook — see `provision_member_ocean_best_effort`'s doc
    // comment for the full reasoning: `organizationMembership.created` can
    // fire long before the org's tenant database is ready (or ever exists),
    // and Clerk/Svix retries are not the recovery mechanism for that — the
    // request-path first-sight trigger and the reconciler sweep are.
    provision_member_ocean_best_effort(state, pool, org_id, user_id, name.as_deref()).await;

    Ok(())
}

/// Bound on the tenant-pool build and on the provisioning `INSERT` this
/// webhook trigger attempts — CodeRabbit review on PR 4 (member-private-
/// oceans): "best-effort, never fatal" only holds if these calls cannot hang
/// forever. Without a timeout, a stalled connect or a lock-contended `INSERT`
/// on the tenant DB would hold `handle_membership_upsert` — and the inbound
/// Svix delivery attempt — open indefinitely, which is still a real
/// availability problem even though it can never turn into a 500 (Svix's own
/// delivery timeout would eventually abandon the connection and retry,
/// achieving nothing this bound doesn't already achieve faster and more
/// cheaply). 5 seconds each, applied independently to the pool build and the
/// insert (so a worst case here is bounded at ~10s, not unbounded) — matching
/// `TenantRegistry::DEFAULT_ACQUIRE_TIMEOUT`'s own rationale ("5s comfortably
/// covers a Neon cold resume handshake while keeping a hung tenant from
/// head-of-line-blocking its caller"), reused here because this is the exact
/// same class of risk on a different caller. Longer than the hot-path
/// request-path trigger's `MEMBER_OCEAN_PROVISION_TIMEOUT` (2s,
/// `tenancy::runtime`) deliberately: nobody is staring at a spinner waiting
/// on a webhook response the way a live user is waiting on their own API
/// call, so this budget can afford to be more patient before giving up. On
/// timeout, each call degrades exactly like any other provisioning failure:
/// logged at INFO and swallowed, never propagated — see this function's own
/// doc comment below.
const MEMBER_OCEAN_WEBHOOK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Trigger 1 (Clerk webhook) of member-private-oceans' three-trigger
/// provisioning design (plan §2.4). Called from [`handle_membership_upsert`]
/// after the member's control-plane membership is upserted.
///
/// **Never returns an error and never affects this webhook's HTTP response.**
/// Every failure mode below — the flag being off, no `TenantRuntime`, no
/// routing row yet (`Missing`), a mid-onboarding row (`Provisioning`), a
/// suspended/offboarded/unrecognized-status row, the tenant pool itself
/// failing to build, or either step timing out
/// ([`MEMBER_OCEAN_WEBHOOK_TIMEOUT`]) — is logged at INFO and swallowed. This
/// mirrors why the org-creation gate and legacy-token fence already ack
/// certain `ProvisionError`s as 200 rather than retry-forever 500s (see
/// `clerk_webhook`'s match arms above): a webhook handler's job is to react
/// to Clerk's event, not to gate its own success on a resource (the org's
/// tenant database) whose readiness Clerk has no visibility into and whose
/// eventual convergence is `workers::member_ocean_reconciler`'s job, not
/// Svix's retry policy.
async fn provision_member_ocean_best_effort(
    state: &AppState,
    control_pool: &sqlx::PgPool,
    org_id: &str,
    user_id: Uuid,
    display_name: Option<&str>,
) {
    if !crate::tenancy::member_ocean::enabled() {
        return;
    }
    // member-private-oceans PR 7, §D "Org-revocation interaction": a revoked
    // org must never gain a provisioned OR restored member ocean via this
    // trigger. Unlike `require_auth`'s first-sight path (which is fenced
    // upstream by `live_org_membership`'s `account_members` re-check — a
    // revoked org has none, so `finish_binding` is never even reached), this
    // webhook handler is NOT gated by that fence: it runs on Clerk's own
    // signed event delivery, not a user's own request, so a replayed or
    // out-of-order `organizationMembership.created|.updated` for an
    // already-revoked org could otherwise still reach
    // `tenancy::member_ocean::provision_one` (which, as of PR 7, can RESTORE
    // archived content, not just create an empty ocean — strictly worse for
    // a revocation that is supposed to be a hard fence with no carve-out).
    // Checked fresh on every call (org_revocations rows are never cleared —
    // see that table's own doc comment — so there is no staleness risk).
    // Bounded by MEMBER_OCEAN_WEBHOOK_TIMEOUT like every other outbound call
    // in this function (CodeRabbit review): this handler must return to Svix
    // promptly, and an unbounded await here (a saturated control pool, a lock
    // on `org_revocations`) would stall it indefinitely, unlike the two
    // timeout-bounded calls below it.
    match tokio::time::timeout(
        MEMBER_OCEAN_WEBHOOK_TIMEOUT,
        crate::tenancy::routing::org_is_revoked(control_pool, org_id),
    )
    .await
    {
        Ok(Ok(true)) => {
            tracing::info!(
                org = %org_id,
                "member-ocean webhook trigger: org is revoked — refusing to provision or restore"
            );
            return;
        }
        Ok(Ok(false)) => {}
        Ok(Err(e)) => {
            tracing::info!(
                org = %org_id,
                error = %e,
                "member-ocean webhook trigger: org_revocations lookup failed — deferring to \
                 first-sight/reconciler"
            );
            return;
        }
        Err(_) => {
            tracing::info!(
                org = %org_id,
                timeout_secs = MEMBER_OCEAN_WEBHOOK_TIMEOUT.as_secs(),
                "member-ocean webhook trigger: org_revocations lookup timed out — deferring to \
                 first-sight/reconciler"
            );
            return;
        }
    }
    let Some(rt) = state.tenant_runtime.as_ref() else {
        return;
    };
    let row = match crate::tenancy::routing::lookup_by_org(control_pool, org_id).await {
        Ok(row) => row,
        Err(e) => {
            tracing::info!(
                org = %org_id,
                error = %e,
                "member-ocean webhook trigger: routing lookup failed — deferring to \
                 first-sight/reconciler"
            );
            return;
        }
    };
    let Some(row) = row else {
        // Routing `Missing`: the tenant database doesn't exist yet (or this
        // deployment never onboarded this org). The exact trap plan §1.4b
        // names for org commons oceans — `organization.created`/membership
        // events fire well before `onboard_tenant.sh` ever runs.
        tracing::info!(
            org = %org_id,
            "member-ocean webhook trigger: no tenant routing row yet (Missing) — deferring \
             to first-sight/reconciler"
        );
        return;
    };
    if !row.is_servable() {
        // `Provisioning` (mid-onboarding), `Suspended`, `Offboarded`, or an
        // unrecognized status — none are a safe pool to provision into.
        tracing::info!(
            org = %org_id,
            status = %row.status,
            "member-ocean webhook trigger: tenant not active yet — deferring to \
             first-sight/reconciler"
        );
        return;
    }
    let tenant_pool = match tokio::time::timeout(
        MEMBER_OCEAN_WEBHOOK_TIMEOUT,
        rt.registry.get_or_build(org_id, row.account_id, &row.db_secret_ref),
    )
    .await
    {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            tracing::info!(
                org = %org_id,
                error = %e,
                "member-ocean webhook trigger: tenant pool build failed — deferring to \
                 first-sight/reconciler"
            );
            return;
        }
        Err(_) => {
            tracing::info!(
                org = %org_id,
                timeout_secs = MEMBER_OCEAN_WEBHOOK_TIMEOUT.as_secs(),
                "member-ocean webhook trigger: tenant pool build timed out — deferring to \
                 first-sight/reconciler"
            );
            return;
        }
    };
    match tokio::time::timeout(
        MEMBER_OCEAN_WEBHOOK_TIMEOUT,
        crate::tenancy::member_ocean::provision_one(
            &tenant_pool,
            control_pool,
            row.account_id,
            user_id,
            display_name,
        ),
    )
    .await
    {
        Ok(Ok(Some(graph_id))) => {
            tracing::info!(
                org = %org_id,
                user_id = %user_id,
                graph_id = %graph_id,
                "member-ocean webhook trigger: provisioned member ocean"
            );
        }
        // Already present — the normal steady state for every membership
        // event after the member's first one, and for a member whose ocean
        // another trigger already won the race for.
        Ok(Ok(None)) => {}
        Ok(Err(e)) => {
            tracing::info!(
                org = %org_id,
                user_id = %user_id,
                error = %e,
                "member-ocean webhook trigger: provision insert failed — deferring to \
                 first-sight/reconciler"
            );
        }
        Err(_) => {
            tracing::info!(
                org = %org_id,
                user_id = %user_id,
                timeout_secs = MEMBER_OCEAN_WEBHOOK_TIMEOUT.as_secs(),
                "member-ocean webhook trigger: provision insert timed out — deferring to \
                 first-sight/reconciler"
            );
        }
    }
}

/// `pub(crate)` so `routes::org_invites::pg_tests` (which already stands up
/// a Clerk Backend API mock server for its own suite) can exercise the
/// stale-delivery guard below directly — see that module's doc comment,
/// "the revoke-then-quick-re-invite race", and `routes::mod`'s note that
/// `routes` itself is crate-private (`mod routes;` in `lib.rs`), which is
/// why this can be `pub(crate)` rather than fully `pub` without leaking a
/// webhook internal outside the crate.
pub(crate) async fn handle_membership_deleted(
    state: &AppState,
    pool: &sqlx::PgPool,
    data: &Value,
) -> Result<(), prov::ProvisionError> {
    let Some(org_id) = data
        .get("organization")
        .and_then(|o| o.get("id"))
        .and_then(|v| v.as_str())
    else {
        return Ok(());
    };
    let Some(clerk_user_id) = data
        .get("public_user_data")
        .and_then(|p| p.get("user_id"))
        .and_then(|v| v.as_str())
    else {
        return Ok(());
    };

    // Stale-delivery guard (org-invites train PR 1 review, finding 2): see
    // `routes::org_invites`'s module doc, "the revoke-then-quick-re-invite
    // race". Best-effort — if we can build a Clerk admin client, ask Clerk
    // whether this user STILL has this org membership right now; if so,
    // this `.deleted` delivery is stale (a revoke followed by a quick
    // re-invite) and must not delete the FRESH membership/release the FRESH
    // seat. If the check itself fails (no key configured, transport error,
    // ...), fail toward TODAY's behavior and process the delete anyway.
    if let Some(clerk) = crate::clerk_admin::ClerkAdminClient::from_env() {
        // Bounded by MEMBER_OCEAN_WEBHOOK_TIMEOUT, same convention as every
        // other outbound call this handler makes — a slow/hung Clerk call
        // must not stall webhook processing past this handler's own budget.
        // A timeout here fails toward TODAY's behavior (the delete
        // proceeds), same posture as an outright Clerk error below.
        match tokio::time::timeout(
            MEMBER_OCEAN_WEBHOOK_TIMEOUT,
            clerk.user_has_org_membership(org_id, clerk_user_id),
        )
        .await
        {
            Ok(Ok(true)) => {
                tracing::info!(
                    org_id, clerk_user_id,
                    "clerk_webhook: organizationMembership.deleted looks stale — Clerk still \
                     reports this membership as current (likely a revoke immediately followed by \
                     a re-invite) — skipping"
                );
                return Ok(());
            }
            Ok(Ok(false)) => {}
            Ok(Err(e)) => {
                tracing::warn!(
                    error = %e, org_id, clerk_user_id,
                    "clerk_webhook: stale-delivery check failed for organizationMembership.deleted \
                     — proceeding with the delete as before this guard existed"
                );
            }
            Err(_) => {
                tracing::warn!(
                    org_id, clerk_user_id,
                    timeout_secs = MEMBER_OCEAN_WEBHOOK_TIMEOUT.as_secs(),
                    "clerk_webhook: stale-delivery check timed out for organizationMembership.deleted \
                     — proceeding with the delete as before this guard existed"
                );
            }
        }
    }

    prov::remove_org_membership(state, pool, org_id, clerk_user_id).await?;
    Ok(())
}

/// `user.deleted` — the Clerk user is gone.
///
/// * A deletion already exists for this Clerk id (the common case: we deleted
///   the Clerk user ourselves in the job's `clerk` step, or a duplicate
///   delivery): record `clerk_user_deleted` and do nothing else. Idempotent.
/// * Otherwise the delete came from outside (Clerk dashboard / API). Start the
///   same job, flagged Clerk-already-deleted so its `clerk` step is a no-op and
///   its `tenant_planes` step detaches any team-org memberships first.
/// * No local user for the Clerk id: nothing was ever provisioned; ack.
pub(crate) async fn handle_user_deleted(
    pool: &sqlx::PgPool,
    data: &Value,
) -> Result<(), prov::ProvisionError> {
    use crate::services::account_deletion as deletion;
    let Some(clerk_user_id) = data.get("id").and_then(|v| v.as_str()) else {
        return Ok(());
    };
    if deletion::note_clerk_deleted(pool, clerk_user_id).await? {
        return Ok(());
    }
    let Some(user_id) = sqlx::query_scalar::<_, Uuid>("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(clerk_user_id)
        .fetch_optional(pool)
        .await?
    else {
        return Ok(());
    };
    // Anchor the deletion on the first account this user may actually wipe. A
    // personal account they merely co-own with someone else is only the
    // fallback anchor (the job still removes just their own rows there), and
    // the Stripe guard refuses billing events only for accounts that are
    // really theirs, so a surviving co-owner's billing is never touched.
    let deletable = deletion::deletable_account_ids(pool, user_id).await?;
    let anchor = match deletable.first() {
        Some(a) => Some(*a),
        None => deletion::personal_account_ids(pool, user_id).await?.into_iter().next(),
    };
    let Some(account_id) = anchor else {
        // Nothing of theirs to wipe, but the row still carries their email,
        // name and Clerk link: anonymize it now.
        tracing::warn!(%user_id, "clerk webhook: user.deleted for a user with no personal account; anonymizing the user row");
        deletion::anonymize_user(pool, user_id).await?;
        return Ok(());
    };
    match deletion::start(pool, user_id, account_id, Some(clerk_user_id), deletion::Initiator::ClerkWebhook).await {
        Ok(_) => Ok(()),
        Err(deletion::StartError::Db(e)) => Err(prov::ProvisionError::Db(e)),
        // Unreachable for ClerkWebhook (it skips the team-org rule).
        Err(deletion::StartError::BlockedByOrgs(_)) | Err(deletion::StartError::SharedAccount(_)) => Ok(()),
    }
}

/// `organization.deleted` (#1396). In order, every step safe to repeat, so a
/// Svix redelivery (or a retry after a 5xx) converges instead of doubling:
///
/// 1. [`prov::revoke_org_access`]: `org_revocations` + tombstones +
///    member-ocean archive intents, and every membership dropped, in one
///    transaction. Upserts and a delete, so a replay rewrites the same rows.
///    `org_revocations` is keyed on the Clerk org id as plain TEXT (no FK to
///    `accounts`), so this succeeds for an org whose `accounts` row is
///    already gone; the other three statements then match no rows.
///    The revocation is ALSO what stops the tenant workers:
///    `tenancy::routing::list_active` leaves out revoked orgs, so every
///    tenant-loop worker drops the org on its next refresh. The one
///    exception is the member-ocean reconciler, which keeps the org so its
///    archive arm stamps `archived_at` and revokes the members' share tokens
///    (`workers::member_ocean_reconciler::harness_config`).
///    `tenant_databases.status` is not touched here (provisioner-only).
///
/// Billing is then ended, unless one of these says it is not ours to end
/// (each is a 200 no-op for steps 2 and 3):
///
/// * the org is a PERSONAL org (`personal_orgs`): after adoption its id is
///   carried by the person's own account, whose subscription is theirs;
/// * in-app org deletion owns the org (a live `org_deletions` row, #1543):
///   that flow cancels Stripe itself. Read only when the table exists;
/// * no `accounts` row (never provisioned here, or the self-serve cleanup
///   sweep, #1533, already cancelled at Stripe and deleted it).
///
/// 2. [`billing::org_trial::lifecycle::end_for_deleted_org`]: a live trial is
///    ended through `org_trial::db::end` (`trialing`/`converting` ->
///    `canceled`, `checkout_open` -> `abandoned` once its Checkout session is
///    confirmed closed at Stripe). A guarded state UPDATE, so a replay finds
///    nothing to end; no trial row is a no-op. Done before step 3 so no trial
///    pass can start charging the seats in between.
/// 3. [`billing::cancel::cancel_deleted_org_subscription`]: the open
///    invoice is voided and the subscription ends now at Stripe, keyed on its
///    id. No Stripe call when there is no mirror row or no subscription id,
///    or when the mirror or Stripe already says it ended.
///
/// A failure in step 2 or 3 is a 5xx, so Svix redelivers and the
/// already-done steps no-op. Svix gives up after about 5 days; nothing
/// reconciles after that yet (#1547). Nothing here reads `tenant_databases`, so a
/// routing row already deleted by in-app deletion changes nothing.
///
/// [`billing::org_trial::lifecycle::end_for_deleted_org`]: crate::billing::org_trial::lifecycle::end_for_deleted_org
/// [`billing::cancel::cancel_deleted_org_subscription`]: crate::billing::cancel::cancel_deleted_org_subscription
pub(crate) async fn handle_org_deleted(
    state: &AppState,
    pool: &sqlx::PgPool,
    data: &Value,
) -> Result<(), prov::ProvisionError> {
    let Some(org_id) = data.get("id").and_then(|v| v.as_str()) else {
        return Ok(());
    };
    prov::revoke_org_access(pool, org_id).await?;

    let personal: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM personal_orgs WHERE clerk_org_id = $1)")
        .bind(org_id)
        .fetch_one(pool)
        .await?;
    if personal {
        tracing::info!(org = %org_id, "clerk webhook: personal organization deleted; access revoked, billing left alone");
        return Ok(());
    }
    if let Some(deletion) = in_app_org_deletion_state(pool, org_id).await? {
        tracing::info!(org = %org_id, deletion = %deletion, "clerk webhook: organization deleted by in-app deletion; access revoked, that flow owns billing");
        return Ok(());
    }
    let account_id: Option<Uuid> = sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(pool)
        .await?;
    let Some(account_id) = account_id else {
        tracing::info!(org = %org_id, "clerk webhook: organization deleted with no accounts row; access revoked, nothing to bill");
        return Ok(());
    };
    let stripe = state.stripe.as_deref();
    let trial = crate::billing::org_trial::lifecycle::end_for_deleted_org(pool, stripe, account_id)
        .await
        .map_err(prov::ProvisionError::Store)?;
    let billing = crate::billing::cancel::cancel_deleted_org_subscription(pool, stripe, account_id)
        .await
        .map_err(|e| match e {
            crate::billing::cancel::CancelError::Db(e) => prov::ProvisionError::Db(e),
            other => prov::ProvisionError::Store(other.to_string()),
        })?;
    tracing::info!(
        org = %org_id,
        %account_id,
        trial = trial.map(|t| t.as_str()).unwrap_or("none"),
        billing = ?billing,
        "clerk webhook: organization deleted; access revoked, workers stopped, billing ended"
    );
    Ok(())
}

/// The live in-app deletion state of `org_id` (`requested`, `grace`,
/// `awaiting_approval`, `destroying` or `done`), or `None` when there is none
/// or only a `canceled` one. In-app org deletion (#1543) adds
/// `public.org_deletions` and its definer `org_deletion_state_for_org(text)`;
/// until that migration exists this is always `None`, so it is read through
/// `to_regclass`/`to_regprocedure` first rather than referenced directly.
async fn in_app_org_deletion_state(pool: &sqlx::PgPool, org_id: &str) -> Result<Option<String>, sqlx::Error> {
    let present: bool = sqlx::query_scalar(
        "SELECT to_regclass('public.org_deletions') IS NOT NULL \
            AND to_regprocedure('public.org_deletion_state_for_org(text)') IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    if !present {
        return Ok(None);
    }
    sqlx::query_scalar("SELECT public.org_deletion_state_for_org($1)")
        .bind(org_id)
        .fetch_one(pool)
        .await
}

// ── Auth-mail self-delivery (Cloudflare Email Service) ──────────────────────
//
// Clerk's own delivery of auth email (sign-in links, OTP codes) rides its
// shared SendGrid IP pool — sending IPs reverse to `o5.ptr1737.clerk.dev` /
// `o6.ptr6413.clerk.dev`, so our deliverability is pooled with every other
// Clerk tenant, and Clerk's own `Message-ID` is a non-FQDN
// (`@geopod-ismtpd-5`) that some filters penalise. Cloudflare Email Sending
// is already verified for `coralswarm.example` (DKIM selector `cf-bounce`,
// `dmarc=pass`, independently scored 10/10 on mail-tester) — this section
// re-sends Clerk's already-rendered template through that identity instead,
// via the "Delivered by Clerk" OFF + `email.created` webhook mechanism
// (https://clerk.com/docs/guides/customizing-clerk/email-sms-templates).

/// Sender identity for self-delivered Clerk auth mail. DO NOT CHANGE this
/// address: it already carries DKIM alignment and address-level sending
/// reputation for `coralswarm.example` — the entire point of this migration is
/// to consolidate auth mail onto that EXISTING reputation, not start a new
/// address's reputation from zero on the mail that most needs to land in
/// the inbox.
const CLOUDFLARE_EMAIL_FROM_ADDRESS: &str = "person@example.com";
const CLOUDFLARE_EMAIL_FROM_NAME: &str = "CoralSwarm";

/// Bound on the outbound Cloudflare Email Service call — same 10s budget and
/// rationale as `routes::org_invites::send_invite_email`'s Resend call: an
/// inbound Svix delivery attempt must not hang on an outbound send.
const CLOUDFLARE_EMAIL_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A dedicated, SHORT-timeout `reqwest::Client` for the Cloudflare Email
/// Service call — built ONCE per process and cheaply cloned thereafter
/// (`reqwest::Client` is internally `Arc`-backed). Deliberately NOT
/// `AppState.http_client`: that field's own doc comment (`lib.rs`) already
/// names this exact situation — "Callers with a tighter outbound budget
/// (e.g. `clerk_admin::clerk_http_client`'s 10s) build their own dedicated
/// client rather than reusing this one" — because `http_client` is sized for
/// the LLM path's `LLM_TIMEOUT_SECS` (default 60s). Mirrors
/// `clerk_admin::clerk_http_client`'s construction exactly:
/// `connect_timeout(5s)` + a total-request `timeout()` set to
/// [`CLOUDFLARE_EMAIL_SEND_TIMEOUT`].
///
/// That `timeout()` is a TOTAL deadline — reqwest's own doc on
/// `ClientBuilder::timeout` says it "is applied from when the request starts
/// connecting until the response body has finished" — so, unlike a
/// `tokio::time::timeout` wrapped around only `.send()`, this one also
/// bounds the `resp.json().await` body read below. That gap was the bug:
/// wrapping just `.send()` left the body read bounded only by whatever
/// timeout the client used for the send happened to carry, which for
/// `AppState.http_client` is `LLM_TIMEOUT_SECS` — 60s, six times this
/// handler's declared 10s budget.
fn cloudflare_email_http_client() -> reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(CLOUDFLARE_EMAIL_SEND_TIMEOUT)
                .build()
                .expect("failed to build Cloudflare Email Service HTTP client")
        })
        .clone()
}

/// `CLERK_EMAIL_SELF_DELIVERY` — the kill switch for everything below.
/// Same fail-closed contract as every other flag in this codebase
/// (`workers::member_ocean_reconciler::parse_enabled_flag`,
/// `tenancy::member_ocean::enabled`): `"1"` or a case-insensitive `"true"`
/// is on; unset or anything else is off. **Defaults OFF.**
///
/// Cloudflare Email Service is public beta with an undisclosed per-account
/// daily send quota. If this path silently failed while Clerk's own
/// "Delivered by Clerk" toggle had already been switched off in the
/// dashboard, NOBODY COULD LOG IN — there would be no fallback sender for
/// that template at all. This flag is the fast half of the revert: flipping
/// it off stops OUR send attempt immediately. It does **not**, by itself,
/// restore Clerk's own delivery — that is controlled independently by the
/// dashboard's per-template "Delivered by Clerk" toggle, which is why the
/// rollout order in this change's PR description flips this flag on FIRST,
/// verifies a real send, and only THEN turns Clerk's own delivery off — and
/// why the rollback step is "flip this OFF, then flip Clerk's dashboard
/// toggle back ON", not just the first half.
///
/// Read fresh on every webhook delivery, not cached in `AppState` at boot:
/// this is a low-volume path (one call per Clerk-rendered email, not a
/// checkout hot path), so there's no throughput case for threading a new
/// field through every `AppState` literal in this file (every test helper)
/// for a value only this one handler touches — the same call this
/// file already made for `CLERK_WEBHOOK_SECRET` in `clerk_webhook` above.
fn clerk_email_self_delivery_enabled() -> bool {
    matches!(
        std::env::var("CLERK_EMAIL_SELF_DELIVERY").ok().as_deref(),
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true")
    )
}

/// `(account_id, api_token)` for the Cloudflare Email Service REST API, both
/// required together. `None` if either is unset or empty — the caller must
/// fail closed (log at ERROR, send nothing) rather than call Cloudflare with
/// a blank credential. Read fresh per call, same posture and rationale as
/// [`clerk_email_self_delivery_enabled`].
fn cloudflare_email_credentials() -> Option<(String, String)> {
    match (
        std::env::var("CLOUDFLARE_ACCOUNT_ID"),
        std::env::var("CLOUDFLARE_API_TOKEN"),
    ) {
        (Ok(account_id), Ok(api_token)) if !account_id.is_empty() && !api_token.is_empty() => {
            Some((account_id, api_token))
        }
        _ => None,
    }
}

/// The domain half of a recipient address, for logging — never the full
/// address (same PII posture as `routes::org_invites::email_domain`, which
/// this mirrors rather than imports: the two route modules have no existing
/// dependency on each other and a one-line helper isn't worth inventing
/// one). Unlike that sibling, this module's [`parse_email_created`] never
/// validates that `to_email_address` contains an `@` — it only checks
/// non-emptiness (see its own doc comment) — so, unlike
/// `org_invites::email_domain`, this must NOT fall back to returning the
/// whole input on a malformed address: `to_domain` is logged at several
/// ERROR-level sites below, and that fallback would put the full address on
/// every one of them. A missing `@` instead returns a fixed redaction
/// placeholder that reveals nothing about the input — this is a log field,
/// not a validity check, so there is no failure mode to surface here.
fn email_domain(email: &str) -> &str {
    match email.split_once('@') {
        Some((_, domain)) => domain,
        None => "(redacted: no @)",
    }
}

/// Fields this handler needs out of an `email.created` payload's `data`
/// object. Field names mirror `EmailJSON` in Clerk's own backend SDK
/// (`packages/backend/src/api/resources/JSON.ts` in `clerk/javascript`) —
/// the authoritative source consulted here over the docs site's prose,
/// which inconsistently calls the webhook `emails.created` (plural); the
/// SDK's own `WebhookEvent` union type pins the wire event name to the
/// singular `email.created` that the dispatch table above matches on.
struct ParsedEmail<'a> {
    to: &'a str,
    subject: &'a str,
    /// The rendered HTML body (Clerk's `body`).
    html: Option<&'a str>,
    /// The rendered plaintext body (Clerk's `body_plain`).
    text: Option<&'a str>,
    /// Clerk's own from-address local part for this template (e.g.
    /// `"noreply"`), extracted for DIAGNOSTICS ONLY. Our outbound From is
    /// fixed (see [`CLOUDFLARE_EMAIL_FROM_ADDRESS`]'s doc comment) and is
    /// never derived from this — logged so an operator can tell which
    /// identity Clerk itself would have used, if that ever needs comparing.
    from_email_name: Option<&'a str>,
    /// The template identifier (e.g. `"sign_in_code"`), for logging.
    slug: Option<&'a str>,
    /// Whether Clerk's own dashboard toggle for this template still has
    /// "Delivered by Clerk" on — see [`handle_email_created`]'s doc comment,
    /// "Why `delivered_by_clerk` gates the send", for why this is checked
    /// before ever calling Cloudflare.
    delivered_by_clerk: Option<bool>,
}

/// Parse an `email.created` payload's `data` object. Returns `Err` with a
/// short, loggable reason — never panics — on anything missing or
/// malformed: a wrong-typed field (e.g. `subject` sent as a number) reads
/// identically to a missing one via `Value::as_str`'s `None`, both handled
/// by the same `?`/`ok_or` below.
fn parse_email_created(data: &Value) -> Result<ParsedEmail<'_>, &'static str> {
    let to = data
        .get("to_email_address")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("missing or empty to_email_address")?;
    let subject = data
        .get("subject")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("missing or empty subject")?;
    let html = data
        .get("body")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let text = data
        .get("body_plain")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    if html.is_none() && text.is_none() {
        return Err("neither body nor body_plain present — nothing to send");
    }
    Ok(ParsedEmail {
        to,
        subject,
        html,
        text,
        from_email_name: data
            .get("from_email_name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty()),
        slug: data
            .get("slug")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty()),
        delivered_by_clerk: data.get("delivered_by_clerk").and_then(Value::as_bool),
    })
}

/// Build the Cloudflare Email Service REST API request body
/// (`POST /accounts/{account_id}/email/sending/send`) for one self-delivered
/// auth email. Pure and side-effect-free so the test below can assert the
/// exact JSON shape without a live network call. Mirrors the schema at
/// <https://developers.cloudflare.com/api/resources/email_sending/methods/send/>:
/// `from`/`to` are `{address, name}` objects (a bare string also works for
/// `to`, used here since we never have a display name for the recipient);
/// `html`/`text` are each optional but at least one is required — enforced
/// by [`parse_email_created`] before this is ever called, not re-checked
/// here.
fn build_cloudflare_send_body(to: &str, subject: &str, html: Option<&str>, text: Option<&str>) -> Value {
    let mut body = json!({
        "from": {
            "address": CLOUDFLARE_EMAIL_FROM_ADDRESS,
            "name": CLOUDFLARE_EMAIL_FROM_NAME,
        },
        "to": to,
        "subject": subject,
    });
    // `json!` above always produces `Value::Object`, so this is infallible.
    let obj = body.as_object_mut().expect("literal object");
    if let Some(html) = html {
        obj.insert("html".to_string(), json!(html));
    }
    if let Some(text) = text {
        obj.insert("text".to_string(), json!(text));
    }
    body
}

/// What a Cloudflare Email Service response means for OUR logging, decided
/// as a pure function of the HTTP status and parsed body so it's
/// unit-testable without a live round trip — see the test below. Response
/// shape: `{"success": bool, "errors": [...], "result": {"message_id": …} |
/// null}` per the REST API reference linked on [`build_cloudflare_send_body`].
enum CloudflareSendOutcome {
    Sent { message_id: String },
    Rejected,
}

/// The double-send guard's decision for one `email.created` event, as a pure
/// function of `data.delivered_by_clerk` — see [`handle_email_created`]'s
/// doc comment, "Why `delivered_by_clerk` gates the send", for the
/// double-send hazard this exists to prevent (every auth email sent twice:
/// once by Clerk, once by us). Split out so this guard is unit-testable in
/// isolation from a live network call, same rationale as
/// [`interpret_cloudflare_response`] below.
#[derive(Debug, PartialEq, Eq)]
enum DeliveredByClerkDecision {
    /// `Some(false)` — Clerk's dashboard toggle for this template is off;
    /// safe to self-deliver via Cloudflare.
    Send,
    /// `Some(true)` — Clerk's dashboard toggle is still on and Clerk is
    /// already sending this template; sending again would double-send.
    DeclineAlreadyDeliveredByClerk,
    /// `None` — the field is absent from an otherwise-parseable payload.
    /// Fails closed: cannot rule out Clerk already delivering this one, so
    /// declines rather than risk a double send on an unrecognized shape.
    DeclineUnknown,
}

fn decide_on_delivered_by_clerk(delivered_by_clerk: Option<bool>) -> DeliveredByClerkDecision {
    match delivered_by_clerk {
        Some(false) => DeliveredByClerkDecision::Send,
        Some(true) => DeliveredByClerkDecision::DeclineAlreadyDeliveredByClerk,
        None => DeliveredByClerkDecision::DeclineUnknown,
    }
}

fn interpret_cloudflare_response(status: reqwest::StatusCode, body: &Value) -> CloudflareSendOutcome {
    if status.is_success() && body.get("success").and_then(Value::as_bool) == Some(true) {
        let message_id = body
            .get("result")
            .and_then(|r| r.get("message_id"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        CloudflareSendOutcome::Sent { message_id }
    } else {
        CloudflareSendOutcome::Rejected
    }
}

/// `email.created` — CoralSwarm's own delivery path for Clerk-rendered
/// transactional email, replacing Clerk's own ESP send with a direct call
/// to Cloudflare Email Service. See this file's "Auth-mail self-delivery"
/// section header for the full deliverability rationale.
///
/// ## Why this never turns the webhook response into a 500
///
/// Every other handler in this file can usefully be retried by Svix: a
/// stalled tenant pool build, a lock-contended insert, a transient DB
/// error — all self-heal on redelivery (see [`handle_membership_upsert`]'s
/// and [`provision_member_ocean_best_effort`]'s doc comments for that exact
/// reasoning applied to org/membership sync). An auth email has NO such
/// window: Svix's own redelivery schedule runs over hours, and a sign-in
/// link or OTP code that stale is worthless to the recipient whether or not
/// the resend succeeds — they will have already given up or asked for a new
/// one through the product's own "resend code" flow, which is the real
/// retry mechanism here, not Svix. 500ing to trigger a redelivery therefore
/// buys nothing and costs something (repeated attempts, alerting noise).
/// So: every failure mode below is logged at ERROR (per this feature's
/// requirement that failures be loud, never a silent 200) and then
/// swallowed — this function always returns `()`, and its caller always
/// acks Svix with 200.
///
/// ## Why `delivered_by_clerk` gates the send
///
/// `data.delivered_by_clerk` reflects Clerk's OWN dashboard toggle for this
/// template at send time — it is a required (non-optional) field on Clerk's
/// `Email` resource, present on every `email.created` delivery regardless of
/// whether this repo has ever touched the toggle. Sending via Cloudflare
/// whenever this flag is on, without also checking that field, would
/// DOUBLE-SEND every auth email for as long as Clerk's dashboard toggle
/// stays on — which is exactly the state this change is deployed in
/// throughout most of its rollout (flag on, dashboard toggle still on,
/// verifying one send at a time). Gating on `delivered_by_clerk == false`
/// makes that rollout safe by construction: flipping [`CLERK_EMAIL_SELF_DELIVERY`]
/// on is a no-op for every template whose dashboard toggle hasn't ALSO been
/// switched off, and a missing field fails closed (declines to send) rather
/// than risk a double send on an unrecognized payload shape.
async fn handle_email_created(data: &Value) {
    if !clerk_email_self_delivery_enabled() {
        tracing::debug!(
            "clerk_webhook: email.created received, CLERK_EMAIL_SELF_DELIVERY is off — no-op \
             (Clerk's own delivery remains authoritative)"
        );
        return;
    }

    let parsed = match parse_email_created(data) {
        Ok(p) => p,
        Err(reason) => {
            tracing::error!(
                reason,
                "clerk_webhook: email.created payload could not be parsed — dropping (no auth \
                 email will be sent for this event; check whether Clerk's payload contract \
                 changed)"
            );
            return;
        }
    };
    let slug = parsed.slug.unwrap_or("(no slug)");
    let to_domain = email_domain(parsed.to);
    let clerk_from_local = parsed.from_email_name.unwrap_or("(unset, defaults to noreply)");

    match decide_on_delivered_by_clerk(parsed.delivered_by_clerk) {
        DeliveredByClerkDecision::Send => {}
        DeliveredByClerkDecision::DeclineAlreadyDeliveredByClerk => {
            tracing::info!(
                slug,
                to_domain,
                "clerk_webhook: email.created has delivered_by_clerk=true — Clerk's dashboard \
                 toggle for this template is still on, so Clerk is already sending it; \
                 declining to send a duplicate via Cloudflare"
            );
            return;
        }
        DeliveredByClerkDecision::DeclineUnknown => {
            tracing::warn!(
                slug,
                to_domain,
                "clerk_webhook: email.created is missing delivered_by_clerk — failing closed \
                 (cannot rule out Clerk already delivering this one), declining to send"
            );
            return;
        }
    }

    let Some((account_id, api_token)) = cloudflare_email_credentials() else {
        tracing::error!(
            slug,
            to_domain,
            "clerk_webhook: CLERK_EMAIL_SELF_DELIVERY is on but CLOUDFLARE_ACCOUNT_ID / \
             CLOUDFLARE_API_TOKEN are not both set — cannot send (misconfigured)"
        );
        return;
    };

    let body = build_cloudflare_send_body(parsed.to, parsed.subject, parsed.html, parsed.text);
    let url =
        format!("https://api.cloudflare.com/client/v4/accounts/{account_id}/email/sending/send");

    // No `tokio::time::timeout` wrapper here: `cloudflare_email_http_client`'s
    // own `timeout()` is a total deadline that already covers `.send()`
    // AND the `resp.json().await` body read below in one budget — see that
    // function's doc comment for why a wrapper around `.send()` alone was
    // the bug.
    match cloudflare_email_http_client()
        .post(&url)
        .bearer_auth(&api_token)
        .json(&body)
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            match resp.json::<Value>().await {
                Ok(cf_body) => match interpret_cloudflare_response(status, &cf_body) {
                    CloudflareSendOutcome::Sent { message_id } => {
                        tracing::info!(
                            slug,
                            to_domain,
                            clerk_from_local,
                            message_id,
                            "clerk_webhook: sent Clerk-rendered auth email via Cloudflare Email \
                             Service"
                        );
                    }
                    CloudflareSendOutcome::Rejected => {
                        // Loud per this feature's error-handling requirement —
                        // this is the one place an operator needs to look to
                        // diagnose a broken send path. Still acks 200 to
                        // Svix; see this function's own doc comment above.
                        tracing::error!(
                            slug,
                            to_domain,
                            status = %status,
                            cf_response = %cf_body,
                            "clerk_webhook: Cloudflare Email Service rejected the send"
                        );
                    }
                },
                Err(e) if e.is_timeout() => {
                    // The body read blew the total deadline — same operator
                    // signal as a send-phase timeout, logged the same way.
                    tracing::error!(
                        slug,
                        to_domain,
                        status = %status,
                        timeout_secs = CLOUDFLARE_EMAIL_SEND_TIMEOUT.as_secs(),
                        "clerk_webhook: Cloudflare Email Service response body read timed out"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        slug,
                        to_domain,
                        status = %status,
                        error = %e,
                        "clerk_webhook: Cloudflare Email Service response body was not valid JSON"
                    );
                }
            }
        }
        Err(e) if e.is_timeout() => {
            tracing::error!(
                slug,
                to_domain,
                timeout_secs = CLOUDFLARE_EMAIL_SEND_TIMEOUT.as_secs(),
                "clerk_webhook: Cloudflare Email Service send timed out"
            );
        }
        Err(e) => {
            tracing::error!(
                slug,
                to_domain,
                error = %e,
                "clerk_webhook: transport error calling Cloudflare Email Service"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine};
    use hmac::{Hmac, Mac};

    // Build a valid Svix signature for a payload, mirroring the sender.
    fn sign(secret_b64: &str, id: &str, ts: &str, body: &str) -> String {
        let key = STANDARD.decode(secret_b64).unwrap();
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
        mac.update(format!("{id}.{ts}.{body}").as_bytes());
        format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()))
    }

    fn headers(id: &str, ts: &str, sig: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("svix-id", id.parse().unwrap());
        h.insert("svix-timestamp", ts.parse().unwrap());
        h.insert("svix-signature", sig.parse().unwrap());
        h
    }

    // A fresh timestamp so the replay window accepts it.
    fn now_ts() -> String {
        chrono::Utc::now().timestamp().to_string()
    }

    #[test]
    fn verify_svix_accepts_valid_signature() {
        let secret_b64 = STANDARD.encode(b"super-secret-key-material");
        let secret = format!("whsec_{secret_b64}");
        let (id, ts, body) = ("msg_1", now_ts(), r#"{"type":"x"}"#);
        let sig = sign(&secret_b64, id, &ts, body);
        assert!(verify_svix(&secret, &headers(id, &ts, &sig), body.as_bytes()));
    }

    #[test]
    fn verify_svix_rejects_tampered_body() {
        let secret_b64 = STANDARD.encode(b"super-secret-key-material");
        let secret = format!("whsec_{secret_b64}");
        let (id, ts, body) = ("msg_1", now_ts(), r#"{"type":"x"}"#);
        let sig = sign(&secret_b64, id, &ts, body);
        assert!(!verify_svix(&secret, &headers(id, &ts, &sig), br#"{"type":"y"}"#));
    }

    #[test]
    fn verify_svix_rejects_wrong_secret() {
        let secret_b64 = STANDARD.encode(b"super-secret-key-material");
        let (id, ts, body) = ("msg_1", now_ts(), r#"{"type":"x"}"#);
        let sig = sign(&secret_b64, id, &ts, body);
        let other = format!("whsec_{}", STANDARD.encode(b"a-different-key"));
        assert!(!verify_svix(&other, &headers(id, &ts, &sig), body.as_bytes()));
    }

    #[test]
    fn verify_svix_rejects_missing_headers() {
        let secret = format!("whsec_{}", STANDARD.encode(b"k"));
        assert!(!verify_svix(&secret, &HeaderMap::new(), b"{}"));
    }

    #[test]
    fn verify_svix_rejects_stale_timestamp() {
        // A correctly-signed but old payload must be rejected (replay).
        let secret_b64 = STANDARD.encode(b"super-secret-key-material");
        let secret = format!("whsec_{secret_b64}");
        let stale = (chrono::Utc::now().timestamp() - 3600).to_string();
        let (id, body) = ("msg_1", r#"{"type":"x"}"#);
        let sig = sign(&secret_b64, id, &stale, body);
        assert!(!verify_svix(&secret, &headers(id, &stale, &sig), body.as_bytes()));
    }

    #[test]
    fn verify_svix_accepts_one_of_multiple_signatures() {
        let secret_b64 = STANDARD.encode(b"super-secret-key-material");
        let secret = format!("whsec_{secret_b64}");
        let (id, ts, body) = ("msg_1", now_ts(), r#"{"type":"x"}"#);
        let good = sign(&secret_b64, id, &ts, body);
        let combined = format!("v1,deadbeef {good}");
        assert!(verify_svix(&secret, &headers(id, &ts, &combined), body.as_bytes()));
    }

    // ── email.created / Cloudflare self-delivery ────────────────────────────
    //
    // These mutate `CLERK_EMAIL_SELF_DELIVERY` / `CLOUDFLARE_ACCOUNT_ID` /
    // `CLOUDFLARE_API_TOKEN`, which `cargo test`'s default in-binary
    // thread-pool parallelism turns into a real race against any OTHER test
    // in this same crate touching the same vars (there are none today, but
    // against each other within this module too) — reusing
    // `mcp::test_support::env_lock()` (`mcp/mod.rs`) rather than a second
    // lock, same precedent as `mcp::auth`'s and `mcp::tools::mod`'s own
    // cross-module reuse of it.

    #[test]
    fn email_self_delivery_flag_defaults_off_and_parses_truthy_values() {
        let _guard = crate::mcp::test_support::env_lock().blocking_lock();
        // SAFETY: `_guard` serializes every mutation of
        // `CLERK_EMAIL_SELF_DELIVERY` across the whole process for the rest
        // of this test's body — no other thread can be reading or writing
        // it concurrently, and this test's own reads (via
        // `clerk_email_self_delivery_enabled`) happen strictly after each
        // write returns, on this same thread.
        unsafe {
            std::env::remove_var("CLERK_EMAIL_SELF_DELIVERY");
        }
        assert!(!clerk_email_self_delivery_enabled());
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::set_var("CLERK_EMAIL_SELF_DELIVERY", "1");
        }
        assert!(clerk_email_self_delivery_enabled());
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::set_var("CLERK_EMAIL_SELF_DELIVERY", "TRUE");
        }
        assert!(clerk_email_self_delivery_enabled());
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::set_var("CLERK_EMAIL_SELF_DELIVERY", "no");
        }
        assert!(!clerk_email_self_delivery_enabled());
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::remove_var("CLERK_EMAIL_SELF_DELIVERY");
        }
    }

    #[test]
    fn cloudflare_email_credentials_require_both_nonempty() {
        let _guard = crate::mcp::test_support::env_lock().blocking_lock();
        // SAFETY: `_guard` serializes every mutation of
        // `CLOUDFLARE_ACCOUNT_ID` / `CLOUDFLARE_API_TOKEN` across the whole
        // process for the rest of this test's body — no other thread can be
        // reading or writing them concurrently, and this test's own reads
        // (via `cloudflare_email_credentials`) happen strictly after each
        // write returns, on this same thread.
        unsafe {
            std::env::remove_var("CLOUDFLARE_ACCOUNT_ID");
            std::env::remove_var("CLOUDFLARE_API_TOKEN");
        }
        assert!(cloudflare_email_credentials().is_none());
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::set_var("CLOUDFLARE_ACCOUNT_ID", "acct_123");
        }
        // Token still unset — must not treat a lone account id as configured.
        assert!(cloudflare_email_credentials().is_none());
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::set_var("CLOUDFLARE_API_TOKEN", "");
        }
        // Empty string is not "set" for this purpose either.
        assert!(cloudflare_email_credentials().is_none());
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::set_var("CLOUDFLARE_API_TOKEN", "tok_abc");
        }
        assert_eq!(
            cloudflare_email_credentials(),
            Some(("acct_123".to_string(), "tok_abc".to_string()))
        );
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::remove_var("CLOUDFLARE_ACCOUNT_ID");
            std::env::remove_var("CLOUDFLARE_API_TOKEN");
        }
    }

    // ── `email_domain` PII redaction ─────────────────────────────────────────

    #[test]
    fn email_domain_returns_the_domain_half_of_a_well_formed_address() {
        assert_eq!(email_domain("user@example.com"), "example.com");
    }

    #[test]
    fn email_domain_redacts_rather_than_leaking_a_malformed_address_with_no_at_sign() {
        // The PII regression this guards against: `to_domain` is logged at
        // several sites in `handle_email_created`, several of them
        // ERROR-level, so a malformed address (no `@`) must never surface
        // as the full address in a log line. This is reachable from a real
        // webhook delivery, not just hypothetical — see
        // `parse_email_created_accepts_a_payload_with_no_at_sign_in_the_address`
        // just below: `parse_email_created` only checks that
        // `to_email_address` is non-empty, never that it contains an `@`.
        let malformed = "not-an-email-address-at-all";
        let domain = email_domain(malformed);
        assert_ne!(
            domain, malformed,
            "email_domain must never return the full address verbatim on a \
             malformed input — that is exactly the PII leak this function \
             exists to prevent"
        );
        assert_eq!(domain, "(redacted: no @)");
    }

    #[test]
    fn parse_email_created_accepts_a_payload_with_no_at_sign_in_the_address() {
        // Proves the precondition `email_domain`'s doc comment relies on:
        // this handler's own parser does NOT validate email shape (unlike
        // `org_invites::valid_email`, which its sibling `email_domain`
        // relies on) — only non-emptiness — so a no-`@` value reaches
        // `handle_email_created` as a normal, successfully-parsed `to`.
        let data = serde_json::json!({
            "to_email_address": "not-an-email-address-at-all",
            "subject": "s",
            "body": "<p>hi</p>",
        });
        let parsed = parse_email_created(&data).expect("no validation of `@` presence");
        assert_eq!(parsed.to, "not-an-email-address-at-all");
    }

    #[test]
    fn parse_email_created_accepts_a_full_valid_payload() {
        let data = serde_json::json!({
            "object": "email",
            "id": "ema_123",
            "slug": "sign_in_code",
            "from_email_name": "noreply",
            "to_email_address": "user@example.com",
            "email_address_id": null,
            "subject": "Your sign-in code",
            "body": "<p>123456</p>",
            "body_plain": "123456",
            "status": "queued",
            "delivered_by_clerk": false,
        });
        let parsed = parse_email_created(&data).expect("valid payload must parse");
        assert_eq!(parsed.to, "user@example.com");
        assert_eq!(parsed.subject, "Your sign-in code");
        assert_eq!(parsed.html, Some("<p>123456</p>"));
        assert_eq!(parsed.text, Some("123456"));
        assert_eq!(parsed.from_email_name, Some("noreply"));
        assert_eq!(parsed.slug, Some("sign_in_code"));
        assert_eq!(parsed.delivered_by_clerk, Some(false));
    }

    #[test]
    fn parse_email_created_accepts_html_only_or_text_only() {
        let html_only = serde_json::json!({
            "to_email_address": "a@example.com",
            "subject": "s",
            "body": "<p>hi</p>",
        });
        let parsed = parse_email_created(&html_only).expect("html-only must parse");
        assert_eq!(parsed.html, Some("<p>hi</p>"));
        assert_eq!(parsed.text, None);

        let text_only = serde_json::json!({
            "to_email_address": "a@example.com",
            "subject": "s",
            "body_plain": "hi",
        });
        let parsed = parse_email_created(&text_only).expect("text-only must parse");
        assert_eq!(parsed.html, None);
        assert_eq!(parsed.text, Some("hi"));
    }

    #[test]
    fn parse_email_created_rejects_missing_fields() {
        assert!(parse_email_created(&serde_json::json!({})).is_err());
        assert!(parse_email_created(&serde_json::json!({
            "subject": "s",
            "body": "<p>hi</p>",
        }))
        .is_err());
        assert!(parse_email_created(&serde_json::json!({
            "to_email_address": "a@example.com",
            "body": "<p>hi</p>",
        }))
        .is_err());
        // Neither body nor body_plain present.
        assert!(parse_email_created(&serde_json::json!({
            "to_email_address": "a@example.com",
            "subject": "s",
        }))
        .is_err());
    }

    #[test]
    fn parse_email_created_rejects_malformed_without_panicking() {
        // Wholly wrong shapes — Value::as_str returning None for a
        // non-string reads identically to a missing field, never panics.
        assert!(parse_email_created(&Value::Null).is_err());
        assert!(parse_email_created(&serde_json::json!([1, 2, 3])).is_err());
        assert!(parse_email_created(&serde_json::json!({
            "to_email_address": 12345,
            "subject": {"nested": "object"},
            "body": true,
        }))
        .is_err());
        // Empty strings must not count as present.
        assert!(parse_email_created(&serde_json::json!({
            "to_email_address": "",
            "subject": "",
            "body": "",
        }))
        .is_err());
    }

    #[tokio::test]
    async fn handle_email_created_is_a_noop_when_the_flag_is_off() {
        let _guard = crate::mcp::test_support::env_lock().lock().await;
        // SAFETY: `_guard` serializes every mutation of these three vars
        // across the whole process for the rest of this test's body — no
        // other thread can be reading or writing them concurrently, and
        // `handle_email_created` below only reads them after this block
        // returns, on this same task.
        unsafe {
            std::env::remove_var("CLERK_EMAIL_SELF_DELIVERY");
            // Deliberately also unset so that if the flag check above were
            // ever accidentally bypassed, this test would still fail closed
            // rather than attempt a real network call.
            std::env::remove_var("CLOUDFLARE_ACCOUNT_ID");
            std::env::remove_var("CLOUDFLARE_API_TOKEN");
        }
        let data = serde_json::json!({
            "to_email_address": "user@example.com",
            "subject": "Sign in to CoralSwarm",
            "body": "<p>code</p>",
            "delivered_by_clerk": false,
        });
        // Must return promptly without panicking; there is nothing further
        // to assert from the caller's side of a fire-and-forget `()` fn —
        // the flag-parsing behavior itself is covered directly by
        // `email_self_delivery_flag_defaults_off_and_parses_truthy_values`.
        handle_email_created(&data).await;
    }

    // ── Double-send guard (`delivered_by_clerk`) ────────────────────────────
    //
    // A regression here means every auth email gets sent TWICE — once by
    // Clerk, once by us — so these are split into two layers:
    //
    // 1. `decide_on_delivered_by_clerk` unit tests below assert the guard's
    //    actual decision for all three input states directly, with no
    //    network involved. This is the definitive test: it fails if the
    //    `Some(true)` or `None` case is ever wired to `Send` by mistake.
    // 2. The two `handle_email_created` tests further below exercise the
    //    full handler with those same payload shapes, with the flag ON but
    //    Cloudflare credentials deliberately UNSET — same "fail closed
    //    without a real network call" posture as
    //    `handle_email_created_is_a_noop_when_the_flag_is_off` above. They
    //    prove the handler actually calls this guard (not just that the
    //    guard function itself is correct) and that the `tracing::info!`/
    //    `tracing::warn!` calls on the decline paths execute without
    //    panicking.

    #[test]
    fn decide_on_delivered_by_clerk_sends_only_when_explicitly_false() {
        assert_eq!(
            decide_on_delivered_by_clerk(Some(false)),
            DeliveredByClerkDecision::Send
        );
    }

    #[test]
    fn decide_on_delivered_by_clerk_declines_when_clerk_is_still_delivering() {
        assert_eq!(
            decide_on_delivered_by_clerk(Some(true)),
            DeliveredByClerkDecision::DeclineAlreadyDeliveredByClerk
        );
    }

    #[test]
    fn decide_on_delivered_by_clerk_declines_when_field_is_missing() {
        assert_eq!(
            decide_on_delivered_by_clerk(None),
            DeliveredByClerkDecision::DeclineUnknown
        );
    }

    #[tokio::test]
    async fn handle_email_created_declines_when_delivered_by_clerk_is_true() {
        let _guard = crate::mcp::test_support::env_lock().lock().await;
        // SAFETY: `_guard` serializes every mutation of these three vars
        // across the whole process for the rest of this test's body — no
        // other thread can be reading or writing them concurrently, and
        // `handle_email_created` below only reads them after this block
        // returns, on this same task.
        unsafe {
            std::env::set_var("CLERK_EMAIL_SELF_DELIVERY", "1");
            // Deliberately unset: if the double-send guard were ever
            // bypassed, this test must still fail closed at the credentials
            // check rather than attempt a real network call to Cloudflare.
            std::env::remove_var("CLOUDFLARE_ACCOUNT_ID");
            std::env::remove_var("CLOUDFLARE_API_TOKEN");
        }
        let data = serde_json::json!({
            "to_email_address": "user@example.com",
            "subject": "Sign in to CoralSwarm",
            "body": "<p>code</p>",
            "delivered_by_clerk": true,
        });
        handle_email_created(&data).await;
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::remove_var("CLERK_EMAIL_SELF_DELIVERY");
        }
    }

    #[tokio::test]
    async fn handle_email_created_declines_when_delivered_by_clerk_is_missing() {
        let _guard = crate::mcp::test_support::env_lock().lock().await;
        // SAFETY: see `handle_email_created_declines_when_delivered_by_clerk_is_true` —
        // same invariant, same guard.
        unsafe {
            std::env::set_var("CLERK_EMAIL_SELF_DELIVERY", "1");
            std::env::remove_var("CLOUDFLARE_ACCOUNT_ID");
            std::env::remove_var("CLOUDFLARE_API_TOKEN");
        }
        let data = serde_json::json!({
            "to_email_address": "user@example.com",
            "subject": "Sign in to CoralSwarm",
            "body": "<p>code</p>",
            // `delivered_by_clerk` deliberately absent.
        });
        handle_email_created(&data).await;
        // SAFETY: see above — still under `_guard`.
        unsafe {
            std::env::remove_var("CLERK_EMAIL_SELF_DELIVERY");
        }
    }

    #[test]
    fn build_cloudflare_send_body_matches_the_rest_api_schema() {
        let body = build_cloudflare_send_body(
            "user@example.com",
            "Your sign-in code",
            Some("<p>123456</p>"),
            Some("123456"),
        );
        assert_eq!(
            body,
            serde_json::json!({
                "from": {"address": "person@example.com", "name": "CoralSwarm"},
                "to": "user@example.com",
                "subject": "Your sign-in code",
                "html": "<p>123456</p>",
                "text": "123456",
            })
        );
    }

    #[test]
    fn build_cloudflare_send_body_omits_absent_html_or_text() {
        let html_only = build_cloudflare_send_body("a@example.com", "s", Some("<p>hi</p>"), None);
        assert!(html_only.get("text").is_none());
        assert_eq!(html_only["html"], "<p>hi</p>");

        let text_only = build_cloudflare_send_body("a@example.com", "s", None, Some("hi"));
        assert!(text_only.get("html").is_none());
        assert_eq!(text_only["text"], "hi");
    }

    #[test]
    fn interpret_cloudflare_response_recognizes_success_and_rejection() {
        let success = serde_json::json!({
            "success": true,
            "errors": [],
            "result": {"delivered": ["a@example.com"], "message_id": "<person@example.com>"},
        });
        match interpret_cloudflare_response(reqwest::StatusCode::OK, &success) {
            CloudflareSendOutcome::Sent { message_id } => {
                assert_eq!(message_id, "<person@example.com>");
            }
            CloudflareSendOutcome::Rejected => panic!("expected Sent"),
        }

        let rejected = serde_json::json!({
            "success": false,
            "errors": [{"code": 10001, "message": "email.sending.error.invalid_request_schema"}],
            "result": null,
        });
        assert!(matches!(
            interpret_cloudflare_response(reqwest::StatusCode::BAD_REQUEST, &rejected),
            CloudflareSendOutcome::Rejected
        ));

        // A 200 with `success:false` must still be treated as rejected —
        // the HTTP status alone is not sufficient.
        let ok_status_but_unsuccessful = serde_json::json!({"success": false, "result": null});
        assert!(matches!(
            interpret_cloudflare_response(reqwest::StatusCode::OK, &ok_status_but_unsuccessful),
            CloudflareSendOutcome::Rejected
        ));
    }
}

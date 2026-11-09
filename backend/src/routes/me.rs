//! `GET /v1/me` — the resolved caller identity.
//!
//! Returns the authenticated user's internal ids and active-account context.
//! The web client needs the resolved `account_id` for flows that reference it
//! directly (e.g. connector OAuth), since under Clerk the frontend only holds
//! the org slug/id, not the internal account UUID.
//!
//! `PUT /v1/me/timezone` — the web client's boot-time IANA timezone report
//! (Today-journal tier feature follow-up, migration `0118_users_timezone`):
//! founder decision replaced the tier sweep's original codebase-wide
//! `America/Chicago` default with a real per-user setting, captured
//! automatically from `Intl.DateTimeFormat().resolvedOptions().timeZone` on
//! app boot — see `web/lib/api.ts`'s `me.reportTimezone` for the client half.
//!
//! `PUT /v1/me/active-graph` + `GET /v1/me`'s `active_graph_id` — the user's
//! ACTIVE OCEAN, persisted per (user, account) instead of per browser
//! (claim-based tenancy decision 8, migration `0201_user_active_graph`).
//! Before this, the selection lived only in `localStorage` under
//! `cs_graph_id`, so it did not survive a second browser, a second device, or
//! a reinstall. The read rides `GET /v1/me`, which the web client already
//! calls on its boot path, so restoring the selection costs no extra request.

use axum::{
    extract::{Extension, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::AppState;

pub async fn get_me(
    State(state): State<Arc<AppState>>,
    tenant: crate::tenancy::extractor::Tenant,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    // ── Server capabilities the client cannot otherwise discover ────────────
    // `ORG_TEAMS_ENABLED` is a fleet-global env flag (with a per-org
    // `org_connector_flags` override since Foundation train PR 1.3) read only
    // by the backend, so the web app has no way to know whether creating a
    // shared workspace under this specific org will succeed. Without this the
    // UI has to choose between two bad options: disable the button for every
    // org (wrong once the flag/override is on), or always offer it and let
    // the caller discover the 403 by clicking (wrong while it's off — the
    // DEFAULT). Publishing the RESOLVED per-org capability (not just the raw
    // env default) lets the button reflect what the server will actually do
    // for THIS org, override included.
    //
    // Report it only where it is meaningful. Outside an org context named
    // teams never depended on this flag, so `null` means "not applicable
    // here", distinct from `false` meaning "you are in an org and it
    // resolved off".
    let org_teams_enabled = match tenant.org_context() {
        Some(ctx) => {
            Value::Bool(crate::routes::teams::org_teams_active_for(&state, &ctx.org_id).await)
        }
        None => Value::Null,
    };
    // ── The caller's active ocean in THIS account (migration 0201) ──────────
    // Read from CONTROL (`state.control_store()`), never `tenant.pool`: the
    // row is keyed on (user_id, account_id) and both of those are identity,
    // not content — the same argument `put_me_timezone` below spells out for
    // `users.timezone`. Routed through `Store::user_active_graph` rather than
    // a bare pool — see that method's doc comment and `Store::user_timezone`
    // next to it, the same shape for the same table's sibling column — which
    // also keeps this file off `scripts/check_scoped_shape_ratchet.sh`'s
    // frozen PgPool set (the guard's own suggested fix, matching
    // `routes::prefs::put_prefs`'s precedent).
    //
    // `None` here is a NORMAL, expected answer, not an error: this user has
    // never reported a selection under this account (every user, on first boot
    // after 0201 ships), or the control store is absent. A DB
    // error also resolves to `None` — see `read_active_graph_id`. The client
    // treats all three identically and falls back, so a missing pointer can
    // never break boot.
    let active_graph_id = match state.control_store() {
        Some(store) => read_active_graph_id(store.as_ref(), user.user_id, user.account_id).await,
        None => None,
    };
    // ── Is this account a personal org? (control migration 0208) ────────────
    //
    // The web client's ENTIRE notion of "am I in an org" reduces today to
    // Clerk's `orgId != null`, and a personal org — a Clerk organization that
    // stands for one person's own ocean — satisfies that check identically to
    // a real organization. Nothing else in this response (or in any payload
    // the client already fetches) can tell the two apart: `org_teams_enabled`
    // above is `null` for both, because `tenant.org_context()` is `None` for
    // both (`tenancy::gate::decide` resolves `TokenKind::PersonalOrg` to
    // `GateOutcome::Personal`, same as a plain personal account — see that
    // function's doc comment).
    //
    // `false` on any lookup failure or absent control store: the same fail-safe
    // direction as `active_graph_id` above, and it degrades to exactly
    // today's (broken) behavior — the client falls back to treating the
    // account as an ordinary org — rather than to a NEW failure mode.
    let is_personal_org = match state.control_store() {
        Some(store) => {
            read_is_personal_org(store.as_ref(), user.account_id, user.user_id).await
        }
        None => false,
    };
    (
        StatusCode::OK,
        Json(json!({
            "user_id": user.user_id,
            "account_id": user.account_id,
            "role": user.role,
            "device_id": user.device_id,
            "org_teams_enabled": org_teams_enabled,
            "active_graph_id": active_graph_id,
            "is_personal_org": is_personal_org,
        })),
    )
}

/// The (user, account)-scoped active-ocean pointer, or `None`.
///
/// ADVISORY, NOT AUTHORITATIVE. The value is whatever the client last
/// reported; nothing here proves the ocean still exists or is still reachable
/// (it cannot — `graphs` is content-plane and, for an org account, lives in
/// that org's own tenant database, which is also why migration 0201 carries no
/// foreign key). Reachability is decided by `GET /v1/graphs`, and the client
/// honours this id only when it appears in that list. So the worst a stale or
/// fabricated pointer can do is fail to match and be ignored.
///
/// Never returns an error. A control-plane hiccup on this ONE field must not
/// fail the whole `/v1/me` response — that response is on the web client's
/// gating boot path, and losing a UI preference is not worth a failed sign-in.
/// Logged at `warn!` so a persistent failure is still visible.
async fn read_active_graph_id(
    store: &dyn crate::store::Store,
    user_id: Uuid,
    account_id: Uuid,
) -> Option<Uuid> {
    match store.user_active_graph(user_id, account_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                error = %e,
                user_id = %user_id,
                account_id = %account_id,
                "user_active_graph lookup failed; reporting no active ocean"
            );
            None
        }
    }
}

/// Whether `account_id` is `user_id`'s own personal org (control migration
/// 0208), or
/// `false` on any lookup failure. See [`get_me`]'s call site for why `false`
/// is the fail-safe direction here rather than `true`.
async fn read_is_personal_org(
    store: &dyn crate::store::Store,
    account_id: Uuid,
    user_id: Uuid,
) -> bool {
    match store.account_is_personal_org(account_id, user_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                "account_is_personal_org lookup failed; reporting false"
            );
            false
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PutTimezoneRequest {
    /// An IANA zone name (`Intl.DateTimeFormat().resolvedOptions().timeZone`
    /// on the browser side) — validated server-side against `chrono_tz::Tz`'s
    /// `FromStr`, never trusted as-is.
    pub timezone: String,
}

/// `PUT /v1/me/timezone`. ALWAYS writes to CONTROL's `users` row via
/// `state.control_store()` (`Store::set_user_timezone`) — never through the
/// `Tenant` extractor's content pool (which resolves to an ORG TENANT
/// database when the caller is on an org host, and `users` does not exist
/// there at all; see migration `0118_users_timezone`'s header) — a user's
/// timezone is a property of their IDENTITY, not of whichever ocean/workspace
/// they happen to be standing in when the browser reports it.
///
/// An invalid/unparseable value (a browser Intl quirk, a stale/unsupported
/// zone name) is silently ignored — `200 {"accepted": false}` — rather than
/// erroring, so a malformed report can never break the client's boot
/// sequence; the column simply stays whatever it already was (`NULL` on a
/// first-ever report). Only writes when the value actually CHANGED
/// (`IS DISTINCT FROM`, inside [`Store::set_user_timezone`]), the same
/// no-op-update convention `clerk_provision::sync_user_display_name` already
/// uses for `users.name`.
pub async fn put_me_timezone(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PutTimezoneRequest>,
) -> (StatusCode, Json<Value>) {
    let Some(store) = state.control_store() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    let tz = body.timezone.trim();
    if tz.is_empty() || tz.parse::<chrono_tz::Tz>().is_err() {
        return (StatusCode::OK, Json(json!({"accepted": false})));
    }

    let previous = match store.user_timezone(user.user_id).await {
        Ok(previous) => previous,
        Err(_) => return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"preferences unavailable"}))),
    };
    if let Err(e) = store.set_user_timezone(user.user_id, tz).await {
        tracing::warn!(error = %e, user_id = %user.user_id, "failed to update users.timezone");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        );
    }

    if previous.as_deref()!=Some(tz) {
        if crate::read_models::dirty::account_changed(&state,user.account_id,Some(user.user_id),&crate::read_models::dirty::models_for(crate::read_models::dirty::DirtyEvent::PrefsChanged)).await.is_err() {
            return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"preferences refresh unavailable"})));
        }
    }
    (StatusCode::OK, Json(json!({"accepted": true})))
}

#[derive(Debug, Deserialize)]
pub struct PutActiveGraphRequest {
    /// The ocean the client just selected (`graphs.id`). Typed `String`, not
    /// `Uuid`, ON PURPOSE: a `Uuid` field makes axum reject a malformed body
    /// with a 422 before the handler runs, and this endpoint's whole posture
    /// (below) is that a bad report is ignored, never fatal. Parsed by
    /// [`parse_active_graph_id`].
    pub graph_id: String,
}

/// Validate a client-reported active-ocean id.
///
/// Shape only — this deliberately does NOT check that the ocean exists or that
/// the caller can reach it. See [`put_me_active_graph`] for why that check
/// would be worth less than it looks.
///
/// The nil UUID is refused alongside garbage: no `graphs.id` is ever nil, so a
/// nil here is a client bug (an uninitialised variable serialised into the
/// body), and storing it would leave a pointer that can never match anything
/// while looking like a real selection in the table.
fn parse_active_graph_id(raw: &str) -> Option<Uuid> {
    let id = raw.trim().parse::<Uuid>().ok()?;
    if id.is_nil() {
        return None;
    }
    Some(id)
}

/// `PUT /v1/me/active-graph`. Records which ocean this user is standing in,
/// per (user, account) — migration `0201_user_active_graph`.
///
/// SAME PLANE ARGUMENT AS [`put_me_timezone`], SAME CONCLUSION, DIFFERENT
/// REASONING — worth stating rather than inheriting. A timezone is a property
/// of the user's IDENTITY, so it belongs on CONTROL's `users` row and could
/// never sensibly live on a tenant database. An active-ocean pointer is *not*
/// pure identity: it names a CONTENT-plane row (`graphs.id`) that, for an org
/// account, lives in that org's own tenant database. So the naive read would
/// put it on the tenant plane, next to the thing it points at.
///
/// It still goes to CONTROL, for a reason the timezone case never has to
/// confront: the pointer must be resolvable per ACCOUNT, and the set of
/// accounts a user moves between is control-plane knowledge. Putting one row
/// per (user, account) in N tenant databases would mean N places to look, N
/// places to clean up on offboarding, and no coherent home at all for the
/// personal plane. Keeping the pointer beside the membership it is scoped by
/// is the only arrangement with one obvious home per pair — and it is exactly
/// why this handler, like `put_me_timezone`, takes NO `Tenant` extractor.
///
/// NO OWNERSHIP CHECK, DELIBERATELY. Verifying that `graph_id` is reachable by
/// this caller would require the content plane (the tenant pool for an org),
/// re-coupling an identity-scoped write to tenancy — and it would buy almost
/// nothing, because the check EXPIRES: an ocean can be deleted, or the
/// caller's access revoked, one second after it passes. A write-time check
/// therefore cannot be the safety property. The actual safety property is that
/// this pointer is NEVER A CAPABILITY: it is readable only by the same
/// (user, account) that wrote it (the PK guarantees that — see 0201's header),
/// and `web/lib/activeOcean.ts` honours it only when it appears in the
/// reachable-ocean list `GET /v1/graphs` independently returns. That
/// intersection is evaluated on EVERY boot, so unlike a write-time check it
/// never goes stale. An unresolvable pointer selects nothing and the client
/// falls back; it does not error.
///
/// A malformed id is ignored — `200 {"accepted": false}` — rather than
/// erroring, for the same reason `put_me_timezone` does it: this is called
/// from the client's boot sequence, and a malformed report must never be able
/// to break sign-in. Only writes when the value actually CHANGED (the
/// `ON CONFLICT ... WHERE ... IS DISTINCT FROM` no-op-update inside
/// [`Store::set_user_active_graph`]), so calling it on every boot costs one
/// cheap statement and no row churn.
///
/// Routed through `state.control_store()` (`Store::set_user_active_graph`),
/// not a bare pool — same rationale as [`get_me`]'s read side.
pub async fn put_me_active_graph(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PutActiveGraphRequest>,
) -> (StatusCode, Json<Value>) {
    let Some(store) = state.control_store() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    let Some(graph_id) = parse_active_graph_id(&body.graph_id) else {
        return (StatusCode::OK, Json(json!({"accepted": false})));
    };

    // `updated_at` here is this table's OWN column and means only "when this
    // user last changed oceans in this account".
    if let Err(e) = store
        .set_user_active_graph(user.user_id, user.account_id, graph_id)
        .await
    {
        tracing::warn!(
            error = %e,
            user_id = %user.user_id,
            account_id = %user.account_id,
            "failed to update user_active_graph"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        );
    }

    (StatusCode::OK, Json(json!({"accepted": true})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_timezone_request_deserializes_from_json() {
        let req: PutTimezoneRequest = serde_json::from_str(r#"{"timezone":"Europe/London"}"#).unwrap();
        assert_eq!(req.timezone, "Europe/London");
    }

    #[test]
    fn a_valid_iana_zone_parses_via_chrono_tz() {
        assert!("America/Chicago".parse::<chrono_tz::Tz>().is_ok());
        assert!("Europe/London".parse::<chrono_tz::Tz>().is_ok());
    }

    #[test]
    fn garbage_does_not_parse_as_an_iana_zone() {
        assert!("Not/AZone".parse::<chrono_tz::Tz>().is_err());
        assert!("".parse::<chrono_tz::Tz>().is_err());
    }

    #[test]
    fn put_active_graph_request_deserializes_from_json() {
        let req: PutActiveGraphRequest =
            serde_json::from_str(r#"{"graph_id":"3f2504e0-4f89-11d3-9a0c-0305e82c3301"}"#).unwrap();
        assert_eq!(req.graph_id, "3f2504e0-4f89-11d3-9a0c-0305e82c3301");
    }

    #[test]
    fn a_well_formed_uuid_is_accepted_as_an_active_ocean_id() {
        let id = Uuid::new_v4();
        assert_eq!(parse_active_graph_id(&id.to_string()), Some(id));
        // Surrounding whitespace is trimmed, matching `put_me_timezone`'s
        // handling of its own body field.
        assert_eq!(parse_active_graph_id(&format!("  {id}  ")), Some(id));
    }

    #[test]
    fn garbage_is_ignored_rather_than_stored_as_an_active_ocean_id() {
        // Each of these once reached the column as-is if the id were typed
        // `Uuid` and axum's 422 were the only gate — and a 422 on the boot
        // path is exactly what this endpoint refuses to produce.
        for bad in ["", "   ", "not-a-uuid", "3f2504e0-4f89-11d3-9a0c", "null"] {
            assert_eq!(parse_active_graph_id(bad), None, "{bad:?} must be refused");
        }
    }

    #[test]
    fn the_nil_uuid_is_refused_even_though_it_parses() {
        // The interesting case: `Uuid::parse_str` accepts it happily, so a
        // parse-only guard would store it. No `graphs.id` is ever nil, so a
        // stored nil is a pointer that can never match anything while looking
        // like a real selection.
        assert!("00000000-0000-0000-0000-000000000000"
            .parse::<Uuid>()
            .is_ok());
        assert_eq!(
            parse_active_graph_id("00000000-0000-0000-0000-000000000000"),
            None
        );
    }
}

// ── Postgres tests ───────────────────────────────────────────────────────────
//
// The active-ocean pointer (migration `0201_user_active_graph`). Wired into
// `.github/workflows/ci.yml`'s `org-invites` Postgres job — the same DSN and
// the same already-built lib test binary `routes::prefs::pg_tests` rides, so
// the marginal cost is seconds — with the standard `test -n
// "$ORGT_TEST_BASE_URL"` guard AND the zero-test filter guard. Both matter:
// without the DSN every test below `return`s and the run reports "ok. N
// passed" in 0.00s, and a renamed filter matches zero tests and also exits 0.
// Neither would have asserted anything.
//
// EVERY refusal/no-op test below READS THE ROW BACK. A `200 {"accepted":
// false}` that still wrote, or an "untouched" grant row that quietly changed,
// is the same defect wearing a passing status code — this repo's recurring
// failure shape.
#[cfg(test)]
mod pg_tests {
    use super::*;
    use axum::extract::State as AxumState;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn admin_conn(base: &str) -> PgConnection {
        PgConnection::connect(base).await.expect("connect admin")
    }
    async fn control_pool(base: &str, db: &str) -> PgPool {
        let mut conn = admin_conn(base).await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        pool
    }

    async fn seed_user(control: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{}@me-pg.test", Uuid::new_v4()))
            .bind(format!("user_{}", Uuid::new_v4()))
            .fetch_one(control)
            .await
            .expect("insert user")
    }

    /// `clerk_org_id = Some(..)` makes it an ORG account, `None` the user's
    /// personal account-of-one.
    async fn seed_account(control: &PgPool, owner: Uuid, clerk_org_id: Option<String>) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(clerk_org_id)
        .fetch_one(control)
        .await
        .expect("insert account")
    }

    fn user_ctx(account_id: Uuid, user_id: Uuid) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "me-pg-test".to_string(),
        }
    }

    fn app_state(control: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(control, None))
    }

    async fn put_active(state: &Arc<AppState>, ctx: UserContext, graph_id: &str) -> (StatusCode, Value) {
        let (status, Json(v)) = put_me_active_graph(
            AxumState(state.clone()),
            Extension(ctx),
            Json(PutActiveGraphRequest {
                graph_id: graph_id.to_string(),
            }),
        )
        .await;
        (status, v)
    }

    /// `GET /v1/me` through the REAL handler, so the assertion covers the JSON
    /// assembly and not just the helper underneath it. `Tenant::legacy` is the
    /// no-`TenancyDecision`/runtime-disabled shape the extractor itself
    /// produces for a personal request — see its doc comment; the caveat there
    /// is about PRODUCTION call paths that an Org decision could reach, which
    /// a `#[cfg(test)]` fixture is not.
    async fn get_me_json(state: &Arc<AppState>, ctx: UserContext) -> Value {
        let tenant = crate::tenancy::extractor::Tenant::legacy(state);
        let (status, Json(v)) = get_me(AxumState(state.clone()), tenant, Extension(ctx)).await;
        assert_eq!(status, StatusCode::OK, "GET /v1/me must not fail: {v}");
        v
    }

    /// The stored row, verbatim. Reading this back is what makes a refusal
    /// test mean anything.
    async fn stored_pointer(control: &PgPool, user_id: Uuid, account_id: Uuid) -> Option<Uuid> {
        sqlx::query_scalar::<_, Uuid>(
            "SELECT graph_id FROM user_active_graph WHERE user_id = $1 AND account_id = $2",
        )
        .bind(user_id)
        .bind(account_id)
        .fetch_optional(control)
        .await
        .expect("read user_active_graph")
    }

    async fn stored_updated_at(
        control: &PgPool,
        user_id: Uuid,
        account_id: Uuid,
    ) -> chrono::DateTime<chrono::Utc> {
        sqlx::query_scalar(
            "SELECT updated_at FROM user_active_graph WHERE user_id = $1 AND account_id = $2",
        )
        .bind(user_id)
        .bind(account_id)
        .fetch_one(control)
        .await
        .expect("read user_active_graph.updated_at")
    }

    // ── 1. The round trip that makes the feature real ────────────────────────

    #[tokio::test]
    async fn the_active_ocean_survives_a_new_browser_via_get_me() {
        let Some(base) = base_url() else {
            eprintln!("skipping the_active_ocean_survives_a_new_browser_via_get_me: set ORGT_TEST_BASE_URL");
            return;
        };
        let control = control_pool(&base, "orgt_me_active1_ctl").await;
        let user = seed_user(&control).await;
        let account = seed_account(&control, user, None).await;
        let state = app_state(control.clone());

        // A brand-new (user, account): no pointer yet. This is the state EVERY
        // user is in on their first boot after 0201 ships, so it has to be a
        // clean `null` and not an error.
        let before = get_me_json(&state, user_ctx(account, user)).await;
        assert_eq!(
            before["active_graph_id"],
            Value::Null,
            "a user who has never reported a selection must read as null, not error: {before}"
        );

        let ocean = Uuid::new_v4();
        let (status, body) = put_active(&state, user_ctx(account, user), &ocean.to_string()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["accepted"], Value::Bool(true), "{body}");

        // The point of the whole PR: a DIFFERENT browser (no localStorage at
        // all) gets the selection back from the server.
        let after = get_me_json(&state, user_ctx(account, user)).await;
        assert_eq!(
            after["active_graph_id"],
            Value::String(ocean.to_string()),
            "GET /v1/me must report the ocean that was reported: {after}"
        );
        assert_eq!(stored_pointer(&control, user, account).await, Some(ocean));
    }

    // ── 2. The load-bearing one: the pointer is scoped per ACCOUNT ───────────

    #[tokio::test]
    async fn one_users_choice_in_org_a_is_invisible_from_org_b() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping one_users_choice_in_org_a_is_invisible_from_org_b: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control = control_pool(&base, "orgt_me_active2_ctl").await;
        // ONE user, standing in TWO accounts — the exact shape `localStorage`
        // gets wrong today: `cs_graph_id` is a single global key and
        // `providers.tsx`'s `resetOcean` never clears it, so org A's ocean id
        // is present during org B's boot and is saved only by failing to match
        // B's reachable list. The PK here makes that structurally impossible.
        let user = seed_user(&control).await;
        let org_a = seed_account(&control, user, Some(format!("org_a_{}", Uuid::new_v4()))).await;
        let org_b = seed_account(&control, user, Some(format!("org_b_{}", Uuid::new_v4()))).await;
        let state = app_state(control.clone());

        let ocean_a = Uuid::new_v4();
        put_active(&state, user_ctx(org_a, user), &ocean_a.to_string()).await;

        let in_b = get_me_json(&state, user_ctx(org_b, user)).await;
        assert_eq!(
            in_b["active_graph_id"],
            Value::Null,
            "org A's ocean must NOT be reported while standing in org B: {in_b}"
        );

        // …and choosing one in B does not disturb A.
        let ocean_b = Uuid::new_v4();
        put_active(&state, user_ctx(org_b, user), &ocean_b.to_string()).await;
        assert_ne!(ocean_a, ocean_b, "fixture precondition: two distinct oceans");
        assert_eq!(stored_pointer(&control, user, org_a).await, Some(ocean_a));
        assert_eq!(stored_pointer(&control, user, org_b).await, Some(ocean_b));
        let back_in_a = get_me_json(&state, user_ctx(org_a, user)).await;
        assert_eq!(
            back_in_a["active_graph_id"],
            Value::String(ocean_a.to_string()),
            "switching back to org A must restore A's own ocean: {back_in_a}"
        );
    }

    // ── 3. A malformed report is ignored AND stores nothing ──────────────────

    #[tokio::test]
    async fn a_malformed_report_cannot_break_boot_or_clobber_a_good_pointer() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_malformed_report_cannot_break_boot_or_clobber_a_good_pointer: set ORGT_TEST_BASE_URL");
            return;
        };
        let control = control_pool(&base, "orgt_me_active4_ctl").await;
        let user = seed_user(&control).await;
        let account = seed_account(&control, user, None).await;
        let state = app_state(control.clone());

        for bad in ["", "not-a-uuid", "00000000-0000-0000-0000-000000000000"] {
            let (status, body) = put_active(&state, user_ctx(account, user), bad).await;
            assert_eq!(status, StatusCode::OK, "{bad:?} must not error the boot path: {body}");
            assert_eq!(body["accepted"], Value::Bool(false), "{bad:?} => {body}");
        }
        assert_eq!(
            stored_pointer(&control, user, account).await,
            None,
            "a refused report must leave NO row behind — the status code is not the evidence"
        );

        // …and a refusal must not destroy a selection that was already good.
        let good = Uuid::new_v4();
        put_active(&state, user_ctx(account, user), &good.to_string()).await;
        for bad in ["", "not-a-uuid", "00000000-0000-0000-0000-000000000000"] {
            put_active(&state, user_ctx(account, user), bad).await;
        }
        assert_eq!(
            stored_pointer(&control, user, account).await,
            Some(good),
            "a later malformed report must not clobber the stored selection"
        );
    }

    // ── 4. Reporting the same ocean on every boot is a genuine no-op ─────────

    #[tokio::test]
    async fn re_reporting_the_same_ocean_writes_nothing() {
        let Some(base) = base_url() else {
            eprintln!("skipping re_reporting_the_same_ocean_writes_nothing: set ORGT_TEST_BASE_URL");
            return;
        };
        let control = control_pool(&base, "orgt_me_active5_ctl").await;
        let user = seed_user(&control).await;
        let account = seed_account(&control, user, None).await;
        let state = app_state(control.clone());

        let ocean = Uuid::new_v4();
        put_active(&state, user_ctx(account, user), &ocean.to_string()).await;
        let first = stored_updated_at(&control, user, account).await;

        // The client reports on every boot; this is what keeps that cheap.
        put_active(&state, user_ctx(account, user), &ocean.to_string()).await;
        assert_eq!(
            stored_updated_at(&control, user, account).await,
            first,
            "an unchanged report must not rewrite the row (the IS DISTINCT FROM no-op)"
        );

        // A genuine change still lands — otherwise the assertion above would
        // also pass for a handler that had stopped writing entirely.
        let moved = Uuid::new_v4();
        put_active(&state, user_ctx(account, user), &moved.to_string()).await;
        assert_eq!(stored_pointer(&control, user, account).await, Some(moved));
        assert!(
            stored_updated_at(&control, user, account).await > first,
            "a real switch must bump this table's own updated_at"
        );
    }

    // ── 5. An unreachable ocean is reported, not an error ───────────────────

    #[tokio::test]
    async fn a_pointer_to_a_nonexistent_ocean_is_stored_and_read_without_erroring() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_pointer_to_a_nonexistent_ocean_is_stored_and_read_without_erroring: set ORGT_TEST_BASE_URL");
            return;
        };
        let control = control_pool(&base, "orgt_me_active6_ctl").await;
        let user = seed_user(&control).await;
        let account = seed_account(&control, user, None).await;
        let state = app_state(control.clone());

        // No `graphs` row exists for this id, and none ever will. Migration
        // 0201 carries NO foreign key on `graph_id` on purpose: for an org
        // account `graphs` lives in that org's own tenant database, where a
        // cross-database FK is not expressible. If someone "tidies up" by
        // adding one, this INSERT starts failing and this test goes red.
        let ghost = Uuid::new_v4();
        let (status, body) = put_active(&state, user_ctx(account, user), &ghost.to_string()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["accepted"], Value::Bool(true), "{body}");

        // `/v1/me` still answers. The client resolves reachability by
        // intersecting this id with `GET /v1/graphs`; a miss falls back to the
        // first reachable ocean rather than failing boot.
        let me = get_me_json(&state, user_ctx(account, user)).await;
        assert_eq!(me["active_graph_id"], Value::String(ghost.to_string()), "{me}");
        assert_eq!(me["user_id"], Value::String(user.to_string()), "{me}");
    }

    // ── 6. `is_personal_org` distinguishes all three account shapes ─────────
    //
    // The one signal `GET /v1/me` publishes so the web client can stop
    // treating a personal org — a Clerk organization that stands for one
    // person's own ocean (control migration 0208) — as an organization to
    // manage. Covers plain personal, personal org, and real org, and pins
    // the real-org path unchanged: that is the regression this feature must
    // never cause.

    async fn seed_user_with_clerk_id(control: &PgPool) -> (Uuid, String) {
        let clerk_user_id = format!("user_{}", Uuid::new_v4());
        let id: Uuid =
            sqlx::query_scalar("INSERT INTO users(email, clerk_user_id) VALUES ($1, $2) RETURNING id")
                .bind(format!("{}@me-pg.test", Uuid::new_v4()))
                .bind(&clerk_user_id)
                .fetch_one(control)
                .await
                .expect("insert user");
        (id, clerk_user_id)
    }

    async fn seed_personal_org(control: &PgPool, clerk_org_id: &str, clerk_user_id: &str) {
        sqlx::query("INSERT INTO personal_orgs(clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(clerk_org_id)
            .bind(clerk_user_id)
            .execute(control)
            .await
            .expect("insert personal_orgs row");
    }

    #[tokio::test]
    async fn is_personal_org_distinguishes_all_three_account_shapes() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping is_personal_org_distinguishes_all_three_account_shapes: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control = control_pool(&base, "orgt_me_isporg_ctl").await;
        let state = app_state(control.clone());

        // Shape 1 — plain personal account (`clerk_org_id IS NULL`).
        let (solo, _solo_clerk_id) = seed_user_with_clerk_id(&control).await;
        let solo_account = seed_account(&control, solo, None).await;
        let solo_me = get_me_json(&state, user_ctx(solo_account, solo)).await;
        assert_eq!(
            solo_me["is_personal_org"],
            Value::Bool(false),
            "a plain personal account must not read as a personal org: {solo_me}"
        );

        // Shape 2 — a REAL organization: `clerk_org_id` set, but no row in
        // `personal_orgs`. The regression-risk path: a real org user must see
        // EXACTLY what they see today.
        let (org_owner, _org_owner_clerk_id) = seed_user_with_clerk_id(&control).await;
        let real_org_id = format!("org_real_{}", Uuid::new_v4());
        let org_account = seed_account(&control, org_owner, Some(real_org_id)).await;
        let org_me = get_me_json(&state, user_ctx(org_account, org_owner)).await;
        assert_eq!(
            org_me["is_personal_org"],
            Value::Bool(false),
            "a real organization must not be reported as a personal org: {org_me}"
        );

        // Shape 3 — a personal org: `clerk_org_id` set AND a `personal_orgs`
        // row naming it, keyed to the account owner's OWN clerk_user_id. This
        // is the whole feature.
        let (personal_owner, personal_owner_clerk_id) = seed_user_with_clerk_id(&control).await;
        let personal_org_id = format!("org_personal_{}", Uuid::new_v4());
        seed_personal_org(&control, &personal_org_id, &personal_owner_clerk_id).await;
        let personal_org_account =
            seed_account(&control, personal_owner, Some(personal_org_id)).await;
        let personal_me = get_me_json(&state, user_ctx(personal_org_account, personal_owner)).await;
        assert_eq!(
            personal_me["is_personal_org"],
            Value::Bool(true),
            "a personal org's own account must be reported as a personal org: {personal_me}"
        );

        // Shape 4 — SOMEONE ELSE'S personal org. From this caller's point of
        // view that is an ordinary organization, so the answer must be false
        // even though the org id is in `personal_orgs`. One member per personal
        // org means nothing reaches this state today; it is asserted because
        // "one member by construction" is the assumption whose failure this
        // whole field exists to clean up. Drop the `users` join from
        // `account_is_personal_org` and only this assertion goes red.
        let (stranger, _stranger_clerk_id) = seed_user_with_clerk_id(&control).await;
        let stranger_me = get_me_json(&state, user_ctx(personal_org_account, stranger)).await;
        assert_eq!(
            stranger_me["is_personal_org"],
            Value::Bool(false),
            "another person's personal org is an ordinary org to this caller: {stranger_me}"
        );
    }
}

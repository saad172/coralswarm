//! `GET/PUT /v1/prefs` — per-account preferences (ADR-0007 D11 / gap 4).
//!
//! A per-account settings document (`UserContext.account_id`): for a personal
//! account these are the user's own prefs (LLM/synthesis mode); for an **org**
//! account they are **org-wide defaults**. "Local only" (`egress_policy`) was
//! removed in the settings launch (decision 16): the only accepted
//! `egress_policy` is `Allow cloud`, and migration 0375 enforces it in the
//! database too. Routed through
//! the Postgres pool directly; 503 with no pool.
//!
//! # Three defects fixed together (2026-08-25)
//!
//! `put_prefs` used to write `user.account_id` straight from the request
//! extension with **no role check**, accept **any JSON object** verbatim
//! (`is_object()` was the whole of validation), and **replace the entire
//! document** on every write.
//!
//! 1. **No role check.** On an org account, ANY member could overwrite the
//!    org's prefs — the org-wide defaults. `UserContext.role`
//!    already carries the live `account_members.role` for the (org, user) pair
//!    (`middleware::auth::live_org_membership` — the DB row, never the token's
//!    claim, so a demotion binds on the next call), and the repo already had
//!    the `is_admin(&user.role)` pattern in `routes::org_invites`. This
//!    handler simply never used it. Note the *web* has told members
//!    "`{org}` sets the default. You may pick something stricter for yourself
//!    — never looser" while its picker wrote the org-wide row for everyone.
//! 2. **`is_object()` was the whole of validation**, so an arbitrary
//!    member-supplied blob replaced a structured settings document. Validating
//!    the value closes that; see [`validate_prefs`].
//! 3. **Whole-blob overwrite.** Two clients doing the ordinary
//!    read-modify-write both sent a full document, so the later write silently
//!    dropped the earlier one's change. There is **no ETag/If-None-Match
//!    support anywhere in this backend** (a repo-wide grep returns one hit and
//!    it is *Google's* event etag), so the fix is a merge-on-write, not a
//!    reach for a header that does not exist: `prefs || EXCLUDED.prefs`
//!    top-level JSONB merge, under the row lock `ON CONFLICT` already takes.
//!
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Map, Value};

use crate::middleware::auth::UserContext;
use crate::routes::org_invites::is_admin;
use crate::AppState;

/// Upper bound on the stored settings document. Not a security boundary on its
/// own — the key allowlist below is — but `connectors` is an opaque object, so
/// without a cap a member can still park an arbitrary payload in a
/// control-plane table. Generous: the real document is ~60 bytes.
const MAX_PREFS_BYTES: usize = 16 * 1024;

fn unavailable() -> (StatusCode, Json<Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
}

fn bad_request(msg: String) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })))
}

fn forbidden(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::FORBIDDEN, Json(json!({ "error": msg })))
}

/// Normalize a policy/mode string: trim, lowercase, `_`/`-` as space. Matches
/// the normalization migration 0375's CHECK constraint applies.
fn normalize(s: &str) -> String {
    s.trim().to_ascii_lowercase().replace(['_', '-'], " ")
}

/// Default-deny validation of the settings document.
///
/// **Allowlist, not blocklist**: an unrecognised key is rejected rather than
/// stored, because a key nothing reads is a key nothing enforces, and this
/// table is control-plane state an org member can reach.
///
/// The allowlist is exactly what the product reads today:
///
/// | key | type | accepted values |
/// |---|---|---|
/// | `llm_mode` | string | `Cloud`, `BYOK`, `Local` |
/// | `egress_policy` | string | `Allow cloud` ("Local only" was removed) |
/// | `connectors` | object | any object (legacy; nothing reads it) |
///
/// `egress_policy` accepts only `Allow cloud`. Every other value, including
/// the removed `Local only` and `Local` spellings, is rejected.
///
/// `connectors` is typed but not enumerated: nothing in the backend reads it
/// (only `web/lib/api.ts`'s doc comment names it), so enumerating its interior
/// would be inventing a contract. It is capped by [`MAX_PREFS_BYTES`] instead.
pub(crate) fn validate_prefs(obj: &Map<String, Value>) -> Result<(), String> {
    const LLM_MODES: [&str; 3] = ["cloud", "byok", "local"];
    const EGRESS_POLICIES: [&str; 1] = ["allow cloud"];

    let mut unknown: Vec<&str> = Vec::new();
    for (k, v) in obj {
        match k.as_str() {
            "llm_mode" => {
                let Some(s) = v.as_str() else {
                    return Err("llm_mode must be a string".to_string());
                };
                if !LLM_MODES.contains(&normalize(s).as_str()) {
                    return Err(format!(
                        "llm_mode must be one of Cloud, BYOK, Local (got {s:?})"
                    ));
                }
            }
            "egress_policy" => {
                let Some(s) = v.as_str() else {
                    return Err("egress_policy must be a string".to_string());
                };
                if !EGRESS_POLICIES.contains(&normalize(s).as_str()) {
                    return Err(format!(
                        "egress_policy must be \"Allow cloud\" (got {s:?}); \
                         \"Local only\" is no longer available"
                    ));
                }
            }
            "connectors" => {
                if !v.is_object() {
                    return Err("connectors must be a JSON object".to_string());
                }
            }
            other => unknown.push(other),
        }
    }
    if !unknown.is_empty() {
        unknown.sort_unstable();
        return Err(format!(
            "unknown preference key(s): {} — accepted keys are llm_mode, egress_policy, connectors",
            unknown.join(", ")
        ));
    }
    Ok(())
}

pub async fn get_prefs(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    let Some(pool) = &state.db else { return unavailable() };
    match sqlx::query_as::<_, (String,)>("SELECT prefs::text FROM account_prefs WHERE account_id = $1")
        .bind(user.account_id)
        .fetch_optional(pool)
        .await
    {
        Ok(Some((s,))) => {
            let prefs: Value = serde_json::from_str(&s).unwrap_or_else(|_| json!({}));
            (StatusCode::OK, Json(json!({ "prefs": prefs })))
        }
        Ok(None) => (StatusCode::OK, Json(json!({ "prefs": {} }))),
        Err(e) => {
            tracing::error!(error = %e, "get_prefs DB error");
            unavailable()
        }
    }
}

pub async fn put_prefs(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    // Accept either the bare prefs object or `{ "prefs": {...} }`.
    let prefs = body.get("prefs").cloned().unwrap_or(body);
    let Some(obj) = prefs.as_object() else {
        return bad_request("prefs must be a JSON object".to_string());
    };

    let Some(pool) = &state.db else { return unavailable() };

    // ── Authorization, before validation and before any write ───────────────
    //
    // On an ORG account this document is the org-wide default, so only an
    // admin/owner may write it. A
    // PERSONAL account's owner writing their own prefs is unprivileged and
    // must keep working; personal contexts resolve `role = "owner"`
    // (`middleware::auth::resolve_clerk_context`), but the gate is on the
    // account being an org, not on the role alone, so this can't drift if
    // that default ever changes.
    //
    // FAIL CLOSED on a read error: "we could not determine whether this is an
    // org" must never resolve to "write it anyway". No write happens on this
    // path — the caller sees 503 and the stored document is untouched.
    // `Store::account_is_org` (`accounts.clerk_org_id IS NOT NULL`) through
    // `control_store()` — the accessor whose own doc comment names
    // `account_is_org` as one of its intended control-table reads. Reusing the
    // Store method rather than open-coding the SQL also keeps this file off
    // `scripts/check_scoped_shape_ratchet.sh`'s frozen PgPool set.
    //
    // Deliberately NOT `Store::account_is_real_org` (PR 829 review, finding
    // 5): `account_is_org` reads `true` for a personal org's own relabelled
    // account exactly like a real org's, but that account's only member is
    // always its owner (`role = "owner"`, `is_admin` true) — so the branch
    // below never fires for one either way — and `account_has_multiple_
    // members` (that same review's finding 1) now makes a SECOND member
    // structurally impossible for a personal org's account. There is no
    // shape left in which the two methods would ever disagree here, so
    // switching would change no observable behavior, only add a caller-
    // relative `user.user_id` argument this call site has no other use for.
    let Some(store) = state.control_store() else {
        return unavailable();
    };
    match store.account_is_org(user.account_id).await {
        Ok(true) if !is_admin(&user.role) => {
            tracing::warn!(
                account_id = %user.account_id,
                user_id = %user.user_id,
                role = %user.role,
                "put_prefs refused: non-admin member may not write org-wide prefs"
            );
            return forbidden(
                "only an organization admin may change this organization's preferences",
            );
        }
        Ok(_) => {}
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "put_prefs: account kind lookup failed — refusing the write (fail closed)");
            return unavailable();
        }
    }

    if let Err(msg) = validate_prefs(obj) {
        return bad_request(msg);
    }

    let serialized = prefs.to_string();
    if serialized.len() > MAX_PREFS_BYTES {
        return bad_request(format!(
            "prefs document is {} bytes; the limit is {MAX_PREFS_BYTES}",
            serialized.len()
        ));
    }

    // ── Merge, don't clobber ────────────────────────────────────────────────
    //
    // `account_prefs.prefs || EXCLUDED.prefs` is a TOP-LEVEL JSONB merge with
    // the incoming document winning key-by-key, evaluated under the row lock
    // `ON CONFLICT DO UPDATE` already takes — so two clients that each
    // read-modify-write a different key both land, instead of the later write
    // silently dropping the earlier one's change.
    //
    // Consequences, stated rather than discovered later: a key cannot be
    // DELETED through this endpoint (send its new value instead), and
    // `connectors` merges at the top level only — sending `connectors`
    // replaces that whole sub-object.
    //
    // `RETURNING` so the response is the document that is actually stored,
    // not an echo of the request. Echoing the request after a merge would
    // report a state the database does not hold. Decoded directly as jsonb
    // (`sqlx::types::Json`) — no text round-trip, and no silent `{}` fallback
    // that would 200 while claiming a document the database does not hold; an
    // impossible decode failure surfaces as the loud Err arm instead.
    //
    // The `jsonb_typeof` CASE is a self-healing guard: `||` on a NON-object
    // left operand does not error on modern Postgres — it appends into an
    // array — so a non-object planted out-of-band (there is no
    // `jsonb_typeof(prefs)='object'` CHECK on 0029's column) would otherwise
    // corrupt further on every write, forever, behind 200s. Replacing it with
    // the incoming validated object restores object-ness on the next write.
    match sqlx::query_scalar::<_, sqlx::types::Json<Value>>(
        "INSERT INTO account_prefs (account_id, prefs) VALUES ($1, $2::jsonb) \
         ON CONFLICT (account_id) DO UPDATE \
           SET prefs = CASE WHEN jsonb_typeof(account_prefs.prefs) = 'object' \
                            THEN account_prefs.prefs || EXCLUDED.prefs \
                            ELSE EXCLUDED.prefs END, \
               updated_at = now() \
         RETURNING prefs",
    )
    .bind(user.account_id)
    .bind(&serialized)
    .fetch_one(pool)
    .await
    {
        Ok(sqlx::types::Json(merged)) => (StatusCode::OK, Json(json!({ "prefs": merged }))),
        Err(e) => {
            tracing::error!(error = %e, "put_prefs DB error");
            unavailable()
        }
    }
}

// ── Unit tests (no database) ─────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(s: &str) -> Map<String, Value> {
        serde_json::from_str::<Value>(s)
            .unwrap()
            .as_object()
            .unwrap()
            .clone()
    }

    #[test]
    fn known_keys_and_values_are_accepted() {
        assert!(validate_prefs(&obj(r#"{}"#)).is_ok());
        assert!(validate_prefs(&obj(r#"{"llm_mode":"Cloud","egress_policy":"Allow cloud"}"#)).is_ok());
        assert!(validate_prefs(&obj(r#"{"llm_mode":"BYOK"}"#)).is_ok());
        assert!(validate_prefs(&obj(r#"{"llm_mode":"Local","egress_policy":"allow_cloud"}"#)).is_ok());
        assert!(validate_prefs(&obj(r#"{"egress_policy":"ALLOW-CLOUD"}"#)).is_ok());
        assert!(validate_prefs(&obj(r#"{"connectors":{"slack":{"on":true}}}"#)).is_ok());
    }

    #[test]
    fn unknown_keys_are_rejected_and_named() {
        let err = validate_prefs(&obj(r#"{"billing_enforcement_enabled":false}"#)).unwrap_err();
        assert!(err.contains("billing_enforcement_enabled"), "{err}");
        let err = validate_prefs(&obj(r#"{"a":1,"b":2}"#)).unwrap_err();
        assert!(err.contains('a') && err.contains('b'), "{err}");
    }

    #[test]
    fn local_only_and_every_other_egress_policy_is_rejected() {
        for bad in [
            r#"{"egress_policy":"Local only"}"#,
            r#"{"egress_policy":"local_only"}"#,
            r#"{"egress_policy":"LOCAL-ONLY"}"#,
            r#"{"egress_policy":"Local"}"#,
            r#"{"egress_policy":"localonly"}"#,
            r#"{"egress_policy":"forbid"}"#,
            r#"{"egress_policy":""}"#,
        ] {
            let err = validate_prefs(&obj(bad)).unwrap_err();
            assert!(err.contains("egress_policy"), "{bad} => {err}");
        }
    }

    #[test]
    fn wrong_types_are_rejected() {
        assert!(validate_prefs(&obj(r#"{"llm_mode":1}"#)).is_err());
        assert!(validate_prefs(&obj(r#"{"egress_policy":null}"#)).is_err());
        assert!(validate_prefs(&obj(r#"{"connectors":"slack"}"#)).is_err());
        assert!(validate_prefs(&obj(r#"{"llm_mode":"Enterprise"}"#)).is_err());
    }
}

// ── Postgres tests ───────────────────────────────────────────────────────────
//
// Wired into `.github/workflows/ci.yml`'s `org-invites` Postgres job (same
// DSN, same already-built lib test binary) with the standard
// `test -n "$ORGT_TEST_BASE_URL"` guard AND the zero-test filter guard. Both matter here: without the DSN every test below `return`s and the
// run reports "ok. N passed" in 0.00s, and a renamed filter matches zero tests
// and also exits 0. Neither would have asserted anything.
#[cfg(test)]
mod pg_tests {
    use super::*;
    use axum::extract::State as AxumState;
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
    async fn admin_conn(base: &str) -> PgConnection {
        PgConnection::connect(base).await.expect("connect admin")
    }
    async fn recreate(conn: &mut PgConnection, db: &str) {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
    }
    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }
    async fn drop_db(mut conn: PgConnection, db: &str) {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// `(account_id, user_id)`. `clerk_org_id = Some(..)` makes it an ORG
    /// account — the exact predicate `account_is_org` reads.
    async fn seed_account(control: &PgPool, clerk_org_id: Option<String>) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email, clerk_user_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(format!("{}@prefs-pg.test", Uuid::new_v4()))
        .bind(format!("user_{}", Uuid::new_v4()))
        .fetch_one(control)
        .await
        .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(clerk_org_id)
        .fetch_one(control)
        .await
        .expect("insert account");
        (account_id, user_id)
    }

    async fn set_prefs_row(control: &PgPool, account_id: Uuid, prefs: &str) {
        sqlx::query(
            "INSERT INTO account_prefs(account_id, prefs) VALUES ($1, $2::jsonb) \
             ON CONFLICT (account_id) DO UPDATE SET prefs = EXCLUDED.prefs",
        )
        .bind(account_id)
        .bind(prefs)
        .execute(control)
        .await
        .expect("seed account_prefs");
    }

    /// The stored document, verbatim. Every refusal test reads this back:
    /// **the status code is not the evidence.**
    async fn stored_prefs(control: &PgPool, account_id: Uuid) -> Option<String> {
        sqlx::query_scalar::<_, String>(
            "SELECT prefs::text FROM account_prefs WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_optional(control)
        .await
        .expect("read account_prefs")
    }

    fn user_ctx(account_id: Uuid, user_id: Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: role.to_string(),
            device_id: "prefs-pg-test".to_string(),
        }
    }

    fn app_state(control: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(control, None))
    }

    async fn put(
        state: &Arc<AppState>,
        ctx: UserContext,
        body: Value,
    ) -> (StatusCode, Value) {
        let (status, Json(v)) =
            put_prefs(AxumState(state.clone()), Extension(ctx), Json(body)).await;
        (status, v)
    }

    // ── 1. The load-bearing one ──────────────────────────────────────────────

    #[tokio::test]
    async fn org_member_cannot_change_the_orgs_egress_policy() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_member_cannot_change_the_orgs_egress_policy: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_prefs1_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let (account_id, owner) =
            seed_account(&control, Some(format!("org_prefs1_{}", Uuid::new_v4()))).await;
        // The org-wide defaults.
        set_prefs_row(
            &control,
            account_id,
            r#"{"llm_mode":"Cloud","egress_policy":"Allow cloud"}"#,
        )
        .await;
        let before = stored_prefs(&control, account_id).await.unwrap();

        let state = app_state(control.clone());
        let (status, body) = put(
            &state,
            user_ctx(account_id, owner, "member"),
            json!({"prefs": {"llm_mode": "Local"}}),
        )
        .await;

        assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");

        // READ THE ROW BACK. A 403 that still wrote is the same defect
        // wearing a passing status code.
        let after = stored_prefs(&control, account_id).await.unwrap();
        assert_eq!(before, after, "the org's prefs document must be byte-identical after the refusal");

        control.close().await;
        drop_db(conn, db).await;
    }

    // ── 2. …and the guard must not be a release outage ───────────────────────

    #[tokio::test]
    async fn org_admin_and_personal_owner_can_still_write() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_admin_and_personal_owner_can_still_write: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_prefs2_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;
        let state = app_state(control.clone());

        // (a) org admin
        let (org_acct, admin) =
            seed_account(&control, Some(format!("org_prefs2_{}", Uuid::new_v4()))).await;
        set_prefs_row(&control, org_acct, r#"{"llm_mode":"Cloud"}"#).await;
        let (status, body) = put(
            &state,
            user_ctx(org_acct, admin, "admin"),
            json!({"prefs": {"llm_mode": "BYOK"}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "an org ADMIN must still be able to write: {body}");
        assert_eq!(body["prefs"]["llm_mode"], json!("BYOK"));
        assert!(
            stored_prefs(&control, org_acct).await.unwrap().contains("BYOK"),
            "the admin's write must actually be stored"
        );

        // (b) personal account owner — unprivileged, must keep working
        let (personal_acct, owner) = seed_account(&control, None).await;
        let (status, body) = put(
            &state,
            user_ctx(personal_acct, owner, "owner"),
            json!({"prefs": {"llm_mode": "Local", "egress_policy": "Allow cloud"}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "a personal owner must still be able to write: {body}");

        // (c) a personal account whose role is somehow NOT admin-ish: the gate
        //     is "is this an org account", not "is this role privileged", so
        //     this must still succeed. Pins that the guard cannot widen into
        //     personal accounts by a role-default change elsewhere.
        let (personal2, user2) = seed_account(&control, None).await;
        let (status, _) = put(
            &state,
            user_ctx(personal2, user2, "member"),
            json!({"prefs": {"llm_mode": "Cloud"}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "the gate must key on org-ness, not on the role alone");

        control.close().await;
        drop_db(conn, db).await;
    }

    // ── 3. Validation, with the row read back ────────────────────────────────

    #[tokio::test]
    async fn unknown_and_malformed_keys_are_rejected_and_nothing_is_stored() {
        let Some(base) = base_url() else {
            eprintln!("skipping unknown_and_malformed_keys_are_rejected_and_nothing_is_stored: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_prefs3_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;
        let state = app_state(control.clone());

        let (account_id, owner) = seed_account(&control, None).await;
        set_prefs_row(&control, account_id, r#"{"egress_policy":"Allow cloud"}"#).await;
        let before = stored_prefs(&control, account_id).await.unwrap();

        for body in [
            json!({"prefs": {"evil": {"deeply": ["nested", "blob"]}}}),
            json!({"prefs": {"billing_enforcement_enabled": false}}),
            json!({"prefs": {"egress_policy": "localonly"}}),
            json!({"prefs": {"egress_policy": "Local only"}}),
            json!({"prefs": {"egress_policy": "local_only"}}),
            json!({"prefs": {"llm_mode": 7}}),
            json!({"prefs": {"connectors": "slack"}}),
        ] {
            let (status, resp) = put(&state, user_ctx(account_id, owner, "owner"), body.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "must reject {body}: {resp}");
            assert_eq!(
                stored_prefs(&control, account_id).await.unwrap(),
                before,
                "nothing may be stored for the rejected body {body}"
            );
        }

        control.close().await;
        drop_db(conn, db).await;
    }

    // ── 4. Concurrency: both writers must survive ────────────────────────────

    #[tokio::test]
    async fn two_concurrent_writers_do_not_clobber_each_other() {
        let Some(base) = base_url() else {
            eprintln!("skipping two_concurrent_writers_do_not_clobber_each_other: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_prefs4_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;
        let state = app_state(control.clone());

        let (account_id, owner) = seed_account(&control, None).await;
        set_prefs_row(
            &control,
            account_id,
            r#"{"llm_mode":"Cloud","egress_policy":"Allow cloud"}"#,
        )
        .await;

        // Two clients that each did the ordinary read-modify-write and are
        // changing DIFFERENT keys. "Both returned 200" is what happens today
        // and is not the assertion — the stored document is.
        let (a, b) = tokio::join!(
            put(
                &state,
                user_ctx(account_id, owner, "owner"),
                json!({"prefs": {"llm_mode": "Local"}})
            ),
            put(
                &state,
                user_ctx(account_id, owner, "owner"),
                json!({"prefs": {"connectors": {"slack": {"on": true}}}})
            ),
        );
        assert_eq!(a.0, StatusCode::OK, "{:?}", a.1);
        assert_eq!(b.0, StatusCode::OK, "{:?}", b.1);

        let stored: Value =
            serde_json::from_str(&stored_prefs(&control, account_id).await.unwrap()).unwrap();
        assert_eq!(
            stored["llm_mode"],
            json!("Local"),
            "writer A's change was clobbered; stored = {stored}"
        );
        assert_eq!(
            stored["connectors"],
            json!({"slack": {"on": true}}),
            "writer B's change was clobbered; stored = {stored}"
        );

        control.close().await;
        drop_db(conn, db).await;
    }

    // ── 5. Fail closed when the account kind cannot be read ──────────────────

    #[tokio::test]
    async fn an_unknown_account_is_treated_as_not_an_org_but_still_validated() {
        let Some(base) = base_url() else {
            eprintln!("skipping an_unknown_account_is_treated_as_not_an_org_but_still_validated: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_prefs5_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;
        let state = app_state(control.clone());

        // No `accounts` row at all: the FK on `account_prefs.account_id` makes
        // the write fail, and it must surface as 503 rather than a 200 that
        // stored nothing.
        let ghost = Uuid::new_v4();
        let (status, _) = put(
            &state,
            user_ctx(ghost, Uuid::new_v4(), "owner"),
            json!({"prefs": {"llm_mode": "Cloud"}}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(stored_prefs(&control, ghost).await.is_none());

        control.close().await;
        drop_db(conn, db).await;
    }

    // ── 6. Migration 0375: the guard UPDATE and the CHECK ────────────────────

    #[tokio::test]
    async fn migration_0375_moves_local_only_accounts_and_blocks_its_return() {
        let Some(base) = base_url() else {
            eprintln!("skipping migration_0375_moves_local_only_accounts_and_blocks_its_return: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_prefs6_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        // The constraint is live: no write path may store a forbidding value,
        // in any spelling the removed enforcer used to honour.
        let (blocked, _) = seed_account(&control, None).await;
        for bad in ["Local only", "local_only", " LOCAL-ONLY ", "local"] {
            let err = sqlx::query("INSERT INTO account_prefs(account_id, prefs) VALUES ($1, jsonb_build_object('egress_policy', $2::text))")
                .bind(blocked)
                .bind(bad)
                .execute(&control)
                .await
                .expect_err("the CHECK must refuse a forbidding egress_policy");
            assert!(err.to_string().contains("account_prefs_egress_policy_not_local_only"), "{bad}: {err}");
        }
        assert!(stored_prefs(&control, blocked).await.is_none());

        // Simulate accounts that predate the constraint, then re-run the
        // migration file exactly as shipped.
        sqlx::query("ALTER TABLE account_prefs DROP CONSTRAINT account_prefs_egress_policy_not_local_only")
            .execute(&control)
            .await
            .unwrap();
        let (a1, _) = seed_account(&control, None).await;
        let (a2, _) = seed_account(&control, None).await;
        let (a3, _) = seed_account(&control, None).await;
        let (a4, _) = seed_account(&control, None).await;
        set_prefs_row(&control, a1, r#"{"llm_mode":"Local","egress_policy":"Local only","connectors":{"x":1}}"#).await;
        set_prefs_row(&control, a2, r#"{"egress_policy":" local_only "}"#).await;
        set_prefs_row(&control, a3, r#"{"egress_policy":"Allow cloud","llm_mode":"BYOK"}"#).await;
        set_prefs_row(&control, a4, r#"{"llm_mode":"Cloud"}"#).await;

        let mut tx = control.begin().await.unwrap();
        sqlx::raw_sql(include_str!("../../migrations_control/0375_account_prefs_no_local_only.sql"))
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let doc = |s: String| serde_json::from_str::<Value>(&s).unwrap();
        let p1 = doc(stored_prefs(&control, a1).await.unwrap());
        assert_eq!(p1["egress_policy"], json!("Allow cloud"));
        assert_eq!(p1["llm_mode"], json!("Local"), "other keys must survive");
        assert_eq!(p1["connectors"], json!({"x": 1}));
        assert_eq!(doc(stored_prefs(&control, a2).await.unwrap())["egress_policy"], json!("Allow cloud"));
        assert_eq!(doc(stored_prefs(&control, a3).await.unwrap()), json!({"egress_policy":"Allow cloud","llm_mode":"BYOK"}));
        assert_eq!(doc(stored_prefs(&control, a4).await.unwrap()), json!({"llm_mode":"Cloud"}), "rows without the key are untouched");
        assert!(sqlx::query("UPDATE account_prefs SET prefs = prefs || '{\"egress_policy\":\"Local only\"}'::jsonb WHERE account_id=$1")
            .bind(a4)
            .execute(&control)
            .await
            .is_err(), "the re-added CHECK must refuse the value again");

        control.close().await;
        drop_db(conn, db).await;
    }
}

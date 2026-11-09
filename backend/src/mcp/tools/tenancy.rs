//! The 2 tenant-SELECTION MCP tools — `list_tenants` / `select_tenant`
//! (`docs/plans/claim-based-tenancy.md` §Phase 3, PR 3.2).
//!
//! Deliberately its OWN file, not folded into `tools/read.rs`/`tools/write.rs`:
//! those two modules are about CONTENT (reefs/atoms/oceans) on whichever
//! plane the request is already bound to; these two tools are about CHOOSING
//! that plane in the first place, operate purely against the CONTROL pool
//! (`self.state.db`), and never touch `super::content_tenant`, `ScopedTx`, or
//! any of the consumer-plane RLS scoping seam the other two files share.
//!
//! Both tools require only READ scope (neither is in [`super::WRITE_TOOLS`]):
//! selecting which tenant to work in is orthogonal to the content-write
//! scope (`auth::effective_scopes`) — a
//! read-only connection must still be able to choose which org's content it
//! reads. `select_tenant` mutates a CONTROL-plane row despite this (it is not
//! `readOnlyHint: true`); see `server.rs`'s `tools/list` snapshot test for the
//! one documented exception this creates to "every non-write tool is
//! read-only".
//!
//! Neither tool is gated on the OTHER's having been called (decision 5's
//! "pre-selection `list_tenants` should be the only callable tool" catalogue
//! gate is explicitly PR 3.3's job, not this one's — see the plan). Both are
//! always in the catalogue, for every scope-eligible caller, regardless of
//! whether a selection already exists.
//!
//! # The one real call site `list_tenants`/`select_tenant` feed
//!
//! `select_tenant` re-verifies live membership (mirroring
//! `middleware::auth::live_org_membership` via
//! `tenant_selection::live_membership_for_selection` — never a hand-rolled
//! copy) BEFORE persisting, then writes `mcp_tenant_selection`
//! (`tenant_selection::store_selection`/`clear_selection`). It never itself
//! decides what a subsequent request may do with that row: the ONLY place
//! that resolves a stored selection into an actual tenancy binding is
//! `mcp::auth::resolve_personal_tenancy`, which re-runs the live check again,
//! fresh, on every call via `tenant_selection::resolve_request_tenancy`. A
//! selection surviving from `select_tenant`'s own check to the NEXT request
//! is never assumed — see `tenant_selection`'s module doc for why.
//!
//! # These tools are no longer the ONLY chooser (#825)
//!
//! Clerk's consent screen renders an organization picker again, so a token
//! can arrive already naming a tenant. A stored selection still OUTRANKS
//! that claim, which is what keeps `select_tenant` meaningful: it is how a
//! caller switches tenant without re-consenting. The one thing it can no
//! longer do is take a consent-bound connection back to the personal ocean —
//! clearing the row just lets the token's own claim decide again — so that
//! branch refuses rather than reporting a success it did not deliver. See
//! `select_tenant`'s empty-`org_id` arm.

use futures::stream::{self, StreamExt};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{tool, tool_router, ErrorData as McpError, RoleServer};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::clerk_admin::ClerkAdminClient;

use super::super::server::{model_only_meta, CoralSwarmMcp};
use super::super::tenant_selection::{self, LiveMembership};
use super::{auth, tool_error, tool_success};

/// Cap on `select_tenant`'s `org_id` input. Generous for any real Clerk
/// organization id (`org_` + a short alphanumeric suffix) while bounding an
/// adversarial caller who sends an oversized string into a `TEXT` column and
/// a live outbound Clerk lookup path (`list_tenants` looks up org NAMES, not
/// arbitrary caller input, so this cap is `select_tenant`-only).
const SELECT_TENANT_ORG_ID_CAP: usize = 128;

/// Row cap on `list_tenants`' `account_members` query (CodeRabbit review,
/// PR #806). Not primarily a resource-exhaustion guard — the query itself is
/// one indexed join, trivial even at a few hundred rows — but a SILENT
/// truncation is its own failure class this project has been bitten by
/// before (absence rendering as success): a user who genuinely belongs to
/// more than this many orgs would otherwise see an incomplete list with no
/// signal that anything was cut. The response body's `"truncated": true`
/// field is set exactly when this cap was hit, so that case is visible
/// instead of silent. No real user is expected to approach this in
/// practice; it exists so the failure mode is "visibly capped", not
/// "unbounded".
const LIST_TENANTS_ORG_CAP: i64 = 200;

/// Concurrency ceiling on the per-org live Clerk display-name lookups
/// `list_tenants` fans out (CodeRabbit review, PR #806): each lookup is an
/// independent outbound HTTPS call through the shared, process-wide
/// `ClerkAdminClient` HTTP client (`clerk_admin::clerk_http_client`'s 5s
/// connect / 10s request timeouts bound how long ONE call may take, not how
/// MANY may run at once) — an unbounded fan-out lets a caller with many
/// memberships open one concurrent outbound request per membership row.
/// `8` is a conservative, arbitrary-but-documented ceiling: comfortably
/// enough to keep this fast even at the `LIST_TENANTS_ORG_CAP` row cap,
/// while never opening more than a handful of connections against Clerk's
/// API at once from a single call.
const CLERK_ORG_NAME_LOOKUP_CONCURRENCY: usize = 8;

/// Resolve a display name for every `org_id`, at most
/// [`CLERK_ORG_NAME_LOOKUP_CONCURRENCY`] outbound Clerk calls in flight at
/// once (`futures::stream::Buffered` — polls up to that many futures
/// concurrently but, unlike `buffer_unordered`, yields results in the SAME
/// order the input futures were given, so `result[i]` is always
/// `org_ids[i]`'s name — the association `list_tenants` needs to zip back
/// against its own `(org_id, role)` rows). Never a `Result`: each element is
/// `org_display_name`'s own `Option<String>`, which already swallows a
/// per-org Clerk failure into `None` — so nothing here can short-circuit the
/// batch on one org's failure the way a `try_collect` over a `Result` stream
/// could.
async fn fetch_org_names_bounded(clerk: &ClerkAdminClient, org_ids: &[String]) -> Vec<Option<String>> {
    // Collected into a `Vec` of futures BEFORE building the stream —
    // `stream::iter(iter.map(|id| async_fn(id)))` fails to compile here
    // ("implementation of `Iterator`/`FnOnce` is not general enough"): a
    // known rustc HRTB-inference rough edge when a lazy `Map` iterator over
    // an `async fn` call is fed straight into `Buffered`'s generic bounds.
    // Eagerly collecting resolves each future's type against the slice's
    // one concrete borrow up front, sidestepping it.
    let futures: Vec<_> = org_ids.iter().map(|org_id| org_display_name(clerk, org_id)).collect();
    stream::iter(futures)
        .buffered(CLERK_ORG_NAME_LOOKUP_CONCURRENCY)
        .collect()
        .await
}

/// The rmcp-generated tool names for the two `#[tool]` methods below (the
/// method name — neither carries an explicit `name = "..."` override).
/// Named here, not just inline string literals, because `server.rs`'s Phase 3
/// PR 3.3 catalogue gate (decision 5) needs to name both from OUTSIDE this
/// file, in two independent places (`list_tools`'s filter and `call_tool`'s
/// gate — see `tools::TENANCY_SELECTION_TOOLS`), the same way `WRITE_TOOLS`
/// is shared by both.
pub(crate) const LIST_TENANTS_TOOL_NAME: &str = "list_tenants";
pub(crate) const SELECT_TENANT_TOOL_NAME: &str = "select_tenant";

#[derive(Debug, Deserialize, JsonSchema)]
struct SelectTenantInput {
    /// The `org_id` of the organization to select — one of `list_tenants`'
    /// `organizations[].org_id` values. Omit (or pass an empty string) to
    /// clear the selection and return to your personal ocean.
    #[serde(default)]
    org_id: Option<String>,
}

/// Best-effort org display name, one live Clerk call per org
/// (`routes::org_invites::org_display_name`'s pattern — `accounts` carries no
/// display-name column at all). `None` on ANY failure (network, 404, a
/// deleted-but-not-yet-reconciled org, ...) — never fabricated, and never
/// escalated into failing the whole `list_tenants` call: a name is a
/// courtesy, the `org_id` is always present and always usable.
async fn org_display_name(clerk: &ClerkAdminClient, org_id: &str) -> Option<String> {
    match clerk.get_organization(org_id).await {
        Ok(info) => Some(info.name),
        Err(e) => {
            tracing::warn!(
                error = %e,
                org_id = %org_id,
                "list_tenants: Clerk org display-name lookup failed"
            );
            None
        }
    }
}

#[tool_router(router = tenancy_tool_router, vis = "pub(crate)")]
impl CoralSwarmMcp {
    /// `list_tenants` — "List organizations".
    #[tool(
        title = "List organizations",
        description = "List the organizations you (the authenticated caller) currently belong \
            to, so you can find out which one to work in. DO NOT PICK ONE YOURSELF. Present the \
            returned list to the user — by name, falling back to the raw org_id when name is \
            null (a display name could not be fetched right now; see name_lookup_unavailable / \
            name_lookup_failed_for) — and wait for the user to choose, then call select_tenant \
            with exactly the org_id they picked. If organizations is empty, there is nothing to \
            choose from: keep working on your personal ocean, no prompt needed. If you already \
            have a selection from an earlier call in this session, you do not need to call this \
            again unless the user asks to switch.",
        annotations(read_only_hint = true, destructive_hint = false, open_world_hint = false),
        meta = model_only_meta()
    )]
    async fn list_tenants(&self, ctx: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        let a = auth(&ctx)?;
        let Some(pool) = self.state.db.clone() else {
            return Ok(tool_error("service_unavailable", "no database configured"));
        };

        // `account_members` joined to `accounts` for this user's CURRENT,
        // non-revoked org memberships — the same wholesale-revocation guard
        // `middleware::auth::live_org_membership` applies, read the other
        // direction (user -> every org, instead of one named org -> this
        // user). Capped at `LIST_TENANTS_ORG_CAP` (see that const's doc for
        // why: not a resource guard, a silent-truncation guard) — `truncated`
        // below is set when the cap was actually hit.
        //
        // `LIMIT cap + 1`, not `LIMIT cap` (same "fetch one extra row"
        // convention `routes::sessions::list_sessions`/`journal_hour_items`
        // use for their own `has_more` flags): a user with EXACTLY
        // `LIST_TENANTS_ORG_CAP` memberships, no more, would otherwise be
        // reported as truncated when nothing was actually cut.
        //
        // The `personal_orgs` exclusion is the SAME clause, character for
        // character, that `tenant_selection::has_any_org_membership` applies
        // — read that function's doc for the full reasoning. It belongs here
        // for a sharper reason than symmetry: a personal org has no
        // `tenant_databases` routing row by design, so OFFERING one as a
        // selectable organization is offering a choice that bricks the
        // connection. `select_tenant` accepts it (the adopted `accounts` row
        // makes `live_org_membership` say yes), the row is stored, and every
        // subsequent request 403s at `gate::decide` from inside `mcp_auth` —
        // before tool dispatch, so `select_tenant` can no longer be reached
        // to undo it. This clause is the first of three layers against that;
        // see `tenant_selection::StoredSelection::OwnPersonalOrg`.
        //
        // `u.id = am.user_id`, never the org id alone: ANOTHER user's
        // personal org is an ordinary organization from this caller's point
        // of view — being invited into one is a real membership with a real
        // database behind it — and must keep appearing in this list.
        let mut rows: Vec<(String, String)> = match sqlx::query_as(
            "SELECT a.clerk_org_id, am.role \
             FROM account_members am \
             JOIN accounts a ON a.id = am.account_id \
             WHERE am.user_id = $1 \
               AND a.clerk_org_id IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = a.clerk_org_id) \
               AND COALESCE(org_deletion_state_for_org(a.clerk_org_id), '') \
                   NOT IN ('requested', 'grace', 'awaiting_approval', 'destroying') \
               AND NOT EXISTS ( \
                   SELECT 1 FROM personal_orgs p \
                   JOIN users u ON u.clerk_user_id = p.clerk_user_id \
                   WHERE p.clerk_org_id = a.clerk_org_id AND u.id = am.user_id \
               ) \
             ORDER BY a.clerk_org_id \
             LIMIT $2",
        )
        .bind(a.user.user_id)
        .bind(LIST_TENANTS_ORG_CAP + 1)
        .fetch_all(&pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, user_id = %a.user.user_id, "list_tenants: account_members lookup failed");
                return Ok(tool_error("service_unavailable", "internal error"));
            }
        };
        let truncated = rows.len() as i64 > LIST_TENANTS_ORG_CAP;
        if truncated {
            rows.truncate(LIST_TENANTS_ORG_CAP as usize);
        }

        // Live Clerk display-name lookup, bounded concurrency
        // (`fetch_org_names_bounded`/`CLERK_ORG_NAME_LOOKUP_CONCURRENCY`) —
        // `accounts` has no display-name column at all (see this file's
        // module doc). A globally-unavailable Clerk admin client (no
        // `CLERK_SECRET_KEY`) or a per-org failure both degrade to a NULL
        // name for the affected org(s), surfaced via the two fields below
        // rather than silently swallowed or fabricated — never fails the
        // whole call.
        let clerk = ClerkAdminClient::from_env();
        let mut name_lookup_unavailable = false;
        let mut name_lookup_failed_for: Vec<String> = Vec::new();
        let organizations: Vec<Value> = match &clerk {
            None => {
                name_lookup_unavailable = true;
                rows.iter()
                    .map(|(org_id, role)| json!({"org_id": org_id, "name": Value::Null, "role": role}))
                    .collect()
            }
            Some(client) => {
                let org_ids: Vec<String> = rows.iter().map(|(org_id, _)| org_id.clone()).collect();
                let names = fetch_org_names_bounded(client, &org_ids).await;
                rows.iter()
                    .zip(names)
                    .map(|((org_id, role), name)| {
                        if name.is_none() {
                            name_lookup_failed_for.push(org_id.clone());
                        }
                        json!({"org_id": org_id, "name": name, "role": role})
                    })
                    .collect()
            }
        };

        let mut payload = json!({
            "organizations": organizations,
            "personal_available": true,
        });
        if truncated {
            payload["truncated"] = json!(true);
        }
        if name_lookup_unavailable {
            payload["name_lookup_unavailable"] = json!(true);
        }
        if !name_lookup_failed_for.is_empty() {
            payload["name_lookup_failed_for"] = json!(name_lookup_failed_for);
        }

        Ok(tool_success(payload, a.mcp_client_name.as_deref()))
    }

    /// `select_tenant` — "Select organization".
    #[tool(
        title = "Select organization",
        description = "Switch this connection to a specific organization's ocean, or back to \
            your personal ocean. Call this ONLY after the user has picked from list_tenants' \
            output — never choose an organization on your own. Pass org_id to select that \
            organization; it is re-verified as a CURRENT live membership before anything is \
            stored, so a stale, mistyped, or already-revoked org_id is refused rather than \
            silently accepted. Omit org_id (or pass an empty string) to clear the selection and \
            return to your personal ocean — unless this connection was signed in against a \
            specific organization, in which case clearing is refused and reconnecting is the \
            only way back to personal. The selection is remembered for this (user, MCP \
            client) pair and re-checked for live membership on every subsequent call, so being \
            removed from the organization takes effect on your very next call, not just at \
            selection time.",
        annotations(read_only_hint = false, destructive_hint = false, idempotent_hint = true, open_world_hint = false),
        meta = model_only_meta()
    )]
    async fn select_tenant(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(input): Parameters<SelectTenantInput>,
    ) -> Result<CallToolResult, McpError> {
        let a = auth(&ctx)?;
        let Some(pool) = self.state.db.clone() else {
            return Ok(tool_error("service_unavailable", "no database configured"));
        };

        // Selection is keyed `(user_id, client_id)` (migration
        // `0203_mcp_tenant_selection`'s header) — a token with neither a
        // `client_id` nor an `azp` claim has no stable identifier to key a
        // row by at all, so there is nothing to persist against.
        let Some(client_id) = a.client_id.as_deref() else {
            return Ok(tool_error(
                "invalid_params",
                "this connection has no client_id/azp claim to key a tenant selection by",
            ));
        };

        let org_id = input.org_id.as_deref().unwrap_or("").trim();
        if org_id.is_empty() {
            // "Clear the selection" means "fall back to whatever this
            // connection would resolve to with no stored row" — which, since
            // #825, is NOT unconditionally the personal ocean: a token whose
            // consent screen named an organization resolves to that
            // organization once the row is gone
            // (`tenant_selection::resolve_selected_or_consented_tenant`'s
            // `(None, ConsentOrg::Org)` arm).
            //
            // So for a consent-bound connection this branch cannot do what
            // its own description promises. Refuse and say why, rather than
            // delete the row and hand back `"status": "cleared"` for a call
            // that changed nothing observable — a success message for an
            // unperformed action is the exact failure class (absence
            // rendering as success) this codebase keeps getting bitten by.
            // The claim lives in the TOKEN; only re-consenting can change it.
            //
            // Switching BETWEEN organizations still works normally for these
            // callers — a stored selection outranks the claim — so this
            // refusal is narrow: it is only about getting back to personal.
            if let Some(consent_org) = a.consent_org.as_deref() {
                return Ok(tool_error(
                    "consent_bound_to_organization",
                    format!(
                        "this connection was authorized for organization {consent_org} on the \
                         sign-in consent screen, so clearing the selection cannot return you to \
                         your personal ocean — the connection's own token names that \
                         organization. To work on your personal ocean, reconnect this MCP \
                         connector and pick your personal row on the consent screen. To work in \
                         a DIFFERENT organization, call select_tenant with that org_id."
                    ),
                ));
            }
            return match tenant_selection::clear_selection(&pool, a.user.user_id, client_id).await {
                Ok(()) => Ok(tool_success(
                    json!({"selected": Value::Null, "status": "cleared"}),
                    a.mcp_client_name.as_deref(),
                )),
                Err(e) => {
                    tracing::error!(error = %e, user_id = %a.user.user_id, "select_tenant: clear failed");
                    Ok(tool_error("service_unavailable", "internal error"))
                }
            };
        }

        if org_id.chars().count() > SELECT_TENANT_ORG_ID_CAP {
            return Ok(tool_error(
                "invalid_params",
                format!("org_id must be at most {SELECT_TENANT_ORG_ID_CAP} chars"),
            ));
        }

        // Refuse the caller's OWN personal org BEFORE the membership check,
        // and before anything is persisted.
        //
        // Defence in depth, and the layer that does not depend on how the id
        // got here. `list_tenants` above no longer offers one, but that only
        // fixes the path we know about: `select_tenant` takes an arbitrary
        // `org_id` string from the caller, and an agent that remembered an id
        // from an older session, or a user reading it off Clerk's own consent
        // screen, reaches this line without `list_tenants` ever running.
        //
        // The membership check below CANNOT catch this. A personal org has a
        // real `accounts` row and a real `account_members` row (Clerk's
        // `organization.created` webhook writes both, and
        // `adopt_personal_org_account` relies on them), so
        // `live_org_membership` answers `CurrentMember` — truthfully. What it
        // has no row for is `tenant_databases`, by design, and that absence
        // is only discovered later, at `gate::decide`, on the NEXT request,
        // as a 403 raised before tool dispatch — at which point this tool is
        // unreachable and the selection cannot be undone. So the refusal has
        // to be stated here, explicitly, rather than emerging from a check
        // that is asking a different question.
        //
        // The message names the reason. A bare "not a member" would be both
        // false and useless: the caller IS a member, and the thing they need
        // to know is that their personal ocean is already where they are.
        match tenant_selection::is_own_personal_org(&pool, org_id, a.user.user_id).await {
            Ok(true) => {
                return Ok(tool_error(
                    "invalid_params",
                    "that organization stands for your own personal ocean, which is already \
                     where you are working — there is nothing to select. Selecting it would \
                     bind a tenant database that deliberately does not exist. Call \
                     select_tenant with no org_id to work on your personal ocean.",
                ));
            }
            Ok(false) => {}
            Err(e) => {
                tracing::error!(
                    error = %e,
                    user_id = %a.user.user_id,
                    org_id = %org_id,
                    "select_tenant: personal-org classification failed"
                );
                return Ok(tool_error("service_unavailable", "internal error"));
            }
        }

        // Live re-check BEFORE persisting — mirrors
        // `middleware::auth::live_org_membership` exactly (reused, not
        // reimplemented — see `tenant_selection::live_membership_for_selection`).
        // Never trust `list_tenants`' earlier snapshot: membership could have
        // changed between that call and this one.
        let membership = match tenant_selection::live_membership_for_selection(&pool, org_id, a.user.user_id).await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    user_id = %a.user.user_id,
                    org_id = %org_id,
                    "select_tenant: live membership check failed"
                );
                return Ok(tool_error("service_unavailable", "internal error"));
            }
        };

        if !matches!(membership, LiveMembership::CurrentMember) {
            return Ok(tool_error(
                "invalid_params",
                "you are not currently a member of that organization",
            ));
        }

        if let Err(e) = tenant_selection::store_selection(&pool, a.user.user_id, client_id, org_id).await {
            tracing::error!(
                error = %e,
                user_id = %a.user.user_id,
                org_id = %org_id,
                "select_tenant: store failed"
            );
            return Ok(tool_error("service_unavailable", "internal error"));
        }

        Ok(tool_success(
            json!({"selected": org_id, "status": "selected"}),
            a.mcp_client_name.as_deref(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn select_tenant_org_id_cap_is_generous_for_a_real_clerk_org_id() {
        // Real Clerk org ids look like `org_2abCDefGhijKLmnOPqrstuv` — comfortably
        // under the cap.
        assert!("org_2abCDefGhijKLmnOPqrstuv".len() < SELECT_TENANT_ORG_ID_CAP);
    }

    // ── fetch_org_names_bounded: the concurrency ceiling is real ──────────────
    //
    // A local mock `GET /v1/organizations/:org_id` (pattern:
    // `routes::org_invites`'s own `spawn_clerk_mock` — a plain axum server on
    // an ephemeral loopback port, pointed at via `ClerkAdminClient::
    // with_base_url` directly, no env mutation needed since the function under
    // test takes an already-constructed client) that tracks how many requests
    // are in flight AT ONCE, via an atomic increment/sleep/decrement around a
    // deliberate per-request delay — long enough that concurrent callers
    // provably overlap, short enough the test stays fast.

    async fn spawn_concurrency_tracking_clerk_mock(
        delay: Duration,
    ) -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (in_flight2, peak2) = (in_flight.clone(), peak.clone());

        let app = axum::Router::new().route(
            "/v1/organizations/{org_id}",
            axum::routing::get(move |axum::extract::Path(org_id): axum::extract::Path<String>| {
                let (in_flight, peak) = (in_flight2.clone(), peak2.clone());
                async move {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(delay).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    axum::Json(serde_json::json!({"id": org_id, "name": format!("Org {org_id}")}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind clerk concurrency mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), in_flight, peak)
    }

    /// The ceiling this test pins, INDEPENDENTLY of
    /// `CLERK_ORG_NAME_LOOKUP_CONCURRENCY` itself — asserting
    /// `observed_peak <= CLERK_ORG_NAME_LOOKUP_CONCURRENCY` would check the
    /// production const against itself: raise the const (or delete the
    /// `.buffered(..)` call and hardcode a huge one) and that assertion moves
    /// right along with the bug, staying green. A literal here is what makes
    /// a regression in the const's own VALUE (not just its presence)
    /// detectable — same "independent count literal, not `all_*().len()`"
    /// discipline as PR #804's `SELECTION_COUNT`/`MEMBERSHIP_COUNT`. If this
    /// ever needs to change, it must change by a human editing BOTH this and
    /// the production const, on purpose, in the same diff.
    const EXPECTED_CONCURRENCY_CEILING: usize = 8;

    /// **The property under test**: bounded to
    /// [`EXPECTED_CONCURRENCY_CEILING`], never higher, and — so this isn't
    /// vacuously true because everything happened to run serially — genuinely
    /// concurrent (peak > 1). 20 org ids, each held open by the mock for
    /// 40ms: fully serial would take ~800ms and peak at 1; unbounded
    /// (`join_all`) would peak at 20; bounded at 8 should peak at exactly 8
    /// and finish in a small handful of 40ms waves.
    #[tokio::test(flavor = "current_thread")]
    async fn org_name_lookup_concurrency_is_bounded_and_genuinely_concurrent() {
        let (base, _in_flight, peak) =
            spawn_concurrency_tracking_clerk_mock(Duration::from_millis(40)).await;
        let clerk = ClerkAdminClient::new(reqwest::Client::new(), "test_secret").with_base_url(base);

        let org_ids: Vec<String> = (0..20).map(|i| format!("org_{i}")).collect();
        let names = fetch_org_names_bounded(&clerk, &org_ids).await;

        // Order/association survives the bounded fan-out: `names[i]` is
        // always `org_ids[i]`'s own name, never shuffled by completion order.
        assert_eq!(names.len(), org_ids.len());
        for (i, name) in names.iter().enumerate() {
            assert_eq!(name.as_deref(), Some(format!("Org org_{i}").as_str()), "index {i} out of order");
        }

        let observed_peak = peak.load(Ordering::SeqCst);
        assert!(
            observed_peak <= EXPECTED_CONCURRENCY_CEILING,
            "peak concurrent Clerk requests ({observed_peak}) exceeded the independently-pinned \
             {EXPECTED_CONCURRENCY_CEILING} ceiling"
        );
        assert!(
            observed_peak > 1,
            "peak concurrency was {observed_peak} — this run proves nothing about bounding if requests \
             never actually overlapped in the first place"
        );
    }
}

//! Clerk Backend API admin client — org-invite lifecycle (org-invites train,
//! PR 1). See `.scratch/org-invites/plan.md` for the settled design.
//!
//! Distinct from [`crate::clerk`] (session-JWT verification) and its two
//! best-effort, `Option`-returning lookups (`fetch_primary_email`,
//! `fetch_display_name`). This module is a small typed REST client for the
//! WRITE-side Clerk Backend API calls the invite lifecycle needs — create a
//! user, create/delete an org membership, delete a user, read back
//! org-membership/user rosters — modeled on `billing::stripe_client::
//! StripeClient`'s shape (a thin struct holding `reqwest::Client` + secret +
//! base URL, `new`/`with_base_url`). Every function returns a typed
//! `Result`, never silently collapses a failure into `None` — the invite
//! endpoint needs to know exactly what went wrong (already-a-member vs. a
//! transient 5xx vs. not-configured) to decide whether to unwind.
//!
//! Base URL is overridable via `CLERK_API_BASE_URL` (default
//! `https://api.clerk.com`), mirroring the `CLERK_JWKS_URL` test-override
//! precedent (`clerk.rs`'s `ClerkConfig::from_env`) — lets the org-invites pg
//! test suite point this client at a local mock.

use std::sync::OnceLock;

use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

/// `CLERK_API_BASE_URL`, defaulting to the real Clerk Backend API origin.
pub fn api_base_url() -> String {
    std::env::var("CLERK_API_BASE_URL").unwrap_or_else(|_| "https://api.clerk.com".to_string())
}

/// A dedicated, SHORT-timeout `reqwest::Client` for this module's outbound
/// calls — built ONCE per process and cheaply cloned thereafter (`reqwest::
/// Client` is internally `Arc`-backed). Deliberately NOT `AppState.
/// http_client`, which is sized for the LLM path's 60s (default) timeout —
/// the org roster's hot-path Clerk enrichment (`routes::teams::
/// list_org_members`) must not be able to tie up a client built for that
/// budget for anywhere near that long. Mirrors `billing::stripe_client::
/// stripe_http_client`'s construction exactly (org-invites train PR 1
/// review, finding 8).
pub fn clerk_http_client() -> Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("failed to build Clerk admin HTTP client")
        })
        .clone()
}

#[derive(Debug, thiserror::Error)]
pub enum ClerkAdminError {
    /// Not currently constructed by anything in this module — `from_env`
    /// signals "no `CLERK_SECRET_KEY`" via `Option::None`, not this variant
    /// (every caller's `let Some(clerk) = ClerkAdminClient::from_env() else
    /// { return unavailable(); }` convention wants exactly that shape).
    /// Kept (and matched defensively in
    /// `routes::org_invites::map_clerk_error`) for a hypothetical future
    /// caller that DOES want a `Result`-returning constructor — dropping it
    /// would be a silent behavioral commitment that no such caller may ever
    /// exist.
    #[allow(dead_code)]
    #[error("CLERK_SECRET_KEY is not configured")]
    NotConfigured,
    #[error("transport error calling Clerk: {0}")]
    Transport(String),
    /// Any non-2xx response. `code`/`message` are Clerk's own
    /// `errors[0].code`/`errors[0].long_message` (falling back to
    /// `errors[0].message`) when present — Clerk's documented error shape.
    #[error("Clerk API error {status}: {code:?} {message}")]
    Api {
        status: u16,
        code: Option<String>,
        message: String,
    },
    /// A 2xx response whose body did not have the shape a caller requires —
    /// loud, never a silent default (mirrors `StripeError::Parse`'s own
    /// rationale).
    #[error("unexpected Clerk response shape: {0}")]
    Parse(String),
}

impl ClerkAdminError {
    /// Clerk's own `errors[0].code` (e.g. `form_identifier_exists`,
    /// `already_a_member_in_organization`), when this is an [`Api`](Self::Api)
    /// error.
    pub fn code(&self) -> Option<&str> {
        match self {
            ClerkAdminError::Api { code, .. } => code.as_deref(),
            _ => None,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            ClerkAdminError::Api { status, .. } => Some(*status),
            _ => None,
        }
    }
}

/// A Clerk user created (or reused) by [`ClerkAdminClient::create_user`].
#[derive(Debug, Clone)]
pub struct CreatedUser {
    pub clerk_user_id: String,
    /// `true` when this call genuinely created a new Clerk user; `false`
    /// when the email already belonged to an existing Clerk user and this is
    /// that user (the `form_identifier_exists` fallback path) — callers use
    /// this to decide whether a later failure should delete the user (never
    /// delete a user we didn't create).
    pub newly_created: bool,
    /// Unix-epoch milliseconds, `None` if this user has never signed in —
    /// captured off whichever Clerk response produced this `CreatedUser` (the
    /// create-user response body for a genuinely new user, or the
    /// `find_user_by_email` fallback's user object for a reused one) so
    /// `create_invite` can derive its `pending` response field instead of
    /// hardcoding it (org-invites train PR 1 review, finding 10).
    pub last_sign_in_at: Option<i64>,
}

/// Read through [`ClerkUser::primary_email`], whose caller is
/// `bin/create_personal_orgs::personal_org_name` (the email local part is
/// step 2 of the display-name chain). Was `#[allow(dead_code)]` while
/// neither had a consumer; that attribute is gone now that one exists.
#[derive(Debug, Clone, Deserialize)]
pub struct ClerkEmailAddress {
    pub id: String,
    pub email_address: String,
    /// Clerk's verification object for this address (`status` is
    /// `"verified"` once the person proved they hold it). Optional on the
    /// wire; an address with no verification object is unverified. Read by
    /// [`ClerkUser::primary_email_verified`], the check `POST /v1/orgs`
    /// (self-serve signup PR A2) makes server-side: a session JWT carries
    /// nothing a client could not have shaped.
    #[serde(default)]
    pub verification: Option<ClerkEmailVerification>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClerkEmailVerification {
    #[serde(default)]
    pub status: Option<String>,
}

/// The subset of a Clerk user object the invite lifecycle needs.
#[derive(Debug, Clone, Deserialize)]
pub struct ClerkUser {
    pub id: String,
    /// Unix-epoch milliseconds, `None` if this user has never signed in —
    /// the "pending" signal the whole invite design derives from.
    pub last_sign_in_at: Option<i64>,
    /// See [`ClerkEmailAddress`]'s own doc comment — read only by
    /// [`Self::primary_email`].
    #[serde(default)]
    pub email_addresses: Vec<ClerkEmailAddress>,
    pub primary_email_address_id: Option<String>,
    /// Clerk's own `first_name`/`last_name` — both `nullable: true` in
    /// Clerk's schema, and both `#[serde(default)]` here so a user object
    /// that omits them entirely still deserializes rather than failing the
    /// whole call. Read by `bin/create_personal_orgs::personal_org_name` to
    /// build the display name a personal org gets in Clerk's picker.
    #[serde(default)]
    pub first_name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
}

impl ClerkUser {
    /// The primary email address, falling back to the first one listed when
    /// `primary_email_address_id` matches none of them. Called by
    /// `bin/create_personal_orgs::personal_org_name`.
    pub fn primary_email(&self) -> Option<&str> {
        let primary_id = self.primary_email_address_id.as_deref();
        self.email_addresses
            .iter()
            .find(|e| Some(e.id.as_str()) == primary_id)
            .or_else(|| self.email_addresses.first())
            .map(|e| e.email_address.as_str())
    }

    /// The primary address, only if Clerk reports it `verified`. With a
    /// `primary_email_address_id` that matches nothing, the first address
    /// stands in (as [`Self::primary_email`] does), and it too must be
    /// verified. A verified SECONDARY address never counts: the address the
    /// person signs in with is the one that must be proven.
    pub fn primary_email_verified(&self) -> Option<&str> {
        let primary_id = self.primary_email_address_id.as_deref();
        let primary = self
            .email_addresses
            .iter()
            .find(|e| Some(e.id.as_str()) == primary_id)
            .or_else(|| self.email_addresses.first())?;
        let verified = primary
            .verification
            .as_ref()
            .and_then(|v| v.status.as_deref())
            .is_some_and(|s| s == "verified");
        verified.then_some(primary.email_address.as_str())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrgMembershipUserData {
    pub user_id: String,
}

/// One row of `GET /v1/organizations/{id}/memberships` — role +
/// invite-metadata + the member's Clerk user id, for the roster join.
#[derive(Debug, Clone, Deserialize)]
pub struct OrgMembership {
    pub role: String,
    #[serde(default)]
    pub public_metadata: Value,
    pub public_user_data: OrgMembershipUserData,
}

/// One row of `GET /v1/users/{id}/organization_memberships` — just enough to
/// count remaining org memberships for the revoke GC guard.
#[derive(Debug, Clone, Deserialize)]
pub struct OrgMembershipSummary {
    pub organization_id: String,
    /// Clerk's CURRENT role for this membership (`org:admin`, ...). The
    /// membership webhooks use it instead of the event payload's role, which
    /// can be stale when deliveries arrive out of order.
    pub role: Option<String>,
}

/// Wire shape of one `GET /v1/users/{id}/organization_memberships` row —
/// `{"organization": {"id": ...}, ...}` — deserialized straight off
/// [`ClerkAdminClient::paginate_list`] and then flattened into
/// [`OrgMembershipSummary`]. Kept private/wire-only rather than folding the
/// nesting into `OrgMembershipSummary` itself, which every caller of
/// [`ClerkAdminClient::list_user_org_memberships`] consumes as a flat type.
#[derive(Debug, Clone, Deserialize)]
struct RawOrgMembership {
    organization: RawOrgMembershipOrg,
    #[serde(default)]
    role: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawOrgMembershipOrg {
    id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrganizationInfo {
    /// The `org_...` id. Was `#[allow(dead_code)]` while
    /// [`ClerkAdminClient::get_organization`] was the only producer and its
    /// only caller (`routes::org_invites::org_display_name`) wanted just
    /// `name`. Now genuinely live: `bin/create_personal_orgs` reads it off
    /// both [`ClerkAdminClient::create_organization`] (the id it records in
    /// `personal_orgs`) and [`ClerkAdminClient::list_user_organizations`]
    /// (the id of an orphan it adopts), so the attribute is gone.
    pub id: String,
    pub name: String,
    /// The org's Clerk slug. Optional on Clerk's schema and routes nothing
    /// (every org is served from app.coralswarm.example). The self-serve
    /// provisioning job only logs a mismatch with its opaque routing key.
    #[serde(default)]
    pub slug: Option<String>,
    /// Clerk's Backend-API-only metadata bag (`private_metadata`), returned
    /// on every `Organization` object — including inside the
    /// `GET /v1/organizations` list envelope, which is what makes
    /// [`ClerkAdminClient::list_user_organizations`] able to recognise an
    /// org this codebase minted without a second round trip per candidate.
    ///
    /// `#[serde(default)]` (⇒ `Value::Null`) rather than required: the field
    /// is optional in Clerk's own schema, and an org that simply carries no
    /// private metadata must deserialize, not error. Readers must therefore
    /// treat "absent" and "null" identically — see
    /// `bin/create_personal_orgs::is_personal_org_for`, which does.
    #[serde(default)]
    pub private_metadata: Value,
}

/// Thin outbound Clerk Backend API client. See this module's doc comment.
pub struct ClerkAdminClient {
    http: Client,
    secret: String,
    base_url: String,
}

impl ClerkAdminClient {
    pub fn new(http: Client, secret: impl Into<String>) -> Self {
        Self {
            http,
            secret: secret.into(),
            base_url: "https://api.clerk.com".to_string(),
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Build from env: `CLERK_SECRET_KEY` (required — `None` when unset,
    /// mirroring `AppState.stripe`/`AppState.resend_api_key`'s "absent
    /// config ⇒ 503" convention) + `CLERK_API_BASE_URL` (optional override,
    /// [`api_base_url`]).
    ///
    /// ALWAYS builds on [`clerk_http_client`] — this module's dedicated
    /// short-timeout client — rather than accepting a caller-supplied
    /// `Client`. Every caller used to pass `AppState.http_client` (sized for
    /// the LLM path's 60s-default timeout), which meant `clerk_http_client`
    /// was constructed but never actually used by any of this module's own
    /// outbound calls (org-invites train PR 1 review, follow-up finding —
    /// the fix for finding 8 wired the client into existence but not into
    /// this constructor). Taking the client choice out of callers' hands
    /// entirely closes that gap for good.
    pub fn from_env() -> Option<Self> {
        let secret = std::env::var("CLERK_SECRET_KEY").ok()?;
        Some(Self::new(clerk_http_client(), secret).with_base_url(api_base_url()))
    }

    /// Send a built request (secret already NOT attached — this attaches
    /// bearer auth). The status is read FIRST, before any body parsing —
    /// then: on success, the body is parsed strictly (empty body ⇒ `Null`,
    /// non-empty-but-non-JSON ⇒ loud [`ClerkAdminError::Parse`], since a 2xx
    /// caller genuinely needs that shape); on a NON-success status, the body
    /// is parsed LENIENTLY — a CDN/proxy 502/503/429 can hand back an HTML
    /// error page instead of Clerk's documented JSON error shape, and that
    /// must still surface as [`ClerkAdminError::Api`] (which callers, e.g.
    /// `routes::org_invites::map_clerk_error`, map to a meaningful response)
    /// rather than degrading into an opaque `Parse` → 500 (org-invites train
    /// PR 1 review, finding 5). The raw (truncated) body text is used as the
    /// message when it isn't Clerk's documented `{"errors": [...]}` shape.
    async fn send(&self, req: reqwest::RequestBuilder) -> Result<Value, ClerkAdminError> {
        let resp = req
            .bearer_auth(&self.secret)
            .send()
            .await
            .map_err(|e| ClerkAdminError::Transport(e.to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| ClerkAdminError::Transport(e.to_string()))?;
        if status.is_success() {
            let body: Value = if text.trim().is_empty() {
                Value::Null
            } else {
                serde_json::from_str(&text)
                    .map_err(|e| ClerkAdminError::Parse(format!("{e}: {text}")))?
            };
            Ok(body)
        } else {
            let parsed: Option<Value> = if text.trim().is_empty() {
                None
            } else {
                serde_json::from_str(&text).ok()
            };
            let (code, message) = match &parsed {
                Some(body) => extract_clerk_error(body),
                None if !text.trim().is_empty() => {
                    (None, text.trim().chars().take(500).collect::<String>())
                }
                None => (None, "unknown Clerk error".to_string()),
            };
            Err(ClerkAdminError::Api {
                status: status.as_u16(),
                code,
                message,
            })
        }
    }

    /// `POST /v1/users` — create a new Clerk user with no password (the
    /// invite flow's "pre-created user, signs in via email-code" shape).
    /// Falls back to a lookup-by-email on Clerk's `form_identifier_exists`
    /// 422 (the email already belongs to a Clerk user) rather than treating
    /// that as an error — the invite endpoint reuses the existing user.
    pub async fn create_user(
        &self,
        email: &str,
        first_name: Option<&str>,
        last_name: Option<&str>,
    ) -> Result<CreatedUser, ClerkAdminError> {
        let mut body = json!({ "email_address": [email] });
        if let Some(v) = first_name {
            body["first_name"] = json!(v);
        }
        if let Some(v) = last_name {
            body["last_name"] = json!(v);
        }
        let url = format!("{}/v1/users", self.base_url);
        match self.send(self.http.post(&url).json(&body)).await {
            Ok(v) => {
                let id = v.get("id").and_then(|i| i.as_str()).ok_or_else(|| {
                    ClerkAdminError::Parse("create_user: response had no id".into())
                })?;
                let last_sign_in_at = v.get("last_sign_in_at").and_then(|t| t.as_i64());
                Ok(CreatedUser {
                    clerk_user_id: id.to_string(),
                    newly_created: true,
                    last_sign_in_at,
                })
            }
            Err(ClerkAdminError::Api { status, code, .. })
                if status == 422 && code.as_deref() == Some("form_identifier_exists") =>
            {
                let existing = self.find_user_by_email(email).await?;
                existing.ok_or_else(|| {
                    ClerkAdminError::Parse(format!(
                        "create_user: Clerk reported {email} as already existing but the \
                         follow-up lookup found no user"
                    ))
                })
            }
            Err(e) => Err(e),
        }
    }

    /// `GET /v1/users?email_address=<email>` — the `create_user` fallback.
    /// Uses the documented BARE repeated-param form (`email_address=<v>`),
    /// not `email_address[]=<v>` — the latter percent-encodes to
    /// `email_address%5B%5D`, an undocumented key Clerk may simply ignore,
    /// which would silently turn this into an UNFILTERED `GET /v1/users`
    /// (org-invites train PR 1 review, finding 3). As defense in depth even
    /// against that (e.g. a future Clerk API change reintroducing the same
    /// failure mode), the first result's own email is verified to actually
    /// match the query before it is trusted — this fallback grants Clerk org
    /// membership to whoever it returns, so silently trusting an unfiltered
    /// list's first row would let an attacker email get invited instead.
    async fn find_user_by_email(
        &self,
        email: &str,
    ) -> Result<Option<CreatedUser>, ClerkAdminError> {
        let url = format!("{}/v1/users", self.base_url);
        let v = self
            .send(self.http.get(&url).query(&[("email_address", email)]))
            .await?;
        // `GET /v1/users` returns the documented paginated
        // `{"data": [...], "total_count": N}` envelope, NOT a bare array —
        // `v.as_array()` on the root always failed here, silently turning
        // every reused-identity invite into a 500 via the `Parse` error
        // below. Accept both shapes, same fallback [`Self::paginate_list`]
        // uses.
        let data = v.get("data").cloned().unwrap_or(v);
        let arr = data.as_array().ok_or_else(|| {
            ClerkAdminError::Parse("find_user_by_email: expected an array".into())
        })?;
        let Some(candidate) = arr.first() else {
            return Ok(None);
        };
        let Some(id) = candidate.get("id").and_then(|i| i.as_str()) else {
            return Ok(None);
        };
        let email_matches = candidate
            .get("email_addresses")
            .and_then(|e| e.as_array())
            .map(|addrs| {
                addrs.iter().any(|a| {
                    a.get("email_address")
                        .and_then(|v| v.as_str())
                        .map(|e| e.eq_ignore_ascii_case(email))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if !email_matches {
            // No `email` field in this log — `clerk_user_id` already
            // identifies the record for follow-up, and the queried address
            // is not this service's PII to write to logs (org-invites train
            // PR 1 review, finding 6).
            tracing::error!(
                clerk_user_id = id,
                "clerk_admin: find_user_by_email got a result whose email does not match the \
                 query — Clerk's email_address filter may have been ignored; refusing to use it"
            );
            return Ok(None);
        }
        let last_sign_in_at = candidate.get("last_sign_in_at").and_then(|t| t.as_i64());
        Ok(Some(CreatedUser {
            clerk_user_id: id.to_string(),
            newly_created: false,
            last_sign_in_at,
        }))
    }

    /// `POST /v1/organizations/{org_id}/memberships` — create the org
    /// membership, carrying the invite `public_metadata`
    /// (`invited_by_user_id`/`invited_by_name`/`invited_at`) on the SAME
    /// call rather than a follow-up `PATCH .../metadata` round trip — Clerk's
    /// Create Organization Membership endpoint accepts `public_metadata`
    /// directly.
    pub async fn create_org_membership(
        &self,
        org_id: &str,
        user_id: &str,
        role: &str,
        public_metadata: Value,
    ) -> Result<(), ClerkAdminError> {
        let url = format!("{}/v1/organizations/{org_id}/memberships", self.base_url);
        let body = json!({ "user_id": user_id, "role": role, "public_metadata": public_metadata });
        self.send(self.http.post(&url).json(&body)).await?;
        Ok(())
    }

    /// `DELETE /v1/organizations/{org_id}/memberships/{user_id}`.
    pub async fn delete_org_membership(
        &self,
        org_id: &str,
        user_id: &str,
    ) -> Result<(), ClerkAdminError> {
        let url = format!(
            "{}/v1/organizations/{org_id}/memberships/{user_id}",
            self.base_url
        );
        self.send(self.http.delete(&url)).await?;
        Ok(())
    }

    /// `PATCH /v1/organizations/{org_id}/memberships/{user_id}` — change the
    /// member's Clerk org role (`org:admin` / `org:member`). Clerk fires
    /// `organizationMembership.updated` afterwards, which is what the
    /// webhook-driven sync keys off; callers that need the local role to
    /// change immediately write it themselves (see `routes::org_members`).
    pub async fn update_org_membership_role(
        &self,
        org_id: &str,
        user_id: &str,
        role: &str,
    ) -> Result<(), ClerkAdminError> {
        let url = format!(
            "{}/v1/organizations/{org_id}/memberships/{user_id}",
            self.base_url
        );
        self.send(self.http.patch(&url).json(&json!({ "role": role })))
            .await?;
        Ok(())
    }

    /// `DELETE /v1/organizations/{org_id}`. Used by account deletion to remove
    /// the user's personal organization (a Clerk org that stands for their own
    /// workspace). A missing org is the caller's "already done" (404).
    pub async fn delete_organization(&self, org_id: &str) -> Result<(), ClerkAdminError> {
        let url = format!("{}/v1/organizations/{org_id}", self.base_url);
        self.send(self.http.delete(&url)).await?;
        Ok(())
    }

    /// `DELETE /v1/users/{user_id}`.
    pub async fn delete_user(&self, user_id: &str) -> Result<(), ClerkAdminError> {
        let url = format!("{}/v1/users/{user_id}", self.base_url);
        self.send(self.http.delete(&url)).await?;
        Ok(())
    }

    /// `GET /v1/users/{user_id}/organization_memberships` — used by the
    /// revoke GC guard to decide whether a never-signed-in ghost user
    /// belongs to any OTHER org before deleting them, and by
    /// [`Self::user_has_org_membership`]'s stale-webhook-delivery guard.
    /// Paginated — see [`Self::paginate_list`]. A single unpaginated
    /// `limit=100` page used to defeat both of those callers for any user in
    /// more than 100 orgs (a user past the first page would silently look
    /// like they belong to no further orgs, which would wrongly GC a
    /// genuinely-still-a-member ghost, and would wrongly treat a
    /// still-current membership as a stale webhook delivery).
    pub async fn list_user_org_memberships(
        &self,
        user_id: &str,
    ) -> Result<Vec<OrgMembershipSummary>, ClerkAdminError> {
        let url = format!(
            "{}/v1/users/{user_id}/organization_memberships",
            self.base_url
        );
        let raw: Vec<RawOrgMembership> = self.paginate_list(&url, &[]).await?;
        Ok(raw
            .into_iter()
            .map(|m| OrgMembershipSummary {
                organization_id: m.organization.id,
                role: m.role,
            })
            .collect())
    }

    /// `GET /v1/users/{user_id}` — at least primary email + `last_sign_in_at`.
    pub async fn get_user(&self, user_id: &str) -> Result<ClerkUser, ClerkAdminError> {
        let url = format!("{}/v1/users/{user_id}", self.base_url);
        let v = self.send(self.http.get(&url)).await?;
        serde_json::from_value(v).map_err(|e| ClerkAdminError::Parse(e.to_string()))
    }

    /// `GET /v1/organizations/{org_id}/memberships` — `role` +
    /// `public_metadata` + `public_user_data.user_id` per membership, for
    /// the roster's invited-by/invited-at columns. Paginated — see
    /// [`Self::paginate_list`].
    pub async fn list_org_memberships(
        &self,
        org_id: &str,
    ) -> Result<Vec<OrgMembership>, ClerkAdminError> {
        let url = format!("{}/v1/organizations/{org_id}/memberships", self.base_url);
        self.paginate_list(&url, &[]).await
    }

    /// `GET /v1/users?organization_id=<org_id>` — full user objects (incl.
    /// `last_sign_in_at`) for the roster's `pending` column. Uses the
    /// documented bare-param form (`organization_id=<v>`, not
    /// `organization_id[]=<v>` — see [`Self::find_user_by_email`]'s doc
    /// comment for why the bracketed form is unsafe here too: an unfiltered
    /// list would silently expand this into "every Clerk user", not just
    /// this org's). Paginated — see [`Self::paginate_list`].
    pub async fn list_org_users(&self, org_id: &str) -> Result<Vec<ClerkUser>, ClerkAdminError> {
        let url = format!("{}/v1/users", self.base_url);
        self.paginate_list(&url, &[("organization_id", org_id)])
            .await
    }

    /// Page through a Clerk Backend API list endpoint whose successful body
    /// is either the documented `{"data": [...], "total_count": N}` shape or
    /// a bare JSON array (some Clerk list endpoints, e.g. `GET /v1/users`,
    /// return the array directly with no `total_count`) — `limit=100` +
    /// increasing `offset`, collecting every page. Without `total_count`,
    /// a short page (fewer than `LIMIT` items) is the end-of-results signal.
    /// Capped at `MAX_PAGES` (50 → up to 5,000 rows) so a runaway/misbehaving
    /// account can't spin this forever; logs a warning if the cap is hit
    /// (org-invites train PR 1 review, finding 6 — previously both callers
    /// hardcoded `limit=100` with no pagination at all, silently truncating
    /// any org/roster past 100 rows).
    async fn paginate_list<T: DeserializeOwned>(
        &self,
        url: &str,
        extra_params: &[(&str, &str)],
    ) -> Result<Vec<T>, ClerkAdminError> {
        let (all, truncated) = self.paginate_list_inner(url, extra_params).await?;
        if truncated {
            tracing::warn!(
                url,
                collected = all.len(),
                "clerk_admin: pagination hit the 50-page cap — results may be incomplete"
            );
        }
        Ok(all)
    }

    /// [`Self::paginate_list`], but hitting the page cap is an ERROR rather
    /// than a warning.
    ///
    /// Use this wherever a MISSING row changes a decision, as opposed to
    /// merely shortening a display list. `paginate_list`'s warn-only
    /// truncation is right for a roster nobody acts on programmatically, and
    /// wrong the moment "not found in this list" is taken to mean "does not
    /// exist" — an operator binary typically initialises no tracing
    /// subscriber at all, so the warning goes nowhere and absence reads as
    /// proof. Same reasoning [`Self::list_all_users`] spells out for its own
    /// count cross-check; this is the variant for endpoints that have no
    /// count endpoint to cross-check against.
    async fn paginate_list_complete<T: DeserializeOwned>(
        &self,
        url: &str,
        extra_params: &[(&str, &str)],
    ) -> Result<Vec<T>, ClerkAdminError> {
        let (all, truncated) = self.paginate_list_inner(url, extra_params).await?;
        if truncated {
            return Err(ClerkAdminError::Parse(format!(
                "{url}: pagination hit the 50-page cap after {} rows, so this list is a                  PREFIX, not the whole set — refusing to let a missing entry be read as                  'not present'",
                all.len()
            )));
        }
        Ok(all)
    }

    /// The shared paging loop. Returns the rows and whether the page cap cut
    /// them short; the two wrappers above decide what that means.
    async fn paginate_list_inner<T: DeserializeOwned>(
        &self,
        url: &str,
        extra_params: &[(&str, &str)],
    ) -> Result<(Vec<T>, bool), ClerkAdminError> {
        const LIMIT: usize = 100;
        const MAX_PAGES: usize = 50;
        let mut all = Vec::new();
        let mut offset = 0usize;
        for page in 0..MAX_PAGES {
            let limit_s = LIMIT.to_string();
            let offset_s = offset.to_string();
            let mut params: Vec<(&str, &str)> = extra_params.to_vec();
            params.push(("limit", &limit_s));
            params.push(("offset", &offset_s));
            let v = self.send(self.http.get(url).query(&params)).await?;
            let total_count = v.get("total_count").and_then(|t| t.as_u64());
            let data = v.get("data").cloned().unwrap_or_else(|| v.clone());
            let arr = data
                .as_array()
                .ok_or_else(|| ClerkAdminError::Parse("paginate_list: expected an array".into()))?;
            let page_len = arr.len();
            for item in arr {
                all.push(
                    serde_json::from_value(item.clone())
                        .map_err(|e| ClerkAdminError::Parse(e.to_string()))?,
                );
            }
            offset += page_len;
            let done = match total_count {
                Some(total) => (offset as u64) >= total,
                None => page_len < LIMIT,
            };
            if done {
                return Ok((all, false));
            }
            if page == MAX_PAGES - 1 {
                return Ok((all, true));
            }
        }
        Ok((all, true))
    }

    /// Whether `user_id` CURRENTLY has a Clerk org membership in `org_id` —
    /// used by `routes::clerk_webhook::handle_membership_deleted` to detect
    /// a stale `organizationMembership.deleted` delivery (see
    /// `routes::org_invites`'s module doc, "the revoke-then-quick-re-invite
    /// race", org-invites train PR 1 review finding 2). Built on
    /// [`Self::list_user_org_memberships`] (a per-user listing already used
    /// by the revoke GC guard) rather than an organization-scoped `?user_id=`
    /// filter on `GET /v1/organizations/{org_id}/memberships` — Clerk's List
    /// Organization Memberships endpoint does not document a per-user filter
    /// param, so filtering client-side over the user's OWN membership list
    /// is the documented-safe path.
    pub async fn user_has_org_membership(
        &self,
        org_id: &str,
        user_id: &str,
    ) -> Result<bool, ClerkAdminError> {
        let memberships = self.list_user_org_memberships(user_id).await?;
        Ok(memberships.iter().any(|m| m.organization_id == org_id))
    }

    /// The user's CURRENT membership in `org_id`, or `None` when Clerk says
    /// they are not a member. The membership webhooks use this to ignore
    /// stale deliveries and to take the role from Clerk rather than the
    /// (possibly out-of-order) event payload.
    pub async fn get_user_org_membership(
        &self,
        org_id: &str,
        user_id: &str,
    ) -> Result<Option<OrgMembershipSummary>, ClerkAdminError> {
        let memberships = self.list_user_org_memberships(user_id).await?;
        Ok(memberships.into_iter().find(|m| m.organization_id == org_id))
    }

    /// `POST /v1/organizations` — create a Clerk organization whose
    /// administrator is `created_by`.
    ///
    /// # `created_by` really does make that user the org's admin
    ///
    /// Verified against Clerk's own Backend API OpenAPI specification
    /// (`clerk/openapi-specs`, `bapi/2024-10-01.yml`, `CreateOrganization`),
    /// not inferred from behaviour. Two things there are load-bearing and
    /// both were read directly:
    ///
    ///   * the `created_by` request property is documented as "The ID of the
    ///     User who will become the administrator for the new organization";
    ///   * `name` is the ONLY required property, so `created_by` is
    ///     genuinely the mechanism for ownership rather than an annotation
    ///     alongside some other required owner field.
    ///
    /// So there is no follow-up [`Self::create_org_membership`] call to
    /// make: Clerk creates the admin membership as part of this one request.
    /// Making one anyway would fail with `already_a_member_in_organization`,
    /// which is the observable form of the same fact.
    ///
    /// # A side effect the same spec documents, and callers must want
    ///
    /// The endpoint's own description states: "The `created_by` user will
    /// see this as their active organization the next time they create a
    /// session, presuming they don't explicitly set a different organization
    /// as active before then." Creating an organization for a user is
    /// therefore NOT invisible to them — it silently changes which
    /// organization their next session starts in. Every caller must have
    /// decided that is acceptable for the user in question.
    ///
    /// # `private_metadata`
    ///
    /// Optional, and omitted from the request body entirely when `None`
    /// (rather than sent as `null`) so this call is byte-identical to a
    /// plain two-field create for callers that do not want a marker. When
    /// `Some`, it is Backend-API-only — invisible to the Frontend API, so a
    /// marker written here cannot be read or forged from a browser — and is
    /// returned on every later read of the org
    /// ([`OrganizationInfo::private_metadata`]), which is what makes it
    /// usable as a durable "this codebase minted this org" tag.
    pub async fn create_organization(
        &self,
        name: &str,
        created_by: &str,
        private_metadata: Option<Value>,
    ) -> Result<OrganizationInfo, ClerkAdminError> {
        let url = format!("{}/v1/organizations", self.base_url);
        let mut body = json!({ "name": name, "created_by": created_by });
        if let Some(md) = private_metadata {
            body["private_metadata"] = md;
        }
        let v = self.send(self.http.post(&url).json(&body)).await?;
        serde_json::from_value(v).map_err(|e| ClerkAdminError::Parse(e.to_string()))
    }

    /// `POST /v1/organizations` for a self-serve team org (`POST /v1/orgs`,
    /// signup PR A2). Differs from [`Self::create_organization`] in two
    /// fields, both chosen by the server and never by the creator:
    ///
    /// * `slug`: an opaque key. Every org is served from app.coralswarm.example
    ///   (founder decision 2026-10-27), so the slug routes nothing; passing
    ///   one stops Clerk deriving it from the name, where two orgs both
    ///   called "Acme" would collide and the second create would fail.
    /// * `max_allowed_memberships`: the seats the creator is buying (security
    ///   review S4). The prod instance's default caps every org at 5
    ///   (checked read-only 2026-10-27), and `0` would be "unlimited", which
    ///   would let a Clerk-native invite (the `<OrganizationProfile/>`
    ///   Members tab, or Clerk's verified-domain enrollment) add members past
    ///   what was paid for without a seat quote. The cap is re-synced with
    ///   the purchased seat count whenever seats change
    ///   (`routes::orgs_create::sync_clerk_membership_cap`).
    pub async fn create_selfserve_organization(
        &self,
        name: &str,
        created_by: &str,
        slug: &str,
        private_metadata: Value,
        max_allowed_memberships: u32,
    ) -> Result<OrganizationInfo, ClerkAdminError> {
        let url = format!("{}/v1/organizations", self.base_url);
        let body = json!({
            "name": name,
            "created_by": created_by,
            "slug": slug,
            "private_metadata": private_metadata,
            "max_allowed_memberships": max_allowed_memberships,
        });
        let v = self.send(self.http.post(&url).json(&body)).await?;
        serde_json::from_value(v).map_err(|e| ClerkAdminError::Parse(e.to_string()))
    }

    /// `PATCH /v1/organizations/{org_id}` setting only
    /// `max_allowed_memberships` (self-serve signup A2, security review S4:
    /// the Clerk-side member cap follows the purchased seat count).
    pub async fn update_organization_max_memberships(
        &self,
        org_id: &str,
        max_allowed_memberships: u32,
    ) -> Result<(), ClerkAdminError> {
        let url = format!("{}/v1/organizations/{org_id}", self.base_url);
        self.send(self.http.patch(&url).json(&json!({ "max_allowed_memberships": max_allowed_memberships })))
            .await?;
        Ok(())
    }

    /// `GET /v1/organizations?user_id=<user_id>` — every organization
    /// `user_id` is a member of, each with its `private_metadata`.
    ///
    /// Uses the documented BARE repeated-param form (`user_id=<v>`), not
    /// `user_id[]=<v>` — see [`Self::find_user_by_email`]'s doc comment for
    /// why the bracketed form is unsafe (it percent-encodes to an
    /// undocumented key Clerk may ignore, silently widening the call into an
    /// UNFILTERED list of every organization in the instance).
    ///
    /// Unlike `find_user_by_email`, this function does NOT need to defend
    /// against that widening itself, and deliberately does not try: it
    /// returns rows rather than acting on one, and its only caller
    /// (`bin/create_personal_orgs`) selects among them by a
    /// `private_metadata` marker containing the very user id it queried for.
    /// An unfiltered list therefore still cannot yield a wrong match — it
    /// would only make this slower. Filtering here on a membership field the
    /// list envelope does not carry would mean a `get_organization` round
    /// trip per candidate for no additional safety.
    ///
    /// Paginated — see [`Self::paginate_list`]. `GET /v1/organizations`
    /// returns the `{"data": [...], "total_count": N}` envelope, so the
    /// end-of-results signal is `total_count`, not a short page.
    pub async fn list_user_organizations(
        &self,
        user_id: &str,
    ) -> Result<Vec<OrganizationInfo>, ClerkAdminError> {
        let url = format!("{}/v1/organizations", self.base_url);
        // COMPLETE, not best-effort: `create_personal_orgs` reads "no marked
        // org in this list" as "this user has no personal org yet" and then
        // CREATES one. A truncated list would turn a capped prefix into a
        // false negative and mint a second Clerk organization for a user who
        // already has one (CodeRabbit review, PR #814). The `personal_orgs`
        // UNIQUE on `clerk_user_id` would then reject the record, leaving a
        // stranded org in Clerk — loud, but only after the damage.
        self.paginate_list_complete(&url, &[("user_id", user_id)])
            .await
    }

    /// `GET /v1/users` — every user in the instance.
    ///
    /// # Why this cross-checks `GET /v1/users/count`
    ///
    /// [`Self::paginate_list`] stops at `MAX_PAGES` (50 × 100 = 5,000 rows)
    /// and reports overflow only through `tracing::warn!`. That is fine for
    /// its existing callers (a single org's roster) and NOT fine here: an
    /// operator binary that enumerates "every user" typically does not
    /// initialise a tracing subscriber at all, so on an instance past 5,000
    /// users the warning goes nowhere and the caller silently processes a
    /// prefix of the instance while reporting success. `GET /v1/users`
    /// additionally returns a BARE ARRAY with no `total_count` (see
    /// `paginate_list`'s own doc comment), so the envelope cannot catch it
    /// either.
    ///
    /// So the total is fetched first, from Clerk's own count endpoint, and a
    /// short collection is a loud error rather than a quiet truncation.
    /// [`ClerkAdminError::Parse`] is reused rather than adding an enum
    /// variant every existing `match` on this type would have to grow: the
    /// shared meaning is "a 2xx response did not have the shape the caller
    /// requires", which a page set that cannot account for the instance's
    /// own stated user count is.
    ///
    /// The comparison is `<`, not `!=`, and that asymmetry is deliberate. A
    /// user CREATED between the count and the last page makes the collection
    /// longer than the count — harmless, and this call still returns
    /// everything it saw. A user DELETED in that window makes it shorter and
    /// will error spuriously; erroring is the safe direction (the operator
    /// re-runs and gets a consistent snapshot), whereas treating short as
    /// fine is exactly the silent truncation this check exists to stop.
    pub async fn list_all_users(&self) -> Result<Vec<ClerkUser>, ClerkAdminError> {
        let total = self.count_users().await?;
        let url = format!("{}/v1/users", self.base_url);
        let users: Vec<ClerkUser> = self.paginate_list(&url, &[]).await?;
        if (users.len() as u64) < total {
            return Err(ClerkAdminError::Parse(format!(
                "list_all_users: collected {} user(s) but GET /v1/users/count reports {total} — \
                 pagination was truncated (paginate_list caps at 50 pages / 5,000 rows) or the \
                 instance changed mid-scan; refusing to report a partial list as complete",
                users.len()
            )));
        }
        Ok(users)
    }

    /// `GET /v1/users/count` — `{"object": "total_count", "total_count": N}`.
    /// Private: it exists only as [`Self::list_all_users`]'s truncation
    /// guard, and a caller that wants a count without the list can add its
    /// own public wrapper when one actually exists.
    async fn count_users(&self) -> Result<u64, ClerkAdminError> {
        let url = format!("{}/v1/users/count", self.base_url);
        let v = self.send(self.http.get(&url)).await?;
        v.get("total_count")
            .and_then(|t| t.as_u64())
            .ok_or_else(|| {
                ClerkAdminError::Parse(
                    "count_users: response had no numeric total_count field".into(),
                )
            })
    }

    /// `GET /v1/organizations/{org_id}` — the org's display name (`accounts`
    /// carries no display-name column on the control plane; Clerk is the
    /// source of truth for it).
    pub async fn get_organization(
        &self,
        org_id: &str,
    ) -> Result<OrganizationInfo, ClerkAdminError> {
        let url = format!("{}/v1/organizations/{org_id}", self.base_url);
        let v = self.send(self.http.get(&url)).await?;
        serde_json::from_value(v).map_err(|e| ClerkAdminError::Parse(e.to_string()))
    }
}

/// Clerk's documented error-body shape: `{"errors":[{"code":..., "message":
/// ..., "long_message": ...}], "clerk_trace_id": ...}`. `long_message` is
/// preferred when present (Clerk's own fuller, user-facing text).
fn extract_clerk_error(body: &Value) -> (Option<String>, String) {
    if let Some(first) = body
        .get("errors")
        .and_then(|e| e.as_array())
        .and_then(|a| a.first())
    {
        let code = first
            .get("code")
            .and_then(|c| c.as_str())
            .map(str::to_string);
        let message = first
            .get("long_message")
            .or_else(|| first.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("unknown Clerk error")
            .to_string();
        (code, message)
    } else {
        (None, "unknown Clerk error".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_clerk_error_reads_code_and_long_message() {
        let body = json!({
            "errors": [{"code": "form_identifier_exists", "message": "identifier exists",
                        "long_message": "That email address is taken"}],
            "clerk_trace_id": "abc"
        });
        let (code, message) = extract_clerk_error(&body);
        assert_eq!(code.as_deref(), Some("form_identifier_exists"));
        assert_eq!(message, "That email address is taken");
    }

    #[test]
    fn extract_clerk_error_falls_back_to_message_without_long_message() {
        let body = json!({"errors": [{"code": "x", "message": "short"}]});
        let (code, message) = extract_clerk_error(&body);
        assert_eq!(code.as_deref(), Some("x"));
        assert_eq!(message, "short");
    }

    #[test]
    fn extract_clerk_error_handles_a_body_with_no_errors_array() {
        let (code, message) = extract_clerk_error(&Value::Null);
        assert_eq!(code, None);
        assert_eq!(message, "unknown Clerk error");
    }

    #[test]
    fn clerk_user_primary_email_prefers_the_primary_id() {
        let user = ClerkUser {
            id: "user_1".into(),
            last_sign_in_at: None,
            email_addresses: vec![
                ClerkEmailAddress {
                    id: "idn_1".into(),
                    email_address: "a@test.dev".into(),
                    verification: None,
                },
                ClerkEmailAddress {
                    id: "idn_2".into(),
                    email_address: "b@test.dev".into(),
                    verification: None,
                },
            ],
            primary_email_address_id: Some("idn_2".into()),
            first_name: None,
            last_name: None,
        };
        assert_eq!(user.primary_email(), Some("b@test.dev"));
    }

    #[test]
    fn clerk_user_primary_email_falls_back_to_first_when_primary_id_unset() {
        let user = ClerkUser {
            id: "user_1".into(),
            last_sign_in_at: None,
            email_addresses: vec![ClerkEmailAddress {
                id: "idn_1".into(),
                email_address: "a@test.dev".into(),
                verification: None,
            }],
            primary_email_address_id: None,
            first_name: None,
            last_name: None,
        };
        assert_eq!(user.primary_email(), Some("a@test.dev"));
    }

    fn email(id: &str, addr: &str, status: Option<&str>) -> ClerkEmailAddress {
        ClerkEmailAddress {
            id: id.into(),
            email_address: addr.into(),
            verification: status.map(|s| ClerkEmailVerification { status: Some(s.into()) }),
        }
    }

    fn user_with(emails: Vec<ClerkEmailAddress>, primary: Option<&str>) -> ClerkUser {
        ClerkUser {
            id: "user_1".into(),
            last_sign_in_at: None,
            email_addresses: emails,
            primary_email_address_id: primary.map(str::to_string),
            first_name: None,
            last_name: None,
        }
    }

    /// Self-serve signup PR A2: only a verified PRIMARY address counts.
    #[test]
    fn primary_email_verified_requires_the_primary_address_itself_to_be_verified() {
        let u = user_with(vec![email("idn_1", "a@test.dev", Some("verified"))], Some("idn_1"));
        assert_eq!(u.primary_email_verified(), Some("a@test.dev"));
        let u = user_with(vec![email("idn_1", "a@test.dev", Some("unverified"))], Some("idn_1"));
        assert_eq!(u.primary_email_verified(), None);
        let u = user_with(vec![email("idn_1", "a@test.dev", None)], Some("idn_1"));
        assert_eq!(u.primary_email_verified(), None, "no verification object is unverified");
        // A verified secondary does not stand in for an unverified primary.
        let u = user_with(
            vec![email("idn_1", "a@test.dev", Some("verified")), email("idn_2", "b@test.dev", Some("unverified"))],
            Some("idn_2"),
        );
        assert_eq!(u.primary_email_verified(), None);
        let u = user_with(vec![], None);
        assert_eq!(u.primary_email_verified(), None);
        // The wire shape Clerk sends.
        let parsed: ClerkUser = serde_json::from_value(json!({
            "id": "user_1", "last_sign_in_at": null, "primary_email_address_id": "idn_1",
            "email_addresses": [{"id": "idn_1", "email_address": "a@test.dev",
                                 "verification": {"status": "verified", "strategy": "email_code"}}]
        }))
        .unwrap();
        assert_eq!(parsed.primary_email_verified(), Some("a@test.dev"));
    }

    #[test]
    fn api_base_url_defaults_to_the_real_clerk_origin() {
        // Direct construction, not the process-global env var (parallel test
        // races) — mirrors `clerk.rs::config_from_env_derives_jwks_url`'s
        // own note about avoiding env races in unit tests.
        assert_eq!(
            std::env::var("CLERK_API_BASE_URL")
                .unwrap_or_else(|_| "https://api.clerk.com".to_string()),
            api_base_url()
        );
    }

    // ── org-invites train PR 1 review, findings 3/5/6 — exercised against a
    // tiny local mock, no `ORGT_TEST_BASE_URL`/Postgres required. ──────────

    use std::sync::Mutex as StdMutex;

    #[tokio::test]
    async fn find_user_by_email_uses_bare_query_params_and_trusts_a_matching_result() {
        let last_query: std::sync::Arc<StdMutex<Option<String>>> = Default::default();
        let captured = last_query.clone();
        let app = axum::Router::new().route(
            "/v1/users",
            axum::routing::get(move |axum::extract::RawQuery(q): axum::extract::RawQuery| {
                let captured = captured.clone();
                async move {
                    *captured.lock().unwrap() = q;
                    axum::Json(json!([{
                        "id": "user_match_1",
                        "email_addresses": [{"id": "idn_1", "email_address": "match@test.dev"}],
                        "primary_email_address_id": "idn_1",
                        "last_sign_in_at": Value::Null,
                    }]))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        let client = ClerkAdminClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"));
        let found = client.find_user_by_email("match@test.dev").await.unwrap();
        assert_eq!(found.unwrap().clerk_user_id, "user_match_1");

        let q = last_query.lock().unwrap().clone().unwrap_or_default();
        assert!(
            q.contains("email_address=match%40test.dev"),
            "expected the bare-param form, query was: {q}"
        );
        assert!(
            !q.contains("%5B%5D"),
            "must not use the undocumented bracketed form: {q}"
        );
    }

    #[tokio::test]
    async fn find_user_by_email_rejects_a_result_whose_email_does_not_match() {
        let app = axum::Router::new().route(
            "/v1/users",
            axum::routing::get(|| async {
                axum::Json(json!([{
                    "id": "user_wrong_1",
                    "email_addresses": [{"id": "idn_1", "email_address": "someone-else@test.dev"}],
                    "primary_email_address_id": "idn_1",
                    "last_sign_in_at": Value::Null,
                }]))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        let client = ClerkAdminClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"));
        let found = client.find_user_by_email("victim@test.dev").await.unwrap();
        assert!(
            found.is_none(),
            "a result whose email doesn't match the query must never be trusted, even if Clerk's \
             filter appears to have been ignored"
        );
    }

    #[tokio::test]
    async fn send_treats_a_non_json_error_body_as_api_not_parse() {
        let app = axum::Router::new().route(
            "/v1/users/u_1",
            axum::routing::get(|| async {
                (
                    axum::http::StatusCode::BAD_GATEWAY,
                    [("content-type", "text/html")],
                    "<html><body>502 Bad Gateway</body></html>",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        let client = ClerkAdminClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"));
        let err = client.get_user("u_1").await.unwrap_err();
        match err {
            ClerkAdminError::Api { status, message, .. } => {
                assert_eq!(status, 502);
                assert!(
                    message.contains("Bad Gateway"),
                    "the raw body text must be preserved as the message: {message}"
                );
            }
            other => panic!("expected ClerkAdminError::Api (never Parse) for a non-JSON error body, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_org_memberships_pages_through_all_results() {
        #[derive(Default)]
        struct MockState {
            queries: Vec<String>,
        }
        let state: std::sync::Arc<StdMutex<MockState>> = Default::default();
        let captured = state.clone();
        let app = axum::Router::new().route(
            "/v1/organizations/org_1/memberships",
            axum::routing::get(move |axum::extract::RawQuery(q): axum::extract::RawQuery| {
                let captured = captured.clone();
                async move {
                    let q = q.unwrap_or_default();
                    captured.lock().unwrap().queries.push(q.clone());
                    let offset: usize = q
                        .split('&')
                        .find_map(|p| p.strip_prefix("offset="))
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    let items: Vec<Value> = if offset == 0 {
                        (0..100)
                            .map(|i| {
                                json!({
                                    "role": "org:member",
                                    "public_metadata": {},
                                    "public_user_data": {"user_id": format!("u{i}")},
                                })
                            })
                            .collect()
                    } else {
                        vec![json!({
                            "role": "org:member",
                            "public_metadata": {},
                            "public_user_data": {"user_id": "u100"},
                        })]
                    };
                    axum::Json(json!({"data": items, "total_count": 101}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        let client = ClerkAdminClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"));
        let all = client.list_org_memberships("org_1").await.unwrap();
        assert_eq!(all.len(), 101, "both pages must be collected");
        assert_eq!(
            state.lock().unwrap().queries.len(),
            2,
            "101 rows at limit=100 must take exactly 2 pages"
        );
    }

    // ── personal-orgs PR: `create_organization` / `list_user_organizations`
    // / `list_all_users`, the three calls `bin/create_personal_orgs` needs.
    // Same tiny-local-mock pattern as the four tests above, no Postgres. ───

    /// Bind an ephemeral port, serve `app` on it, and hand back a
    /// `ClerkAdminClient` already pointed at it. The four tests above each
    /// repeat this seven-line dance inline; the tests below share it so a
    /// change to the mock harness cannot make one of them silently stop
    /// exercising what it claims to.
    async fn mock_client(app: axum::Router) -> ClerkAdminClient {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        ClerkAdminClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"))
    }

    /// What the mock below records about the one request it received.
    #[derive(Debug, Default)]
    struct SeenRequest {
        method: Option<String>,
        path: Option<String>,
        body: Option<Value>,
    }

    /// A router that answers `POST`-or-anything at `/v1/organizations` with a
    /// canned Clerk `Organization` object, recording the method, path and
    /// JSON body it was called with.
    ///
    /// Registered as a `fallback`, not as a route, and deliberately so. Both
    /// choices exist to make the method and path assertions POSITIVE rather
    /// than inferred from an error:
    ///
    ///   * under `post("/v1/organizations")`, a client sending the wrong VERB
    ///     gets a 405 and the test fails with "Clerk API error 405";
    ///   * under any exact-path route, a client posting to the wrong PATH
    ///     gets a 404 and the test fails with "Clerk API error 404".
    ///
    /// Either failure is red, but neither is the assertion the test claims to
    /// make. A fallback answers EVERY method at EVERY path with a well-formed
    /// `Organization`, so a wrong verb or a wrong path still succeeds at the
    /// transport level and is then caught by the recorded-value assertions
    /// below, naming exactly what was wrong.
    fn org_create_mock(seen: std::sync::Arc<StdMutex<SeenRequest>>) -> axum::Router {
        axum::Router::new().fallback(
            move |method: axum::http::Method,
                  uri: axum::http::Uri,
                  axum::Json(body): axum::Json<Value>| {
                let seen = seen.clone();
                async move {
                    let mut g = seen.lock().unwrap();
                    g.method = Some(method.to_string());
                    g.path = Some(uri.path().to_string());
                    g.body = Some(body);
                    axum::Json(json!({
                        "object": "organization",
                        "id": "org_created_1",
                        "name": "Ada Lovelace",
                        "slug": "ada-lovelace",
                        "private_metadata": {"coralswarm_personal_org": "user_ada"},
                    }))
                }
            },
        )
    }

    #[tokio::test]
    async fn create_organization_posts_name_and_created_by_to_v1_organizations() {
        let seen: std::sync::Arc<StdMutex<SeenRequest>> = Default::default();
        let client = mock_client(org_create_mock(seen.clone())).await;

        let org = client
            .create_organization("Ada Lovelace", "user_ada", None)
            .await
            .expect("the mock returns a well-formed Organization");

        let g = seen.lock().unwrap();
        assert_eq!(
            g.method.as_deref(),
            Some("POST"),
            "Clerk's CreateOrganization is a POST"
        );
        assert_eq!(
            g.path.as_deref(),
            Some("/v1/organizations"),
            "the path must be exactly /v1/organizations"
        );
        // Whole-body equality, not field spot-checks: this pins that NO
        // extra field is sent either (`private_metadata` was `None` here, and
        // Clerk treats an explicit `null` differently from an absent key).
        assert_eq!(
            g.body.as_ref().expect("a JSON body must have been sent"),
            &json!({"name": "Ada Lovelace", "created_by": "user_ada"}),
        );

        assert_eq!(org.id, "org_created_1");
        assert_eq!(org.name, "Ada Lovelace");
    }

    #[tokio::test]
    async fn create_organization_sends_private_metadata_when_given() {
        let seen: std::sync::Arc<StdMutex<SeenRequest>> = Default::default();
        let client = mock_client(org_create_mock(seen.clone())).await;

        client
            .create_organization(
                "Ada Lovelace",
                "user_ada",
                Some(json!({"coralswarm_personal_org": "user_ada"})),
            )
            .await
            .unwrap();

        let g = seen.lock().unwrap();
        assert_eq!(
            g.body.as_ref().unwrap(),
            &json!({
                "name": "Ada Lovelace",
                "created_by": "user_ada",
                "private_metadata": {"coralswarm_personal_org": "user_ada"},
            }),
        );
    }

    #[tokio::test]
    async fn create_organization_reads_private_metadata_off_the_response() {
        let seen: std::sync::Arc<StdMutex<SeenRequest>> = Default::default();
        let client = mock_client(org_create_mock(seen)).await;

        let org = client
            .create_organization("Ada Lovelace", "user_ada", None)
            .await
            .unwrap();
        assert_eq!(
            org.private_metadata
                .get("coralswarm_personal_org")
                .and_then(|v| v.as_str()),
            Some("user_ada"),
            "the marker a caller wrote must be readable back off the create response"
        );
    }

    /// **A capped org list must never be read as "the user has no personal
    /// org".**
    ///
    /// `create_personal_orgs` decides whether to CREATE from the absence of a
    /// marked org in this list. `paginate_list`'s warn-only truncation makes
    /// absence unprovable — and an operator binary initialises no tracing
    /// subscriber, so the warning goes nowhere. A user past the 50-page cap
    /// whose marked org sits outside the prefix would get a SECOND Clerk
    /// organization minted for them.
    ///
    /// This serves 50 full pages, so the cap is hit with more results
    /// pending, and requires an error rather than a short list.
    /// (CodeRabbit review, PR #814.)
    #[tokio::test]
    async fn a_truncated_user_org_list_is_an_error_not_a_short_list() {
        // Always a FULL page, so `done` is never reached and the cap decides.
        let app = axum::Router::new().route(
            "/v1/organizations",
            axum::routing::get(|| async {
                let data: Vec<Value> = (0..100)
                    .map(|i| json!({"object": "organization", "id": format!("org_{i}"), "name": "x"}))
                    .collect();
                // `total_count` far beyond anything 50 pages can reach.
                axum::Json(json!({ "data": data, "total_count": 1_000_000 }))
            }),
        );
        let client = mock_client(app).await;

        let err = client
            .list_user_organizations("user_many")
            .await
            .expect_err("a truncated list must be an error, never a silent prefix");
        let msg = err.to_string();
        assert!(
            msg.contains("PREFIX"),
            "the error must say the list is a prefix, so the reader knows absence proves \
             nothing here — got: {msg}"
        );
    }

    /// The permissive paginator is UNCHANGED for its existing callers: a
    /// truncated roster is still returned, not an error. Without this, the
    /// fix above could have been made by tightening `paginate_list` itself
    /// and quietly breaking every org-roster caller.
    #[tokio::test]
    async fn the_permissive_paginator_still_tolerates_truncation() {
        let app = axum::Router::new().route(
            "/v1/organizations/org_big/memberships",
            axum::routing::get(|| async {
                let data: Vec<Value> = (0..100)
                    .map(|i| {
                        json!({
                            "role": "org:member",
                            "public_user_data": {"user_id": format!("user_{i}")},
                        })
                    })
                    .collect();
                axum::Json(json!({ "data": data, "total_count": 1_000_000 }))
            }),
        );
        let client = mock_client(app).await;

        let rows = client
            .list_org_memberships("org_big")
            .await
            .expect("a roster listing must tolerate truncation, as it always has");
        assert_eq!(
            rows.len(),
            5_000,
            "50 pages of 100 — the cap, returned rather than refused"
        );
    }

    #[tokio::test]
    async fn list_user_organizations_uses_the_bare_user_id_param_and_keeps_private_metadata() {
        let last_query: std::sync::Arc<StdMutex<Option<String>>> = Default::default();
        let captured = last_query.clone();
        let app = axum::Router::new().route(
            "/v1/organizations",
            axum::routing::get(move |axum::extract::RawQuery(q): axum::extract::RawQuery| {
                let captured = captured.clone();
                async move {
                    *captured.lock().unwrap() = q;
                    axum::Json(json!({
                        "data": [
                            {"object": "organization", "id": "org_plain", "name": "Acme"},
                            {"object": "organization", "id": "org_marked", "name": "Ada",
                             "private_metadata": {"coralswarm_personal_org": "user_ada"}},
                        ],
                        "total_count": 2,
                    }))
                }
            }),
        );
        let client = mock_client(app).await;

        let orgs = client.list_user_organizations("user_ada").await.unwrap();
        // Non-emptiness asserted against an independent literal BEFORE any
        // per-element claim, so nothing below can pass vacuously over an
        // empty vector.
        assert_eq!(orgs.len(), 2, "both orgs in the envelope must be returned");
        assert_eq!(
            orgs[0].private_metadata,
            Value::Null,
            "an org with no private_metadata key must deserialize to Null, not fail the call"
        );
        assert_eq!(
            orgs[1]
                .private_metadata
                .get("coralswarm_personal_org")
                .and_then(|v| v.as_str()),
            Some("user_ada"),
            "the list envelope carries private_metadata — no per-candidate round trip needed"
        );

        let q = last_query.lock().unwrap().clone().unwrap_or_default();
        assert!(
            q.contains("user_id=user_ada"),
            "expected the bare-param form, query was: {q}"
        );
        assert!(
            !q.contains("%5B%5D"),
            "must not use the undocumented bracketed form: {q}"
        );
    }

    /// A `/v1/users` + `/v1/users/count` mock whose count and page contents
    /// are set independently, so a test can create the exact truncation this
    /// guard exists to catch.
    fn users_mock(users: Vec<Value>, reported_count: u64) -> axum::Router {
        axum::Router::new()
            .route(
                "/v1/users/count",
                axum::routing::get(move || async move {
                    axum::Json(json!({"object": "total_count", "total_count": reported_count}))
                }),
            )
            .route(
                "/v1/users",
                axum::routing::get(move || {
                    let users = users.clone();
                    async move { axum::Json(Value::Array(users)) }
                }),
            )
    }

    fn mock_user(id: &str) -> Value {
        json!({
            "object": "user",
            "id": id,
            "last_sign_in_at": Value::Null,
            "primary_email_address_id": Value::Null,
            "email_addresses": [],
        })
    }

    #[tokio::test]
    async fn list_all_users_returns_every_user_when_the_count_agrees() {
        let client = mock_client(users_mock(
            vec![mock_user("user_a"), mock_user("user_b")],
            2,
        ))
        .await;

        let users = client.list_all_users().await.unwrap();
        assert_eq!(users.len(), 2, "both users must be returned");
        assert_eq!(users[0].id, "user_a");
        assert_eq!(users[1].id, "user_b");
    }

    #[tokio::test]
    async fn list_all_users_refuses_a_page_set_shorter_than_the_reported_count() {
        // Two users on the wire, three claimed by /v1/users/count — exactly
        // the shape a 50-page truncation produces, without needing 5,000
        // fixtures to reach it.
        let client = mock_client(users_mock(
            vec![mock_user("user_a"), mock_user("user_b")],
            3,
        ))
        .await;

        let err = client
            .list_all_users()
            .await
            .expect_err("a short page set must never be reported as the whole instance");
        match err {
            ClerkAdminError::Parse(msg) => {
                assert!(
                    msg.contains("collected 2") && msg.contains("reports 3"),
                    "the error must name both numbers so an operator can see the gap: {msg}"
                );
            }
            other => panic!("expected ClerkAdminError::Parse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_all_users_tolerates_a_user_created_mid_scan() {
        // Longer than the count, not shorter: a user created between the
        // count call and the last page. Must NOT error — see
        // `list_all_users`'s own doc comment on why the check is `<`.
        let client = mock_client(users_mock(
            vec![mock_user("user_a"), mock_user("user_b")],
            1,
        ))
        .await;

        let users = client.list_all_users().await.unwrap();
        assert_eq!(users.len(), 2);
    }
}

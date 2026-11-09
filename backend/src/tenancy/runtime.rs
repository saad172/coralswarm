//! Runtime plumbing for the binding gate (org-tenancy PR 1.2, part 2).
//!
//! [`TenantRuntime`] is the per-process state the gate needs: the feature flag,
//! the apex domain, the [`TenantRegistry`] (per-tenant pools), and a short-TTL
//! cache of `tenant_databases` routing rows so the gate does not hit the control
//! DB on every request. It lives on `AppState` as `Option<Arc<TenantRuntime>>`;
//! `None` (or `enabled == false`) means the gate is a pass-through and behavior
//! is byte-identical to single-tenant today.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lru::LruCache;
use sqlx::PgPool;
use uuid::Uuid;

use super::gate::RoutingState;
use super::registry::TenantRegistry;
use super::routing::{self, TenantDatabase, TenantStatus};
use super::secrets::SecretResolver;

/// Routing-row cache TTL. Short enough that a status change (suspend/offboard)
/// or a fresh onboarding takes effect within this window — matching the ~60s
/// revocation SLA — yet long enough to keep the control DB off the per-request
/// hot path.
const ROUTING_TTL: Duration = Duration::from_secs(30);

/// Hard cap on cached routing slugs. The cache is bounded LRU so that a flood of
/// distinct (fuzzed) Host slugs pointing at nonexistent tenants cannot grow it
/// without bound — old entries are evicted rather than accumulating. Far above
/// any realistic active-tenant count.
const ROUTING_CACHE_CAP: usize = 8192;

/// TTL for the first-sight member-ocean provisioning cache
/// (member-private-oceans plan §2.4, trigger 2 —
/// see [`TenantRuntime::maybe_provision_member_ocean`]). Once a request has
/// attempted (or skipped, because of a cache HIT) provisioning for a given
/// `(org_id, user_id)` pair, no further attempt happens for this long —
/// bounding the added cost to "one statement per member per process per this
/// window," not one per request. 30 minutes, matching
/// [`super::registry::DEFAULT_IDLE_TTL`] (a tenant pool this idle would
/// itself be evicted and rebuilt, re-running `ensure_org_ocean`'s idempotent
/// commons-ocean self-heal on the next cold build regardless) — long enough
/// that steady-state traffic essentially never re-attempts, short enough
/// that a member added between webhook/reconciler activity still converges
/// within one TTL window purely from their own requests.
const MEMBER_OCEAN_TTL: Duration = Duration::from_secs(30 * 60);

/// Hard cap on the member-ocean first-sight cache. Same bounded-LRU rationale
/// as [`ROUTING_CACHE_CAP`]: keyed on `(org_id, user_id)` rather than a
/// fuzzable Host slug, so in practice this bounds "distinct members seen by
/// this process across [`MEMBER_OCEAN_TTL`]," comfortably above any realistic
/// per-process working set.
const MEMBER_OCEAN_CACHE_CAP: usize = 8192;

/// Bound on the member-ocean provisioning `INSERT` a cache-MISS issues from
/// [`TenantRuntime::maybe_provision_member_ocean`] — CodeRabbit review on PR 4
/// (member-private-oceans): unlike a cache HIT (one mutex lock, zero I/O),
/// this call runs directly on an AUTHENTICATED REQUEST's own thread, so an
/// unbounded await here doesn't just delay ocean provisioning, it hangs the
/// caller's actual API response. Deliberately SHORTER than the reconciler's
/// `RECONCILE_TIMEOUT` (30s, a background sweep with nobody waiting on it) —
/// 2 seconds is chosen to keep the worst-case added tail latency on this hot
/// path small enough to stay inside ordinary API timeout budgets, while still
/// comfortably covering a normal `INSERT` round-trip (measured ~190µs against
/// a warm local pool in this PR's own benchmark; even a real network hop to a
/// remote Neon tenant is ordinarily well under 100ms). On timeout, the
/// attempt is treated exactly like any other provisioning failure — logged
/// and swallowed, never propagated to the caller — because provisioning is
/// best-effort and the webhook (trigger 1) and reconciler sweep (trigger 3)
/// both still converge regardless of what this one attempt did.
const MEMBER_OCEAN_PROVISION_TIMEOUT: Duration = Duration::from_secs(2);

/// The per-request tenant binding produced by the gate for an org request: the
/// verified org, its account, the AUTHENTICATED user this binding was resolved
/// for, and its (fingerprint-checked) tenant pool.
///
/// `user_id` (org-tenancy PR 1.7 security fix) makes the "this context is only
/// ever valid for the request's own verified user" invariant structural rather
/// than out-of-band: `account_members` does not exist on the tenant plane (see
/// `reachability.rs`'s module doc), so nothing downstream can re-check
/// membership from a bare `Uuid` parameter alone. Carrying the gate-verified
/// user on the context itself lets `ReachabilityResolver` refuse to serve any
/// `user_id` other than this one, closing the "some future caller passes a
/// different user_id than the one this context was built for" widening.
#[derive(Clone)]
pub struct TenantContext {
    pub org_id: String,
    pub account_id: Uuid,
    pub user_id: Uuid,
    /// The lane this request runs on. The gate binds the `work` lane here;
    /// for a `RouteClass::PageRead` request the `Tenant` extractor swaps in
    /// `page_pool`, so every consumer of `ctx.pool` on that request (the
    /// extractor's own handles, `ReachabilityResolver`) is on the page lane
    /// without knowing about lanes.
    pub pool: PgPool,
    /// The `page` lane (`docs/plans/page-load-standard/design.md` §4.3).
    /// `None` means this context has one lane only: worker-built contexts
    /// that reuse connector funnels, tests, and a tenant whose budget is too
    /// small to split. A `PageRead` request then runs on `pool`.
    pub page_pool: Option<PgPool>,
}

impl std::fmt::Debug for TenantContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantContext")
            .field("org_id", &self.org_id)
            .field("account_id", &self.account_id)
            .field("user_id", &self.user_id)
            .finish_non_exhaustive()
    }
}

impl TenantContext {
    /// The `TenantId` this context is scoped to — always `Org(self.org_id)`
    /// (a `TenantContext` only ever exists for the org plane; `Personal`
    /// requests never build one). Convenience for callers threading the
    /// tenant dimension into `DekCache`/`crypto::resolve_dek_for_tenant`.
    pub fn tenant_id(&self) -> super::TenantId {
        super::TenantId::Org(self.org_id.clone())
    }
}

/// What plane a request was bound to. The gate ALWAYS stamps this extension when
/// the runtime is enabled, so a route that finds it absent knows the gate never
/// ran (a layering bug) rather than silently defaulting to the consumer plane.
#[derive(Clone, Debug)]
pub enum TenancyDecision {
    /// Consumer/personal plane (apex).
    Personal,
    /// Tenant plane for a specific org.
    Org(TenantContext),
}

struct CachedRouting {
    row: Option<TenantDatabase>,
    at: Instant,
}

pub struct TenantRuntime {
    pub enabled: bool,
    /// The API apex: the domain whose `Host` header ROUTES a request
    /// (`<slug>.<apex>` → that org's tenant DB). Read from
    /// `TENANT_APEX_DOMAIN`.
    pub apex: String,
    /// The WEB apex: the domain the browser UI is served from, i.e. the domain
    /// an `Origin` header carries. Never consulted by the binding gate (it
    /// never ROUTES a request the way `apex` does). Two legitimate uses:
    ///
    /// 1. CORS ([`crate`]'s `is_allowed_cors_origin`) — classifying an
    ///    incoming browser `Origin`.
    /// 2. Building a user-facing web URL to send a browser TO
    ///    ([`crate::AppState::personal_web_base_url`], and
    ///    [`crate::AppState::org_web_base_url`] which now returns the same
    ///    value) — the OAuth-install redirect, the meeting-pane return, emails
    ///    and meeting/session share links. This is the same trust boundary as
    ///    (1) read the other direction: a host CORS already accepts as a
    ///    legitimate UI origin is exactly the host safe to redirect a browser to.
    ///
    /// **Per-org web subdomains are retired.** Everything is served from
    /// `app.{web_apex}` (`app.coralswarm.example`); the org is named by the token's
    /// `org_id` claim, not by the host, so there is no `<slug>.{web_apex}` UI to
    /// build a link to or to CORS-allow. The API apex (`apex`, e.g.
    /// `api.coralswarm.example`) is still the host a request is ROUTED on, and it
    /// stays distinct from `web_apex`: classifying a browser Origin against the
    /// API apex alone would make every legitimate UI origin `Foreign` and
    /// CORS-reject the entire product, and building a redirect against the API
    /// apex alone would send a browser to a host that never serves a UI.
    ///
    /// Read from `WEB_APEX_DOMAIN`; when unset it DEFAULTS TO `apex`, which is
    /// exactly today's single-apex behavior (same-domain UI and API), so
    /// nothing changes for a deploy that never sets it. Both uses above are
    /// additionally gated on `enabled` by their own callers (never consulted
    /// while the binding gate is a pass-through), so a deploy that hasn't
    /// cut over yet never builds a redirect from this defaulted value either.
    pub web_apex: String,
    pub registry: Arc<TenantRegistry>,
    /// Org-tenancy PR 3.3 cutover-safety gate: when `true`,
    /// `clerk_provision::ensure_org_account` refuses to auto-create a
    /// brand-new org's control `accounts` row (and, transitively, its
    /// consumer-plane graph/DEK) from the Clerk `organization.created`
    /// webhook or a stray org JWT hitting `resolve_clerk_context`'s
    /// first-sight provisioning — see that function's doc comment for the
    /// exact behavior and how it reconciles with the graph/DEK
    /// shadow-provisioning fence (PR 1.2). Read from `ORG_SELFSERVE_DISABLED`;
    /// default `false` (today's behavior, unchanged) — independent of
    /// `enabled`, since it governs an account-creation DECISION, not
    /// per-request Host routing.
    pub org_selfserve_disabled: bool,
    // Bounded LRU so Host-header fuzzing can't grow it without bound.
    routing_cache: Mutex<LruCache<String, CachedRouting>>,
    /// Process-local TTL cache for the member-ocean first-sight trigger — see
    /// [`MEMBER_OCEAN_TTL`] and [`Self::maybe_provision_member_ocean`]. A
    /// separate mutex from `routing_cache`: the two caches are touched at
    /// different points in the gate (phase 1 vs. after a `TenantContext` is
    /// built) and have unrelated eviction/TTL policies, so sharing a lock
    /// would only add unrelated contention.
    member_ocean_seen: Mutex<LruCache<(String, Uuid), Instant>>,
}

fn new_routing_cache() -> Mutex<LruCache<String, CachedRouting>> {
    Mutex::new(LruCache::new(
        NonZeroUsize::new(ROUTING_CACHE_CAP).expect("cap > 0"),
    ))
}

fn new_member_ocean_cache() -> Mutex<LruCache<(String, Uuid), Instant>> {
    Mutex::new(LruCache::new(
        NonZeroUsize::new(MEMBER_OCEAN_CACHE_CAP).expect("cap > 0"),
    ))
}

/// Parse a boolean feature-flag env var: `"1"` or a case-insensitive `"true"`
/// is on; unset or anything else is off (fail closed / default-off, matching
/// every tenancy flag in this module).
fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

impl TenantRuntime {
    /// Build from environment. Returns `None` when no apex domain is configured
    /// (the gate cannot run without knowing the apex). `MULTITENANT_ENABLED`
    /// (default off) gates whether the gate actually enforces; the runtime is
    /// still constructed so the flag can be flipped without a redeploy path
    /// change. `ORG_SELFSERVE_DISABLED` (org-tenancy PR 3.3, default off) is
    /// read independently — see [`Self::org_selfserve_disabled`].
    /// `WEB_APEX_DOMAIN` (CORS-only, default = `TENANT_APEX_DOMAIN`) is read
    /// here too — see [`Self::web_apex`].
    pub fn from_env(resolver: Arc<dyn SecretResolver>) -> Option<Arc<Self>> {
        let apex = std::env::var("TENANT_APEX_DOMAIN").ok()?;
        if apex.trim().is_empty() {
            return None;
        }
        // Unset / blank → the API apex, i.e. today's single-domain behavior.
        let web_apex = std::env::var("WEB_APEX_DOMAIN")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| apex.clone());
        Some(Arc::new(TenantRuntime {
            enabled: env_flag("MULTITENANT_ENABLED"),
            apex,
            web_apex,
            registry: Arc::new(TenantRegistry::new(resolver)),
            org_selfserve_disabled: env_flag("ORG_SELFSERVE_DISABLED"),
            routing_cache: new_routing_cache(),
            member_ocean_seen: new_member_ocean_cache(),
        }))
    }

    /// Test/explicit constructor. `org_selfserve_disabled` defaults to `false`
    /// (today's behavior) — use
    /// [`Self::new_with_org_selfserve_disabled`] to exercise the org-creation
    /// gate (org-tenancy PR 3.3) — and `web_apex` defaults to `apex` (the
    /// single-domain shape) — use [`Self::new_with_web_apex`] to exercise the
    /// split-apex CORS policy.
    pub fn new(enabled: bool, apex: impl Into<String>, registry: Arc<TenantRegistry>) -> Arc<Self> {
        Self::new_with_org_selfserve_disabled(enabled, apex, registry, false)
    }

    /// Test/explicit constructor for the split API/web apex topology (T1) —
    /// see [`Self::web_apex`].
    pub fn new_with_web_apex(
        enabled: bool,
        apex: impl Into<String>,
        web_apex: impl Into<String>,
        registry: Arc<TenantRegistry>,
    ) -> Arc<Self> {
        Arc::new(TenantRuntime {
            enabled,
            apex: apex.into(),
            web_apex: web_apex.into(),
            registry,
            org_selfserve_disabled: false,
            routing_cache: new_routing_cache(),
            member_ocean_seen: new_member_ocean_cache(),
        })
    }

    /// Test/explicit constructor exercising the org-creation gate (org-tenancy
    /// PR 3.3) independently of `enabled`.
    pub fn new_with_org_selfserve_disabled(
        enabled: bool,
        apex: impl Into<String>,
        registry: Arc<TenantRegistry>,
        org_selfserve_disabled: bool,
    ) -> Arc<Self> {
        let apex = apex.into();
        Arc::new(TenantRuntime {
            enabled,
            web_apex: apex.clone(),
            apex,
            registry,
            org_selfserve_disabled,
            routing_cache: new_routing_cache(),
            member_ocean_seen: new_member_ocean_cache(),
        })
    }

    /// Lock the routing cache, recovering from poison rather than panicking. The
    /// critical sections are tiny, synchronous, and cannot corrupt the LRU, so a
    /// poisoned lock (a thread panicked elsewhere while holding it) leaves the map
    /// consistent — recover the guard and carry on, so a stray panic can never
    /// wedge the auth path.
    fn lock_cache(&self) -> std::sync::MutexGuard<'_, LruCache<String, CachedRouting>> {
        self.routing_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Lock the member-ocean first-sight cache, same poison-recovery
    /// discipline as [`Self::lock_cache`].
    fn lock_member_ocean_cache(
        &self,
    ) -> std::sync::MutexGuard<'_, LruCache<(String, Uuid), Instant>> {
        self.member_ocean_seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Resolve the routing row for a subdomain slug, cached for [`ROUTING_TTL`].
    /// Returns the full row (or `None` if no tenant owns the slug); the caller
    /// derives [`RoutingState`] via [`routing_state`].
    pub async fn resolve_routing(
        &self,
        control_pool: &PgPool,
        slug: &str,
    ) -> Result<Option<TenantDatabase>, sqlx::Error> {
        // Scope the lock so it is never held across the DB await below.
        {
            let mut cache = self.lock_cache();
            if let Some(hit) = cache.get(slug) {
                if hit.at.elapsed() < ROUTING_TTL {
                    return Ok(hit.row.clone());
                }
            }
        }
        let row = routing::lookup_by_slug(control_pool, slug).await?;
        self.lock_cache().put(
            slug.to_string(),
            CachedRouting {
                row: row.clone(),
                at: Instant::now(),
            },
        );
        Ok(row)
    }

    /// Invalidate a slug's cached routing (e.g. on onboarding/offboarding).
    pub fn invalidate_routing(&self, slug: &str) {
        self.lock_cache().pop(slug);
    }

    /// Invalidate EVERY cache key a slug rename affects — the one call site for
    /// Clerk `organization.updated` reconciliation
    /// (`tenancy::routing::reconcile_slug`), deliberately a single method
    /// rather than three [`Self::invalidate_routing`] calls a future edit could
    /// half-drop.
    ///
    /// All three keys are load-bearing, and the cache is a FLAT keyspace shared
    /// by both lookups (see [`Self::resolve_routing_by_org`]'s cache-key-safety
    /// note), so dropping only one leaves a stale entry that serves or rejects
    /// wrongly:
    ///
    ///   * `old_slug` — still cached as an ACTIVE tenant, so the abandoned host
    ///     would keep routing traffic into the tenant DB after Clerk stopped
    ///     naming it;
    ///   * `new_slug` — very likely cached as `None` (a NEGATIVE entry: the web
    ///     app starts redirecting to the new host the moment Clerk renames the
    ///     org, and those requests reach the gate before the webhook does), so
    ///     the correct host would 404 for the rest of the TTL. This is the one
    ///     that is easy to forget precisely because nothing "looked up" the new
    ///     slug on purpose;
    ///   * `clerk_org_id` — [`Self::resolve_routing_by_org`] caches the WHOLE
    ///     routing row under the org id, so that entry still carries the old
    ///     `slug` field.
    ///
    /// Process-local by construction: on a multi-instance deploy only the
    /// instance that received the webhook clears its own cache, and the others
    /// converge within [`ROUTING_TTL`] (30s). Bounded, self-healing staleness —
    /// the same window a suspend/offboard already accepts.
    pub fn invalidate_routing_rename(&self, clerk_org_id: &str, old_slug: &str, new_slug: &str) {
        let mut cache = self.lock_cache();
        cache.pop(old_slug);
        cache.pop(new_slug);
        cache.pop(clerk_org_id);
    }

    /// Resolve the routing row by `clerk_org_id` (org-tenancy PR 4.3), cached
    /// in the SAME bounded LRU [`resolve_routing`] uses. Only called for an
    /// **apex** request carrying an org token (never for a personal token or
    /// a subdomain request, so this adds zero queries to the common-case hot
    /// path).
    ///
    /// This lookup used to exist only to pick a rejection message, because the
    /// gate refused every org token on the apex. Claim-based tenancy
    /// (`docs/plans/claim-based-tenancy.md` PR 1.2) made it **the** tenant
    /// selector: the row it returns is what `gate::decide`'s `Apex` arm binds,
    /// and its `db_secret_ref`/`account_id` are what
    /// [`Self::finish_binding`] builds the pool from. A wrong row here is a
    /// wrong tenant, not a wrong error string — which is why `decide` still
    /// compares the row's org to the claim even though this function looked it
    /// up BY that claim.
    ///
    /// `mcp::auth::resolve_personal_tenancy` calls this directly — once, to
    /// resolve the bound org's own `account_id` ahead of `finish_binding`
    /// (see that function's doc for why: a selection-sourced `org_claim` has
    /// no claim-derived `account_id` to already agree with it) — and
    /// `finish_binding` itself then calls it again for the same org id,
    /// which lands on the warm cache entry this first call just populated.
    /// Both of `/mcp`'s org-id sources reach it that way: a stored
    /// `select_tenant` selection, and — since #825 restored `user:org:read`
    /// and the personal resource stopped stripping org claims — the token's
    /// own consent-screen claim.
    ///

    /// Cache-key safety: a Clerk `clerk_org_id` (always `org_...`, containing
    /// an underscore) can never collide with a cached subdomain slug (which
    /// `tenancy::gate::is_valid_slug` restricts to lowercase-alnum-hyphen, no
    /// underscore) — the two lookups share one cache without a namespacing
    /// prefix.
    pub async fn resolve_routing_by_org(
        &self,
        control_pool: &PgPool,
        clerk_org_id: &str,
    ) -> Result<Option<TenantDatabase>, sqlx::Error> {
        {
            let mut cache = self.lock_cache();
            if let Some(hit) = cache.get(clerk_org_id) {
                if hit.at.elapsed() < ROUTING_TTL {
                    return Ok(hit.row.clone());
                }
            }
        }
        let row = routing::lookup_by_org(control_pool, clerk_org_id).await?;
        self.lock_cache().put(
            clerk_org_id.to_string(),
            CachedRouting {
                row: row.clone(),
                at: Instant::now(),
            },
        );
        Ok(row)
    }

    /// **Phase 1 of the gate — runs BEFORE the request is authenticated.**
    ///
    /// Classifies the request `Host` (never a forwarded header — see
    /// [`HostPrecheck`]) and, for an org subdomain, resolves its routing row,
    /// then applies the token-independent half of the policy
    /// ([`gate::precheck`]). Everything it can settle without the token — a
    /// foreign host, and an unknown / provisioning / suspended / offboarded
    /// subdomain — is rejected here, so the caller never verifies a JWT and
    /// never reaches `resolve_clerk_context`'s provisioning WRITES for a host
    /// that was doomed anyway (org-tenancy plan line 50: "unknown subdomain →
    /// 404 fail-closed *before auth*").
    ///
    /// `control_pool` is an `Option` so a foreign host is 404'd with no
    /// database at all; a subdomain that needs the routing lookup with no pool
    /// configured fails closed (503).
    ///
    /// Returns the classification (plus the routing row it already fetched, so
    /// [`Self::finish_binding`] does not repeat the query) for the caller to
    /// hand back once the token is verified.
    ///
    /// [`gate::precheck`]: super::gate::precheck
    pub async fn precheck_host(
        &self,
        control_pool: Option<&PgPool>,
        host: &str,
    ) -> Result<HostPrecheck, GateReject> {
        use super::gate::{classify_host, precheck, HostTarget};

        let target = classify_host(host, &self.apex);
        let routing_row = match &target {
            HostTarget::Subdomain(slug) => {
                let pool = control_pool.ok_or_else(|| {
                    tracing::error!("tenant routing lookup requires a control pool");
                    GateReject::Unavailable
                })?;
                self.resolve_routing(pool, slug).await.map_err(|e| {
                    tracing::error!(error = %e, "tenant routing lookup failed");
                    GateReject::Unavailable
                })?
            }
            // `Foreign` needs no lookup (always 404); the apex's org-token
            // lookup is keyed by the VERIFIED token's `clerk_org_id`, so it
            // belongs to phase 2.
            HostTarget::Foreign | HostTarget::Apex => None,
        };
        if let Some(outcome) = precheck(&target, &routing_state(&routing_row)) {
            return Err(reject_for(outcome));
        }
        Ok(HostPrecheck {
            target,
            routing_row,
        })
    }

    /// **Phase 2 of the gate — runs once the token is verified.**
    ///
    /// Applies the full [`gate::decide`] policy to the already-classified host
    /// from [`Self::precheck_host`] plus the verified token's `org_id`, and —
    /// only on an `Org` outcome — acquires the fingerprint-verified tenant pool
    /// from the registry. Every reject returns before any tenant pool is
    /// touched. `org_claim` is the verified Clerk token's `org_id` (or `None`).
    /// `user_id` is the verified caller's own id (from the same `UserContext`
    /// `account_id` was resolved alongside) — stamped onto the resulting
    /// `TenantContext` so `ReachabilityResolver` can refuse to serve any other
    /// user_id later (see `TenantContext`'s doc comment).
    ///
    /// The org-match check (`claims.org_id` vs the routed tenant) inherently
    /// needs verified claims, which is exactly why it lives here and not in
    /// phase 1.
    ///
    /// `display_name` (member-private-oceans PR 4) is the caller's own
    /// display-name hint — `require_auth` passes the verified Clerk token's
    /// `name` claim — used ONLY on an `Org` outcome, to name a first-sight
    /// member ocean (see [`Self::maybe_provision_member_ocean`]); it is
    /// otherwise unused (a `Personal` outcome never provisions one). `None`
    /// is always safe: the ocean falls back to the same generic name the
    /// other two triggers use.
    ///
    /// [`gate::decide`]: super::gate::decide
    /// Takes the [`TokenKind`](super::gate::TokenKind) itself rather than an
    /// `Option<&str>` org id.
    ///
    /// A personal organization and a real one are BOTH `Some(org_id)` — the
    /// difference is a classification this function cannot make and must not
    /// guess. Deriving the kind here from a bare string would send a personal
    /// org into `TokenKind::Org`, where it is refused for lacking the routing
    /// row it is never supposed to have; the alternative, a boolean beside the
    /// id, is exactly the shape that lets one call site pass the wrong one.
    /// The caller proves the classification
    /// (`middleware::auth::NormalizedClaims::token_kind`) and hands the result
    /// over.
    pub async fn finish_binding(
        &self,
        control_pool: &PgPool,
        pre: HostPrecheck,
        token: super::gate::TokenKind,
        account_id: Uuid,
        user_id: Uuid,
        display_name: Option<&str>,
    ) -> Result<TenancyDecision, GateReject> {
        use super::gate::{decide, GateOutcome, HostTarget, TokenKind};

        let HostPrecheck {
            target: host_target,
            routing_row,
        } = pre;
        let routing_row = match (&host_target, &token) {
            // Already fetched by `precheck_host` (which had to consult it to
            // know the tenant is servable at all) — never re-queried.
            (HostTarget::Subdomain(_), _) => routing_row,
            // Claim-based tenancy PR 1.2: on the apex the org's OWN routing
            // row — looked up by `clerk_org_id`, never by subdomain (there is
            // no slug on the apex) — is what NAMES the tenant, and its status
            // is what decides whether the door opens at all (`Active` binds;
            // every other state, and the lookup error below, refuse). Before
            // PR 1.2 this same lookup only chose a rejection message. A
            // personal token on the apex needs no lookup at all (`decide`
            // never consults `routing` in that branch), so this adds zero
            // queries to the common-case hot path.
            // A personal org is deliberately NOT looked up. It has no routing
            // row by design, so the query could only ever return `None` — and
            // `decide` ignores the routing state for this variant anyway. See
            // `gate::decide`'s Apex arm.
            (HostTarget::Apex, TokenKind::PersonalOrg(_)) => None,
            (HostTarget::Apex, TokenKind::Org(org_id)) => self
                .resolve_routing_by_org(control_pool, org_id)
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, org = %org_id, "tenant routing-by-org lookup failed");
                    GateReject::Unavailable
                })?,
            _ => None,
        };
        match decide(&host_target, &token, &routing_state(&routing_row)) {
            GateOutcome::Personal => Ok(TenancyDecision::Personal),
            GateOutcome::Org(org_id) => {
                // routing_state maps Some(active) → Active only, so the row is
                // present here — but never panic on the auth path: if the
                // invariant is ever broken, fail closed with 503.
                let row = match routing_row {
                    Some(r) => r,
                    None => {
                        tracing::error!(
                            org = %org_id,
                            "gate invariant broken: Org outcome without a routing row"
                        );
                        return Err(GateReject::Unavailable);
                    }
                };
                // `row.account_id` (the routing row's own record of which
                // account owns this org), not the request's own `account_id`
                // parameter — see `TenantRegistry::ensure_org_ocean`'s doc for
                // why this call site is where the org ocean gets guaranteed to
                // exist. The two are expected to agree (both ultimately trace
                // back to `clerk_provision::ensure_org_account`'s single
                // `accounts` row for this org), but the routing row is the
                // more direct source for "this org's account_id" and needs no
                // assumption about how the caller resolved its own.
                let lanes = self
                    .registry
                    .get_or_build_lanes(&org_id, row.account_id, &row.db_secret_ref)
                    .await
                    .map_err(|e| {
                        tracing::error!(error = %e, org = %org_id, "tenant pool build failed");
                        GateReject::Unavailable
                    })?;
                let pool = lanes.work;
                // Member-ocean provisioning, trigger 2 of 3 (member-private-
                // oceans PR 4, plan §2.4) — see
                // `maybe_provision_member_ocean`'s doc comment for why THIS
                // exact call site is the order-independent trigger. Never
                // fails or delays this outcome: every internal error is
                // logged and swallowed.
                self.maybe_provision_member_ocean(
                    &pool,
                    control_pool,
                    &org_id,
                    account_id,
                    user_id,
                    display_name,
                )
                .await;
                Ok(TenancyDecision::Org(TenantContext {
                    org_id,
                    account_id,
                    user_id,
                    pool,
                    page_pool: Some(lanes.page),
                }))
            }
            other => Err(reject_for(other)),
        }
    }

    /// Trigger 2 (request-path first-sight) of member-private-oceans'
    /// three-trigger provisioning design
    /// (`docs/plans/member-private-oceans.md` §2.4). Called from
    /// [`Self::finish_binding`] immediately after a `TenantContext`'s pool is
    /// built for an `Org` outcome.
    ///
    /// **Why this call site is order-independent.** By construction, a
    /// `TenantContext` only ever exists once `TenantRegistry::get_or_build`
    /// has returned `Ok` — which means the tenant database is provably up,
    /// migrated, and fingerprint-verified as belonging to this exact org
    /// (see `registry::TenantRegistry`'s module doc). There is no "tenant DB
    /// doesn't exist yet" case to trip over here, unlike trigger 1 (the
    /// Clerk webhook), which can fire the instant `organization.created`
    /// lands — long before any tenant database exists (plan §1.4b, the exact
    /// trap that left org commons oceans missing for a long time). Whichever
    /// of the three triggers reaches a live tenant pool for a given member
    /// FIRST is the one that lays the ocean down; the other two collide on
    /// `uq_graphs_one_primary_per_owner`'s `ON CONFLICT ... DO NOTHING` and
    /// no-op (see `tenancy::member_ocean`'s module doc).
    ///
    /// **Hot-path cost.** This runs on EVERY authenticated org request
    /// (inside `finish_binding`, which every such request goes through), so
    /// it is guarded by [`MEMBER_OCEAN_TTL`]: a cache HIT — the overwhelming
    /// majority of requests once a process has seen a given `(org_id,
    /// user_id)` once — costs one bounded mutex lock and zero database
    /// round-trips. Only a cache MISS (first request for this member on this
    /// process, or the TTL has elapsed) issues the single `INSERT ... ON
    /// CONFLICT ... DO NOTHING RETURNING id` from
    /// `tenancy::member_ocean::provision_one`. Never blocks the request on a
    /// slow/failed insert propagating an error: every outcome here (flag
    /// off, cache hit, insert success, insert already-present, insert error)
    /// returns without affecting the `Ok(TenancyDecision::Org(..))` the
    /// caller is about to return.
    ///
    /// A failed (or timed-out) insert attempt is deliberately NOT marked
    /// "seen" — the next request for this `(org_id, user_id)` retries
    /// immediately rather than waiting out a full [`MEMBER_OCEAN_TTL`] window
    /// for what may have been a transient error; the reconciler sweep
    /// converges regardless either way.
    ///
    /// **Bounded by [`MEMBER_OCEAN_PROVISION_TIMEOUT`]** (CodeRabbit review on
    /// PR 4): a cache MISS runs this `INSERT` directly on the authenticated
    /// request's own thread, so an unbounded await here would let a slow or
    /// wedged tenant connection hang the caller's actual API response, not
    /// merely delay ocean provisioning. See that constant's own doc comment
    /// for the exact value and why it is shorter than the reconciler's
    /// `RECONCILE_TIMEOUT`. On timeout, this degrades exactly like any other
    /// provisioning failure: logged and swallowed, never propagated to the
    /// caller.
    async fn maybe_provision_member_ocean(
        &self,
        tenant_pool: &PgPool,
        control_pool: &PgPool,
        org_id: &str,
        account_id: Uuid,
        user_id: Uuid,
        display_name: Option<&str>,
    ) {
        if !crate::tenancy::member_ocean::enabled() {
            return;
        }
        let key = (org_id.to_string(), user_id);
        {
            let mut cache = self.lock_member_ocean_cache();
            if let Some(seen_at) = cache.get(&key) {
                if seen_at.elapsed() < MEMBER_OCEAN_TTL {
                    return;
                }
            }
        }
        match tokio::time::timeout(
            MEMBER_OCEAN_PROVISION_TIMEOUT,
            crate::tenancy::member_ocean::provision_one(
                tenant_pool,
                control_pool,
                account_id,
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
                    "member-ocean first-sight trigger: provisioned member ocean"
                );
            }
            // Already present — steady state for every request after the
            // first, or a member whose ocean another trigger already won.
            Ok(Ok(None)) => {}
            Ok(Err(e)) => {
                tracing::info!(
                    org = %org_id,
                    user_id = %user_id,
                    error = %e,
                    "member-ocean first-sight trigger: provision insert failed; will retry on \
                     next request"
                );
                return;
            }
            Err(_) => {
                tracing::info!(
                    org = %org_id,
                    user_id = %user_id,
                    timeout_secs = MEMBER_OCEAN_PROVISION_TIMEOUT.as_secs(),
                    "member-ocean first-sight trigger: provision insert timed out; will retry on \
                     next request"
                );
                return;
            }
        }
        self.lock_member_ocean_cache().put(key, Instant::now());
    }

    /// Test-only: evict a single `(org_id, user_id)` entry from the
    /// member-ocean first-sight cache, simulating [`MEMBER_OCEAN_TTL`] having
    /// elapsed without an actual 30-minute wait. `pub` under the same
    /// `cfg(any(test, feature = "test-support"))` gate as
    /// [`crate::test_app_state`] — see that function's doc comment for why
    /// this is how `backend/tests/*.rs` integration suites reach otherwise-
    /// private state. Never compiled into a production build.
    #[cfg(any(test, feature = "test-support"))]
    pub fn clear_member_ocean_cache_for_test(&self, org_id: &str, user_id: Uuid) {
        self.lock_member_ocean_cache().pop(&(org_id.to_string(), user_id));
    }

    /// Both phases back to back — the original single-shot entry point, kept
    /// as the COMPOSITION of [`Self::precheck_host`] + [`Self::finish_binding`]
    /// so it can never drift from what the middleware actually does. Callers
    /// that authenticate before they route (tests, `/mcp`-style surfaces where
    /// the token is already verified) use this; `require_auth` splits the two
    /// halves around JWT verification instead. `display_name` is threaded
    /// straight through to [`Self::finish_binding`] — see that method's doc
    /// comment.
    pub async fn resolve_binding(
        &self,
        control_pool: &PgPool,
        host: &str,
        token: super::gate::TokenKind,
        account_id: Uuid,
        user_id: Uuid,
        display_name: Option<&str>,
    ) -> Result<TenancyDecision, GateReject> {
        let pre = self.precheck_host(Some(control_pool), host).await?;
        self.finish_binding(control_pool, pre, token, account_id, user_id, display_name)
            .await
    }
}

/// What [`TenantRuntime::precheck_host`] learned about a request's `Host`
/// before the request was authenticated, handed back to
/// [`TenantRuntime::finish_binding`] once the token is verified.
///
/// **Host only, never a forwarded header.** The classification is derived from
/// the `Host` header alone (topology T1 puts the client's Host on the wire
/// verbatim — no proxy, no forwarded-header trust). `X-Forwarded-Host` and
/// friends are never read anywhere on this path; `binding_gate_pg` and
/// `cross_tenant_probe_pg` both carry a regression probe that spoofs them and
/// asserts tenant selection is unchanged, so "correct by omission" cannot
/// silently become "trusts a proxy header" in a later edit.
#[derive(Debug, Clone)]
pub struct HostPrecheck {
    /// How the `Host` classified against the configured apex.
    target: super::gate::HostTarget,
    /// The routing row for a `Subdomain` target (already known to be
    /// `active` — `precheck_host` rejected every other state). `None` for
    /// apex/foreign targets.
    routing_row: Option<TenantDatabase>,
}

impl HostPrecheck {
    /// The classified host — read-only; nothing outside this module may
    /// synthesize a classification that did not come from a real `Host`.
    pub fn target(&self) -> &super::gate::HostTarget {
        &self.target
    }
}

/// Map a pure [`GateOutcome`] rejection to its [`GateReject`]. Shared by both
/// gate phases so a status can never differ between the pre-auth and post-auth
/// path.
///
/// [`GateOutcome`]: super::gate::GateOutcome
fn reject_for(outcome: super::gate::GateOutcome) -> GateReject {
    use super::gate::GateOutcome;
    match outcome {
        GateOutcome::NotFound => GateReject::NotFound,
        GateOutcome::Provisioning => GateReject::Provisioning,
        GateOutcome::Unavailable => GateReject::Unavailable,
        GateOutcome::Forbidden(msg) => GateReject::Forbidden(msg),
        // Not reachable: both callers match the proceed outcomes first. Fail
        // closed rather than panicking on the auth path.
        GateOutcome::Personal | GateOutcome::Org(_) => {
            tracing::error!("gate invariant broken: proceed outcome mapped as a rejection");
            GateReject::Unavailable
        }
    }
}

/// A gate rejection, mapped to an HTTP status by the middleware.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateReject {
    /// 404 — unknown subdomain / foreign host.
    NotFound,
    /// 503 with `"status":"provisioning"`: the org's routing row is being
    /// built (apex only; self-serve signup PR A2).
    Provisioning,
    /// 503 — a control/tenant DB error (or the legacy subdomain arms'
    /// mid-provisioning answer).
    Unavailable,
    /// 403 with a reason.
    Forbidden(&'static str),
}

/// Map a resolved routing row to the gate's [`RoutingState`].
pub fn routing_state(row: &Option<TenantDatabase>) -> RoutingState {
    match row {
        None => RoutingState::Missing,
        Some(r) => match r.status() {
            Some(TenantStatus::Active) => RoutingState::Active(r.clerk_org_id.clone()),
            Some(TenantStatus::Provisioning) => RoutingState::Provisioning,
            Some(TenantStatus::Suspended) => RoutingState::Suspended,
            Some(TenantStatus::Offboarded) => RoutingState::Offboarded,
            // Unrecognized status → fail closed (treat as not active).
            None => RoutingState::Offboarded,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// A `tenant_databases` row with the given `status` text. Every other
    /// column is irrelevant to [`routing_state`]; only `status` and
    /// `clerk_org_id` are read.
    fn row_with_status(status: &str) -> TenantDatabase {
        TenantDatabase {
            clerk_org_id: "org_a".into(),
            account_id: Uuid::nil(),
            slug: "acme".into(),
            db_secret_ref: "TENANT_ACME_DSN".into(),
            region: None,
            substrate: "neon".into(),
            schema_version: None,
            status: status.into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// [`routing_state`] is the only translation between a control-plane row
    /// and the pure gate policy, so `tenancy::gate`'s exhaustive invariants
    /// are worth exactly what this function is worth: a row that maps to the
    /// wrong [`RoutingState`] defeats them without touching them.
    ///
    /// Claim-based tenancy (`docs/plans/claim-based-tenancy.md` PR 1.2) makes
    /// that load-bearing in a way it is not today. Right now `Active` on the
    /// apex is refused like every other state, so mistranslating one status as
    /// another is invisible. After PR 1.2, `Active` is the single state that
    /// *serves* — so this mapping becomes a way to serve a tenant that is
    /// un-provisioned, half-built, write-frozen or torn down.
    #[test]
    fn routing_state_maps_each_status_to_its_own_state() {
        assert_eq!(routing_state(&None), RoutingState::Missing);
        assert_eq!(
            routing_state(&Some(row_with_status("active"))),
            RoutingState::Active("org_a".into())
        );
        assert_eq!(
            routing_state(&Some(row_with_status("provisioning"))),
            RoutingState::Provisioning
        );
        assert_eq!(
            routing_state(&Some(row_with_status("suspended"))),
            RoutingState::Suspended
        );
        assert_eq!(
            routing_state(&Some(row_with_status("offboarded"))),
            RoutingState::Offboarded
        );
    }

    /// **Only the literal `active` status may become `Active`.** A status the
    /// backend does not recognize — a value written by a newer release, a
    /// migration mid-flight, a typo in an ops runbook — must fail closed.
    ///
    /// `TenantStatus::parse` already returns `None` for these; this pins the
    /// consequence rather than the mechanism, so the property survives a
    /// rewrite of the parser.
    #[test]
    fn an_unrecognized_status_never_becomes_active() {
        for status in [
            "",
            " active",
            "Active",
            "ACTIVE",
            "active ",
            "activ",
            "actively",
            "deprovisioning",
            "unknown",
            "null",
        ] {
            let state = routing_state(&Some(row_with_status(status)));
            assert!(
                !matches!(state, RoutingState::Active(_)),
                "status {status:?} was treated as an active, servable tenant: {state:?}"
            );
        }
    }
}

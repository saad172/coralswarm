//! Meeting-transcript resolution + chunking (`docs/plans/remote-mcp-v1.md`
//! §3.3 item 4, §3.2 `get_transcript`).
//!
//! `resolve_raw_text` is promoted verbatim (behavior-preserving) from
//! `routes::meetings` so both the HTTP `GET /v1/meetings/summary` route and
//! the MCP `get_transcript` tool share one blob/DEK-resolution path. The
//! chunker is new: newline-aware, deterministic, fixed-size (15k chars)
//! pagination over the (optionally corrections-overlaid) transcript text.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.5)
//!
//! [`get_transcript`] reads two RLS-covered tables (`meeting_transcripts` and
//! `transcript_corrections`, migration `0083_consumer_rls`), with an R2 blob
//! fetch + AES decrypt sitting BETWEEN them ([`resolve_raw_text_for`]). So
//! this module uses the **per-statement** scoping shape — one short
//! `begin_account_scoped` transaction around each Store call — rather than one
//! transaction spanning the whole function: pinning a pooled connection across
//! that network round trip is exactly what PR #291's `JobScope` rationale
//! rules out. Neither read depends on the other's uncommitted state, so
//! nothing is lost.
//!
//! The scoping account is the OCEAN'S OWNING account, resolved from `graph_id`
//! through `Store::graph_account_id` — i.e. the audited
//! `resolve_graph_account_id` `SECURITY DEFINER` bootstrap from migration
//! `0084` (PR #284), which stays deliberately unscoped (it is how the account
//! is discovered). It must be the ocean's owner and not the caller's active
//! account: both callers reach this after a REACHABILITY check
//! (`routes::meetings::meeting_summary` and the MCP `get_transcript` tool),
//! and reachability is multi-account by design (Model B), so a legitimate
//! reader may sit in a different account than the ocean's owner.
//!
//! Signatures are unchanged: `get_transcript`/`resolve_raw_text_for` already
//! receive the caller's CONTENT-plane `pool` + `decision`, which is everything
//! the dispatch needs. That deliberately avoids a drive-by edit of
//! `mcp/tools/read.rs`, this module's other caller, which a parallel
//! conversion PR owns.
//!
//! ## The org arm is scoped too, and by the acting member (closes #848's gap)
//!
//! [`transcript_scope`] used to return `None` on an Org decision, so this
//! module's two content reads ran straight against the tenant pool with no
//! session GUC. That was written on the rationale "tenant databases get
//! whole-database isolation, not RLS". The isolation is real, and it covers
//! the CROSS-ORG boundary only. `migrations_tenant/0009_tenant_rls` §3 puts a
//! member-ocean policy on `meeting_transcripts` AND on
//! `transcript_corrections`, both resolved through
//! `tenant_rls_visible_graph_ids()`. With neither `app.user_id` nor
//! `app.tenant_actor` set, `tenant_rls_actor_user_id()` is NULL, so that
//! predicate keeps the org and personal oceans and drops every member and
//! team ocean.
//!
//! The symptom that produced was quiet. At OPS-4-T — the ops step that moves
//! each tenant DSN to the NOBYPASSRLS role — `get_transcript` on a meeting in
//! a member-private or team ocean answered `TranscriptError::NotFound`, and
//! its correction list read as "no corrections applied". Neither is an error,
//! so neither logged. Only the table-owner exemption (`0009` §7 does not
//! FORCE row security) was hiding it.
//!
//! The org arm now opens [`crate::tenancy::scope::begin_tenant_scoped`] with
//! the gate-verified acting member, which is the same identity
//! `Tenant::begin_member_scoped` carries on every other org request path. The
//! reviewed service principal was rejected: see [`begin_transcript_scoped`]
//! for what each primitive does and does not enforce.
//!
//! Scope resolution that FAILS is a refusal now, not a fall-through to the
//! unscoped read — the unscoped read fails as a plain empty result, so a
//! fix that degraded to it would leave the operator-visible half of the
//! defect exactly where it was. `backend/tests/transcript_scope_org_rls_pg.rs`
//! is the negative control.

use std::sync::Arc;

use uuid::Uuid;

use crate::{
    routes::meetings::apply_corrections, store::MeetingTranscript, tenancy::extractor::Tenant,
    AppState,
};

/// Fixed chunk size in characters (~4-5k tokens). Deliberately NOT adaptive —
/// a fixed grid is what keeps `total_chunks` stable across an entire
/// `get_transcript` pagination conversation (`docs/plans/remote-mcp-v1.md`
/// §3.2).
pub(crate) const CHUNK_CHARS: usize = 15_000;

// ── Raw text resolution (promoted from routes::meetings) ─────────────────────

/// Resolve a stored transcript's verbatim text: inline when present, else
/// decrypt the R2 blob with the account DEK (mirrors
/// `routes::atoms::get_atom`). Returns `None` when neither path yields text
/// (logged, never fatal) — e.g. R2 configured but the blob fetch failed, or
/// the account has no DEK.
///
/// Tenancy (org-tenancy PR 1.6): `tenant` carries the request's content-plane
/// pool + `TenancyDecision`, and MUST be the same plane `store` resolves on
/// (callers get both off the same `Tenant`) — every axum-routed call site
/// (routes/meetings, routes/shares, routes/session_shares) uses this
/// signature. Delegates to [`resolve_raw_text_for`] (org-tenancy PR 2.3),
/// which the MCP tools call directly instead — MCP has no axum `Tenant` to
/// extract, only its own `mcp::tools::content_tenant`-resolved pool/decision
/// pair.
///
/// DEK resolution dispatches on `tenant.decision` — an org tenant's DEK lives
/// in its OWN `account_keys` table (PR 1.3), never the control-plane
/// `accounts.dek_wrapped` this unconditionally read before the split.
///
/// NOT flag-off byte-identical (declared, mirroring PR 1.4's P2-1 note on
/// `routes::atoms::get_atom`): this used to call the UNCACHED
/// `crypto::resolve_dek`; the dispatch helper's Personal/None arm calls the
/// CACHED `resolve_dek_cached` instead — after a `rotate_dek` this path can
/// now serve a stale DEK for up to the 5-minute cache TTL, where it
/// previously always read fresh. Benign in practice (rotation is rare, and
/// every other DEK-resolving route already reads through the cache) and
/// declared here rather than silently changed.
#[cfg(feature = "test-support")]
pub(crate) async fn resolve_raw_text(
    state: &Arc<AppState>,
    tenant: &Tenant,
    store: &dyn crate::store::Store,
    graph_id: Uuid,
    transcript: &MeetingTranscript,
) -> Option<String> {
    resolve_raw_text_for(
        state,
        store,
        tenant.pool.as_ref(),
        tenant.decision.as_ref(),
        graph_id,
        transcript,
    )
    .await
}

/// Tenancy-aware [`resolve_raw_text`] (org-tenancy PR 2.3): `pool` is the
/// caller's CONTENT-plane pool (tenant pool under an Org decision, `state.db`
/// otherwise) and `decision` drives DEK dispatch — an org transcript's DEK
/// lives in the TENANT `account_keys`, resolved via `resolve_dek_for_context`
/// (whose account-match assert holds: a tenant DB carries exactly one org
/// account). `state` is still needed for R2 (blob storage is shared; keys are
/// per-account) — `state.db` is deliberately NOT read here.
pub(crate) async fn resolve_raw_text_for(
    state: &Arc<AppState>,
    store: &dyn crate::store::Store,
    pool: Option<&sqlx::PgPool>,
    decision: Option<&crate::tenancy::runtime::TenancyDecision>,
    graph_id: Uuid,
    transcript: &MeetingTranscript,
) -> Option<String> {
    if let Some(inline) = transcript.raw_inline.clone() {
        return Some(inline);
    }
    let blob_key = transcript.raw_blob_key.as_deref()?;
    let r2 = state.r2.as_ref()?;
    let db = pool?;
    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(error = %e, "resolve_raw_text: graph account lookup failed");
            return None;
        }
    };
    match crate::crypto::resolve_dek_cached_dispatch(decision, db, account_id, &state.dek_cache)
        .await
    {
        Ok(Some(dek)) => match r2.get_atom_blob(blob_key, &dek).await {
            Ok(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
            Err(e) => {
                tracing::warn!(error = %e, blob_key = %blob_key, "resolve_raw_text: R2 fetch failed");
                None
            }
        },
        Ok(None) => {
            tracing::warn!(account_id = %account_id, "resolve_raw_text: account has no DEK");
            None
        }
        Err(e) => {
            tracing::warn!(error = %e, "resolve_raw_text: DEK resolution failed");
            None
        }
    }
}

// ── Per-statement RLS scope (org-tenancy OPS-4 phase B.5) ────────────────────

/// Where this call's RLS-covered reads must run, and as whom.
///
/// # Opaque on purpose
///
/// This was a type alias, `Option<TranscriptScopeTarget<'a>>`, and that was
/// harmless while it was `pub(crate)`. It stopped being harmless the moment
/// the dispatch was widened to `pub` for a separate-compilation-unit test: an
/// `Option` alias is a constructor for its own `None` at whatever visibility
/// the alias carries. Making it public therefore let any caller write
/// `scoped_get_meeting_transcript(store, None, ..)` and select the UNSCOPED
/// `Store::get_meeting_transcript` branch — the read this whole module exists
/// to remove — without ever calling [`transcript_scope`]. The scope is now a
/// wrapper whose single field is private to this module, so `transcript_scope`
/// is the only way to obtain ANY scope, scoped or unscoped. Nothing outside
/// this module can name the unscoped state into existence.
///
/// The unscoped state means there is genuinely nothing to scope: the request
/// has no content-plane Postgres pool at all (so no RLS). That is the ONLY unscoped case now. A scope that was required but
/// could not be produced is [`ScopeRefused`], never an unscoped scope; see
/// [`transcript_scope`].
///
/// # Visibility
///
/// Crate-visible (with [`transcript_scope`] and
/// [`scoped_get_meeting_transcript`] below) so `routes::shares`' share-viewer
/// payload builder reuses this exact dispatch instead of growing a second,
/// subtly-different copy of it — the meeting-share viewers read the SAME
/// `meeting_transcripts` row under the SAME "ocean owner, per statement" rule
/// this module already established.
///
/// `pub` rather than `pub(crate)` for one further reason, recorded in
/// `services/mod.rs`: `backend/tests/transcript_scope_org_rls_pg.rs` is a
/// separate compilation unit and has to drive the DISPATCH, not just the
/// primitives it is made of — see that file's
/// `the_org_dispatch_is_what_makes_the_read_scoped`. Only the ability to
/// RESOLVE and PASS a scope is widened. The ability to fabricate one is not,
/// and [`is_scoped`](Self::is_scoped) is the whole of what a caller outside
/// the crate can learn from one.
///
/// `Copy` because each RLS-covered read on this path takes one by value, one
/// per statement.
///
/// # The opacity is asserted, not asserted-by-comment
///
/// This PR exists because a scope fix shipped with no automated protection, so
/// the opacity gets protection of its own rather than a paragraph. Both blocks
/// below are doctests: they compile from OUTSIDE the crate — which is the only
/// vantage point where this property is even a question — and they run in the
/// `cargo test --workspace` lane that already gates every push.
///
/// A caller outside the crate cannot fabricate the unscoped state, which is
/// the one input that selects the UNSCOPED `Store::get_meeting_transcript`
/// branch:
///
/// ```compile_fail,E0423
/// use coralswarm_backend::services::transcripts::TranscriptScope;
/// let forged: TranscriptScope<'static> = TranscriptScope(None);
/// ```
///
/// `E0423` is "cannot initialize a tuple struct which contains private
/// fields", the reason that block must fail for. **Measured, not assumed: the
/// annotation is NOT enforced** — this rustdoc passes the test with a
/// deliberately wrong code (`E0308`) pinned, so the code documents the
/// expected reason and does not gate it. What IS enforced is that the block
/// fails at all: neutering its body to `let _ok: u8 = 1;` turns it red with
/// "Test compiled successfully, but it's marked `compile_fail`".
///
/// So the anti-vacuity cover is the second block, not the annotation. A
/// compile-failure test that passes because the type was renamed, moved or
/// deleted proves nothing, so the legitimate surface is compiled in the same
/// breath and a rename breaks it loudly:
///
/// ```
/// use coralswarm_backend::services::transcripts::TranscriptScope;
/// fn inspect(scope: TranscriptScope<'_>) -> bool {
///     scope.is_scoped()
/// }
/// ```
///
/// What that pair does not catch: a change to the WRAPPED type that leaves the
/// field private. `TranscriptScope(None)` would then fail for a different
/// reason and the first block would stay green. That residue is named here
/// rather than papered over — the property under guard is "no external
/// constructor", and it is that property both blocks hold together.
#[derive(Debug, Clone, Copy)]
pub struct TranscriptScope<'a>(Option<TranscriptScopeTarget<'a>>);

impl<'a> TranscriptScope<'a> {
    /// Whether this scope opens a scoped transaction, or is the pool-less
    /// plane's "nothing to scope".
    ///
    /// Read-only and detail-free: no pool, no account, no acting member. It
    /// replaces the `Option::is_some` the alias used to hand out, and it is
    /// the narrowest statement of the mutation that shipped — an `Org`
    /// decision on a live pool must yield a REAL scope. Asserted by
    /// `transcript_scope_org_rls_pg.rs`'s
    /// `the_org_dispatch_is_what_makes_the_read_scoped`.
    ///
    /// Knowing the answer buys no way to change it: there is no constructor.
    pub fn is_scoped(&self) -> bool {
        self.0.is_some()
    }

    /// Lend the resolved target to the two places inside this crate that open
    /// a transaction on it: [`scoped_get_meeting_transcript`] /
    /// [`scoped_list_transcript_corrections`] here, and
    /// `routes::shares::scoped_meeting_occurred_at`.
    ///
    /// `pub(crate)` AND read-only. It lends what [`transcript_scope`] built;
    /// the field itself stays private to this module, so not even a
    /// crate-internal caller can assemble a scope — with a different acting
    /// member, or with none at all.
    #[cfg(feature = "test-support")]
    pub(crate) fn target(&self) -> Option<&TranscriptScopeTarget<'a>> {
        self.0.as_ref()
    }
}

/// A resolved [`TranscriptScope`]: the pool to open on, the account to set,
/// and — on the org plane only — the member the read is made on behalf of.
///
/// Its fields are private to this module and it has no constructor, so
/// [`transcript_scope`] is the only way to obtain one. `routes::shares` holds
/// a [`TranscriptScope`] and hands the target back to
/// [`begin_transcript_scoped`]; it cannot assemble a scope of its own with a
/// different acting user. `pub(crate)` — it is only ever reached through
/// [`TranscriptScope::target`], which is itself `pub(crate)`, so the type is
/// not nameable outside the crate at all.
///
/// `Copy` because each RLS-covered read on this path takes one by value, one
/// per statement.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TranscriptScopeTarget<'a> {
    pool: &'a sqlx::PgPool,
    /// The OCEAN'S owning account — `app.account_id`.
    account_id: Uuid,
    /// The acting member — `app.user_id` — on the org plane only. `None` on
    /// the consumer plane, where no policy reads it.
    member_user_id: Option<Uuid>,
}

/// A scope was required and could not be produced. The caller must refuse the
/// read. It must not fall through to an unscoped one.
///
/// Carries no detail on purpose. [`transcript_scope`] logs the reason where it
/// knows it, and both call sites render every reason to the client the same
/// way: the transcript is not there. A caller able to tell "unknown ocean"
/// from "lookup failed" could leak that difference.
#[derive(Debug)]
pub struct ScopeRefused;

/// Resolve [`TranscriptScope`] for one `(pool, decision, graph_id)`.
///
/// The ORG arm carries the acting member. It used to return `None`, on the
/// rationale "a tenant database gets whole-database isolation, so there is
/// nothing to scope". The isolation is real and it covers the CROSS-ORG
/// boundary only. Inside one tenant database,
/// `migrations_tenant/0009_tenant_rls` puts `member_ocean_isolation` on both
/// tables this scope guards, resolved through
/// `tenant_rls_visible_graph_ids()`, and that predicate hides every
/// member-private and every team ocean from a transaction that set neither
/// `app.user_id` nor `app.tenant_actor`. See [`begin_transcript_scoped`] for
/// which primitive each plane opens and why.
///
/// `Store::graph_account_id` is deliberately UNSCOPED — it is migration 0084's
/// audited `resolve_graph_account_id` SECURITY DEFINER bootstrap, the thing
/// that breaks the "you cannot scope to an account you have not discovered
/// yet" cycle.
pub async fn transcript_scope<'a>(
    store: &dyn crate::store::Store,
    pool: Option<&'a sqlx::PgPool>,
    decision: Option<&crate::tenancy::runtime::TenancyDecision>,
    graph_id: Uuid,
) -> Result<TranscriptScope<'a>, ScopeRefused> {
    // The acting member, taken from the request's own gate-verified
    // `TenantContext` and from nowhere else — the same id
    // `tenancy::reachability`'s `require_matching_user` refuses to serve any
    // other user against, and the same one `Tenant::begin_member_scoped`
    // reads. No handler parameter can reach this value.
    let member_user_id = match decision {
        Some(crate::tenancy::runtime::TenancyDecision::Org(ctx)) => Some(ctx.user_id),
        _ => None,
    };

    let Some(pool) = pool else {
        if member_user_id.is_some() {
            // An org decision with no content-plane pool is a layering bug.
            // Refuse rather than take the pool-less
            // path, which would run the plain Store methods on some other
            // handle.
            tracing::error!(
                %graph_id,
                "transcript_scope: org request carries no content-plane pool; refusing the read"
            );
            return Err(ScopeRefused);
        }
        return Ok(TranscriptScope(None));
    };

    match store.graph_account_id(graph_id).await {
        Ok(Some(account_id)) => Ok(TranscriptScope(Some(TranscriptScopeTarget {
            pool,
            account_id,
            member_user_id,
        }))),
        // No such ocean. Reachability has already passed at every call site,
        // so this is a race with ocean deletion. It must read to the client
        // exactly like the caller's own not-found arm, and it must not become
        // an unscoped read.
        Ok(None) => {
            tracing::warn!(%graph_id, "transcript_scope: no such ocean; refusing the read");
            Err(ScopeRefused)
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                %graph_id,
                "transcript_scope: graph account lookup failed; refusing the read"
            );
            Err(ScopeRefused)
        }
    }
}

/// Open the short transaction ONE RLS-covered read on this path runs in.
///
/// The primitive is chosen by plane, and that choice is the access-control
/// decision on this path:
///
/// * **Org** — [`crate::tenancy::scope::begin_tenant_scoped`]. It sets
///   `app.account_id` AND `app.user_id`, so `0009_tenant_rls`'s
///   `member_ocean_isolation` policy admits the org's commons oceans, the
///   acting member's own member-private oceans, and the team oceans that
///   member belongs to. It admits nothing else. A member-private ocean owned
///   by somebody else stays hidden, which is the check the policy exists to
///   perform.
/// * **Personal / no decision** — [`crate::tenancy::scope::begin_account_scoped`].
///   It sets `app.account_id` only, which is the whole of the consumer plane's
///   boundary (`0083_consumer_rls`). No consumer-plane policy reads
///   `app.user_id`, so setting it there would imply a per-user boundary the
///   consumer plane does not have.
///
/// **Not `begin_tenant_system_scoped_without_account`.** That primitive is the
/// reviewed, opt-in stand-down of the member-ocean backstop, for background
/// work that acts across every ocean in a tenant and has no honest acting
/// user. A transcript read has one: it is made on behalf of a specific viewer.
/// The service principal would satisfy the policy unconditionally, which would
/// end the silent not-found and answer no access-control question at all.
///
/// # The deliberate limit, in both of its shapes
///
/// `0009`'s `member_ocean_isolation` has no SHARE-CAPABILITY arm. It knows
/// ownership and team membership; it does not know that a share token was
/// minted. `routes::shares::view_share` serves a `'link'` share to ANY
/// authenticated caller holding the token and runs no reachability check on
/// it. So a `'link'` share read by a viewer the policy does not admit returns
/// an all-null body, and there are TWO viewers like that, not one:
///
/// * a **member-private** ocean, viewed by an authed org member who does not
///   own it;
/// * a **team** ocean, viewed by an authed org member who is not on that
///   team.
///
/// The second is not a corollary of the first. It fails a different conjunct
/// of the same predicate — a `team_members` membership `EXISTS` rather than an
/// `owner_user_id` match — and it is just as reachable: `create_share` mints a
/// `'link'` share for any ocean its minter can reach, and a team ocean's
/// minter is a teammate who can hand that URL to anybody in the org.
///
/// Both degrade identically, and both are the same result the share path gave
/// before this scope existed. Widening either is a policy change with its own
/// review, not something a service principal should buy silently.
/// `transcript_scope_org_rls_pg.rs`'s
/// `the_org_dispatch_is_what_makes_the_read_scoped` asserts both, so the limit
/// is a measured property rather than a paragraph, and cannot widen by
/// accident.
///
/// `pub(crate)` so `routes::shares`' `atoms` read on the same share-viewer path
/// opens through this same dispatch rather than a second copy of it.
pub(crate) async fn begin_transcript_scoped(
    target: &TranscriptScopeTarget<'_>,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, crate::store::StoreError> {
    match target.member_user_id {
        Some(user_id) => {
            crate::tenancy::scope::begin_tenant_scoped(target.pool, target.account_id, user_id)
                .await
        }
        None => crate::tenancy::scope::begin_account_scoped(target.pool, target.account_id).await,
    }
}

/// A log-safe one-word name for the arm [`begin_transcript_scoped`] will take.
///
/// Kinds only. It deliberately exposes no account, no user and no ocean, so
/// it is safe in any log line on this path.
///
/// It exists so that a read which came back EMPTY can say why it might have,
/// without a second unscoped existence probe to find out — that probe is the
/// read this whole series removed, and it would leak the existence of a row
/// the policy just hid.
///
/// `org-member` is the discriminating value: it means `0009_tenant_rls`'s
/// `member_ocean_isolation` was in force, so "hidden from this viewer" is a
/// live explanation for the empty result alongside "never ingested". Under
/// `account` and `none` no per-user policy applies, and an empty result means
/// the row is not there.
///
/// `pub` for the same reason as [`transcript_scope`]: a log line that
/// mis-states which policy was in force is worse than no log line, so
/// `transcript_scope_org_rls_pg.rs` asserts this function's value on a REAL
/// org scope rather than trusting the match arms by inspection. It returns a
/// static string and reads no field out to its caller, so widening it
/// discloses nothing.
pub fn scope_kind(scope: TranscriptScope<'_>) -> &'static str {
    match scope.0 {
        Some(target) if target.member_user_id.is_some() => "org-member",
        Some(_) => "account",
        None => "none",
    }
}

/// [`crate::store::Store::get_meeting_transcript`] under `scope`.
///
/// A failure to OPEN the scope propagates as `Err`. It never degrades to the
/// unscoped Store method: that read is the defect, and it fails as a plain
/// empty result that the caller cannot tell from a meeting nobody ingested.
pub async fn scoped_get_meeting_transcript(
    store: &dyn crate::store::Store,
    scope: TranscriptScope<'_>,
    graph_id: Uuid,
    source_id: &str,
) -> Result<Option<MeetingTranscript>, crate::store::StoreError> {
    match scope.0 {
        Some(target) => {
            let mut tx = begin_transcript_scoped(&target).await?;
            let row = store
                .get_meeting_transcript_scoped(&mut tx, graph_id, source_id)
                .await;
            // Read-only: rollback is as durable as commit and returns the
            // connection to the pool a statement sooner.
            tx.rollback().await.ok();
            row
        }
        None => store.get_meeting_transcript(graph_id, source_id).await,
    }
}

/// [`crate::store::Store::list_transcript_corrections`] under `scope`. Same
/// refusal contract as [`scoped_get_meeting_transcript`].
async fn scoped_list_transcript_corrections(
    store: &dyn crate::store::Store,
    scope: TranscriptScope<'_>,
    graph_id: Uuid,
    source_id: &str,
) -> Result<Vec<crate::store::TranscriptCorrection>, crate::store::StoreError> {
    match scope.0 {
        Some(target) => {
            let mut tx = begin_transcript_scoped(&target).await?;
            let rows = store
                .list_transcript_corrections_scoped(&mut tx, graph_id, source_id)
                .await;
            tx.rollback().await.ok();
            rows
        }
        None => store.list_transcript_corrections(graph_id, source_id).await,
    }
}

// ── Chunking ──────────────────────────────────────────────────────────────────

/// One page of a chunked transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TranscriptChunk {
    pub text: String,
    pub chunk_index: usize,
    pub total_chunks: usize,
    pub total_chars: usize,
}

/// Compute every chunk boundary (a char index into `chars`, exclusive end of
/// its chunk) for the whole text in one deterministic pass: nominal windows
/// of [`CHUNK_CHARS`] chars, with each window's cut pulled back to the
/// nearest preceding `\n` (keeping the newline itself in the chunk it ends),
/// falling back to the nearest preceding whitespace, falling back to a hard
/// cut at the nominal boundary when neither exists (or would produce an
/// empty/negative-length chunk). Boundaries are strictly increasing and
/// partition `chars` with no gaps or overlaps: this function's own boundary
/// list always round-trips exactly. [`chunk_text`] (the actual public
/// extractor) may still prepend a synthetic speaker label to a continuation
/// chunk's text — see its doc comment — which is the one case where
/// concatenating `chunk_text`'s OUTPUT no longer byte-for-byte reconstructs
/// the original.
///
/// Pure function of `chars` alone — the same text always yields the same
/// boundary list, which is what keeps `total_chunks` and every chunk's
/// content stable across repeated calls (`get_transcript` pagination).
fn chunk_boundaries(chars: &[char]) -> Vec<usize> {
    let len = chars.len();
    let mut boundaries = Vec::new();
    let mut start = 0usize;
    while start < len {
        let nominal_end = (start + CHUNK_CHARS).min(len);
        if nominal_end == len {
            boundaries.push(len);
            break;
        }
        let mut cut = nominal_end;
        if let Some(nl) = (start..nominal_end).rev().find(|&i| chars[i] == '\n') {
            // Keep the newline itself in the emitted (earlier) chunk.
            cut = nl + 1;
        } else if let Some(ws) = (start..nominal_end)
            .rev()
            .find(|&i| chars[i].is_whitespace())
        {
            cut = ws + 1;
        }
        // A pathological pull-back to (or before) `start` must not happen —
        // every chunk has to make forward progress.
        if cut <= start {
            cut = nominal_end;
        }
        boundaries.push(cut);
        start = cut;
    }
    boundaries
}

/// Heuristic line-initial speaker-label detector: `"Name: text"` where the
/// candidate name spans at most 4 words, is at most [`MAX_SPEAKER_LABEL_CHARS`]
/// characters, is non-empty, and isn't a URL. Mirrors
/// `packages/core::chunker::parse_turn`'s colon-form heuristic (the bracket
/// `"[Name] text"` form is never persisted — `Chunk::text_with_speaker` only
/// ever emits the colon form), so a chunk's speaker attribution is detected
/// the same way ingest originally attached it.
///
/// The character cap (in addition to the word-count cap) guards against a
/// pathological transcript whose "name" is one extremely long word (or
/// several words merged with no spaces) — without it, a malformed transcript
/// could produce a synthetic `"[continued] <name>: "` continuation-chunk
/// prefix (see [`chunk_text`]) long enough to push that chunk's text past the
/// documented [`CHUNK_CHARS`] cap (dogfooding gap-sweep #2, item 3).
fn line_speaker_label(line: &str) -> Option<&str> {
    let colon_pos = line.find(':')?;
    let candidate = line[..colon_pos].trim();
    // `candidate` is everything BEFORE the first colon, so it can never
    // itself contain "://" (that requires a colon). Check the line at/after
    // the colon instead, so a line opening with a bare URL like
    // "http://example.com more text" isn't mistaken for a speaker label.
    if candidate.is_empty() || line[colon_pos..].starts_with("://") {
        return None;
    }
    if candidate.split_whitespace().count() > 4 {
        return None;
    }
    if candidate.chars().count() > MAX_SPEAKER_LABEL_CHARS {
        return None;
    }
    Some(candidate)
}

/// Max character length of a detected speaker-label candidate (well over any
/// real speaker name; see [`line_speaker_label`]'s doc comment).
const MAX_SPEAKER_LABEL_CHARS: usize = 80;

/// Scan backward from char index `before` (exclusive) for the most recent
/// line opening with a speaker label ([`line_speaker_label`]), returning the
/// speaker name. Used to re-attribute a chunk that starts mid speaker-turn.
///
/// Item 7, batch 4 maintainability review: scans `chars[..before]` backward
/// line-by-line via index arithmetic, only collecting each CANDIDATE line
/// into an owned `String` (as `line_speaker_label` needs `&str`) rather than
/// the entire preceding transcript prefix up front — the prior version's
/// `chars[..before].iter().collect()` allocated and copied the whole prefix
/// on every call regardless of how close the label actually was, so cost
/// scaled with position in the transcript. `rposition` on a shrinking
/// `chars[..end]` slice still only walks back as far as the previous
/// newline each iteration (it starts scanning from the slice's own end, not
/// index 0), so total work across every iteration is bounded by the
/// distance actually scanned back from `before` — the same total distance a
/// single continuous backward scan would cover, not one restarted per line.
fn last_speaker_before(chars: &[char], before: usize) -> Option<String> {
    let mut end = before.min(chars.len());
    // `str::lines()` doesn't yield a trailing empty line for text ending in
    // '\n' (there's no "line after the final terminator") — peel that
    // guaranteed-empty, guaranteed-non-matching piece off up front so the
    // loop below never tests it; the terminator itself still correctly
    // bounds the PRECEDING piece via the search below. Verified equivalent
    // to the old `.lines().rev()` behavior by exhaustive fuzz comparison
    // across ~7M random (string, cut-point) pairs, including runs of
    // consecutive newlines and `\r\n`.
    if end > 0 && chars[end - 1] == '\n' {
        end -= 1;
    }
    loop {
        let nl = chars[..end].iter().rposition(|&c| c == '\n');
        let line_start = nl.map_or(0, |i| i + 1);
        let line: String = chars[line_start..end].iter().collect();
        if let Some(label) = line_speaker_label(&line) {
            return Some(label.to_string());
        }
        match nl {
            Some(i) => end = i,
            None => return None,
        }
    }
}

/// Extract chunk `chunk_index` of `text` under the fixed-size, newline-aware
/// scheme (see [`chunk_boundaries`]). `Err` names the valid range when
/// `chunk_index` is out of bounds (`invalid_params` per §3.2).
///
/// Continuation-chunk speaker re-attribution (dogfooding gap-sweep #2, item
/// 6, 2026-07-18): `chunk_boundaries` has no notion of a speaker turn, so a
/// turn long enough to straddle a chunk boundary produces a continuation
/// chunk with no leading `"Name: "` label — the ONLY place attribution lives
/// in the raw/corrected text. When a non-first chunk doesn't itself open with
/// a detected label, this looks back into the text preceding the chunk for
/// the last such label and prepends a synthetic `"[continued] Name: "`
/// marker, so the chunk is self-describing in isolation. This means
/// concatenating every chunk's text no longer byte-for-byte reconstructs the
/// original for transcripts with a mid-boundary speaker turn (an intentional,
/// display-only exception to the otherwise-exact round-trip guarantee).
pub(crate) fn chunk_text(text: &str, chunk_index: usize) -> Result<TranscriptChunk, String> {
    let chars: Vec<char> = text.chars().collect();
    let total_chars = chars.len();
    let boundaries = chunk_boundaries(&chars);
    // Empty text still yields exactly one (empty) chunk at index 0, rather
    // than zero valid chunk indices.
    let total_chunks = boundaries.len().max(1);
    if chunk_index >= total_chunks {
        return Err(format!(
            "chunk_index must be in range 0..{total_chunks} (got {chunk_index})"
        ));
    }
    let start = if chunk_index == 0 {
        0
    } else {
        boundaries[chunk_index - 1]
    };
    let end = boundaries.get(chunk_index).copied().unwrap_or(0);
    let mut text: String = chars[start..end].iter().collect();

    if chunk_index > 0 {
        let opens_with_label = text.lines().next().and_then(line_speaker_label).is_some();
        if !opens_with_label {
            if let Some(speaker) = last_speaker_before(&chars, start) {
                let prefixed = format!("[continued] {speaker}: {text}");
                // `text` alone is already bounded to `CHUNK_CHARS` by
                // `chunk_boundaries`, but prepending the synthetic label is
                // OUTSIDE that boundary calculation — `speaker` is capped at
                // `MAX_SPEAKER_LABEL_CHARS` by `line_speaker_label`, which
                // keeps this an unlikely case, but defensively trim the
                // trailing content (never the prefix itself) so a chunk's
                // returned text never exceeds the documented `CHUNK_CHARS`
                // cap regardless (dogfooding gap-sweep #2, item 3).
                text = if prefixed.chars().count() > CHUNK_CHARS {
                    prefixed.chars().take(CHUNK_CHARS).collect()
                } else {
                    prefixed
                };
            }
        }
    }

    Ok(TranscriptChunk {
        text,
        chunk_index,
        total_chunks,
        total_chars,
    })
}

// ── get_transcript: variant selection + chunk ─────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TranscriptVariant {
    Raw,
    Corrected,
}

impl TranscriptVariant {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TranscriptVariant::Raw => "raw",
            TranscriptVariant::Corrected => "corrected",
        }
    }
}

pub(crate) struct GetTranscriptResult {
    pub variant: &'static str,
    pub chunk_index: usize,
    pub total_chunks: usize,
    pub total_chars: usize,
    pub corrections_applied: usize,
    pub text: String,
}

pub(crate) enum TranscriptError {
    /// The meeting row doesn't exist (or isn't reachable — callers never
    /// distinguish the two, per the IDOR non-leak convention).
    NotFound,
    /// The row exists but its text could not be resolved (R2/DEK failure) —
    /// distinct from `NotFound` per §3.2.
    Unavailable,
    /// `chunk_index >= total_chunks`; message names the valid range.
    InvalidChunkIndex(String),
}

/// Load a meeting's transcript, apply the requested variant's
/// corrections overlay (`corrected` only; raw text is never mutated), and
/// return the requested chunk. `graph_id`/`source_id` are assumed already
/// authorized by the caller (reachability re-derived from `user_id`, never
/// from caller-supplied ids alone).
///
/// `pool`/`decision` are the caller's CONTENT-plane pool + tenancy decision
/// (org-tenancy PR 2.3) — see [`resolve_raw_text_for`]. The sole caller (the
/// MCP `get_transcript` tool) passes its `Tenant`-resolved handles; a
/// personal/flag-off caller passing `(state.db.as_ref(), None)` is
/// byte-identical to the pre-2.3 behavior.
//
// 8 params: the pre-2.3 six plus the tenancy pair (pool + decision), which
// must travel together (the DEK dispatch reads both) — same rationale as
// `CoralSwarmMcp::audit`'s existing allow.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn get_transcript(
    state: &Arc<AppState>,
    tenant: &crate::tenancy::extractor::Tenant,
    user: &crate::UserContext,
    graph_id: Uuid,
    source_id: &str,
    variant: TranscriptVariant,
    chunk_index: usize,
) -> Result<GetTranscriptResult, TranscriptError> {
    if tenant.pool.is_some() && matches!(variant, TranscriptVariant::Corrected) {
        return Err(TranscriptError::Unavailable);
    }
    let store = tenant.store.as_ref().ok_or(TranscriptError::NotFound)?;
    let store = store.as_ref();
    let pool = tenant.pool.as_ref();
    let decision = tenant.decision.as_ref();
    // The legacy meeting projection is not proof of a permitted source version.
    let captured = if pool.is_some() {
        let account = crate::source_evidence::authorize(tenant, user, graph_id)
            .await
            .map_err(|_| TranscriptError::NotFound)?;
        if !crate::source_evidence::still_admitted(state, tenant, user, graph_id, account).await {
            return Err(TranscriptError::NotFound);
        }
        let mut tx = tenant
            .begin_member_scoped(account)
            .await
            .map_err(|_| TranscriptError::NotFound)?;
        // The reader's team-seat history floor (#1545) on the meeting itself;
        // `load_version_scoped` floors the version it then opens.
        let id:Option<Uuid>=sqlx::query_scalar(&format!("SELECT v.version_id FROM meeting_transcripts m JOIN source_items s ON s.graph_id=m.graph_id AND s.source_item_id=m.source_item_id JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.version_id=s.current_version_id WHERE m.graph_id=$1 AND m.source_id=$2 AND s.access_state='permitted' AND {floor}", floor = crate::history_floor::admits_transcript("m"))).bind(graph_id).bind(source_id).fetch_optional(&mut *tx).await.map_err(|_|TranscriptError::NotFound)?;
        let id = id.ok_or(TranscriptError::NotFound)?;
        let version = crate::source_evidence::load_version_scoped(&mut tx, graph_id, id)
            .await
            .map_err(|_| TranscriptError::NotFound)?
            .ok_or(TranscriptError::NotFound)?;
        tx.commit().await.map_err(|_| TranscriptError::NotFound)?;
        Some((account, version))
    } else {
        None
    };
    // RLS scoping (org-tenancy OPS-4 phase B.5): resolve the OCEAN'S owning
    // account once, then run each RLS-covered read in its own short scoped
    // transaction — see the module doc for why per-statement rather than one
    // per call. `None` means ONE thing: no content-plane pool at all (so no
    // RLS). The org plane is scoped like
    // every other plane, and an owner that will not resolve is a refusal.
    let scope = match transcript_scope(store, pool, decision, graph_id).await {
        Ok(s) => s,
        // Already logged with its reason. Refuse. Falling through to an
        // unscoped read would produce this same `NotFound` on the org plane
        // with nothing in the log to say why.
        Err(ScopeRefused) => return Err(TranscriptError::NotFound),
    };

    let transcript = match scoped_get_meeting_transcript(store, scope, graph_id, source_id).await {
        Ok(Some(t)) => t,
        // The read SUCCEEDED and returned nothing. Two causes look identical
        // from here: the meeting was never ingested, or `0009_tenant_rls`'s
        // member-ocean policy hid it from this viewer. Telling them apart
        // would need a second unscoped existence probe, which is the read
        // this series removed.
        //
        // `warn!`, not `error!`: nothing failed. When the cause is the
        // policy, the policy did its job, and an `error!` here would page
        // somebody for a correct refusal. `warn!`, not `info!`/`debug!`: the
        // hazard being removed is SILENCE at production log levels — "a
        // viewer got an empty transcript because the policy hid it" was
        // indistinguishable from a genuinely empty transcript, and that is
        // the failure mode this whole series exists to remove.
        //
        // Ids and kinds only: the ocean id and the scope kind. No viewer
        // identity, no `source_id`, no content.
        Ok(None) => {
            tracing::warn!(
                %graph_id,
                scope = scope_kind(scope),
                "get_transcript: the scoped read returned no transcript row; answering \
                 NotFound. Under scope=org-member this is EITHER a meeting nobody ingested OR \
                 one the member-ocean policy hid from this viewer"
            );
            return Err(TranscriptError::NotFound);
        }
        Err(e) => {
            tracing::error!(error = %e, "get_transcript: transcript load error");
            return Err(TranscriptError::NotFound);
        }
    };

    let raw_text = match &captured {
        Some((account, version)) => {
            crate::source_evidence::read_text(state, tenant, *account, version).await
        }
        None => resolve_raw_text_for(state, store, pool, decision, graph_id, &transcript).await,
    }
    .ok_or(TranscriptError::Unavailable)?;
    if captured
        .as_ref()
        .and_then(|(_, v)| v.content_sha256.as_ref())
        .is_some_and(|h| h != &crate::services::source_reingest::content_hash_hex(&raw_text))
    {
        return Err(TranscriptError::NotFound);
    };

    // A failure here used to vanish into `unwrap_or_default()`: the caller was
    // served the RAW text and told zero corrections were applied, which is
    // indistinguishable from a meeting that has none. The read is degraded on
    // purpose (a correction overlay is an enrichment, and losing it must not
    // fail the whole transcript), but it is no longer silent.
    let corrections = match scoped_list_transcript_corrections(store, scope, graph_id, source_id)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(
                error = %e,
                %graph_id,
                "get_transcript: corrections load failed; serving the transcript with none applied"
            );
            Vec::new()
        }
    };
    let applied: Vec<(String, String)> = corrections
        .iter()
        .filter(|c| c.status == "applied")
        .map(|c| (c.surface_form.clone(), c.canonical.clone()))
        .collect();
    let corrections_applied = applied.len();

    let text = match variant {
        TranscriptVariant::Raw => raw_text,
        TranscriptVariant::Corrected => apply_corrections(&raw_text, &applied),
    };

    let chunk = chunk_text(&text, chunk_index).map_err(TranscriptError::InvalidChunkIndex)?;

    if let Some((account, version)) = &captured {
        if !crate::source_evidence::still_admitted(state, tenant, user, graph_id, *account).await {
            return Err(TranscriptError::NotFound);
        }
        let mut tx = tenant
            .begin_member_scoped(*account)
            .await
            .map_err(|_| TranscriptError::NotFound)?;
        let valid:bool=sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM source_items s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id WHERE s.graph_id=$1 AND s.source_item_id=$2 AND s.access_state='permitted' AND s.current_version_id=$3 AND v.version_id=$3 AND v.raw_inline IS NOT DISTINCT FROM $4 AND v.raw_blob_key IS NOT DISTINCT FROM $5 AND v.content_sha256 IS NOT DISTINCT FROM $6 AND {floor})", floor = crate::history_floor::admits_version("v")))
            .bind(graph_id).bind(version.source_item_id).bind(version.version_id).bind(&version.raw_inline).bind(&version.raw_blob_key).bind(&version.content_sha256).fetch_one(&mut *tx).await.map_err(|_|TranscriptError::NotFound)?;
        tx.commit().await.map_err(|_| TranscriptError::NotFound)?;
        if !valid {
            return Err(TranscriptError::NotFound);
        }
    }
    Ok(GetTranscriptResult {
        variant: variant.as_str(),
        chunk_index: chunk.chunk_index,
        total_chunks: chunk.total_chunks,
        total_chars: chunk.total_chars,
        corrections_applied,
        text: chunk.text,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_reconstructs_original_text() {
        let mut text = String::new();
        for i in 0..2000 {
            text.push_str(&format!("line {i} of the transcript with some words\n"));
        }
        let total = chunk_text(&text, 0).unwrap().total_chunks;
        let mut reconstructed = String::new();
        for i in 0..total {
            reconstructed.push_str(&chunk_text(&text, i).unwrap().text);
        }
        assert_eq!(reconstructed, text);
    }

    #[test]
    fn short_text_is_a_single_chunk() {
        let text = "hello world";
        let chunk = chunk_text(text, 0).unwrap();
        assert_eq!(chunk.total_chunks, 1);
        assert_eq!(chunk.text, text);
        assert_eq!(chunk.total_chars, text.chars().count());
    }

    #[test]
    fn empty_text_is_one_empty_chunk() {
        let chunk = chunk_text("", 0).unwrap();
        assert_eq!(chunk.total_chunks, 1);
        assert_eq!(chunk.text, "");
        assert_eq!(chunk.total_chars, 0);
    }

    #[test]
    fn chunk_index_at_or_past_total_is_an_error() {
        let text = "short text";
        let chunk = chunk_text(text, 0).unwrap();
        assert!(chunk_text(text, chunk.total_chunks).is_err());
        assert!(chunk_text(text, chunk.total_chunks + 5).is_err());
    }

    #[test]
    fn ceiling_over_long_text_produces_multiple_chunks() {
        // Build text comfortably over 3 chunk widths with frequent newlines
        // so cuts land on them, not the whitespace/hard-cut fallback.
        let mut text = String::new();
        while text.chars().count() < CHUNK_CHARS * 3 + 500 {
            text.push_str("the quick brown fox jumps over the lazy dog\n");
        }
        let first = chunk_text(&text, 0).unwrap();
        assert!(first.total_chunks >= 3);
        // Every non-final chunk should end on a real char boundary at or
        // before the nominal window — i.e. no chunk exceeds CHUNK_CHARS.
        for i in 0..first.total_chunks {
            let c = chunk_text(&text, i).unwrap();
            assert!(c.text.chars().count() <= CHUNK_CHARS);
        }
    }

    #[test]
    fn chunk_boundaries_prefer_newlines_over_hard_cuts() {
        // A newline sits just before the nominal boundary; the chunk should
        // end there rather than mid-word at the hard 15000 cut.
        let mut text = String::new();
        for _ in 0..(CHUNK_CHARS - 10) {
            text.push('a');
        }
        text.push('\n');
        text.push_str("the rest of the transcript continues here");
        let chunk0 = chunk_text(&text, 0).unwrap();
        assert!(chunk0.text.ends_with('\n'));
    }

    #[test]
    fn corrections_overlay_applied_only_to_corrected_variant() {
        let raw = "Jordan spoke about the budget.";
        let applied = vec![("Jordan".to_string(), "Morgan".to_string())];
        let corrected = apply_corrections(raw, &applied);
        assert_eq!(corrected, "Morgan spoke about the budget.");
        // Raw stays verbatim — the overlay is display-only, never mutates
        // storage (asserted at the `apply_corrections` unit-test layer too).
        assert_eq!(raw, "Jordan spoke about the budget.");
    }

    // ── Continuation-chunk speaker re-attribution (dogfooding gap-sweep #2,
    // item 6) ─────────────────────────────────────────────────────────────

    #[test]
    fn line_speaker_label_accepts_a_short_colon_prefixed_name() {
        assert_eq!(line_speaker_label("Alice: hello there"), Some("Alice"));
        assert_eq!(line_speaker_label("Bob Jones: hi"), Some("Bob Jones"));
    }

    #[test]
    fn line_speaker_label_rejects_long_prefixes_and_no_colon() {
        assert_eq!(
            line_speaker_label("this is way too many words before the colon: text"),
            None
        );
        assert_eq!(line_speaker_label("no colon in this line at all"), None);
    }

    /// Dogfooding gap-sweep #2, item 3: a pathologically long single "word"
    /// before the colon (well within the 4-word cap, but far past any real
    /// speaker name) must still be rejected on character length alone.
    #[test]
    fn line_speaker_label_rejects_an_extremely_long_candidate() {
        let long_name = "a".repeat(200);
        let line = format!("{long_name}: hello there");
        assert_eq!(line_speaker_label(&line), None);

        // A candidate right at the boundary is still accepted...
        let boundary_name = "a".repeat(MAX_SPEAKER_LABEL_CHARS);
        let boundary_line = format!("{boundary_name}: hi");
        assert_eq!(
            line_speaker_label(&boundary_line),
            Some(boundary_name.as_str())
        );

        // ...one character over is rejected.
        let over_name = "a".repeat(MAX_SPEAKER_LABEL_CHARS + 1);
        let over_line = format!("{over_name}: hi");
        assert_eq!(line_speaker_label(&over_line), None);
    }

    /// checked+fixed items #3 (batch 1): a bare URL at the start of a line
    /// must not be mistaken for a speaker label. Before the fix, the "://"
    /// guard checked `candidate` (the substring BEFORE the first colon),
    /// which by construction can never contain "://" — so this line's
    /// `candidate` was `"http"`, which passed every check.
    #[test]
    fn line_speaker_label_rejects_a_leading_url() {
        assert_eq!(line_speaker_label("http://example.com is the link"), None);
        assert_eq!(
            line_speaker_label("https://example.com/path?x=1 see this"),
            None
        );
    }

    /// Item 7, batch 4 maintainability review: `last_speaker_before` was
    /// rewritten to scan backward via index arithmetic instead of collecting
    /// the whole preceding prefix into a `String` before calling `.lines()`.
    /// Direct coverage of the resulting behavior, including the fiddly
    /// `str::lines()` edge case (a trailing '\n' doesn't produce an extra
    /// empty "line") that the rewrite has to replicate by hand — cross-
    /// checked against the old `.lines().rev()` implementation via an
    /// exhaustive fuzz comparison (not itself part of this suite) before
    /// landing.
    #[test]
    fn last_speaker_before_finds_the_nearest_preceding_label() {
        let text = "Alice: hi there\nBob: how are you\nsome more text";
        let chars: Vec<char> = text.chars().collect();
        // Cut point mid-way through "some more text" — nearest label back is Bob.
        assert_eq!(
            last_speaker_before(&chars, chars.len()),
            Some("Bob".to_string())
        );
        // Cut point right after Alice's line — nearest (and only) label is Alice.
        let cut = "Alice: hi there\n".chars().count();
        assert_eq!(last_speaker_before(&chars, cut), Some("Alice".to_string()));
    }

    #[test]
    fn last_speaker_before_handles_no_label_and_empty_input() {
        let text = "just some prose\nwith no speaker labels at all";
        let chars: Vec<char> = text.chars().collect();
        assert_eq!(last_speaker_before(&chars, chars.len()), None);
        assert_eq!(last_speaker_before(&[], 0), None);
    }

    #[test]
    fn last_speaker_before_skips_a_trailing_newline_without_a_phantom_empty_line() {
        // A trailing '\n' must not be mistaken for an extra empty "line" —
        // `str::lines()` doesn't emit one for text ending in the terminator,
        // and the nearest label (Alice) must still be found looking further
        // back, exactly as the old `.lines().rev()` implementation did.
        let text = "Alice: hi\n\n\n";
        let chars: Vec<char> = text.chars().collect();
        assert_eq!(
            last_speaker_before(&chars, chars.len()),
            Some("Alice".to_string())
        );
    }

    #[test]
    fn continuation_chunk_gets_a_synthetic_speaker_label() {
        // Alice's single turn is long enough to straddle a chunk boundary —
        // no newline/speaker label anywhere near the nominal cut, so the
        // fallback hard-cut (or whitespace cut) lands mid-turn.
        let mut text = String::from("Alice: ");
        while text.chars().count() < CHUNK_CHARS + 500 {
            text.push_str("word ");
        }
        let chunk0 = chunk_text(&text, 0).unwrap();
        assert!(
            chunk0.total_chunks >= 2,
            "expected the turn to span >=2 chunks"
        );
        assert!(chunk0.text.starts_with("Alice: "));

        let chunk1 = chunk_text(&text, 1).unwrap();
        assert!(
            chunk1.text.starts_with("[continued] Alice: "),
            "continuation chunk should be re-labeled, got: {:?}",
            &chunk1.text[..chunk1.text.len().min(40)]
        );
    }

    #[test]
    fn chunk_that_already_opens_with_a_label_is_left_alone() {
        // A newline just before the nominal boundary lets the cut land
        // exactly at the start of a new speaker's turn — no re-labeling
        // needed since the chunk already opens with its own label.
        let mut text = String::new();
        for _ in 0..(CHUNK_CHARS - 10) {
            text.push('a');
        }
        text.push('\n');
        text.push_str("Bob: the rest of the transcript continues here");
        let chunk1 = chunk_text(&text, 1).unwrap();
        assert!(chunk1.text.starts_with("Bob: "));
        assert!(!chunk1.text.starts_with("[continued]"));
    }

    #[test]
    fn continuation_chunk_with_no_prior_speaker_is_left_unlabeled() {
        // No colon-labeled turn anywhere in the text — nothing to attribute,
        // so the continuation chunk is returned as-is (also what keeps the
        // plain round-trip tests above valid for unlabeled transcripts).
        let mut text = String::new();
        while text.chars().count() < CHUNK_CHARS + 500 {
            text.push_str("plain prose with no speaker turns at all, just words. ");
        }
        let chunk1 = chunk_text(&text, 1).unwrap();
        assert!(!chunk1.text.starts_with("[continued]"));
    }

    #[test]
    fn chunking_is_stable_across_repeated_calls() {
        let mut text = String::new();
        for i in 0..3000 {
            text.push_str(&format!("entry number {i}\n"));
        }
        let first_pass: Vec<TranscriptChunk> = {
            let total = chunk_text(&text, 0).unwrap().total_chunks;
            (0..total).map(|i| chunk_text(&text, i).unwrap()).collect()
        };
        let second_pass: Vec<TranscriptChunk> = {
            let total = chunk_text(&text, 0).unwrap().total_chunks;
            (0..total).map(|i| chunk_text(&text, i).unwrap()).collect()
        };
        assert_eq!(first_pass, second_pass);
    }
}

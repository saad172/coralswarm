//! Deletion (§7.3 of docs/plans/graph-temporal-memory.md) — the primitive
//! that closes the compliance gap the plan opens with: **there is no
//! `DELETE` on atoms or nodes anywhere in the router.** You can disconnect
//! Slack but not remove what it ingested.
//!
//! # One primitive, three entry points
//!
//! ```text
//! delete_atom_cascade(atom_id):
//!     atom row + node_atoms links
//!   + R2 blob
//!   + any reef left with zero corals
//!   + edges cascade via FK
//!
//! entry points:
//!   coral    one atom_id                          -> delete_coral
//!   source   all atoms WHERE source_id = X         -> delete_source
//!   subject  all atoms in a person-reef's node_atoms -> delete_subject
//!
//! propagation: match (source_type, source_id) across
//! reachable oceans, so a promoted copy dies with its original
//! ```
//!
//! [`delete_atom_cascade`] is the single mechanism every entry point bottoms
//! out in — it deletes exactly ONE atom row (+ its `node_atoms` links, its R2
//! blob, and any reef that atom left coral-less), in the SAME idempotent,
//! retry-safe transaction shape [`crate::services::retraction::
//! unpublish_atom`] already proved for the structurally identical §4.9
//! problem (see "Reused from §4.9" below). The three entry points differ only
//! in HOW they resolve the *set* of atom ids to feed it, and — for all three
//! — in expanding that set across every ocean in the SAME account that holds
//! a promoted copy sharing the same `(source_type, source_id)` (see
//! "Propagation" below).
//!
//! # Reused from §4.9, verbatim
//!
//! The R2-vs-Postgres commit ordering is copy-identical to
//! `services::retraction::unpublish_atom` (which is itself copy-identical to
//! `workers::member_ocean_reconciler::purge_one_graph`): delete the blob
//! FIRST, inside the transaction that later deletes the rows, and commit
//! LAST. Re-derived reasoning is not repeated here — see that module's own
//! doc comment for the full "what happens when Postgres and R2 disagree"
//! account, which applies to this file unchanged. The coral-less-reef rule
//! (`nodes` has no FK to `atoms`, so a natively-derived reef would otherwise
//! survive its own evidence and keep stating a deleted claim as a bare,
//! uncited title) is also unchanged from §4.9 — same rule, same reason, same
//! implementation shape.
//!
//! # What did NOT carry over from §4.9, and why: authorization
//!
//! Retraction (§4.9) is "take back something you published into SOMEONE
//! ELSE'S ocean" — its authority question is "who gets to reverse a
//! disclosure decision that was never theirs to make", answered with
//! "only the original promoter", failing closed when provenance is absent.
//!
//! **Deletion is a different operation on a different object.** It removes
//! content from the CALLER'S OWN ocean(s) — the one(s) their account
//! actually owns — not a copy that crossed into somebody else's. There is no
//! "original promoter" question to ask here at all: nobody is reversing
//! anyone else's disclosure, they are exercising authority over their own
//! account's data. So this module does NOT import `check_promoter`, and does
//! NOT fail closed on "no provenance recorded" — that concept does not apply;
//! every atom this module touches is scoped, by construction, to graphs
//! belonging to ONE account (see [`resolve_account_id_for_graph`] and every
//! entry point's `WHERE g.account_id = $account_id` predicate).
//!
//! Two different gates for three entry points, decided deliberately rather
//! than inherited wholesale:
//!
//! * **[`delete_coral`] / [`delete_source`]** — gated on ordinary graph
//!   REACHABILITY (`ReachabilityResolver::graph_reachable_by_user`, checked by
//!   the route layer before either function is called), the SAME bar every
//!   other ocean-mutating endpoint in this router already clears
//!   (`promote_source`, `dedup_entities`, `patch_node`, …). Deleting a single
//!   coral, or every copy of a single ingested source event, is bounded,
//!   targeted, and no more consequential than the other member-initiated
//!   writes that already only require reachability. There is no reason to
//!   hold this pair to a higher bar than the rest of the router when neither
//!   one, alone, can reach outside the caller's own account (propagation is
//!   account-scoped — see below).
//!
//! * **[`delete_subject`]** — gated on the caller's `account_members.role`
//!   being `owner` or `admin` (mirroring the existing `escrow.rs`/
//!   `teams::invite_member` "admin-scoped" pattern — see [`is_admin_role`]),
//!   NOT merely reachability. This is the deliberately STRONGER gate the
//!   plan's brief asks for, and the justification is specific to this one
//!   entry point: subject deletion is a single bulk action, driven by a
//!   heuristic (reef membership), that can delete corals about — or merely
//!   incidentally mentioning — OTHER PEOPLE, all at once, with no per-coral
//!   review. An ordinary member being able to unilaterally erase a
//!   colleague's evidentiary trail from the shared ocean on the strength of
//!   an extraction heuristic is a materially bigger blast radius than "I
//!   deleted the coral (or the ingested source) I personally have a
//!   complaint about" — and it is exactly the shape of action an account's
//!   admins/owners, not any one member, should be the ones authorizing. This
//!   check is enforced INSIDE [`delete_subject`] itself (not only at the
//!   route), so a future second call site cannot reintroduce the weaker gate
//!   by accident — the same defense-in-depth reasoning
//!   `services::retraction`'s module doc gives for keeping its own check
//!   inside the transaction rather than trusting the route alone.
//!
//! No admin/owner override exists for [`delete_coral`]/[`delete_source`], and
//! no further escalation beyond admin/owner exists for [`delete_subject`] —
//! both are deliberate, matching §4.9's own "no ocean-owner override" stance:
//! adding one is a product decision, not something to smuggle in here.
//!
//! # Propagation
//!
//! `source_id` (paired with `source_type`) is a coral's identity ACROSS
//! oceans (`UNIQUE (graph_id, source_type, source_id)` — the same natural key
//! `services::promotion` already uses to dedupe a copy on re-promote, see its
//! `ON CONFLICT (graph_id, source_type, source_id) DO NOTHING`). That is what
//! makes propagation a LOOKUP rather than a graph walk: to find every
//! promoted copy of a coral, look up every OTHER atom row sharing its
//! `(source_type, source_id)` — no traversal of `node_promotions` or
//! `source_publications` is needed, because promotion (same-pool or cross-DB
//! org) always preserves this pair on copy.
//!
//! **"Reachable oceans" here means every graph in the SAME account as the
//! atom(s) an entry point starts from** — resolved via
//! [`resolve_account_id_for_graph`], never trusted from the caller. This is
//! deliberately narrower than the full cross-plane reachability union
//! `tenancy::reachability` computes for READS: propagation must never be able
//! to reach a DIFFERENT account's graphs (that would turn "delete my own
//! coral" into "delete a coral belonging to an account I don't control"),
//! whereas a same-pool promotion between two graphs of ONE account (e.g. a
//! personal ocean promoted into that account's team ocean) is squarely inside
//! what this module is entitled to touch.
//!
//! **Known, documented gap — cross-DB org promotion is NOT reached.** A
//! coral promoted from the consumer/control plane into a separate ORG TENANT
//! database (`services::promotion`'s cross-DB path) lives in a physically
//! different Postgres database; no single SQL statement (and no single
//! `PgPool`) can see both sides at once. This module's propagation query
//! therefore only ever deletes copies living in the SAME physical database as
//! the graph the entry point started from — same-pool promotions (personal ↔
//! team ↔ org-within-one-account) are fully covered; cross-DB org-promoted
//! copies are NOT deleted by this call and must be retracted separately
//! (`services::retraction::unpublish_atom`, target-side, from within the
//! target tenant database) or cleaned up by a future cross-pool orchestrator.
//! This is the SAME shape of accepted, stated gap `services::retraction`'s
//! own module doc records for pre-migration-0111 cross-plane provenance — a
//! real limitation, named here rather than silently left for someone to
//! discover.
//!
//! Propagation is computed ONCE per entry-point call (a single upfront
//! `SELECT`), then each resolved `(graph_id, atom_id)` pair is deleted by its
//! own call to [`delete_atom_cascade`] in its OWN transaction — see that
//! function's doc for why (idempotency: a failure partway through a large
//! propagated set leaves the already-deleted atoms deleted, and a retry of
//! the WHOLE entry-point call simply re-resolves a smaller remaining set and
//! continues; nothing to resume by index). Accepted race, stated plainly: a
//! promotion that completes AFTER the upfront `SELECT` but before the loop
//! finishes is not caught by that same call — a subsequent retry (of the same
//! entry point) is required to catch it, exactly the kind of residual window
//! `services::retraction` accepts for its own commit-ambiguity case.
//!
//! # Subject deletion is APPROXIMATE — read before wiring a compliance claim
//! to it
//!
//! See [`delete_subject`]'s own doc comment for the caveat that must reach a
//! customer, not just this file.
//!
//! # RLS scoping — FIXED (was a live, hard-failing bug)
//!
//! Every function in this module used to run its reads/writes against a bare
//! `pool`/`pool.begin()` — no `tenancy::scope::begin_account_scoped` anywhere
//! — even though `atoms`, `nodes`, `node_atoms` and `graphs` are all on
//! migration `0083`'s consumer-plane RLS roster. That included
//! [`resolve_account_id_for_graph`] itself, which ran a raw
//! `SELECT account_id FROM graphs WHERE id = $1` directly against `graphs` —
//! an RLS-covered table — rather than the audited `resolve_graph_account_id`
//! `SECURITY DEFINER` bootstrap every other call site in this codebase uses
//! for exactly this "which account owns this graph?" question.
//!
//! Verified against production: `DATABASE_URL`'s user IS `coralswarm_app` and
//! `rolbypassrls = f`. `current_setting('app.account_id')` with no
//! `missing_ok` (0083's deliberate strict form) RAISES rather than returning
//! zero rows, so this was a hard database error on every call to
//! `DELETE /v1/graphs/{graph_id}/corals/{atom_id}`,
//! `DELETE /v1/graphs/{graph_id}/sources` and
//! `DELETE /v1/graphs/{graph_id}/subjects/{node_id}` — not a dormant,
//! theoretical gap. See `backend/tests/retraction_deletion_rls_pg.rs` for the
//! app-role reproduction (fails without this fix, passes with it).
//!
//! The fix, in one sentence: [`resolve_account_id_for_graph`] now calls
//! `resolve_graph_account_id` instead of reading `graphs` raw, and every
//! subsequent read/write in the three entry points and
//! [`delete_atom_cascade`] runs inside a transaction opened through
//! `begin_account_scoped(pool, account_id)` — the SAME `account_id`
//! [`resolve_account_id_for_graph`] resolved once per entry-point call, reused
//! unchanged across the whole propagation set (propagation is deliberately
//! restricted to ONE account — see "Propagation" above — so this is not an
//! approximation). Each entry point's own setup reads (the exists/entity
//! checks, [`expand_by_source_identity`]) run in ONE short-lived scoped
//! transaction that is rolled back (read-only); [`delete_atom_cascade`] then
//! opens its OWN separate scoped transaction per resolved atom, exactly as
//! before this fix — `begin_account_scoped` is `pool.begin()` plus one extra
//! `set_config`, so none of this module's ordering/idempotency/retry
//! guarantees changed, only whether the connection they run on is now also
//! RLS-scoped.

use std::collections::HashSet;

use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_core::chunker::ChunkStrategy;
use coralswarm_core::r2::{validate_blob_key, R2Error};

use crate::services::blob_store::PromotionBlobStore;
use crate::store::{append_op_tx, sync};

/// D6/D7's fixed outcome vocabulary (B2 unit 02, FM-5) — the ONLY three
/// values a `retraction_audit_log.outcome` row may hold, and the exact
/// strings the D6 UI (built separately, not by this module) renders. Do not
/// add a fourth without updating the migration's `CHECK` constraint.
pub mod outcome {
    pub const REMOVED: &str = "removed";
    pub const OUT_OF_WINDOW: &str = "out_of_window";
    pub const NOT_A_MEMBER: &str = "not_a_member";
}

/// One per-(ocean, surface) outcome (D7's refinement — keyed by
/// `(ocean, surface)`, not by ocean alone, so "removed from this ocean but
/// its summaries were not rewritten" is expressible) produced by a
/// retraction. Every [`AtomCascadeOutcome`]/[`CascadeOutcome`] carries the
/// full set its call produced; `services::retraction_audit::record` mirrors
/// them into `retraction_audit_log` (FM-5) when a control-plane pool is
/// available. `surface` is free text in the schema (not a fixed enum — new
/// surfaces are expected over time); every value THIS module writes is one
/// of `"content"`, `"meeting_transcript"`, `"coding_session"`,
/// `"source_evidence"`, `"journal_rollup"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceOutcome {
    pub graph_id: Uuid,
    /// D8's author-only trigger — `graphs.kind IN ('personal', 'member')`.
    /// Resolved once per `graph_id` inside `delete_atom_cascade`'s own
    /// RLS-scoped transaction (the only place this ocean's `kind` is
    /// reachable — see that call site's own comment).
    pub is_personal_ocean: bool,
    /// D7's identity triple, part 1/3 — the originating atom's own
    /// `source_type` (e.g. `"slack"`, `"meeting"`, `"coding_session"`).
    pub source_type: String,
    /// D7's identity triple, part 2/3 — at whatever granularity this
    /// surface addresses: an atom's own `source_id`, a meeting's base
    /// `source_id`, or a `coding_sessions.session_id`.
    pub source_id: String,
    /// D7's identity triple, part 3/3. `Some` for the `"content"` surface
    /// (one specific atom row); `None` for `"meeting_transcript"`/
    /// `"coding_session"`/`"journal_rollup"`, which address a GROUP of
    /// atoms via `source_id` rather than one atom.
    pub atom_id: Option<Uuid>,
    pub surface: &'static str,
    /// One of the [`outcome`] module's three constants.
    pub outcome: &'static str,
    /// Ids of derived artifacts left standing despite this outcome — D7's
    /// `survivors[]`. Empty for `outcome::REMOVED` (nothing survived).
    ///
    /// **RESERVED — no production call site in this module populates this
    /// with anything but `vec![]` today** (noted on review, finding 8). The
    /// natural non-empty case is `outcome::OUT_OF_WINDOW`'s `"journal_
    /// rollup"` row — the specific `journal_rollups` row(s) covering this
    /// atom's hour are the concrete "thing left standing" — but reliably
    /// resolving atom → rollup (matching `hour_start`/`user_id`/`graph_id`
    /// the same way `journal_sweep`'s own aggregation does) is real,
    /// separately-verifiable work this PR did not do; shipping an
    /// under-verified correlation here would put a WRONG id in a row this
    /// table's own header says must never carry anything but ids. The
    /// schema (`retraction_audit_log_manifest_survivors_shape`, migration
    /// 0196) already constrains the shape (a JSON array of bare UUID
    /// strings) for whenever this IS populated.
    pub survivors: Vec<String>,
}

/// Outcome of one [`delete_atom_cascade`] call — the ONE primitive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomCascadeOutcome {
    /// `false` when the atom was ALREADY gone (a prior call completed, or
    /// nothing was ever there) — idempotent success, never an error, exactly
    /// matching `services::retraction::UnpublishOutcome::atom_was_present`.
    pub atom_was_present: bool,
    /// Every reef this call's atom was the LAST remaining coral of, deleted
    /// as a side effect (§4.9's coral-less-reef rule). A single atom can be
    /// linked to more than one reef, so this can hold more than one id.
    pub deleted_reef_ids: Vec<Uuid>,
    /// FM-2/FM-6/FM-3-disclosure surfaces this ONE atom's deletion reached
    /// (or, for `journal_rollup`, merely named as out of the self-heal
    /// window) — see [`SurfaceOutcome`]. Empty when this atom was not the
    /// last remaining coral of its meeting/session and its own timestamp is
    /// still within the journal lookback window.
    pub surface_outcomes: Vec<SurfaceOutcome>,
}

/// Outcome of one multi-atom entry point ([`delete_coral`], [`delete_source`],
/// [`delete_subject`]) — the union of every [`delete_atom_cascade`] call it
/// made, across every `(graph_id, atom_id)` the entry point's seed resolution
/// plus propagation produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CascadeOutcome {
    /// D6 UI (routed separately from B2 unit 02/#718 — see
    /// `docs/plans/capture/02-b2-retract-fixes.md`'s "D6 UI" section):
    /// the SAME id [`run_cascade`]/`services::retraction_audit::record`
    /// wrote every one of this call's `surface_outcomes` rows under. A nil
    /// UUID (`Uuid::default()` via `#[derive(Default)]`) on the early
    /// "unknown graph" no-op returns — harmless, since `atoms_deleted` and
    /// `surface_outcomes` are empty there too, so no caller has anything to
    /// key a report on. Surfaced in the route's JSON response
    /// (`routes::deletion::cascade_json`) so the D6 post-retract report can
    /// address `GET /v1/retractions/{retraction_id}` for this exact call
    /// without inventing client-side state.
    pub retraction_id: Uuid,
    /// Every atom actually deleted by this call (excludes atoms that were
    /// already gone before this call started — same "already retracted is
    /// success, but not double-counted" contract as §4.9).
    pub atoms_deleted: Vec<(Uuid /* graph_id */, Uuid /* atom_id */)>,
    /// Every reef deleted as a side effect of losing its last coral,
    /// labelled with the graph it lived in (propagation can span more than
    /// one graph in the same account).
    pub deleted_reef_ids: Vec<(Uuid /* graph_id */, Uuid /* node_id */)>,
    /// Every OTHER graph (besides the one the entry point was called
    /// against) this call touched via `(source_type, source_id)`
    /// propagation — surfaced so a caller/UI can say "and 2 promoted copies
    /// elsewhere" rather than the deletion silently reaching further than
    /// the one ocean the request named.
    pub propagated_graph_ids: Vec<Uuid>,
    /// FM-5/D7: the union of every [`AtomCascadeOutcome::surface_outcomes`]
    /// this call produced — one `"content"` `outcome::REMOVED` row per atom
    /// in `atoms_deleted` (that row is already included in each atom's own
    /// `surface_outcomes`, seeded once inside `delete_atom_cascade` — this
    /// field is a plain `extend`, never a second, separately-added row; an
    /// earlier version of this doc comment claimed otherwise, which would
    /// have meant double-counting — see [`run_cascade`]), plus any
    /// FM-2/FM-6/FM-3-disclosure rows the same atoms produced.
    pub surface_outcomes: Vec<SurfaceOutcome>,
}

#[derive(Debug, thiserror::Error)]
pub enum DeletionError {
    #[error("blob error: {0}")]
    Blob(#[from] R2Error),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
    /// [`delete_subject`]'s stronger gate — see that function's doc comment
    /// and this module's "Authorization" section. Distinct from a plain `Db`
    /// error so callers/logs can tell "refused" from "broke".
    #[error("caller's account role does not permit subject deletion (owner/admin required)")]
    InsufficientRole,
    /// The named subject node either does not exist in the given graph, or is
    /// not a `type = 'entity'` reef — see [`delete_subject`]'s doc comment
    /// for why only entity reefs are valid subjects.
    #[error("subject reef not found, or is not an entity reef")]
    SubjectNotFound,
}

/// The one place BOTH the higher-blast-radius gate lives and is unit-tested
/// independently of any database — mirrors
/// `services::retraction::check_promoter`'s reasoning for keeping policy
/// logic as a pure function the DB-touching code merely calls.
///
/// Matches the EXISTING admin-gating convention in this codebase
/// (`routes::escrow::is_admin`, `routes::teams::invite_member`'s owner
/// check) rather than inventing a new one: exactly `owner` or `admin`, the
/// same as `routes::org_invites::is_admin`. Roles reach here from
/// `account_members.role` (CHECK-constrained to `owner`/`admin`/`member`,
/// migration 0001; Clerk roles are collapsed by `clerk_provision::
/// map_org_role` before they are stored) or from fixed literals, so exact
/// matching accepts the same stored roles the old substring match did. It
/// fails closed on a raw, unmapped Clerk role (`org:admin`,
/// `org:billing_admin`) instead of granting admin on a substring.
pub(crate) fn is_admin_role(role: &str) -> bool {
    // Settings launch PR 2.8: no admin/owner authority for a non-interactive
    // credential — see `middleware::auth::authority_allowed`.
    (role == "owner" || role == "admin") && crate::middleware::auth::authority_allowed()
}

// ── The one primitive ────────────────────────────────────────────────────────

type AtomRow = (
    Uuid,                          // graph_id (defensive re-read)
    String,                        // source_type
    String,                        // source_id
    String,                        // content_blob_key
    Option<String>,                // content_inline
    Vec<String>,                   // participants
    chrono::DateTime<chrono::Utc>, // timestamp
    Option<Uuid>,                  // ingested_by
    String,                        // scope
    String,                        // fts_language
    chrono::DateTime<chrono::Utc>, // created_at
);

type NodeRow = (
    String,                        // type
    String,                        // title
    serde_json::Value,             // body
    String,                        // scope
    Option<Uuid>,                  // owner_id
    chrono::DateTime<chrono::Utc>, // created_at
    i32,                           // version
);

/// Which RLS scope every transaction in a cascade opens. `Account` is the
/// historical behavior for every entry point. `TenantSystem` stands the
/// tenant plane's member-ocean backstop down for the WHOLE cascade (every
/// transaction it opens), for background connector propagation that acts on
/// one member's private ocean with no end-user identity to present: without
/// it, a tenant database hides the member's graph from an account-scoped
/// transaction and the cascade would find nothing and "succeed".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CascadeScope {
    Account,
    TenantSystem,
    /// A transaction acting as ONE end user (`begin_tenant_scoped`: account +
    /// `app.user_id`): on a tenant DB under the NOBYPASSRLS role it sees that
    /// member's private ocean and the shared oceans — never another member's
    /// private ocean. Used by the user-initiated retraction routes.
    Member(Uuid),
}

impl CascadeScope {
    /// The scope a retraction runs under, from who is acting: a real user
    /// acts as themselves; connector/system actors keep account scope.
    fn for_actor(actor: crate::services::connector_audit::Actor) -> Self {
        match actor {
            crate::services::connector_audit::Actor::User(u) => CascadeScope::Member(u),
            _ => CascadeScope::Account,
        }
    }
}

async fn begin_cascade_tx(
    pool: &PgPool,
    account_id: Uuid,
    scope: CascadeScope,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, crate::store::StoreError> {
    match scope {
        CascadeScope::Account => crate::tenancy::scope::begin_account_scoped(pool, account_id).await,
        CascadeScope::TenantSystem => {
            crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await
        }
        CascadeScope::Member(user_id) => {
            crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await
        }
    }
}

/// **The one primitive.** Delete ONE atom — its row, its `node_atoms` links,
/// its R2 blob, and (§4.9's rule) any reef that atom was the last coral of —
/// in one idempotent, retry-safe transaction. Every entry point in this
/// module ([`delete_coral`], [`delete_source`], [`delete_subject`]) bottoms
/// out here, once per resolved `(graph_id, atom_id)`.
///
/// # Ordering (identical to `services::retraction::unpublish_atom`)
///
/// 1. Read the atom row `FOR UPDATE`. Not found → idempotent success, nothing
///    to do (`atom_was_present: false`).
/// 2. Capture the atom's candidate reefs (`node_atoms`) BEFORE the delete
///    cascades that join table away.
/// 3. Queue graph-owned ciphertext cleanup in this transaction. The durable
///    worker performs object-store deletion only after the SQL commit.
/// 4. Delete the `atoms` row (cascading `node_atoms`) and append a tombstone
///    sync op. Any SQL error also rolls back the cleanup job.
/// 5. For each candidate reef, re-read `FOR UPDATE`; if it now has zero
///    remaining corals, delete it too (cascading its edges via FK) and
///    append its own tombstone op.
/// 6. Commit.
///
/// This function does NOT authorize the caller — every entry point above it
/// is responsible for its own gate (reachability for `delete_coral`/
/// `delete_source`, owner/admin for `delete_subject`) BEFORE resolving the
/// `(graph_id, atom_id)` pairs it calls this with. This mirrors
/// `services::retraction::unpublish_atom`'s own separation, except that here
/// there is no per-atom promoter check to make — deletion authority is
/// decided once, at the entry point, not re-derived per atom.
///
/// `account_id` must be the account that owns `graph_id` — every caller in
/// this module resolves it ONCE via [`resolve_account_id_for_graph`] (the
/// audited `resolve_graph_account_id` bootstrap) before entering the
/// propagation loop, and passes the SAME value to every
/// [`delete_atom_cascade`] call that loop makes. That reuse is safe, not just
/// convenient: propagation ([`expand_by_source_identity`]) is deliberately
/// restricted to graphs in that ONE account (module doc, "Propagation"), so
/// `account_id` is invariant across every `(graph_id, atom_id)` pair one
/// entry-point call produces. This function does NOT re-derive it from
/// `graph_id` itself — see `resolve_account_id_for_graph`'s doc comment for
/// why re-deriving per-call would just be a redundant bootstrap round trip,
/// not a correctness difference.
///
/// `is_personal_ocean_hint` (finding 10, independent review — performance):
/// `Some(v)` skips this function's own `graphs.kind` lookup and uses `v`
/// directly; `None` resolves it internally (the original per-call
/// behavior). [`run_cascade`] passes `Some` from a per-`graph_id` cache it
/// maintains across its whole loop — `is_personal_ocean` is INVARIANT for a
/// given `graph_id` within one entry-point call (the ocean's `kind` cannot
/// change mid-call), so re-querying it once per ATOM when a `delete_subject`
/// call can touch hundreds of atoms in the SAME graph was pure waste. Direct
/// callers (this function is `pub`, driven directly by
/// `retraction_deletion_rls_pg.rs`'s own RLS proof) pass `None` and keep the
/// simpler, self-contained behavior — they process one atom at a time and
/// gain nothing from caching.
pub async fn delete_atom_cascade(
    pool: &PgPool,
    account_id: Uuid,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    graph_id: Uuid,
    atom_id: Uuid,
    is_personal_ocean_hint: Option<bool>,
) -> Result<AtomCascadeOutcome, DeletionError> {
    delete_atom_cascade_scoped(
        pool,
        account_id,
        blobs,
        device_id,
        graph_id,
        atom_id,
        is_personal_ocean_hint,
        CascadeScope::Account,
        false,
    )
    .await
}

/// [`delete_atom_cascade`] with an explicit [`CascadeScope`].
#[allow(clippy::too_many_arguments)]
async fn delete_atom_cascade_scoped(
    pool: &PgPool,
    account_id: Uuid,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    graph_id: Uuid,
    atom_id: Uuid,
    is_personal_ocean_hint: Option<bool>,
    scope: CascadeScope,
    preserve_source_history: bool,
) -> Result<AtomCascadeOutcome, DeletionError> {
    let mut tx = begin_cascade_tx(pool, account_id, scope).await?;
    let outcome = delete_atom_cascade_tx(&mut tx, account_id, blobs, device_id,
        graph_id, atom_id, is_personal_ocean_hint, preserve_source_history, None).await?;
    tx.commit().await?;
    Ok(outcome)
}

/// Borrow the caller's transaction for whole-source deletion. Deferring node
/// pruning allows the union of affected nodes to be locked in sorted order.
#[allow(clippy::too_many_arguments)]
async fn delete_atom_cascade_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    graph_id: Uuid,
    atom_id: Uuid,
    is_personal_ocean_hint: Option<bool>,
    preserve_source_history: bool,
    deferred_nodes: Option<&mut Vec<Uuid>>,
) -> Result<AtomCascadeOutcome, DeletionError> {
    let identity: Option<(String, String)> = sqlx::query_as(
        "SELECT source_type,source_id FROM atoms WHERE graph_id=$1 AND atom_id=$2",
    ).bind(graph_id).bind(atom_id).fetch_optional(&mut **tx).await?;
    if let Some((source_type, source_id)) = &identity {
        crate::source_evidence::lock_identity_tx(tx, graph_id,
            crate::workers::journal_sweep::strip_chunk_suffix(source_id)).await?;
        lock_source_evidence_tx(tx, graph_id, source_type,
            crate::workers::journal_sweep::strip_chunk_suffix(source_id)).await?;
    }

    let evidence_sources: Vec<(String,String,String)> = sqlx::query_as(
        "SELECT DISTINCT s.source_type,s.source_id,s.provider_namespace FROM source_items s \
         JOIN source_versions v ON v.source_item_id=s.source_item_id AND v.graph_id=s.graph_id \
         JOIN source_version_chunks c ON c.source_version_id=v.version_id AND c.graph_id=v.graph_id \
         WHERE c.graph_id=$1 AND c.atom_id=$2",
    ).bind(graph_id).bind(atom_id).fetch_all(&mut **tx).await?;

    let atom: Option<AtomRow> = sqlx::query_as(
        "SELECT graph_id, source_type, source_id, content_blob_key, content_inline, \
                participants, \"timestamp\", ingested_by, scope, fts_language, created_at \
         FROM atoms WHERE atom_id = $1 AND graph_id = $2 FOR UPDATE",
    )
    .bind(atom_id)
    .bind(graph_id)
    .fetch_optional(&mut **tx)
    .await?;

    // `content_inline` is read here (it's part of the row) but deliberately
    // never carried into the delete tombstone below — see FM-1's note on
    // `atom_snapshot`.
    let Some((
        atom_graph_id,
        source_type,
        source_id,
        content_blob_key,
        _content_inline,
        participants,
        atom_timestamp,
        ingested_by,
        scope,
        fts_language,
        created_at,
    )) = atom
    else {
        return Ok(AtomCascadeOutcome {
            atom_was_present: false,
            deleted_reef_ids: vec![],
            surface_outcomes: vec![],
        });
    };
    debug_assert_eq!(atom_graph_id, graph_id);

    // FM-5/D8: carried on every `SurfaceOutcome` this call produces —
    // `graphs` may live on an ORG TENANT database `retraction_audit_log`'s
    // writer (a control-plane pool) cannot see at all, so a call site
    // inside THIS transaction (content-plane, RLS-scoped) is the only place
    // the ocean's `kind` is ever reachable. `graphs.kind IN
    // ('personal', 'member')` is D8's "single-viewer ocean" (a personal
    // account's own ocean, or an org member's private ocean —
    // `migrations_control/0090_member_ocean_columns.sql` widened
    // `graphs_kind_check` to admit `'member'`); `'team'`/`'org'` are shared.
    // `is_personal_ocean_hint` (finding 10): `Some` skips this query
    // entirely — see this function's own doc comment for why [`run_cascade`]
    // supplies a cached value instead of re-querying per atom.
    let is_personal_ocean: bool = match is_personal_ocean_hint {
        Some(v) => v,
        None => {
            sqlx::query_scalar(IS_PERSONAL_OCEAN_QUERY)
                .bind(graph_id)
                .fetch_one(&mut **tx)
                .await?
        }
    };

    let mut candidate_node_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT node_id FROM node_atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_all(&mut **tx)
            .await?;

    // Destructive object-store work runs only from the durable cleanup job
    // after this transaction commits. A later SQL error must leave referenced
    // ciphertext intact. Legacy inline fixtures/copies can carry foreign keys;
    // only this graph's owned object may be scheduled by its deletion.
    let graph_segment=content_blob_key.rsplit('/').nth(1);
    if graph_segment==Some(graph_id.to_string().as_str()) {
        queue_blob_cleanup_tx(tx,account_id,graph_id,&content_blob_key).await?;
    } else if blobs.is_some() {
        validate_blob_key(&content_blob_key)?;
    }

    let now = chrono::Utc::now();
    sqlx::query("DELETE FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .execute(&mut **tx)
        .await?;

    // FM-1 (B2 unit 02): the delete tombstone deliberately does NOT carry
    // `content_inline`. Before this fix it copied the atom's plaintext body
    // straight into `op_log` at the exact moment retract is meant to make
    // that plaintext gone — an extra, unswept copy of the thing being
    // retracted, sitting in "the 30-day trash can"
    // (`migrations_control/0023_sync_oplog.sql:19-21`) forever, because
    // `Store::purge_expired_tombstones`'s sweep only ever matched
    // `op_type = 'delete'` rows and the plaintext ALSO lived, unpurged at
    // any age, in the separate `upsert` ops `store::record_atom_content_op`
    // appends whenever an atom's inline content is (re)written
    // (`store::sync::atom_content_op`). The identity/metadata below is
    // still enough to make `op_log`'s "30-day trash can" honest about what
    // was deleted; only the body is gone. The sibling half of this fix is
    // `Store::purge_expired_tombstones`(`_scoped`), which now also reaches
    // those upsert ops. See this field's own `#[allow(dead_code)]` sibling,
    // `sync::restore_op_from_tombstone` ("Restore from trash", unshipped):
    // once this fix lands, restoring an atom from its tombstone can only
    // ever recover identity/metadata, never the body — deliberate, not an
    // oversight. A 30-day undo window that keeps a live plaintext copy is
    // exactly the mistake this fix exists to close.
    let atom_snapshot = serde_json::json!({
        sync::field::ATOM_ID: atom_id.to_string(),
        sync::field::GRAPH_ID: graph_id.to_string(),
    });
    append_op_tx(
        tx,
        &sync::atom_delete_op(
            Uuid::new_v4(),
            atom_snapshot,
            atom_id,
            graph_id,
            now,
            device_id,
        ),
    )
    .await?;

    // §4.9's coral-less-reef rule, reused verbatim: any candidate reef left
    // with ZERO remaining corals is deleted too. Re-read `FOR UPDATE` (not
    // reused from any earlier read) so a concurrent link into the same reef
    // between this transaction's start and now is never raced past.
    // ── FM-5/D7: the base "content" outcome every deleted atom gets ──
    let mut surface_outcomes = vec![SurfaceOutcome {
        graph_id,
        is_personal_ocean,
        source_type: source_type.clone(),
        source_id: source_id.clone(),
        atom_id: Some(atom_id),
        surface: "content",
        outcome: outcome::REMOVED,
        survivors: vec![],
    }];

    // ── FM-2 (B2 unit 02): meeting transcript / corrections / share link ──
    // Same transaction, same "recheck after this delete, act only once the
    // count reaches zero" discipline as the coral-less-reef loop above.
    if !preserve_source_history {
        for (kind,native_id,namespace) in &evidence_sources {
            if let Some(o) = redact_orphaned_source_evidence(
                tx,blobs,account_id,graph_id,is_personal_ocean,kind,native_id,
                Some(namespace),&mut candidate_node_ids,
            ).await? { surface_outcomes.push(o); }
        }
        // Legacy meeting raw data has no chunk-version links; preserve its
        // established last-chunk cleanup without broadening it to other sources.
        if evidence_sources.is_empty() && matches!(ChunkStrategy::for_source_type(&source_type),ChunkStrategy::Transcript) {
            if let Some(o) = redact_orphaned_source_evidence(
                tx,blobs,account_id,graph_id,is_personal_ocean,"transcript",crate::workers::journal_sweep::strip_chunk_suffix(&source_id),
                Some("legacy:meeting"),&mut candidate_node_ids,
            ).await? { surface_outcomes.push(o); }
        }
    }

    // Extraction locks a version before attaching its node evidence. Match that
    // order: last-source redaction above locks versions and collects every
    // affected node; only now lock and prune the combined candidate set.
    let deleted_reef_ids = if let Some(deferred) = deferred_nodes {
        deferred.extend(candidate_node_ids);
        Vec::new()
    } else {
        prune_nodes_without_evidence(tx, graph_id, &candidate_node_ids, device_id).await?
    };

    // ── FM-6 (B2 unit 02): coding sessions ──
    if let Some(o) = delete_orphaned_coding_session(
        tx,
        graph_id,
        is_personal_ocean,
        &source_type,
        &source_id,
    )
    .await?
    {
        surface_outcomes.push(o);
    }

    // Existing persisted derived prose is invalidated synchronously at every
    // age by the atom lifecycle trigger; no lookback exemption remains.
    surface_outcomes.push(SurfaceOutcome { graph_id,is_personal_ocean,
        source_type:source_type.clone(),source_id:source_id.clone(),atom_id:Some(atom_id),
        surface:"journal_rollup",outcome:outcome::REMOVED,survivors:vec![],
    });

    Ok(AtomCascadeOutcome {
        atom_was_present: true,
        deleted_reef_ids,
        surface_outcomes,
    })
}

// ── FM-2: meeting transcripts ────────────────────────────────────────────────

/// The closed set `coralswarm_core::chunker::ChunkStrategy::for_source_type`
/// classifies as `ChunkStrategy::Transcript` — meeting-transcript-shaped
/// ingest source types. Reused (not re-derived) by
/// [`redact_orphaned_source_evidence`]'s gate and its sibling-count
/// query — see that function's own doc comment for why matching a CLOSED
/// SET here, not a single exact `source_type`, is what actually closes both
/// halves of finding 3 (the destructive collision AND its mirror).
const TRANSCRIPT_SOURCE_TYPES: &[&str] = &["transcript", "zoom", "meet", "teams_transcript"];

/// D8's "single-viewer ocean" predicate — shared, verbatim, by
/// `delete_atom_cascade`'s own (hint-less) fallback query and
/// `resolve_is_personal_ocean`'s cache-populating query, so the two can
/// never drift into disagreeing about what `is_personal_ocean` means.
const IS_PERSONAL_OCEAN_QUERY: &str =
    "SELECT kind = ANY(ARRAY['personal', 'member']) FROM graphs WHERE id = $1";

/// Finding 4 (independent review, HIGH): escapes `\`, `%`, `_` in `base`
/// (Postgres' default `LIKE` escape character is `\`) and appends the
/// literal `:chunk:%` suffix — same escaping convention as
/// `store::session_source_id_prefix` (`store/mod.rs:3289`), reused here for
/// the identical reason. An UNESCAPED `format!("{base}:chunk:%")` lets a
/// literal `_`/`%` in `base` act as a wildcard: `routes::ingest.rs`'s own
/// exemplar `"msg_001"` would, unescaped, ALSO match `"msgX001:chunk:0"` (a
/// DIFFERENT meeting's chunk) — inflating the sibling count and silently
/// leaving FM-2 unfixed for the real meeting whose actual last chunk was
/// just retracted.
///
/// `pub(crate)` (gsuite Train G4 PR 4): `connectors::google_chat::
/// propagate_deletions` needs the IDENTICAL escaping rule to resolve a Chat
/// message's concrete chunk `source_id`s before calling [`delete_source`]
/// (which matches `source_id` EXACTLY — see its own doc). Reused rather than
/// re-derived precisely because getting this escaping wrong was a HIGH
/// finding once already.
pub(crate) fn escaped_chunk_pattern(base: &str) -> String {
    let mut out = String::with_capacity(base.len() + 8);
    for c in base.chars() {
        if c == '\\' || c == '%' || c == '_' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push_str(":chunk:%");
    out
}

/// Resolve the CONCRETE `atoms.source_id`s stored anywhere in `account_id`
/// for a provider's bare item ids: each bare id itself (the degenerate
/// un-chunked case) plus every `"{id}:chunk:N"` sibling `routes::ingest`
/// writes, matched with [`escaped_chunk_pattern`]. The required first hop
/// before [`delete_source`], which matches `source_id` EXACTLY: a connector
/// that passes the bare provider id straight in matches nothing in
/// production (every ingested coral is chunked) and only APPEARS to
/// propagate the deletion — `connectors::google_chat::propagate_deletions`'s
/// doc records this exact pre-existing gap for Gmail and Calendar, and
/// `connectors::google_calendar`/`connectors::microsoft_calendar`'s
/// personal-plane arms go through here, and so does
/// `connectors::google_gmail::propagate_deletions` (fix/gmail-personal-delete,
/// the last of the three to close the gap).
///
/// Scoped `begin_tenant_system_scoped` like every connector deletion read.
pub(crate) async fn concrete_source_ids(
    pool: &PgPool,
    account_id: Uuid,
    source_type: &str,
    bare_ids: &[String],
) -> Result<Vec<String>, DeletionError> {
    if bare_ids.is_empty() {
        return Ok(Vec::new());
    }
    let patterns: Vec<String> = bare_ids.iter().map(|n| escaped_chunk_pattern(n)).collect();
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT a.source_id FROM atoms a \
         JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND a.source_type = $2 \
           AND (a.source_id = ANY($3) OR a.source_id LIKE ANY($4))",
    )
    .bind(account_id)
    .bind(source_type)
    .bind(bare_ids)
    .bind(&patterns)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(s,)| s).collect())
}

/// A canonical node loses its evidence-derived title only after every legacy
/// chunk AND semantic atom is gone. Lock first so concurrent FK associations
/// cannot race the final support check.
async fn prune_nodes_without_evidence(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    candidates: &[Uuid],
    device_id: &str,
) -> Result<Vec<Uuid>, DeletionError> {
    let mut ids = candidates.to_vec();
    ids.sort();
    ids.dedup();
    let mut deleted = Vec::new();
    for node_id in ids {
        let node: Option<NodeRow> = sqlx::query_as(
            "SELECT type,title,body,scope,owner_id,created_at,version FROM nodes WHERE node_id=$1 AND graph_id=$2 FOR UPDATE",
        ).bind(node_id).bind(graph_id).fetch_optional(&mut **tx).await?;
        let Some((node_type,_title,_body,node_scope,owner_id,created_at,version)) = node else {continue;};
        let supported: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM node_atoms WHERE node_id=$1) OR EXISTS(SELECT 1 FROM node_semantic_atoms WHERE node_id=$1)",
        ).bind(node_id).fetch_one(&mut **tx).await?;
        if supported {continue;}
        sqlx::query("DELETE FROM nodes WHERE node_id=$1").bind(node_id).execute(&mut **tx).await?;
        append_op_tx(tx, &sync::node_delete_op(
            Uuid::new_v4(), node_id, graph_id, &node_type, "", "{}",
            &node_scope, owner_id, created_at, chrono::Utc::now(), i64::from(version), device_id,
        )).await?;
        deleted.push(node_id);
    }
    Ok(deleted)
}

/// Source locks precede versions and canonical nodes so capture, extraction,
/// replacement and deletion use one consistent order.
async fn lock_source_evidence_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid, source_type: &str, source_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND (source_type=$2 OR ($4 AND source_type='transcript' AND provider_namespace='legacy:meeting')) AND source_id=$3 ORDER BY source_item_id FOR UPDATE")
        .bind(graph_id).bind(source_type).bind(source_id).bind(matches!(ChunkStrategy::for_source_type(source_type),ChunkStrategy::Transcript)).fetch_all(&mut **tx).await?;
    sqlx::query("SELECT v.version_id FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE s.graph_id=$1 AND (s.source_type=$2 OR ($4 AND s.source_type='transcript' AND s.provider_namespace='legacy:meeting')) AND s.source_id=$3 ORDER BY v.version_id FOR UPDATE OF v")
        .bind(graph_id).bind(source_type).bind(source_id).bind(matches!(ChunkStrategy::for_source_type(source_type),ChunkStrategy::Transcript)).fetch_all(&mut **tx).await?;
    Ok(())
}

/// Durable ciphertext cleanup survives deletion of the source row. These keys
/// come exclusively from scoped persisted rows, never from a request payload.
async fn queue_blob_cleanup_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid, graph_id: Uuid, key: &str,
) -> Result<(), DeletionError> {
    validate_blob_key(key)?;
    let parts: Vec<&str> = key.split('/').collect();
    let (org_id, graph_part) = match parts.as_slice() {
        [graph, _] => (None, *graph),
        [org, graph, _] => (Some((*org).to_string()), *graph),
        _ => return Err(sqlx::Error::Protocol("invalid persisted source blob key".into()).into()),
    };
    if graph_part != graph_id.to_string() {
        return Err(sqlx::Error::Protocol("persisted source blob belongs to a different graph".into()).into());
    }
    crate::jobs::queue::enqueue_tx(tx, &crate::jobs::JobKind::DeleteSourceBlob {
        graph_id, account_id, org_id, blob_key: key.to_string(),
    }, Some(account_id), Some(&format!("delete-source-blob:{key}")), 0).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn redact_orphaned_source_evidence(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    blobs: Option<&dyn PromotionBlobStore>,
    account_id: Uuid,
    graph_id: Uuid,
    is_personal_ocean: bool,
    source_type: &str,
    source_id: &str,
    namespace: Option<&str>,
    candidate_node_ids: &mut Vec<Uuid>,
) -> Result<Option<SurfaceOutcome>, DeletionError> {
    let base = source_id;
    let remaining: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM atoms WHERE graph_id=$1 AND (source_type=$2 OR ($5 AND source_type=ANY($6))) AND \
         (source_id=$3 OR (source_id LIKE $4 AND substring(source_id FROM char_length($3)+1) ~ '^:chunk:[0-9]+$')))",
    ).bind(graph_id).bind(source_type).bind(base).bind(escaped_chunk_pattern(base)).bind(namespace==Some("legacy:meeting")).bind(TRANSCRIPT_SOURCE_TYPES)
        .fetch_one(&mut **tx).await?;
    if remaining { return Ok(None); }
    purge_source_evidence_tx(tx,blobs,account_id,graph_id,is_personal_ocean,
        source_type,base,namespace,candidate_node_ids).await
}

/// Removes the source even when no retrieval chunks ever existed. Callers own
/// graph authorization; namespace narrows a provider's deletion where known.
#[allow(clippy::too_many_arguments)]
async fn purge_source_evidence_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    _blobs: Option<&dyn PromotionBlobStore>,
    account_id: Uuid, graph_id: Uuid, is_personal_ocean: bool,
    source_type: &str, source_id: &str, namespace: Option<&str>,
    candidate_node_ids: &mut Vec<Uuid>,
) -> Result<Option<SurfaceOutcome>, DeletionError> {
    let items: Vec<Uuid> = sqlx::query_scalar(
        "SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_type=$2 AND source_id=$3 \
         AND ($4::text IS NULL OR provider_namespace=$4) ORDER BY source_item_id FOR UPDATE",
    ).bind(graph_id).bind(source_type).bind(source_id).bind(namespace).fetch_all(&mut **tx).await?;
    sqlx::query("SELECT version_id FROM source_versions WHERE graph_id=$1 AND source_item_id=ANY($2) ORDER BY version_id FOR UPDATE")
        .bind(graph_id).bind(&items).fetch_all(&mut **tx).await?;
    let mut nodes: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT na.node_id FROM node_semantic_atoms na JOIN semantic_atoms a ON a.semantic_atom_id=na.semantic_atom_id AND a.graph_id=na.graph_id \
         JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id WHERE v.graph_id=$1 AND v.source_item_id=ANY($2)",
    ).bind(graph_id).bind(&items).fetch_all(&mut **tx).await?;
    // Legacy-only associations were collected before chunk deletion removed
    // their node_atoms rows. They need the same privacy scrub as semantic links.
    nodes.extend(candidate_node_ids.iter().copied());
    nodes.sort();nodes.dedup();
    let mut keys: Vec<String> = sqlx::query_scalar(
        "SELECT raw_blob_key FROM source_versions WHERE graph_id=$1 AND source_item_id=ANY($2) AND raw_blob_key IS NOT NULL",
    ).bind(graph_id).bind(&items).fetch_all(&mut **tx).await?;
    // Older meeting projections may not yet have any version. Their exact
    // transcript-type gate preserves the existing cross-type collision safety.
    let transcript: Option<(Uuid,Option<String>)> = if matches!(ChunkStrategy::for_source_type(source_type),ChunkStrategy::Transcript) {
        sqlx::query_as("SELECT id,raw_blob_key FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2 AND (source_item_id=ANY($3) OR (source_item_id IS NULL AND ($4::text IS NULL OR $4='legacy:meeting'))) FOR UPDATE")
            .bind(graph_id).bind(source_id).bind(&items).bind(namespace).fetch_optional(&mut **tx).await?
    } else { None };
    if let Some((_, Some(key))) = &transcript { keys.push(key.clone()); }
    keys.sort(); keys.dedup();
    for key in keys {
        validate_blob_key(&key)?;
        queue_blob_cleanup_tx(tx,account_id,graph_id,&key).await?;
    }
    if let Some((id,_)) = &transcript {
        sqlx::query("DELETE FROM transcript_corrections WHERE graph_id=$1 AND source_id=$2")
            .bind(graph_id).bind(source_id).execute(&mut **tx).await?;
        sqlx::query("DELETE FROM meeting_transcripts WHERE id=$1").bind(id).execute(&mut **tx).await?;
        sqlx::query("UPDATE meeting_shares SET revoked_at=now() WHERE graph_id=$1 AND source_id=$2 AND revoked_at IS NULL")
            .bind(graph_id).bind(source_id).execute(&mut **tx).await?;
    }
    sqlx::query("DELETE FROM source_items WHERE graph_id=$1 AND source_item_id=ANY($2)")
        .bind(graph_id).bind(&items).execute(&mut **tx).await?;
    sqlx::query("SELECT node_id FROM nodes WHERE graph_id=$1 AND node_id=ANY($2) ORDER BY node_id FOR UPDATE")
        .bind(graph_id).bind(&nodes).fetch_all(&mut **tx).await?;
    // Privacy retraction intentionally ends merge undo for these identities:
    // historical merge snapshots are content-bearing derived copies too.
    sqlx::query("DELETE FROM node_merges WHERE graph_id=$1 AND (keep_id=ANY($2) OR dup_id=ANY($2))")
        .bind(graph_id).bind(&nodes).execute(&mut **tx).await?;
    // Legacy aliases/identifier imports lack per-source provenance. Removing
    // all unowned aliases/imported identifiers from affected survivors is the
    // conservative privacy choice; account/human identity bindings survive.
    sqlx::query("UPDATE nodes SET body=body-'aliases',version=version+1,updated_at=now() WHERE graph_id=$1 AND node_id=ANY($2) AND body ? 'aliases'")
        .bind(graph_id).bind(&nodes).execute(&mut **tx).await?;
    // A surviving node still has old sync snapshots of its removed aliases.
    // Replace that history with the sanitized current row so existing devices
    // receive the redaction while independently supported identity survives.
    sqlx::query("DELETE FROM op_log WHERE graph_id=$1 AND entity='node' AND entity_id=ANY(SELECT id::text FROM unnest($2::uuid[]) id)")
        .bind(graph_id).bind(&nodes).execute(&mut **tx).await?;
    type SanitizedNode = (Uuid,String,String,serde_json::Value,String,Option<Uuid>,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>,i32);
    let sanitized:Vec<SanitizedNode>=sqlx::query_as("SELECT node_id,type,title,body,scope,owner_id,created_at,updated_at,version FROM nodes WHERE graph_id=$1 AND node_id=ANY($2) ORDER BY node_id FOR UPDATE")
        .bind(graph_id).bind(&nodes).fetch_all(&mut **tx).await?;
    for (id,kind,title,body,scope,owner,created,updated,version) in sanitized {
        append_op_tx(tx,&sync::node_upsert_op(Uuid::new_v4(),id,graph_id,&kind,&title,&body.to_string(),&scope,owner,created,updated,i64::from(version),"evidence-lifecycle")).await?;
    }
    sqlx::query("DELETE FROM reef_identifiers WHERE graph_id=$1 AND node_id=ANY($2) AND source NOT IN ('account','human','user')")
        .bind(graph_id).bind(&nodes).execute(&mut **tx).await?;
    candidate_node_ids.extend(nodes);
    if items.is_empty() && transcript.is_none() { return Ok(None); }
    Ok(Some(SurfaceOutcome {graph_id,is_personal_ocean,source_type:source_type.to_string(),
        source_id:source_id.to_string(),atom_id:None,surface:if transcript.is_some() { "meeting_transcript" } else { "source_evidence" },outcome:outcome::REMOVED,survivors:vec![]}))
}

// ── FM-6: coding sessions ────────────────────────────────────────────────────

/// FM-6 (B2 unit 02): when the atom [`delete_atom_cascade`] just deleted was
/// the LAST remaining checkpoint of a coding session, delete the
/// `coding_sessions` row itself — not just its corals — so `list_sessions`
/// returns nothing for it and none of `title`/`project_key`/`repo_path`/
/// `hostname`/`model`/`started_at`/`last_activity_at` is readable by any
/// surface afterwards (today it survives whole with `coral_count = 0`,
/// `Store::list_coding_sessions`' own per-row correlated subquery against
/// the `"cc:{session_id}:"` `source_id` prefix,
/// `backend/src/store/mod.rs:6209-6212` at transcription time).
///
/// `coding_session_shares` needs no separate handling here: its
/// `(graph_id, session_id)` FK IS `ON DELETE CASCADE`
/// (`migrations_control/0077_coding_session_shares.sql:47-48`), so removing
/// the session row already takes its share links with it — unlike
/// `meeting_shares`, which carries no such FK (see
/// [`redact_orphaned_source_evidence`]'s own note).
///
/// Called from inside [`delete_atom_cascade`]'s own transaction. Returns
/// `Ok(None)` when the atom's `source_type` is not `"coding_session"`, when
/// no session row matches its `source_id`, or when a sibling checkpoint atom
/// is still present.
async fn delete_orphaned_coding_session(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    is_personal_ocean: bool,
    source_type: &str,
    source_id: &str,
) -> Result<Option<SurfaceOutcome>, DeletionError> {
    // **Gated on the `"cc:"` source_id PREFIX, not `source_type` (fixed on
    // review, finding 6, HIGH).** An earlier revision gated on
    // `source_type == "coding_session"` — wrong, because that is NOT how
    // this system's own canonical "does this atom belong to a coding
    // session" definition works: `Store::list_coding_sessions`'
    // `coral_count` subquery (`store/mod.rs`, the query FM-6's own
    // acceptance test — `list_sessions` returning nothing — actually
    // depends on) matches PURELY on the `"cc:{session_id}:"` `source_id`
    // prefix, with NO `source_type` filter at all. `source_type` only
    // becomes `"coding_session"` via migration `0098`/`0011`'s BACKFILL —
    // and that backfill's own `NOT EXISTS` clause PERMANENTLY skips
    // relabeling any atom that collides with an existing `coding_session`-
    // typed row at the same `(graph_id, source_id)` (`migrations_tenant/
    // 0011_coding_session_source_type.sql`) — so a real, currently-listed
    // checkpoint can legitimately keep `source_type = 'note'`/`'chat'`
    // forever on an otherwise fully-migrated, functioning tenant. Gating on
    // `source_type` there meant retracting exactly that atom left the
    // session's husk behind — the literal FM-6 symptom this fix exists to
    // close. The prefix check below is a fast-path only (skip the reverse
    // lookup for atoms that obviously aren't session checkpoints); the
    // reverse lookup itself is what actually decides correctness, by
    // requiring a REAL `coding_sessions` row to match.
    if !source_id.starts_with("cc:") {
        return Ok(None);
    }
    // Reverse lookup: which `coding_sessions` row does this atom's
    // `source_id` belong to? `session_id` may itself contain colons
    // (`add_context`'s validation has no charset restriction beyond a
    // length cap), so the only correct join is against the ACTUAL session
    // rows via the escaped `"cc:{session_id}:"` LIKE prefix, longest match
    // wins — same predicate/tiebreak shape `Store::list_coding_sessions`'
    // own `coral_count` subquery and `Store::journal_range_aggregate` both
    // use for their own reverse lookups, and for the identical
    // prefix-collision reason (`session_source_id_prefix`'s own doc
    // comment: "team" / "team:alpha").
    let session_id: Option<String> = sqlx::query_scalar(
        r"SELECT session_id FROM coding_sessions
          WHERE graph_id = $1
            AND $2 LIKE ('cc:' || REPLACE(REPLACE(REPLACE(
                    session_id, '\', '\\'), '%', '\%'), '_', '\_')
                    || ':%')
          ORDER BY length(session_id) DESC
          LIMIT 1",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(session_id) = session_id else {
        return Ok(None);
    };

    let prefix = crate::store::session_source_id_prefix(&session_id);
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_id LIKE $2")
            .bind(graph_id)
            .bind(&prefix)
            .fetch_one(&mut **tx)
            .await?;
    if remaining > 0 {
        return Ok(None);
    }

    sqlx::query("DELETE FROM coding_sessions WHERE graph_id = $1 AND session_id = $2")
        .bind(graph_id)
        .bind(&session_id)
        .execute(&mut **tx)
        .await?;

    Ok(Some(SurfaceOutcome {
        graph_id,
        is_personal_ocean,
        source_type: source_type.to_string(),
        source_id: session_id,
        atom_id: None,
        surface: "coding_session",
        outcome: outcome::REMOVED,
        survivors: vec![],
    }))
}

// ── Shared plumbing: account resolution + propagation ───────────────────────

/// Resolve the `account_id` that owns `graph_id` — authoritative, never
/// trusted from a caller. Every entry point uses this (not
/// `UserContext::account_id`) to scope propagation, because a user may
/// belong to more than one account (`tenancy::reachability`'s D9 union), so
/// the ACTIVE session account is not necessarily the account that owns the
/// specific graph named in the request. Deriving it from the graph itself,
/// the same way `services::retraction` derives its promoter set from the
/// target row rather than from any caller-supplied value, keeps propagation
/// scoped to the right account regardless of which of the caller's accounts
/// is "active".
async fn resolve_account_id_for_graph(
    pool: &PgPool,
    graph_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    // `resolve_graph_account_id` (migration `0084_bootstrap_scope_functions`)
    // — the audited `SECURITY DEFINER` bootstrap carve-out, NOT a raw
    // `SELECT account_id FROM graphs WHERE id = $1` against the ambient pool.
    // `graphs` is on migration `0083`'s RLS roster (a direct-`account_id`
    // table), so the raw form this function used to run was itself an
    // unscoped read against an RLS-covered table — exactly the chicken-and-egg
    // this bootstrap function exists to break (you cannot scope a connection
    // to an account_id you don't know yet), and, once the app connects as the
    // non-BYPASSRLS `coralswarm_app` role, a hard failure on every call
    // (`current_setting('app.account_id')` with no `missing_ok` raises rather
    // than returning zero rows). Returns only a bare uuid, never any other
    // `graphs` column, so it cannot be used to exfiltrate graph content — see
    // `Store::graph_account_id`'s identical use of this same function.
    //
    // A scalar function call in a SELECT list always returns exactly one row,
    // with NULL for "no such graph" — `fetch_one` into `Option<Uuid>`, not
    // `fetch_optional` (matching `Store::graph_account_id`'s own comment).
    sqlx::query_scalar("SELECT resolve_graph_account_id($1)")
        .bind(graph_id)
        .fetch_one(pool)
        .await
}

/// Expand a seed set of `(graph_id, atom_id)` pairs to include every OTHER
/// atom, anywhere in `account_id`, sharing a `(source_type, source_id)` pair
/// with one of the seeds — see this module's "Propagation" doc section for
/// the full account of what this can and cannot reach. A no-op (returns the
/// seeds unchanged) when `seeds` is empty.
///
/// Runs on an already-`begin_account_scoped` transaction — `atoms`/`graphs`
/// are both on migration `0083`'s RLS roster, so this can no longer be a
/// plain-pool read (see this module's PR history: it used to be, and that
/// was a live bug once the app stopped connecting as the BYPASSRLS owner
/// role). Every entry point below opens exactly one such transaction for its
/// whole setup phase (exists-check plus this expansion), then rolls it back
/// (read-only) before handing the resolved `(graph_id, atom_id)` targets to
/// [`run_cascade`], which opens its OWN separate scoped transaction per atom.
async fn expand_by_source_identity(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    seeds: &[(Uuid, Uuid)],
) -> Result<Vec<(Uuid, Uuid)>, sqlx::Error> {
    if seeds.is_empty() {
        return Ok(Vec::new());
    }
    let seed_atom_ids: Vec<Uuid> = seeds.iter().map(|(_, a)| *a).collect();
    let pairs: Vec<(String, String)> =
        sqlx::query_as("SELECT DISTINCT source_type, source_id FROM atoms WHERE atom_id = ANY($1)")
            .bind(&seed_atom_ids)
            .fetch_all(&mut **tx)
            .await?;

    let mut expanded: HashSet<(Uuid, Uuid)> = seeds.iter().copied().collect();
    if !pairs.is_empty() {
        let types: Vec<String> = pairs.iter().map(|p| p.0.clone()).collect();
        let ids: Vec<String> = pairs.iter().map(|p| p.1.clone()).collect();
        // Mirrors services::promotion's own (source_type, source_id) natural-key
        // join shape (see that module's copy_atoms_to_graph), scoped to ONE
        // account rather than to a single target graph. `g.account_id = $1`
        // is kept as an explicit application-level predicate (defense in
        // depth, same convention as this module's other queries) even though
        // the scoped transaction's RLS backstop now enforces the identical
        // boundary at the database level too.
        let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT a.graph_id, a.atom_id FROM atoms a \
             JOIN graphs g ON g.id = a.graph_id \
             JOIN UNNEST($2::text[], $3::text[]) AS s(source_type, source_id) \
               ON a.source_type = s.source_type AND a.source_id = s.source_id \
             WHERE g.account_id = $1",
        )
        .bind(account_id)
        .bind(&types)
        .bind(&ids)
        .fetch_all(&mut **tx)
        .await?;
        expanded.extend(rows);
    }
    Ok(expanded.into_iter().collect())
}

/// Org-plane confinement of a user-initiated deletion's expansion set.
///
/// [`expand_by_source_identity`] (and [`delete_source`]'s lookup) reach every
/// graph of the account holding the same `(source_type, source_id)`. That is
/// right on a single-owner (personal) account. On an ORG account every member
/// has their OWN member-private ocean (`graphs.kind = 'member'`), and members
/// legitimately hold independent atoms for the same upstream object (Google
/// gives every attendee's calendar the same event id; a Meet conference
/// transcript has one id for all attendees). A member deleting THEIR copy must
/// never retract another member's. So on an org account (one with at least one
/// `kind = 'member'` graph):
///
/// * origin is PRIVATE (`member`/`personal`): keep the origin graph itself plus
///   promoted copies in SHARED graphs (`org`/`team`) — the §7.3 "a promoted
///   copy dies with its original" contract — and drop every OTHER private
///   graph.
/// * origin is SHARED (`org`/`team`): keep the origin graph only; deleting from
///   a shared ocean never reaches anyone's private ocean (nor sibling shared
///   oceans).
///
/// Accounts with no member graph (personal accounts) are returned unchanged.
/// Residual, deliberate: a shared-ocean copy promoted by ANOTHER member from
/// their own private copy of the same source id is still reached by the
/// original's deletion (atoms carry no promoter provenance to distinguish it).
async fn confine_to_callers_scope(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    origin_graph_id: Uuid,
    targets: Vec<(Uuid, Uuid)>,
) -> Result<Vec<(Uuid, Uuid)>, sqlx::Error> {
    if targets.is_empty() {
        return Ok(targets);
    }
    let kinds: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT id, kind FROM graphs WHERE account_id = $1")
            .bind(account_id)
            .fetch_all(&mut **tx)
            .await?;
    let kind_of: std::collections::HashMap<Uuid, &str> =
        kinds.iter().map(|(g, k)| (*g, k.as_str())).collect();
    // Org plane = a member-private graph is visible, OR a target sits in a
    // graph this transaction cannot see (a tenant DB hides other members'
    // private graphs from a member-scoped transaction) — that hidden graph is
    // exactly the case this must not treat as a plain single-owner account.
    let org_plane = kinds.iter().any(|(_, k)| k == "member")
        || targets.iter().any(|(g, _)| !kind_of.contains_key(g));
    if !org_plane {
        return Ok(targets);
    }
    Ok(confine_targets(&kind_of, origin_graph_id, targets))
}

/// Pure filter behind [`confine_to_callers_scope`], FAIL CLOSED: a target is
/// kept only if it is the caller's own origin graph, or — when the origin is
/// private (`member`/`personal`) — its kind is KNOWN to be shared (`org` /
/// `team`). A graph missing from `kind_of` (hidden by RLS) or of any other
/// kind never counts as shared.
fn confine_targets(
    kind_of: &std::collections::HashMap<Uuid, &str>,
    origin_graph_id: Uuid,
    targets: Vec<(Uuid, Uuid)>,
) -> Vec<(Uuid, Uuid)> {
    let origin_private = matches!(
        kind_of.get(&origin_graph_id).copied(),
        Some("member") | Some("personal")
    );
    targets
        .into_iter()
        .filter(|(g, _)| {
            *g == origin_graph_id
                || (origin_private && matches!(kind_of.get(g).copied(), Some("org") | Some("team")))
        })
        .collect()
}

/// Run [`delete_atom_cascade`] once per resolved `(graph_id, atom_id)` pair,
/// accumulating the union outcome. Each atom gets its OWN transaction (see
/// [`delete_atom_cascade`]'s doc for why), so a failure partway through a
/// large set leaves every already-processed atom deleted — the caller
/// retries the WHOLE entry point and this loop simply re-resolves a smaller
/// remaining set (already-deleted atoms just come back `atom_was_present:
/// false` and are excluded from `atoms_deleted`, exactly the idempotent
/// "retracting something already retracted is success" contract §4.9
/// established).
///
/// Finding 10 (independent review, performance): a small, scoped, read-only
/// lookup of D8's "single-viewer ocean" predicate for ONE `graph_id` —
/// [`run_cascade`] calls this at most once per distinct graph per call
/// (never once per atom) and caches the result. Same predicate
/// `delete_atom_cascade` uses internally when it has no hint — kept as one
/// definition (a single `SELECT` text) so the two can never drift apart.
async fn resolve_is_personal_ocean(
    pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    scope: CascadeScope,
) -> Result<bool, DeletionError> {
    let mut tx = begin_cascade_tx(pool, account_id, scope).await?;
    let is_personal_ocean: bool = sqlx::query_scalar(IS_PERSONAL_OCEAN_QUERY)
        .bind(graph_id)
        .fetch_one(&mut *tx)
        .await?;
    tx.rollback().await.ok();
    Ok(is_personal_ocean)
}

/// **FM-5 audit write happens PER ATOM, inside this loop (findings 2/5,
/// independent review)** — immediately after that atom's OWN deletion
/// commits, not batched after the whole loop finishes. `retraction_id` is
/// minted ONCE by the caller (an entry point) and passed in here unchanged,
/// so every atom in one logical retract call carries the SAME id (the D6 UI
/// still renders one list per `retraction_id`) even though each atom's row
/// is written as its own, independent step. See `services::retraction_audit`'s
/// module doc for the full "why per-atom, and what residual race remains"
/// account — this is NOT the same shape as the R2-vs-Postgres ordering this
/// module's OWN doc describes elsewhere, but is the identical CLASS of
/// accepted two-system trade-off.
#[allow(clippy::too_many_arguments)]
async fn run_cascade(
    pool: &PgPool,
    account_id: Uuid,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    origin_graph_id: Uuid,
    targets: Vec<(Uuid, Uuid)>,
    control_pool: Option<&PgPool>,
    audit_actor: crate::services::connector_audit::Actor,
    retraction_id: Uuid,
    scope: CascadeScope,
    preserve_source_history: bool,
) -> Result<CascadeOutcome, DeletionError> {
    let mut outcome = CascadeOutcome {
        retraction_id,
        ..CascadeOutcome::default()
    };
    let mut propagated: HashSet<Uuid> = HashSet::new();
    // Finding 10 (independent review, performance): `is_personal_ocean` is
    // INVARIANT per `graph_id` within one call — resolved at most ONCE per
    // distinct graph here (propagation can span more than one), instead of
    // once per ATOM inside `delete_atom_cascade`. A `delete_subject` call
    // touching hundreds of atoms in one graph used to run that query
    // hundreds of times for an answer that never changes.
    let mut is_personal_ocean_cache: std::collections::HashMap<Uuid, bool> =
        std::collections::HashMap::new();
    for (graph_id, atom_id) in targets {
        if graph_id != origin_graph_id {
            propagated.insert(graph_id);
        }
        let is_personal_ocean_hint = match is_personal_ocean_cache.get(&graph_id) {
            Some(&v) => v,
            None => {
                let v = resolve_is_personal_ocean(pool, account_id, graph_id, scope).await?;
                is_personal_ocean_cache.insert(graph_id, v);
                v
            }
        };
        let single = delete_atom_cascade_scoped(
            pool,
            account_id,
            blobs,
            device_id,
            graph_id,
            atom_id,
            Some(is_personal_ocean_hint),
            scope,
            preserve_source_history,
        )
        .await?;
        if single.atom_was_present {
            outcome.atoms_deleted.push((graph_id, atom_id));
        }
        outcome.deleted_reef_ids.extend(
            single
                .deleted_reef_ids
                .into_iter()
                .map(|node_id| (graph_id, node_id)),
        );
        // FM-5/D7: `delete_atom_cascade` already includes the base
        // `"content"` `removed` row (plus any FM-2/FM-6/FM-3-disclosure
        // rows) whenever `atom_was_present` — nothing further to add when
        // the atom was already gone (an idempotent no-op retry must not
        // manufacture a second audit trail for the same fact). Written
        // NOW, for THIS atom alone, before the loop moves on — see this
        // function's own doc comment (findings 2/5) for why.
        record_retraction_audit(
            control_pool,
            account_id,
            audit_actor,
            retraction_id,
            &single.surface_outcomes,
        )
        .await?;
        outcome.surface_outcomes.extend(single.surface_outcomes);
    }
    outcome.propagated_graph_ids = propagated.into_iter().collect();
    Ok(outcome)
}

// ── Entry point 1: coral ─────────────────────────────────────────────────────

/// **Coral** entry point (§7.3): delete ONE atom by id, plus any promoted
/// copy of it elsewhere in the same account (propagation).
///
/// # Authorization
/// Caller must be able to reach `graph_id` (`ReachabilityResolver` —
/// checked by the route BEFORE this is called). No stronger gate: deleting a
/// single coral you can already read/act on is no more consequential than
/// any other member write this router already allows at that bar.
///
/// # FM-5 audit (B2 unit 02)
/// `audit_actor`/`control_pool` — see [`delete_source`]'s own doc comment
/// for the full durability-tier contract; `control_pool: None` disables the
/// audit write entirely (documented opt-out, used by callers that have no
/// control-plane pool in hand).
pub async fn delete_coral(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    graph_id: Uuid,
    atom_id: Uuid,
    audit_actor: crate::services::connector_audit::Actor,
    control_pool: Option<&PgPool>,
) -> Result<CascadeOutcome, DeletionError> {
    let Some(account_id) = resolve_account_id_for_graph(pool, graph_id).await? else {
        // Unknown graph — nothing to delete, idempotent no-op (matches
        // §4.9's "not found = already gone" contract; the route's own
        // reachability gate is what turns a genuinely unknown/foreign graph
        // into a 404 before this is ever reached).
        return Ok(CascadeOutcome::default());
    };
    // One scoped, read-only transaction for the whole setup phase (exists
    // check + propagation expansion) — `atoms`/`graphs` are both on
    // migration 0083's RLS roster, so both reads need `account_id` set.
    // Rolled back below (nothing here is a write); `run_cascade` opens its
    // own separate scoped transaction per resolved atom.
    let scope = CascadeScope::for_actor(audit_actor);
    let mut tx = begin_cascade_tx(pool, account_id, scope).await?;
    // Confirm the atom actually belongs to this graph before treating it as
    // a seed — a caller-supplied atom_id from a DIFFERENT graph must not
    // silently become a seed for propagation scoped to the wrong account.
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM atoms WHERE atom_id = $1 AND graph_id = $2)",
    )
    .bind(atom_id)
    .bind(graph_id)
    .fetch_one(&mut *tx)
    .await?;
    let seeds = if exists {
        vec![(graph_id, atom_id)]
    } else {
        vec![]
    };
    let targets = expand_by_source_identity(&mut tx, account_id, &seeds).await?;
    let targets = confine_to_callers_scope(&mut tx, account_id, graph_id, targets).await?;
    tx.rollback().await.ok();
    // Minted ONCE per logical call (findings 2/5) — every atom this call
    // resolves shares this id in its own, per-atom audit write inside
    // `run_cascade`.
    let retraction_id = Uuid::new_v4();
    run_cascade(
        pool, account_id, blobs, device_id, graph_id, targets, control_pool, audit_actor, retraction_id,
        scope,
        false,
    )
    .await
}

/// Shared FM-5 audit-write step [`run_cascade`] calls PER ATOM (findings
/// 2/5, independent review — see `services::retraction_audit`'s own module
/// doc, "PER-ATOM, not per-batch", for why this moved out of a single
/// post-loop call at each entry point). See [`delete_source`]'s doc comment
/// for the `Actor::User` (hard bar) vs `Actor::System`/`Actor::Webhook`
/// (best-effort) split this dispatches on. `control_pool: None` is a
/// documented no-op (audit disabled) regardless of actor — see
/// [`delete_coral`]'s own note.
async fn record_retraction_audit(
    control_pool: Option<&PgPool>,
    account_id: Uuid,
    actor: crate::services::connector_audit::Actor,
    retraction_id: Uuid,
    surface_outcomes: &[SurfaceOutcome],
) -> Result<(), DeletionError> {
    use crate::services::connector_audit::Actor;
    let Some(control_pool) = control_pool else {
        return Ok(());
    };
    match actor {
        Actor::User(_) => {
            crate::services::retraction_audit::record(
                control_pool,
                account_id,
                actor,
                retraction_id,
                surface_outcomes,
            )
            .await?;
        }
        Actor::System | Actor::Webhook => {
            crate::services::retraction_audit::record_best_effort(
                control_pool,
                account_id,
                actor,
                retraction_id,
                surface_outcomes,
            )
            .await;
        }
    }
    Ok(())
}

// ── Entry point 1b: atoms confined to ONE graph (no propagation) ─────────────

/// Delete the named `atom_ids` — each only if it actually lives in
/// `graph_id` — and NOTHING else: unlike [`delete_coral`]/[`delete_source`]
/// there is **no source-identity expansion** to sibling/promoted copies in
/// other graphs of the account.
///
/// Exists for connectors on an ORG account where several members legitimately
/// hold their OWN independent atom for the same upstream `(source_type,
/// source_id)` (Google gives every attendee's calendar the same event id). The
/// natural-key expansion above is right for "every copy of one ingested
/// source event (its original plus every promoted copy)" in a single-owner
/// account and wrong here: member A's cancellation of THEIR copy must never
/// retract member B's. Used by `connectors::google_calendar`'s org-member
/// deletion propagation. Residual, deliberate: a copy a human PROMOTED into a
/// shared ocean is not reached (it is an explicit decision of its own and is
/// retracted through the ordinary retract routes).
///
/// Idempotent exactly like the other entry points (an already-gone atom is a
/// no-op). Unknown graph => empty outcome.
pub(crate) async fn delete_atoms_in_graph_only(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    graph_id: Uuid,
    atom_ids: &[Uuid],
    audit_actor: crate::services::connector_audit::Actor,
    control_pool: Option<&PgPool>,
) -> Result<CascadeOutcome, DeletionError> {
    delete_atoms_in_graph_only_with_history(pool, blobs, device_id, graph_id, atom_ids,
        audit_actor, control_pool, false).await
}

/// Replace retrieval chunks while preserving the immutable upstream versions.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replace_atoms_in_graph_only(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    graph_id: Uuid,
    atom_ids: &[Uuid],
    audit_actor: crate::services::connector_audit::Actor,
    control_pool: Option<&PgPool>,
) -> Result<CascadeOutcome, DeletionError> {
    delete_atoms_in_graph_only_with_history(pool, blobs, device_id, graph_id, atom_ids,
        audit_actor, control_pool, true).await
}

#[allow(clippy::too_many_arguments)]
async fn delete_atoms_in_graph_only_with_history(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    graph_id: Uuid,
    atom_ids: &[Uuid],
    audit_actor: crate::services::connector_audit::Actor,
    control_pool: Option<&PgPool>,
    preserve_source_history: bool,
) -> Result<CascadeOutcome, DeletionError> {
    let Some(account_id) = resolve_account_id_for_graph(pool, graph_id).await? else {
        return Ok(CascadeOutcome::default());
    };
    let mut tx = begin_cascade_tx(pool, account_id, CascadeScope::TenantSystem).await?;
    // Only atoms that really belong to `graph_id` become targets — a caller-
    // supplied id from another graph is dropped, never deleted.
    let in_graph: Vec<Uuid> = sqlx::query_scalar(
        "SELECT atom_id FROM atoms WHERE graph_id = $1 AND atom_id = ANY($2)",
    )
    .bind(graph_id)
    .bind(atom_ids)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    let targets: Vec<(Uuid, Uuid)> = in_graph.into_iter().map(|a| (graph_id, a)).collect();
    run_cascade(
        pool,
        account_id,
        blobs,
        device_id,
        graph_id,
        targets,
        control_pool,
        audit_actor,
        Uuid::new_v4(),
        CascadeScope::TenantSystem,
        preserve_source_history,
    )
    .await
}

/// Retire positional retrieval chunks a re-capture no longer produces (the
/// source shrank), inside the capture's OWN transaction so a concurrent
/// re-capture can never adopt a chunk this call then deletes. Same primitive
/// and history rule as [`replace_atoms_in_graph_only`]: each atom goes
/// through [`delete_atom_cascade_tx`] with `preserve_source_history`, so
/// immutable source versions stay, ciphertext cleanup is queued durably for
/// after commit, and reefs left with no evidence are pruned once at the end.
/// No retraction audit row: this is replacement, not a user retraction.
///
/// The caller selects `atom_ids` (graph-local, positional, not bound to any
/// other source item). Returns the atoms actually deleted.
pub(crate) async fn retire_positional_chunks_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    device_id: &str,
    graph_id: Uuid,
    atom_ids: &[Uuid],
) -> Result<Vec<Uuid>, DeletionError> {
    let mut ids = atom_ids.to_vec();
    ids.sort();
    ids.dedup();
    let mut deferred = Vec::new();
    let mut deleted = Vec::new();
    for atom_id in ids {
        let outcome = delete_atom_cascade_tx(tx, account_id, None, device_id, graph_id,
            atom_id, None, true, Some(&mut deferred)).await?;
        if outcome.atom_was_present {
            deleted.push(atom_id);
        }
    }
    prune_nodes_without_evidence(tx, graph_id, &deferred, device_id).await?;
    Ok(deleted)
}

/// Delete one source identity from exactly one authorized graph, including a
/// snapshot that produced zero chunks. A provider namespace prevents one
/// mailbox's native IDs from reaching another mailbox's evidence.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn delete_source_identity_in_graph(
    pool:&PgPool,blobs:Option<&dyn PromotionBlobStore>,device_id:&str,
    graph_id:Uuid,source_type:&str,source_id:&str,namespace:Option<&str>,
    audit_actor:crate::services::connector_audit::Actor,control_pool:Option<&PgPool>,
) -> Result<CascadeOutcome,DeletionError> {
    let Some(account_id)=resolve_account_id_for_graph(pool,graph_id).await? else {
        return Ok(CascadeOutcome::default());
    };
    let mut tx=begin_cascade_tx(pool,account_id,match audit_actor {crate::services::connector_audit::Actor::User(_)=>CascadeScope::for_actor(audit_actor),_=>CascadeScope::TenantSystem}).await?;
    let mut outcome=CascadeOutcome {retraction_id:Uuid::new_v4(),..Default::default()};
    delete_source_identity_tx(&mut tx,account_id,blobs,device_id,graph_id,source_type,
        source_id,namespace,true,&mut outcome).await?;
    tx.commit().await?;
    record_retraction_audit(control_pool,account_id,audit_actor,outcome.retraction_id,&outcome.surface_outcomes).await?;
    record_source_deletion_audit(pool,account_id,audit_actor,source_type,source_id,&outcome).await?;
    Ok(outcome)
}

/// Capture and deletion take the same transaction-scoped source lock. The
/// lock, chunk enumeration, chunk removal, history purge and node cleanup all
/// use THIS connection. Cancellation rolls back; a one-connection pool works.
#[allow(clippy::too_many_arguments)]
async fn delete_source_identity_tx(
    tx:&mut sqlx::Transaction<'_,sqlx::Postgres>,account_id:Uuid,
    blobs:Option<&dyn PromotionBlobStore>,device_id:&str,graph_id:Uuid,
    source_type:&str,source_id:&str,namespace:Option<&str>,chunked:bool,
    outcome:&mut CascadeOutcome,
) -> Result<(),DeletionError> {
    let base=if chunked {source_id} else {crate::workers::journal_sweep::strip_chunk_suffix(source_id)};
    crate::source_evidence::lock_identity_tx(tx,graph_id,base).await?;
    lock_source_evidence_tx(tx,graph_id,source_type,base).await?;
    // Microsoft native IDs are mailbox-local. An unlinked legacy chunk is
    // eligible only when its stored ingest actor proves this mailbox owner.
    let legacy_owner=namespace.filter(|ns|ns.starts_with("microsoft:connection:") || ns.starts_with("declared:user:"))
        .and_then(|ns|ns.rsplit(':').next()).and_then(|id|Uuid::parse_str(id).ok());
    let ids:Vec<Uuid>=sqlx::query_scalar(
        "SELECT a.atom_id FROM atoms a WHERE a.graph_id=$1 AND a.source_type=$2 \
         AND (a.source_id=$3 OR ($6 AND a.source_id LIKE $4 AND substring(a.source_id FROM char_length($3)+1) ~ '^:chunk:[0-9]+$')) \
         AND ($5::text IS NULL OR EXISTS(SELECT 1 FROM source_version_chunks c \
             JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id \
             JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id \
             WHERE c.atom_id=a.atom_id AND c.graph_id=a.graph_id AND s.provider_namespace=$5 AND s.source_type=$2 AND s.source_id=$3) \
             OR (($7::uuid IS NULL OR a.ingested_by=$7) AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.atom_id=a.atom_id AND c.graph_id=a.graph_id) \
                 AND NOT EXISTS(SELECT 1 FROM source_items s WHERE s.graph_id=a.graph_id AND s.source_type=$2 AND s.source_id=$3 AND s.provider_namespace<>$5))) \
         AND (NOT ($6 AND $3 ~ ':chunk:[0-9]+$' AND a.source_id=$3) \
             OR EXISTS(SELECT 1 FROM source_version_chunks c \
                 JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id \
                 JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id \
                 WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id AND s.source_type=$2 AND s.source_id=$3 AND ($5::text IS NULL OR s.provider_namespace=$5))) \
         AND (NOT $6 OR NOT EXISTS(SELECT 1 FROM source_version_chunks c \
             JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id \
             JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id \
             WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id \
             AND (s.source_type<>$2 OR s.source_id<>$3 OR ($5::text IS NOT NULL AND s.provider_namespace<>$5)))) ORDER BY a.atom_id",
    ).bind(graph_id).bind(source_type).bind(source_id).bind(escaped_chunk_pattern(source_id)).bind(namespace).bind(chunked).bind(legacy_owner)
        .fetch_all(&mut **tx).await?;
    let personal:bool=sqlx::query_scalar(IS_PERSONAL_OCEAN_QUERY).bind(graph_id).fetch_one(&mut **tx).await?;
    let mut nodes=Vec::new();
    for atom_id in ids {
        let single=delete_atom_cascade_tx(tx,account_id,blobs,device_id,graph_id,atom_id,Some(personal),true,Some(&mut nodes)).await?;
        if single.atom_was_present {outcome.atoms_deleted.push((graph_id,atom_id));}
        outcome.surface_outcomes.extend(single.surface_outcomes);
    }
    // Legacy exact-chunk callers retain sibling source history. Whole-source
    // and UUID/provider callers delete every matching chunk and the source.
    let remaining:bool=sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM atoms WHERE graph_id=$1 AND source_type=$2 AND \
         (source_id=$3 OR (source_id LIKE $4 AND substring(source_id FROM char_length($3)+1) ~ '^:chunk:[0-9]+$')))",
    ).bind(graph_id).bind(source_type).bind(base).bind(escaped_chunk_pattern(base)).fetch_one(&mut **tx).await?;
    if chunked || !remaining {
        if let Some(surface)=purge_source_evidence_tx(tx,blobs,account_id,graph_id,personal,
            source_type,base,namespace,&mut nodes).await? {outcome.surface_outcomes.push(surface);}
        if namespace.is_none() && matches!(ChunkStrategy::for_source_type(source_type),ChunkStrategy::Transcript) {
            if let Some(surface)=redact_orphaned_source_evidence(tx,blobs,account_id,graph_id,personal,
                "transcript",base,Some("legacy:meeting"),&mut nodes).await? {outcome.surface_outcomes.push(surface);}
        }
    }
    outcome.deleted_reef_ids.extend(prune_nodes_without_evidence(tx,graph_id,&nodes,device_id).await?
        .into_iter().map(|id|(graph_id,id)));
    Ok(())
}

async fn record_source_deletion_audit(
    pool:&PgPool,account_id:Uuid,actor:crate::services::connector_audit::Actor,
    source_type:&str,source_id:&str,outcome:&CascadeOutcome,
) -> Result<(),DeletionError> {
    let entry=crate::services::connector_audit::AuditEntry {
        account_id,actor,action:"deletion_propagated",
        provider:Some(crate::services::connector_audit::provider_from_source_type(source_type).to_string()),
        subject:Some(source_id.to_string()),
        detail:Some(serde_json::json!({"atoms_deleted":outcome.atoms_deleted.len(),"reefs_deleted":outcome.deleted_reef_ids.len(),"graphs_touched":outcome.propagated_graph_ids.len()})),
    };
    match actor {
        crate::services::connector_audit::Actor::User(_)=>crate::services::connector_audit::record(pool,entry).await?,
        _=>crate::services::connector_audit::record_or_queue(pool,entry).await,
    }
    Ok(())
}

/// UUID-addressed source retraction; the HTTP caller first authorizes graph.
#[allow(clippy::too_many_arguments)]
pub async fn delete_source_item_in_graph(
    pool:&PgPool,blobs:Option<&dyn PromotionBlobStore>,device_id:&str,
    graph_id:Uuid,source_item_id:Uuid,audit_actor:crate::services::connector_audit::Actor,
    control_pool:Option<&PgPool>,
) -> Result<CascadeOutcome,DeletionError> {
    let Some(account_id)=resolve_account_id_for_graph(pool,graph_id).await? else {return Ok(CascadeOutcome::default());};
    let mut tx=begin_cascade_tx(pool,account_id,CascadeScope::for_actor(audit_actor)).await?;
    let identity:Option<(String,String,String)>=sqlx::query_as(
        "SELECT source_type,source_id,provider_namespace FROM source_items WHERE graph_id=$1 AND source_item_id=$2",
    ).bind(graph_id).bind(source_item_id).fetch_optional(&mut *tx).await?;
    let Some((kind,native,namespace))=identity else{return Ok(CascadeOutcome::default());};
    crate::source_evidence::lock_identity_tx(&mut tx,graph_id,&native).await?;
    // The UUID may have been deleted while waiting for the identity guard.
    // Never reinterpret an old UUID as a newly recreated source identity.
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM source_items WHERE graph_id=$1 AND source_item_id=$2)")
        .bind(graph_id).bind(source_item_id).fetch_one(&mut *tx).await?;
    if !exists { return Ok(CascadeOutcome::default()); }
    let mut outcome=CascadeOutcome {retraction_id:Uuid::new_v4(),..Default::default()};
    delete_source_identity_tx(&mut tx,account_id,blobs,device_id,graph_id,&kind,&native,
        Some(&namespace),true,&mut outcome).await?;
    tx.commit().await?;
    record_retraction_audit(control_pool,account_id,audit_actor,outcome.retraction_id,&outcome.surface_outcomes).await?;
    record_source_deletion_audit(pool,account_id,audit_actor,&kind,&native,&outcome).await?;
    Ok(outcome)
}

// ── Entry point 2: source ────────────────────────────────────────────────────

/// **Source** entry point (§7.3): delete every atom, anywhere in the same
/// account, whose `(source_type, source_id)` matches — i.e. every copy of
/// one ingested source event (its original plus every promoted copy). This
/// is a lookup, not a graph walk, per this module's "Propagation" doc: the
/// `(source_type, source_id)` pair IS the coral's identity across oceans.
///
/// `origin_graph_id` anchors WHICH account's graphs this call is entitled to
/// touch (resolved authoritatively — see [`resolve_account_id_for_graph`]);
/// it need not itself contain a matching atom (e.g. a caller retracting a
/// source from an ocean they can reach, whose only remaining copy already
/// lives in a sibling ocean of the same account).
///
/// # Authorization
/// Same as [`delete_coral`]: reachability on `origin_graph_id`, no stronger
/// gate. This can delete MANY atoms, but every one of them is already fully
/// within the caller's own account (never crosses accounts) and all share
/// one unambiguous identity (the exact source event) — the caller is not
/// guessing at a heuristic, they are naming the exact thing to remove.
///
/// `audit_actor` (m365 enterprise train PR 4.2, plan decision #11 part 1):
/// who/what caused this propagation, for the `connector_audit_log`
/// `deletion_propagated` row this function emits after a successful
/// cascade — see `services::connector_audit`'s module doc. A real user
/// (`routes::deletion::delete_source_route`) gets the HARD durability bar:
/// [`connector_audit::Actor::User`] routes the emit through `record`, whose
/// failure is propagated as `DeletionError::Db` — an audited action that
/// cannot write its audit row fails here too, even though the atoms are
/// already gone (same "the response must reflect the compliance
/// invariant" reasoning as `routes::oauth::callback`'s `install` emit).
/// `System`/`Webhook` (the gsuite Calendar/Gmail deletion-propagation sync
/// paths) route through `record_or_queue` instead: those callers already
/// treat a `delete_source` failure as retriable (see their own doc
/// comments), so failing the whole cascade over an audit-row failure would
/// be a regression, not a strengthening.
pub async fn delete_source(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    origin_graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    audit_actor: crate::services::connector_audit::Actor,
    control_pool: Option<&PgPool>,
) -> Result<CascadeOutcome, DeletionError> {
    delete_source_impl(
        pool, blobs, device_id, origin_graph_id, source_type, source_id, audit_actor, control_pool,
        false, false, None,
    )
    .await
}

/// Delete a provider's bare item and its numeric ingest chunks as one audited
/// operation. Only connector paths with this explicit source-id convention use
/// this entry point; user retractions and other formats retain exact matching.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn delete_chunked_source(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    origin_graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    audit_actor: crate::services::connector_audit::Actor,
    control_pool: Option<&PgPool>,
) -> Result<CascadeOutcome, DeletionError> {
    delete_source_impl(
        pool, blobs, device_id, origin_graph_id, source_type, source_id, audit_actor, control_pool,
        false, true, None,
    )
    .await
}

/// Provider deletion names the trusted connector namespace as well as native
/// identity. Same native IDs in another mailbox/workspace remain untouched.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn delete_chunked_source_in_namespace(
    pool:&PgPool,blobs:Option<&dyn PromotionBlobStore>,device_id:&str,
    graph_id:Uuid,source_type:&str,source_id:&str,namespace:&str,
    audit_actor:crate::services::connector_audit::Actor,control_pool:Option<&PgPool>,
) -> Result<CascadeOutcome,DeletionError> {
    delete_source_impl(pool,blobs,device_id,graph_id,source_type,source_id,
        audit_actor,control_pool,false,true,Some(namespace)).await
}

/// [`delete_source`] for a USER-initiated retraction (`routes::deletion`): on
/// an org account the expansion is confined to the caller's own scope — see
/// [`confine_to_callers_scope`]. Connector callers (an upstream deletion that
/// really should retract every member's copy) keep using [`delete_source`].
/// Identical to [`delete_source`] on accounts with no member-private oceans.
#[allow(clippy::too_many_arguments)]
pub async fn delete_source_in_callers_scope(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    origin_graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    audit_actor: crate::services::connector_audit::Actor,
    control_pool: Option<&PgPool>,
) -> Result<CascadeOutcome, DeletionError> {
    delete_source_impl(
        pool, blobs, device_id, origin_graph_id, source_type, source_id, audit_actor, control_pool,
        true, false, None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn delete_source_impl(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    origin_graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    audit_actor: crate::services::connector_audit::Actor,
    control_pool: Option<&PgPool>,
    confine: bool,
    chunked: bool,
    namespace: Option<&str>,
) -> Result<CascadeOutcome, DeletionError> {
    let Some(account_id) = resolve_account_id_for_graph(pool, origin_graph_id).await? else {
        return Ok(CascadeOutcome::default());
    };
    let scope = if confine { CascadeScope::for_actor(audit_actor) }
        else if namespace.is_some() && !matches!(audit_actor,crate::services::connector_audit::Actor::User(_)) {CascadeScope::TenantSystem}
        else { CascadeScope::Account };
    let mut tx=begin_cascade_tx(pool,account_id,scope).await?;
    let graph_ids:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM graphs WHERE account_id=$1 ORDER BY id")
        .bind(account_id).fetch_all(&mut *tx).await?;
    let graph_targets=graph_ids.into_iter().map(|id|(id,Uuid::nil())).collect();
    let graph_targets=if confine {confine_to_callers_scope(&mut tx,account_id,origin_graph_id,graph_targets).await?} else {graph_targets};
    let mut outcome=CascadeOutcome {retraction_id:Uuid::new_v4(),..Default::default()};
    for (graph_id,_) in graph_targets {
        let before=outcome.atoms_deleted.len()+outcome.surface_outcomes.len();
        delete_source_identity_tx(&mut tx,account_id,blobs,device_id,graph_id,source_type,
            source_id,namespace,chunked,&mut outcome).await?;
        if graph_id!=origin_graph_id && before!=outcome.atoms_deleted.len()+outcome.surface_outcomes.len() {
            outcome.propagated_graph_ids.push(graph_id);
        }
    }
    tx.commit().await?;
    record_retraction_audit(control_pool,account_id,audit_actor,outcome.retraction_id,&outcome.surface_outcomes).await?;
    record_source_deletion_audit(pool,account_id,audit_actor,source_type,source_id,&outcome).await?;
    Ok(outcome)
}

// ── Entry point 3: subject ───────────────────────────────────────────────────

/// **Subject** entry point (§7.3): delete every coral currently linked
/// (`node_atoms`) to a person-reef, plus any promoted copy of any of those
/// corals elsewhere in the same account (propagation). The graph already
/// indexes exactly this: `node_atoms` for a person's reef enumerates every
/// coral linked to them, so there is no additional index or scan to build.
///
/// `subject_node_id` must name a `type = 'entity'` reef inside `graph_id`
/// (`SubjectNotFound` otherwise) — a "person" in this graph model IS an
/// entity reef, and restricting to that type keeps this entry point from
/// being pointed at a `decision`/`thread`/`reference` reef to bulk-delete an
/// unrelated set of corals under looser semantics than the compliance
/// use-case it exists for.
///
/// Once every linked coral is gone, the subject reef itself has zero
/// remaining corals and is deleted by the SAME coral-less-reef rule every
/// other entry point uses — no special-casing needed here.
///
/// # CAVEAT THAT MUST REACH A CUSTOMER: subject deletion is APPROXIMATE
///
/// Reef membership is extraction-derived and lossy:
///
/// * It **misses** corals that genuinely mention or concern this person but
///   that extraction never linked to their reef (a missed entity mention, an
///   indirect reference, a nickname the extractor didn't resolve).
/// * It **includes** corals that are only INCIDENTALLY about this person —
///   linked because they were mentioned in passing, not because the coral is
///   substantively about them.
///
/// This is a real limitation of any compliance claim built on it, and it
/// must be stated to a customer at the call site that invokes this
/// capability — not left to live only in a spec document nobody reads. See
/// `routes::deletion::delete_subject_route`'s response shape, which surfaces
/// this same caveat text on the wire for exactly that reason.
///
/// # Authorization — the ONE entry point with a stronger gate than reachability
///
/// `caller_role` must satisfy [`is_admin_role`] (`owner` or `admin` on
/// `account_members`/its org-role mapping) — checked HERE, inside the
/// function, not only by the route, so a future second call site cannot
/// silently reintroduce the weaker reachability-only bar. See this module's
/// top-level "Authorization" section for the full justification: this is a
/// single bulk action, driven by a heuristic, that can erase other people's
/// evidentiary trail from the shared ocean all at once — a materially bigger
/// blast radius than the other two entry points, and the reason it does NOT
/// inherit their bar.
/// FM-5 audit (B2 unit 02): `caller_user_id` — always a real, gate-verified
/// user (there is no System/Webhook caller of subject deletion) —
/// and `control_pool`, see [`delete_coral`]'s own note on the `None` opt-out.
#[allow(clippy::too_many_arguments)]
pub async fn delete_subject(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    graph_id: Uuid,
    subject_node_id: Uuid,
    caller_role: &str,
    caller_user_id: Uuid,
    control_pool: Option<&PgPool>,
) -> Result<CascadeOutcome, DeletionError> {
    if !is_admin_role(caller_role) {
        return Err(DeletionError::InsufficientRole);
    }
    let Some(account_id) = resolve_account_id_for_graph(pool, graph_id).await? else {
        return Err(DeletionError::SubjectNotFound);
    };
    // Scoped, read-only setup transaction — `nodes`/`node_atoms`/`atoms` are
    // all on migration 0083's RLS roster (same reasoning as `delete_coral`'s
    // and `delete_source`'s setup transactions above).
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id).await?;
    let is_entity_subject: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1 AND graph_id = $2 AND type = 'entity')",
    )
    .bind(subject_node_id)
    .bind(graph_id)
    .fetch_one(&mut *tx)
    .await?;
    if !is_entity_subject {
        tx.rollback().await.ok();
        return Err(DeletionError::SubjectNotFound);
    }

    let seeds: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT a.graph_id, a.atom_id FROM node_atoms na \
         JOIN atoms a ON a.atom_id = na.atom_id \
         WHERE na.node_id = $1 AND a.graph_id = $2",
    )
    .bind(subject_node_id)
    .bind(graph_id)
    .fetch_all(&mut *tx)
    .await?;

    let targets = expand_by_source_identity(&mut tx, account_id, &seeds).await?;
    // Semantic-only entity nodes have no node_atoms rows. Delete the exact
    // indexed source evidence too, rather than returning an empty success.
    let sources:Vec<(String,String,String)>=sqlx::query_as(
        "SELECT DISTINCT s.source_type,s.source_id,s.provider_namespace FROM node_semantic_atoms n \
         JOIN semantic_atoms a ON a.graph_id=n.graph_id AND a.semantic_atom_id=n.semantic_atom_id \
         JOIN source_versions v ON v.graph_id=a.graph_id AND v.version_id=a.source_version_id \
         JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id \
         WHERE n.graph_id=$1 AND n.node_id=$2 ORDER BY s.source_id,s.source_type,s.provider_namespace",
    ).bind(graph_id).bind(subject_node_id).fetch_all(&mut *tx).await?;
    // Acquire every source/version lock before any helper can prune nodes.
    for (kind,native,_) in &sources {
        crate::source_evidence::lock_identity_tx(&mut tx,graph_id,native).await?;
        lock_source_evidence_tx(&mut tx,graph_id,kind,native).await?;
    }
    let actor=crate::services::connector_audit::Actor::User(caller_user_id);
    let retraction_id=Uuid::new_v4();
    let mut semantic=CascadeOutcome {retraction_id,..Default::default()};
    for (kind,native,namespace) in &sources {
        delete_source_identity_tx(&mut tx,account_id,blobs,device_id,graph_id,kind,native,
            Some(namespace),true,&mut semantic).await?;
    }
    tx.commit().await?;
    record_retraction_audit(control_pool,account_id,actor,retraction_id,&semantic.surface_outcomes).await?;
    let mut legacy=run_cascade(pool,account_id,blobs,device_id,graph_id,targets,
        control_pool,actor,retraction_id,CascadeScope::Account,false).await?;
    legacy.atoms_deleted.extend(semantic.atoms_deleted);
    legacy.deleted_reef_ids.extend(semantic.deleted_reef_ids);
    legacy.surface_outcomes.extend(semantic.surface_outcomes);
    Ok(legacy)

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_is_admin() {
        assert!(is_admin_role("owner"));
    }

    #[test]
    fn admin_is_admin() {
        assert!(is_admin_role("admin"));
    }

    #[test]
    fn unmapped_clerk_role_is_not_admin() {
        // clerk_provision::map_org_role collapses Clerk roles to the exact
        // literals before they are stored. A raw, unmapped Clerk role that a
        // future caller passes through must fail closed, not match on a
        // substring.
        assert!(!is_admin_role("org:admin"));
        assert!(!is_admin_role("org:billing_admin"));
        assert!(!is_admin_role("billing_admin"));
    }

    #[test]
    fn plain_member_is_not_admin() {
        assert!(!is_admin_role("member"));
    }

    #[test]
    fn empty_role_is_not_admin() {
        assert!(!is_admin_role(""));
    }

    // ── Org-plane confinement of user-initiated deletion (pg-gated) ──────────
    //
    //   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //     cargo test -p coralswarm-backend --lib services::deletion::tests::org_

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn org_base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }

    fn org_with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }

    /// One tenant DB, one org account: Alice's and Bob's member-private oceans
    /// plus a shared `org` ocean, each holding an atom for the SAME
    /// `(google_calendar, shared-evt)` source (every attendee's copy, plus a
    /// promoted copy in the shared ocean).
    struct OrgFixture {
        pool: PgPool,
        account_id: Uuid,
        base: String,
        db: String,
        alice_user: Uuid,
        alice_graph: Uuid,
        bob_graph: Uuid,
        shared_graph: Uuid,
        alice_atom: Uuid,
        bob_atom: Uuid,
        shared_atom: Uuid,
    }

    impl OrgFixture {
        async fn setup(base: &str, db: &str) -> Self {
            let mut admin = PgConnection::connect(base).await.expect("connect admin");
            admin
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .unwrap();
            admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
            admin.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&org_with_db(base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
            sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_deletion_scope')")
                .execute(&pool)
                .await
                .unwrap();
            let account_id = Uuid::new_v4();
            let alice_user = Uuid::new_v4();
            let mut graphs = Vec::new();
            for (kind, owner) in [
                ("member", Some(alice_user)),
                ("member", Some(Uuid::new_v4())),
                ("org", None),
            ] {
                let g: Uuid = sqlx::query_scalar(
                    "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
                     VALUES ($1, 'ocean', $2, $3, $4) RETURNING id",
                )
                .bind(account_id)
                .bind(kind)
                .bind(kind == "member")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
                graphs.push(g);
            }
            let mut atoms = Vec::new();
            for g in &graphs {
                let a: Uuid = sqlx::query_scalar(
                    "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
                     VALUES ($1, 'google_calendar', 'shared-evt', $2, now()) RETURNING atom_id",
                )
                .bind(g)
                .bind(format!("blob-{g}"))
                .fetch_one(&pool)
                .await
                .unwrap();
                atoms.push(a);
            }
            Self {
                pool,
                account_id,
                base: base.to_string(),
                db: db.to_string(),
                alice_user,
                alice_graph: graphs[0],
                bob_graph: graphs[1],
                shared_graph: graphs[2],
                alice_atom: atoms[0],
                bob_atom: atoms[1],
                shared_atom: atoms[2],
            }
        }

        async fn present(&self, atom: Uuid) -> bool {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM atoms WHERE atom_id = $1")
                .bind(atom)
                .fetch_one(&self.pool)
                .await
                .unwrap()
                == 1
        }

        /// A pool connected AS the real NOBYPASSRLS `coralswarm_tenant_app`
        /// role (same recipe as `google_calendar`'s role test), so RLS is
        /// actually enforced and a vacuous pass is impossible.
        async fn app_role_pool(&self) -> PgPool {
            let pw = "org_deletion_scope_tenant_role_test_only_pw";
            let stmt = format!("ALTER ROLE coralswarm_tenant_app WITH PASSWORD '{pw}'");
            let mut ok = false;
            for attempt in 0..10u32 {
                if sqlx::query(&stmt).execute(&self.pool).await.is_ok() {
                    ok = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50 * u64::from(attempt + 1))).await;
            }
            assert!(ok, "set test password on coralswarm_tenant_app");
            let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(
                &org_with_db(&self.base, &self.db),
            )
            .unwrap()
            .username("coralswarm_tenant_app")
            .password(pw);
            PgPoolOptions::new()
                .max_connections(2)
                .connect_with(opts)
                .await
                .expect("connect AS coralswarm_tenant_app")
        }

        async fn teardown(self) {
            self.pool.close().await;
            let mut admin = PgConnection::connect(&self.base).await.unwrap();
            admin
                .execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db).as_str())
                .await
                .ok();
        }
    }

    #[tokio::test]
    async fn org_connector_source_deletion_uses_tenant_system_without_widening_user_scope_pg() {
        let Some(base)=org_base_url() else {return;};
        let f=OrgFixture::setup(&base,"org_del_source_connector_scope").await;
        let app=f.app_role_pool().await;
        let bob:Uuid=sqlx::query_scalar("SELECT owner_user_id FROM graphs WHERE id=$1")
            .bind(f.bob_graph).fetch_one(&f.pool).await.unwrap();
        let namespace=format!("google_calendar:connection:{}:{}",f.account_id,f.alice_user);
        let mut tx=crate::tenancy::scope::begin_tenant_system_scoped(&app,f.account_id).await.unwrap();
        let metadata=serde_json::json!({});
        let mut items=Vec::new();
        for native in ["shared-evt","zero_%_chunk:0"] {
            let capture=crate::source_evidence::capture_source_version_tx(&mut tx,None,f.alice_graph,f.account_id,
                &crate::source_evidence::CaptureSource {
                    source_type:"google_calendar",provider_namespace:&namespace,source_id:native,
                    normalized_text:"A member-private calendar event.",representation:"normalized_text",metadata:&metadata,
                    occurred_at:None,participants:&[],ingested_by:Some(f.alice_user),scope:"private",duration_ms:None,segments:None,
                },Some("org_deletion_scope"),&[7;32]).await.unwrap();
            if native=="shared-evt" {
                crate::source_evidence::link_verified_chunks_tx(&mut tx,f.alice_graph,capture.version_id,&[f.alice_atom]).await.unwrap();
            }
            items.push(capture.source_item_id);
        }
        tx.commit().await.unwrap();
        let denied=delete_source_item_in_graph(&app,None,"test",f.alice_graph,items[0],
            crate::services::connector_audit::Actor::User(bob),None).await.unwrap();
        assert!(denied.atoms_deleted.is_empty());
        assert!(f.present(f.alice_atom).await,"another member must not gain provider system privileges");
        let deleted=delete_source_identity_in_graph(&app,None,"test",f.alice_graph,"google_calendar",
            "shared-evt",Some(&namespace),crate::services::connector_audit::Actor::Webhook,None).await.unwrap();
        assert_eq!(deleted.atoms_deleted,vec![(f.alice_graph,f.alice_atom)]);
        delete_chunked_source_in_namespace(&app,None,"test",f.alice_graph,"google_calendar",
            "zero_%_chunk:0",&namespace,crate::services::connector_audit::Actor::System,None).await.unwrap();
        let remaining:i64=sqlx::query_scalar("SELECT count(*) FROM source_items WHERE source_item_id=ANY($1)")
            .bind(&items).fetch_one(&f.pool).await.unwrap();
        assert_eq!(remaining,0,"provider system scope must reach member-private zero-chunk snapshots");
        assert!(f.present(f.bob_atom).await);
        assert!(f.present(f.shared_atom).await);
        app.close().await;
        f.teardown().await;
    }

    /// `DELETE /v1/graphs/{g}/corals/{a}` on an org account: Alice deleting
    /// HER copy retracts her atom and the promoted copy in the shared ocean,
    /// never Bob's private copy of the same upstream event.
    #[tokio::test]
    async fn org_user_coral_deletion_does_not_reach_another_members_ocean_pg() {
        let Some(base) = org_base_url() else {
            eprintln!("skipping org_user_coral_deletion_does_not_reach_another_members_ocean_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let f = OrgFixture::setup(&base, "org_del_scope_coral").await;
        delete_coral(
            &f.pool,
            None,
            "dev",
            f.alice_graph,
            f.alice_atom,
            crate::services::connector_audit::Actor::System,
            None,
        )
        .await
        .unwrap();
        assert!(!f.present(f.alice_atom).await, "the caller's own atom is deleted");
        assert!(!f.present(f.shared_atom).await, "the promoted copy in a shared ocean dies with its original");
        assert!(f.present(f.bob_atom).await, "another member's private copy must survive");
        f.teardown().await;
    }

    /// `DELETE /v1/graphs/{g}/sources` (the user route's entry point) — same
    /// confinement; and the connector-facing `delete_source` keeps its
    /// account-wide semantics (an upstream deletion retracts every copy).
    #[tokio::test]
    async fn org_user_source_deletion_does_not_reach_another_members_ocean_pg() {
        let Some(base) = org_base_url() else {
            eprintln!("skipping org_user_source_deletion_does_not_reach_another_members_ocean_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let f = OrgFixture::setup(&base, "org_del_scope_source").await;
        delete_source_in_callers_scope(
            &f.pool,
            None,
            "dev",
            f.alice_graph,
            "google_calendar",
            "shared-evt",
            crate::services::connector_audit::Actor::System,
            None,
        )
        .await
        .unwrap();
        assert!(!f.present(f.alice_atom).await);
        assert!(!f.present(f.shared_atom).await);
        assert!(f.present(f.bob_atom).await, "another member's private copy must survive");

        // Connector-driven (upstream) deletion is deliberately unchanged.
        delete_source(
            &f.pool,
            None,
            "dev",
            f.alice_graph,
            "google_calendar",
            "shared-evt",
            crate::services::connector_audit::Actor::System,
            None,
        )
        .await
        .unwrap();
        assert!(!f.present(f.bob_atom).await, "delete_source (connector path) stays account-wide");
        f.teardown().await;
    }

    /// Deleting from a SHARED ocean touches that shared ocean only — never any
    /// member's private copy.
    #[tokio::test]
    async fn org_shared_ocean_deletion_stays_in_the_shared_ocean_pg() {
        let Some(base) = org_base_url() else {
            eprintln!("skipping org_shared_ocean_deletion_stays_in_the_shared_ocean_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let f = OrgFixture::setup(&base, "org_del_scope_shared").await;
        delete_coral(
            &f.pool,
            None,
            "dev",
            f.shared_graph,
            f.shared_atom,
            crate::services::connector_audit::Actor::System,
            None,
        )
        .await
        .unwrap();
        assert!(!f.present(f.shared_atom).await);
        assert!(f.present(f.alice_atom).await, "a shared-ocean delete must not reach a private ocean");
        assert!(f.present(f.bob_atom).await, "a shared-ocean delete must not reach a private ocean");

        delete_source_in_callers_scope(
            &f.pool,
            None,
            "dev",
            f.shared_graph,
            "google_calendar",
            "shared-evt",
            crate::services::connector_audit::Actor::System,
            None,
        )
        .await
        .unwrap();
        assert!(f.present(f.alice_atom).await);
        assert!(f.present(f.bob_atom).await);
        f.teardown().await;
    }

    /// Personal plane (no member-private oceans) is unchanged: a personal
    /// ocean's deletion still reaches the account's other oceans.
    #[tokio::test]
    async fn org_confinement_leaves_a_personal_account_unchanged_pg() {
        let Some(base) = org_base_url() else {
            eprintln!("skipping org_confinement_leaves_a_personal_account_unchanged_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let f = OrgFixture::setup(&base, "org_del_scope_personal").await;
        // Turn the fixture into a personal-shaped account: no member graphs.
        sqlx::query("UPDATE graphs SET kind = 'personal', owner_user_id = NULL, is_primary = (id = $2) WHERE id = ANY($1)")
            .bind(vec![f.alice_graph, f.bob_graph])
            .bind(f.alice_graph)
            .execute(&f.pool)
            .await
            .unwrap();
        delete_coral(
            &f.pool,
            None,
            "dev",
            f.alice_graph,
            f.alice_atom,
            crate::services::connector_audit::Actor::System,
            None,
        )
        .await
        .unwrap();
        assert!(!f.present(f.alice_atom).await);
        assert!(!f.present(f.bob_atom).await, "single-owner accounts keep account-wide propagation");
        assert!(!f.present(f.shared_atom).await);
        f.teardown().await;
    }

    /// The user routes AS the real NOBYPASSRLS `coralswarm_tenant_app` role,
    /// acting as Alice (`Actor::User` => member-scoped transaction): her own
    /// atom and the promoted shared copy are retracted, Bob's private copy
    /// survives, and the atoms really were visible to RLS (so this cannot
    /// pass vacuously by finding nothing).
    #[tokio::test]
    async fn org_user_deletion_works_and_is_confined_as_the_real_tenant_app_role_pg() {
        let Some(base) = org_base_url() else {
            eprintln!("skipping org_user_deletion_works_and_is_confined_as_the_real_tenant_app_role_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let f = OrgFixture::setup(&base, "org_del_scope_role").await;
        let app = f.app_role_pool().await;
        let actor = crate::services::connector_audit::Actor::User(f.alice_user);

        let out = delete_coral(&app, None, "dev", f.alice_graph, f.alice_atom, actor, None)
            .await
            .unwrap();
        assert!(
            out.atoms_deleted.iter().any(|(_, a)| *a == f.alice_atom),
            "alice's own atom must be visible to and deleted by her member-scoped cascade \
             (an empty outcome would mean RLS hid it — a vacuous pass)"
        );
        assert!(!f.present(f.alice_atom).await);
        assert!(!f.present(f.shared_atom).await, "promoted shared copy dies with the original");
        assert!(f.present(f.bob_atom).await, "bob's private copy must survive");

        // Source route: re-seed Alice's copy, then delete by source identity.
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'google_calendar', 'shared-evt', 'blob-alice-2', now())",
        )
        .bind(f.alice_graph)
        .execute(&f.pool)
        .await
        .unwrap();
        let out = delete_source_in_callers_scope(
            &app, None, "dev", f.alice_graph, "google_calendar", "shared-evt", actor, None,
        )
        .await
        .unwrap();
        assert_eq!(out.atoms_deleted.len(), 1, "exactly alice's re-seeded copy is deleted");
        assert!(f.present(f.bob_atom).await, "bob's private copy must still survive");

        app.close().await;
        f.teardown().await;
    }

    /// Fail-closed: a target graph the transaction cannot classify (absent
    /// from `kind_of`, as when RLS hides another member's private ocean) or of
    /// an unknown kind is dropped, never treated as shared.
    #[test]
    fn org_confine_targets_fails_closed_on_unknown_graph_kinds() {
        let (origin, shared, team, hidden, weird) =
            (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let mut kind_of = std::collections::HashMap::new();
        kind_of.insert(origin, "member");
        kind_of.insert(shared, "org");
        kind_of.insert(team, "team");
        kind_of.insert(weird, "something_new");
        let targets: Vec<(Uuid, Uuid)> =
            [origin, shared, team, hidden, weird].iter().map(|g| (*g, Uuid::new_v4())).collect();
        let kept: Vec<Uuid> =
            confine_targets(&kind_of, origin, targets).into_iter().map(|(g, _)| g).collect();
        assert_eq!(kept, vec![origin, shared, team]);
    }

    /// Same, end to end as the real `coralswarm_tenant_app` role: Alice's
    /// member-scoped transaction cannot see Bob's graph at all (absent from
    /// `kinds`), and Bob's atom must still be dropped from the targets.
    #[tokio::test]
    async fn org_confine_drops_a_graph_hidden_from_the_callers_scope_pg() {
        let Some(base) = org_base_url() else {
            eprintln!("skipping org_confine_drops_a_graph_hidden_from_the_callers_scope_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let f = OrgFixture::setup(&base, "org_del_scope_hidden").await;
        let app = f.app_role_pool().await;
        let mut tx = crate::tenancy::scope::begin_tenant_scoped(&app, f.account_id, f.alice_user)
            .await
            .unwrap();
        let visible: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert!(!visible.contains(&f.bob_graph), "precondition: RLS hides bob's graph from alice");
        let targets = vec![
            (f.alice_graph, f.alice_atom),
            (f.bob_graph, f.bob_atom),
            (f.shared_graph, f.shared_atom),
        ];
        let kept = confine_to_callers_scope(&mut tx, f.account_id, f.alice_graph, targets)
            .await
            .unwrap();
        tx.rollback().await.ok();
        let kept_graphs: Vec<Uuid> = kept.into_iter().map(|(g, _)| g).collect();
        assert!(!kept_graphs.contains(&f.bob_graph), "a hidden graph must be dropped");
        assert!(kept_graphs.contains(&f.alice_graph) && kept_graphs.contains(&f.shared_graph));
        app.close().await;
        f.teardown().await;
    }
}

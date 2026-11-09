//! Retraction (§4.9 of docs/plans/graph-temporal-memory.md) — the prerequisite
//! for live sync (§4.4). Two distinct verbs:
//!
//! * **Revoke** ([`revoke_source_publication`]) stops FUTURE corals flowing
//!   out of a published source. Published history stays intact by design —
//!   this is a one-column UPDATE (`source_publications.revoked_at`), not a
//!   delete of anything.
//! * **Unpublish** ([`unpublish_atom`]) is an explicit, targeted retraction:
//!   remove ONE already-promoted coral from a target ocean, along with its
//!   `node_atoms` link, its R2 blob, and — if that was the coral's target
//!   reef's LAST remaining coral — the reef itself.
//!
//! # Why a coral-less reef must be deleted, not merely left behind
//!
//! `nodes` has no FK to `atoms`/`node_atoms` (a reef is a title + body, not a
//! join). Under §4.2's native derivation, an entity/decision reef in a shared
//! ocean typically exists ONLY because `ExtractEntities`/`ExtractRelationships`
//! derived it from a coral that landed there — so if unpublish removes that
//! coral and simply stops, the reef survives its own evidence and keeps
//! stating the (possibly sensitive) claim as a bare, uncited title forever.
//! That is precisely the failure mode this module exists to prevent: it is
//! not enough to make a claim un-citable, an unpublish must make it
//! un-statable. Every other derived row (`relationship_edge`,
//! `supersession_edge`, `node_atoms`, `node_promotions`) cascades off the
//! `nodes` FK already (`ON DELETE CASCADE`, migration `0011_nodes` /
//! `0026_ocean_model` / `0040_relationship_edges`), so deleting the reef is
//! sufficient — nothing else needs an explicit statement.
//!
//! # The hardest transaction in the system — R2 vs Postgres ordering
//!
//! An R2 object delete is not transactional with a Postgres COMMIT, so ANY
//! ordering has a window where the two can disagree. This module makes the
//! SAME choice already reviewed and shipped for the structurally identical
//! problem in `workers::member_ocean_reconciler::purge_one_graph` (member-ocean
//! offboarding, which also deletes rows + R2 blobs together): **delete the
//! blob FIRST, inside the same Postgres transaction that later deletes the
//! rows, and only THEN commit.** Concretely, per call:
//!
//! 1. Read the target atom row `FOR UPDATE` (still inside the transaction).
//!    Not found → already unpublished (idempotent success, see below).
//! 2. If blob transport is configured, delete the R2 object at its
//!    `content_blob_key`. **A failure here (including a validation failure)
//!    rolls the transaction back** — nothing else has been touched, so this
//!    is unambiguous: the DB is byte-identical to before the call, and the
//!    blob is either gone or untouched. The caller retries later.
//! 3. Only once the blob delete has SUCCEEDED does this function delete the
//!    `atoms` row (cascading `node_atoms`) and, for any reef that row was
//!    the last coral of, the `nodes` row (cascading its edges).
//! 4. Commit.
//!
//! **What happens when Postgres and R2 disagree, in either direction:**
//!
//! * Blob delete fails/errors: by construction NO row has been touched yet
//!   (step 2 runs before step 3), so this is the unambiguous, fully-safe
//!   case — roll back, retry later. `R2Client::delete_atom_blob`'s own
//!   contract ("succeeds even if the object does not exist — S3 DELETE is
//!   idempotent") is what makes an unbounded number of retries safe: a retry
//!   that re-deletes an object gone from a PRIOR successful-but-uncommitted
//!   attempt is a no-op, not an error.
//! * Blob delete succeeds, then the COMMIT itself fails or is ambiguous
//!   (e.g. the connection drops after Postgres processed COMMIT server-side):
//!   genuinely ambiguous, same as `services::promotion`'s own accepted
//!   commit-ambiguity residual and `purge_one_graph`'s ("a timed-out commit
//!   is genuinely ambiguous... without a safe way to act on it"). Two
//!   sub-cases:
//!     - The commit actually landed: blob gone, rows gone, fully consistent.
//!       A retry finds no atom row and returns the idempotent "already gone"
//!       outcome — correct, nothing left to do.
//!     - The commit did NOT land (rolled back server-side): the atom row
//!       (and its blob KEY, stored on that row) is still there, but the
//!       blob object itself is already gone — a citation would 404 on
//!       content fetch until the operation is retried. This is the one real
//!       residual window, and it is self-healing: a retry re-reads the
//!       still-present row, re-attempts the (already-idempotent, so
//!       harmless-if-repeated) blob delete, and this time completes the row
//!       deletes and commits. The window closes on the very next retry, and
//!       nothing about it can silently persist — either the row is there
//!       (retry completes it) or it is gone (nothing to retry).
//!   Postgres is NEVER told to delete a row before its blob is confirmed
//!   gone, so the one ordering this design forbids by construction — a
//!   citable row whose blob was never even attempted — cannot happen.
//!
//! This is also what makes the whole operation **idempotent and safe to
//! retry blindly**: "the atom row is gone" is the ONLY signal the function
//! needs, and it implies (by the ordering above) that the blob delete for it
//! either already succeeded or was never needed — never that a blob might
//! still be sitting there while its row is already gone. A caller can retry
//! `unpublish_atom` an unbounded number of times with the same arguments and
//! it will always converge to "fully retracted", including from the one
//! ambiguous-commit residual window above.
//!
//! # Authorization — only the ORIGINAL PROMOTER may retract
//!
//! Both verbs are gated on the caller being the person who made the
//! disclosure decision in the first place, NOT on the caller merely being
//! able to reach the shared ocean. This is a deliberate narrowing of §4.9's
//! first shipped shape (which allowed any member of the target ocean), taken
//! because it is the only gate consistent with the consent model the whole of
//! §4 rests on (ADR-0007 D8: promotion is the ONE path into a shared ocean,
//! one-way and consented). The publisher owns reversing their own
//! disclosure; a reader does not get to reverse someone else's.
//!
//! **There is deliberately NO ocean-owner / admin override.** That variant
//! was considered and explicitly not chosen; adding one is a product decision,
//! not an implementation detail, and must not be smuggled in here.
//!
//! ## Where "the promoter" is read from, and why it is that column
//!
//! [`unpublish_atom`] resolves the promoter of a TARGET atom as
//! `nodes.owner_id`, over every reef in the target graph that (a) the atom is
//! linked to via `node_atoms` and (b) is itself a published copy
//! (`promoted_from_node_id IS NOT NULL`, migration `0111`/`0021`, §4.5).
//!
//! That is ONE rule with ONE query, and it holds byte-identically on BOTH
//! promotion paths — which is the whole point, because these two paths having
//! diverged is exactly how the rename-orphans-a-reef defect of §4.5 arose:
//!
//! * Same-pool (`Store::promote_node`) binds `promoted_by` into the target
//!   copy's `owner_id` and `source_node_id`/`src_graph_id` into
//!   `promoted_from_node_id`/`promoted_from_graph_id`, in one INSERT.
//! * Cross-DB org (`services::promotion::write_promotion_rows`) binds
//!   `caller_user_id` into `owner_id` and the consumer-plane
//!   `source_node_id`/`source_graph_id` into the same two columns, in a
//!   structurally identical INSERT.
//!
//! Neither path's `ON CONFLICT ... DO UPDATE` touches `owner_id`, so it stays
//! the FIRST promoter across every re-promote — literally "the original
//! promoter", which is what was asked for.
//!
//! `node_promotions.promoted_by` is deliberately NOT consulted, for two
//! reasons: it does not exist at all on the cross-DB org path (that table's
//! `source_node_id` FK targets a `nodes` row in the wrong database — see
//! `services::promotion`'s module doc), so consulting it would make the two
//! paths enforce DIFFERENT rules; and its upsert DOES overwrite
//! `promoted_by` on every re-promote, so it names the LAST promoter, not the
//! original. Consequence to know about, not papered over: if user A promotes
//! a reef and user B later re-promotes the same reef (same-pool), B is a
//! publisher of that content but is NOT authorized to unpublish it here.
//!
//! ## Missing provenance FAILS CLOSED — deliberately, and loudly
//!
//! Some already-published content has NO promoter recorded anywhere:
//!
//! 1. Cross-DB org promotions made BEFORE migration `0111` — that path wrote
//!    no `node_promotions` row and had no identity columns to write, and
//!    `0111`'s backfill can only recover same-pool provenance (its own header
//!    says the cross-plane case "is NOT backfillable ... by construction").
//! 2. Corals that reached a target ocean without ending up linked to a
//!    promoted reef copy at all — `promote_source`/`promote_session` both
//!    copy corals and link them warn-and-continue, so a link failure leaves a
//!    coral in the target with no reef vouching for it.
//!
//! For those, [`unpublish_atom`] returns
//! [`RetractionError::ProvenanceUnavailable`] and retracts NOTHING. Same for
//! [`revoke_source_publication`] when `source_publications.consented_by` is
//! NULL.
//!
//! Why deny rather than allow, stated so it can be argued with:
//!
//! * Falling back to "anyone who can reach the ocean" for exactly the rows
//!   where provenance is absent would silently reinstate the model this
//!   change removes, and would reinstate it precisely over the OLDEST
//!   cross-plane content — i.e. an attacker wanting to retract someone else's
//!   disclosure would just pick pre-`0111` content. A fail-open on the
//!   unknown case is a fail-open on the largest legacy case.
//! * Retraction is irreversible and destructive (atom row + R2 blob +
//!   possibly the whole reef). A wrong DENY is fully recoverable — the
//!   content is still there and an operator can act. A wrong ALLOW destroys
//!   content with no undo.
//! * An authorization decision taken on ABSENT evidence must fail closed.
//!
//! Cost, stated plainly rather than discovered later: **pre-`0111` cross-DB
//! org-promoted content is not retractable through this API at all** until
//! provenance is backfilled or an explicit, reviewed break-glass path is
//! added. §4.9 exists because "content that cannot be taken back" is a real
//! problem, so this is a genuine regression for that specific legacy set and
//! is called out in `routes::nodes`' own handler docs too. Both denial cases
//! are observable — a distinct error variant on the wire AND a `WARN`-level
//! structured log carrying graph/atom/caller — so the real frequency is
//! measurable instead of guessed at.
//!
//! ## Where the check runs, and what it does NOT depend on
//!
//! The check is plain application-level SQL executed INSIDE the same
//! transaction as the mutation it guards, before any destructive step. It
//! does **not** depend on RLS scoping — every predicate the decision needs is
//! written out explicitly, including the `n.graph_id = $2` target scoping —
//! so it is correct regardless of whether the connection is additionally
//! RLS-scoped. That independence is what makes it safe for [`unpublish_atom`]
//! and [`revoke_source_publication`] to ALSO now open their own transactions
//! through `tenancy::scope::begin_account_scoped` rather than a bare
//! `pool.begin()` (see below): the authorization decision was never leaning
//! on RLS to begin with, so adding a real `app.account_id` scope changes
//! nothing about how this check behaves, only whether Postgres itself now
//! also backstops it.
//!
//! ## RLS scoping — FIXED for both verbs
//!
//! `unpublish_atom` reads/writes `atoms`/`node_atoms`/`nodes` — all on
//! migration `0083`'s consumer-plane RLS roster — and `revoke_source_
//! publication` reads/writes `source_publications` (RLS-covered since
//! migration `0111`, not `0083` itself — see that migration's own `CREATE
//! POLICY account_isolation_select/_insert/_update/_delete` lines; the table
//! didn't exist when `0083` shipped). Both now open their owned transaction
//! through `begin_account_scoped(pool, account_id)` (account_id resolved via
//! `resolve_graph_account_id`, this module's own bootstrap helper, above)
//! instead of a bare `pool.begin()`.
//!
//! Verified against production: `DATABASE_URL`'s user IS `coralswarm_app` and
//! `rolbypassrls = f` — the OPS-4 role switch this module doc used to say
//! "hasn't happened yet" has ALREADY HAPPENED, so the unscoped form was a
//! live, hard-failing bug for BOTH verbs, not a dormant one for either.
//! Confirmed empirically for `revoke_source_publication` specifically that
//! this holds even though `source_publications` is EMPTY in production today
//! (nothing in the tree writes it yet — see `routes::nodes`'s own comment on
//! `unpublish_atom_route`, "nothing in the tree WRITES source_publications
//! yet"): Postgres evaluates the RLS `USING`/`WITH CHECK` qual — which is
//! what calls `current_setting('app.account_id')` — as part of planning the
//! statement, not lazily per matched row, so a table with zero rows still
//! raises `42704 unrecognized configuration parameter "app.account_id"` on
//! an unscoped `coralswarm_app` connection. An empty table was never a
//! mitigation. See `backend/tests/retraction_deletion_rls_pg.rs` for the
//! app-role reproduction of both verbs (fails without this fix, passes with
//! it) and this PR's description for the full account.
//!
//! `revoke_source_publication`'s `account_id` is resolved from
//! `source_graph_id`, not `target_graph_id` — see that function's own doc
//! comment for why either anchor is equivalent for every legitimate row this
//! table holds (same-pool promotion, the only kind left after org-tenancy PR
//! #414 deleted the cross-DB promotion service, is always same-account by
//! construction — `0111`'s `WITH CHECK` on INSERT/UPDATE requires app.
//! account_id to equal BOTH anchors simultaneously, which would be
//! unsatisfiable by any single-account scope if a legitimate row could ever
//! span two accounts).
//!
//! A full sweep of this file (`grep -n 'pool\.begin\|fetch_.*(pool)\|
//! execute(pool)' backend/src/services/retraction.rs`) turned up no other
//! bare-pool access against an RLS-covered table — `unpublish_atom` and
//! `revoke_source_publication` were the only two, and both are fixed here,
//! so `scripts/rls_coverage.manifest` flips this file from `pending` to
//! `converted`.
//!
//! ## Known bypass this module CANNOT close on its own
//!
//! `POST /v1/sync/push` authorizes an op batch by GRAPH REACHABILITY and then
//! applies `(atom, delete)` and `(node_atom, upsert)` ops straight from the
//! payload (`Store::apply_remote_op`). So any member of a shared ocean can
//! still (a) delete a promoted atom row outright with a crafted tombstone op,
//! and (b) forge a `node_atoms` link from a reef they DO own into someone
//! else's atom, which would make this module's query name them as a recorded
//! promoter. Neither door is in this file — see the report accompanying this
//! change. The gate below is still worth having (it closes the two documented
//! §4.9 verbs, which are the surfaces a UI drives), but it must not be
//! described as a complete boundary while that push path is open.

use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_core::r2::{validate_blob_key, R2Error};

use crate::services::blob_store::PromotionBlobStore;
use crate::store::{append_op_tx, sync};

/// What one [`unpublish_atom`] call did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnpublishOutcome {
    /// `false` when the target atom was ALREADY gone (a prior call
    /// completed, or nothing was ever there) — the idempotent no-op case.
    /// Never an error: retracting something already retracted is success.
    pub atom_was_present: bool,
    /// Target reefs deleted as a side effect because this was their LAST
    /// remaining coral (§4.9's coral-less-reef rule). Empty when the
    /// unpublished coral's reef(s) still have other corals, or when the
    /// atom was already gone.
    pub deleted_reef_ids: Vec<Uuid>,
}

/// What one [`revoke_source_publication`] call did. Replaces the previous
/// bare `bool` because the authorization change made "nothing happened" three
/// genuinely different outcomes, and collapsing them would hide exactly the
/// distinction this change is about (an authorized idempotent no-op vs. a row
/// that never existed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevokeOutcome {
    /// The publication was live and this call flipped `revoked_at`.
    Revoked,
    /// The publication exists, the caller is its consenting publisher, and it
    /// was already revoked — idempotent no-op, `revoked_at` untouched.
    AlreadyRevoked,
    /// No publication row for that `(source_id, source_graph_id,
    /// target_graph_id)` triple. Nothing was ever published this way, so
    /// there is no protected object — a successful no-op, not a denial.
    NotPublished,
}

impl RevokeOutcome {
    /// The legacy wire field: `true` only when this call actually flipped
    /// `revoked_at`. Kept so the route's JSON shape is unchanged.
    pub fn revoked_now(self) -> bool {
        matches!(self, RevokeOutcome::Revoked)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            RevokeOutcome::Revoked => "revoked",
            RevokeOutcome::AlreadyRevoked => "already_revoked",
            RevokeOutcome::NotPublished => "not_published",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RetractionError {
    #[error("blob error: {0}")]
    Blob(#[from] R2Error),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
    /// A promoter IS recorded for this content and it is somebody else.
    /// Nothing was retracted. See the module doc's authorization section —
    /// there is deliberately no owner/admin override that turns this into a
    /// success.
    #[error("caller is not the original promoter of this content")]
    NotPromoter,
    /// NO promoter is recorded for this content anywhere, so "was the caller
    /// the promoter?" is unanswerable. Fails closed, deliberately and
    /// distinguishably from [`RetractionError::NotPromoter`] — the two mean
    /// very different things operationally (a policy denial vs. a data gap
    /// inherited from pre-`0111` cross-plane promotion). Nothing was
    /// retracted.
    #[error("no promotion provenance recorded for this content — cannot establish the original promoter")]
    ProvenanceUnavailable,
}

/// Outcome of the ONE authorization decision both §4.9 verbs make. Kept as a
/// separate three-state value (rather than a bool) so "denied" and
/// "unanswerable" can never collapse into each other by accident, which is
/// precisely the silent-default this change exists to avoid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromoterCheck {
    /// `recorded` names at least one promoter and the caller is one of them.
    Allowed,
    /// `recorded` is non-empty and the caller is not in it.
    NotPromoter,
    /// `recorded` is empty — no provenance exists for this object at all.
    ProvenanceUnavailable,
}

/// The single decision function BOTH entry points call, so `unpublish_atom`
/// and `revoke_source_publication` can never drift into enforcing different
/// policies (they read provenance from different tables — they must not also
/// interpret it differently).
///
/// `caller_user_id` is always the AUTHENTICATED caller threaded down from
/// `UserContext` (JWT-verified in `middleware::auth`), never a value taken
/// from a request body, query string or op payload.
/// Does `blob_key`'s own graph segment equal `expected_graph_id`? Callers
/// assert [`validate_blob_key`] first (as `unpublish_atom` does), so both
/// accepted shapes — `{graph}/{atom}` and `{org}/{graph}/{atom}` — are
/// already known-valid UUIDs in the graph position; this only compares the
/// value.
///
/// The one structural guard standing between "delete this coral's own R2
/// object" and "delete a DIFFERENT ocean's object because a row's stored key
/// silently drifted from its own `graph_id`" — see `unpublish_atom`'s Step 2
/// comment for why that drift is expected to be impossible today, and why the
/// assertion exists anyway.
fn blob_key_graph_segment_matches(blob_key: &str, expected_graph_id: Uuid) -> bool {
    let graph_segment = match blob_key.split('/').collect::<Vec<&str>>().as_slice() {
        [_atom_only] => return false, // shouldn't happen post-validate_blob_key
        [graph, _atom] => *graph,
        [_org, graph, _atom] => *graph,
        _ => return false,
    };
    matches!(Uuid::parse_str(graph_segment), Ok(g) if g == expected_graph_id)
}

fn check_promoter(recorded: &[Uuid], caller_user_id: Uuid) -> PromoterCheck {
    if recorded.is_empty() {
        PromoterCheck::ProvenanceUnavailable
    } else if recorded.contains(&caller_user_id) {
        PromoterCheck::Allowed
    } else {
        PromoterCheck::NotPromoter
    }
}

impl PromoterCheck {
    /// Map to the wire/error surface. `Allowed` is the only `Ok`.
    fn into_result(self) -> Result<(), RetractionError> {
        match self {
            PromoterCheck::Allowed => Ok(()),
            PromoterCheck::NotPromoter => Err(RetractionError::NotPromoter),
            PromoterCheck::ProvenanceUnavailable => Err(RetractionError::ProvenanceUnavailable),
        }
    }
}

/// Resolve the `account_id` that owns `graph_id` — the ONE sanctioned way to
/// discover it before a scoped transaction can be opened. Delegates to
/// `resolve_graph_account_id` (migration `0084_bootstrap_scope_functions`),
/// the audited `SECURITY DEFINER` bootstrap carve-out `Store::graph_account_id`
/// and `services::deletion`'s own `resolve_account_id_for_graph` both use for
/// exactly this reason: it returns ONLY a bare `account_id`, never any content
/// column, so it cannot be used to exfiltrate anything, and it runs
/// deliberately UNSCOPED (a plain pool query, not inside
/// `begin_account_scoped`) because its whole purpose is answering "which
/// account owns this graph?" before the account_id needed to scope a
/// connection is known at all — wrapping it in `begin_account_scoped` would
/// reintroduce the exact chicken-and-egg deadlock it exists to break.
///
/// Deliberately NOT `caller_user_id`'s own `UserContext::account_id`: a user
/// can belong to more than one account (`tenancy::reachability`'s Model B
/// union), so the caller's ACTIVE session account is not necessarily the
/// account that owns `target_graph_id`. Scoping to the wrong account would
/// not be a security hole (RLS can only ever hide rows, never expose extra
/// ones), but it WOULD be a correctness bug indistinguishable from data loss:
/// the atom genuinely exists, the caller is genuinely authorized, but the
/// `FOR UPDATE` read below would come back empty under the wrong account's
/// GUC — exactly the "quiet empty result" this backstop exists to prevent
/// turning into.
async fn resolve_graph_account_id(pool: &PgPool, graph_id: Uuid) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT resolve_graph_account_id($1)")
        .bind(graph_id)
        .fetch_one(pool)
        .await
}

/// Every user id RECORDED as having published `target_atom_id` into
/// `target_graph_id` — see the module doc's authorization section for why
/// this specific column pair and no other.
///
/// Runs on the caller's transaction so the decision and the mutation it
/// guards cannot be split by a concurrent writer, and scopes the join to
/// `target_graph_id` in SQL rather than leaning on RLS (which is not active
/// on this connection — module doc again).
async fn recorded_atom_promoters(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target_graph_id: Uuid,
    target_atom_id: Uuid,
) -> Result<Vec<Uuid>, sqlx::Error> {
    let rows: Vec<Option<Uuid>> = sqlx::query_scalar(
        "SELECT DISTINCT n.owner_id \
         FROM node_atoms na \
         JOIN nodes n ON n.node_id = na.node_id \
         WHERE na.atom_id = $1 \
           AND n.graph_id = $2 \
           AND n.promoted_from_node_id IS NOT NULL \
           AND n.owner_id IS NOT NULL",
    )
    .bind(target_atom_id)
    .bind(target_graph_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().flatten().collect())
}

type AtomRow = (
    Uuid,                  // graph_id (defensive re-read; must equal the argument)
    String,                // source_type
    String,                // source_id
    String,                // content_blob_key
    Option<String>,        // content_inline
    Vec<String>,           // participants
    chrono::DateTime<Utc>, // timestamp
    Option<Uuid>,          // ingested_by
    String,                // scope
    String,                // fts_language
    chrono::DateTime<Utc>, // created_at
);

type NodeRow = (
    String,                // type
    String,                // title
    serde_json::Value,     // body
    String,                // scope
    Option<Uuid>,          // owner_id
    chrono::DateTime<Utc>, // created_at
    i32,                   // version
);

/// Retract ONE promoted coral from a target ocean — see the module doc for
/// the full ordering/idempotency contract. `pool` is whichever database the
/// TARGET graph lives in (the caller has already resolved same-pool vs org
/// tenant, exactly like `services::promotion`'s callers do); this function
/// never touches the source ocean — unpublish is purely a target-side
/// operation, matching ADR-0006's one-way copy-on-promote model.
///
/// `blobs` mirrors `services::promotion::PromotionDeps::blobs`: `None` in
/// dev / R2-off deployments, where content lives entirely in
/// `content_inline` and there is no blob to delete.
///
/// `caller_user_id` is the AUTHENTICATED acting user. Only the original
/// promoter of the coral may retract it — see the module doc's authorization
/// section, including why missing provenance fails closed with a distinct
/// error rather than silently allowing or silently denying. This parameter is
/// mandatory and unfakeable by design: there is no "system"/`None` arm that
/// would let a caller opt out of the check.
pub async fn unpublish_atom(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    device_id: &str,
    target_graph_id: Uuid,
    target_atom_id: Uuid,
    caller_user_id: Uuid,
) -> Result<UnpublishOutcome, RetractionError> {
    // Scope the transaction to the account that owns the TARGET graph — see
    // `resolve_graph_account_id`'s own doc comment for why this is resolved
    // fresh from the graph rather than trusted from the caller's own active
    // account. `None` (graph does not exist / was concurrently deleted) is
    // the same idempotent "already gone" outcome as an absent atom row: there
    // is nothing left to retract either way.
    //
    // This is a plain parameter, not a caller-supplied transaction: the
    // R2-vs-Postgres commit ordering the module doc requires (own the
    // transaction start-to-finish) is unaffected by ALSO issuing one extra
    // `set_config('app.account_id', ...)` statement at the top of that same
    // owned transaction — `begin_account_scoped` is `pool.begin()` plus
    // exactly that, nothing more.
    let Some(account_id) = resolve_graph_account_id(pool, target_graph_id).await? else {
        return Ok(UnpublishOutcome {
            atom_was_present: false,
            deleted_reef_ids: vec![],
        });
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id).await?;

    // Step 1: read the atom row FOR UPDATE. Not found = already unpublished
    // (idempotent success) — see the module doc's ordering guarantee for why
    // "row gone" always means "there is nothing left to clean up".
    let atom: Option<AtomRow> = sqlx::query_as(
        "SELECT graph_id, source_type, source_id, content_blob_key, content_inline, \
                participants, \"timestamp\", ingested_by, scope, fts_language, created_at \
         FROM atoms WHERE atom_id = $1 AND graph_id = $2 FOR UPDATE",
    )
    .bind(target_atom_id)
    .bind(target_graph_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some((
        graph_id,
        source_type,
        source_id,
        content_blob_key,
        content_inline,
        participants,
        atom_timestamp,
        ingested_by,
        scope,
        fts_language,
        created_at,
    )) = atom
    else {
        tx.rollback().await.ok();
        return Ok(UnpublishOutcome {
            atom_was_present: false,
            deleted_reef_ids: vec![],
        });
    };
    debug_assert_eq!(graph_id, target_graph_id);

    // Step 1b: AUTHORIZE — only the original promoter may retract, and a
    // missing provenance record is a hard, distinguishable denial rather
    // than a silent allow or a silent deny (module doc, "Authorization").
    //
    // Placed here on purpose: after the idempotent "already gone" arm above
    // (there is no object left to protect, and returning the same no-op the
    // promoter would get avoids turning this endpoint into an existence
    // oracle for a graph the caller can already read), and BEFORE every
    // destructive step — the blob delete, the row deletes and the coral-less
    // reef rule all sit downstream of it. Inside the transaction, so it
    // cannot be split from the mutation it guards.
    let promoters = recorded_atom_promoters(&mut tx, target_graph_id, target_atom_id).await?;
    let decision = check_promoter(&promoters, caller_user_id);
    if decision != PromoterCheck::Allowed {
        tx.rollback().await.ok();
        match decision {
            PromoterCheck::NotPromoter => tracing::warn!(
                graph_id = %target_graph_id,
                atom_id = %target_atom_id,
                caller_user_id = %caller_user_id,
                recorded_promoters = ?promoters,
                "unpublish_atom DENIED: caller is not the original promoter (§4.9 \
                 authorization; there is deliberately no ocean-owner override)"
            ),
            PromoterCheck::ProvenanceUnavailable => tracing::warn!(
                graph_id = %target_graph_id,
                atom_id = %target_atom_id,
                caller_user_id = %caller_user_id,
                "unpublish_atom DENIED: no promotion provenance recorded for this coral, so \
                 the original promoter cannot be established — failing closed. Expected for \
                 cross-DB org promotions made BEFORE migration 0111 (not backfillable by \
                 construction) and for corals copied into a target ocean without ever being \
                 linked to a promoted reef copy. This coral is currently UNRETRACTABLE \
                 through the API; see services::retraction's module doc"
            ),
            PromoterCheck::Allowed => unreachable!("guarded above"),
        }
        return Err(decision.into_result().unwrap_err());
    }

    // The reefs this atom is currently linked to — captured BEFORE the
    // atoms delete cascades node_atoms away, so the coral-less check below
    // has something to check.
    let candidate_node_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT node_id FROM node_atoms WHERE atom_id = $1")
            .bind(target_atom_id)
            .fetch_all(&mut *tx)
            .await?;

    // Step 2: delete the blob FIRST — see the module doc for the full
    // ordering rationale. Any failure here rolls back with NOTHING else
    // touched (unambiguous, safe to retry).
    //
    // **Never the SOURCE ocean's object — pinned invariant, not just a hope.**
    // A promoted copy's row never carries the source atom's `content_blob_key`:
    // both same-pool copy paths (`Store::copy_atoms_to_graph` /
    // `copy_atoms_to_graph_scoped` — the ONLY promotion copy paths left after
    // org-tenancy PR #414 deleted the cross-DB `services::promotion`, which
    // this module's own doc still describes; that description is stale) mint
    // a FRESH `atom_id` and a FRESH, TARGET-namespaced key
    // (`{target_graph_id}/{new_atom_id}`, optionally org-prefixed) for every
    // copy — see either function's own doc comment ("never the source's
    // key"). Promotion is also structurally one-way into a WIDER ocean
    // (ADR-0006/0007), so `target_graph_id` here can never equal the source
    // reef's own graph. Both facts together mean a target row's
    // `content_blob_key` can never resolve into the source ocean's object
    // namespace — but "can never" is exactly the kind of claim a future
    // change to either copy path could quietly break, so it is asserted here,
    // not just documented: the key's own graph segment must equal
    // `target_graph_id` before anything is deleted through it.
    if let Some(blobs) = blobs {
        if let Err(e) = validate_blob_key(&content_blob_key) {
            tx.rollback().await.ok();
            return Err(e.into());
        }
        if !blob_key_graph_segment_matches(&content_blob_key, target_graph_id) {
            tracing::error!(
                graph_id = %target_graph_id,
                atom_id = %target_atom_id,
                content_blob_key = %content_blob_key,
                "unpublish_atom: content_blob_key's graph segment does not match the target \
                 graph — refusing to delete an R2 object that may belong to a DIFFERENT ocean \
                 (never touches the source ocean, module doc). Row left untouched; this atom \
                 needs manual investigation, not a retry."
            );
            tx.rollback().await.ok();
            return Err(RetractionError::Blob(R2Error::InvalidKey));
        }
        if let Err(e) = blobs.delete_blob(&content_blob_key).await {
            tx.rollback().await.ok();
            return Err(e.into());
        }
    }

    // Step 3: only now delete rows — the blob is confirmed gone (or was
    // never applicable).
    let now = Utc::now();
    sqlx::query("DELETE FROM atoms WHERE atom_id = $1")
        .bind(target_atom_id)
        .execute(&mut *tx)
        .await?;
    let atom_snapshot = json!({
        sync::field::ATOM_ID:          target_atom_id.to_string(),
        sync::field::GRAPH_ID:         target_graph_id.to_string(),
        sync::field::SOURCE_TYPE:      source_type,
        sync::field::SOURCE_ID:        source_id,
        sync::field::CONTENT_BLOB_KEY: content_blob_key,
        sync::field::CONTENT_INLINE:   content_inline,
        sync::field::PARTICIPANTS:     participants,
        sync::field::TIMESTAMP:        atom_timestamp.to_rfc3339(),
        sync::field::INGESTED_BY:      ingested_by.map(|u| u.to_string()),
        sync::field::SCOPE:            scope,
        sync::field::FTS_LANGUAGE:     fts_language,
        sync::field::CREATED_AT:       created_at.to_rfc3339(),
    });
    append_op_tx(
        &mut tx,
        &sync::atom_delete_op(
            Uuid::new_v4(),
            atom_snapshot,
            target_atom_id,
            target_graph_id,
            now,
            device_id,
        ),
    )
    .await?;

    // §4.9's coral-less-reef rule: any candidate reef left with ZERO
    // remaining corals is deleted too (its edges cascade off the `nodes` FK
    // — see the module doc). `nodes` is re-read `FOR UPDATE` here (not
    // reused from any earlier read) so a concurrent promote linking a fresh
    // coral to the same reef between this transaction's start and now is
    // never raced past.
    let mut deleted_reef_ids = Vec::new();
    for node_id in candidate_node_ids {
        let node: Option<NodeRow> = sqlx::query_as(
            "SELECT type, title, body, scope, owner_id, created_at, version \
             FROM nodes WHERE node_id = $1 FOR UPDATE",
        )
        .bind(node_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((node_type, title, body, node_scope, owner_id, node_created_at, version)) = node
        else {
            // Already gone (a concurrent unpublish/merge got there first) —
            // nothing to do, not an error.
            continue;
        };
        let supported: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM node_atoms WHERE node_id=$1) OR EXISTS(SELECT 1 FROM node_semantic_atoms WHERE node_id=$1)",
        ).bind(node_id).fetch_one(&mut *tx).await?;
        if supported { continue; }
        sqlx::query("DELETE FROM nodes WHERE node_id = $1")
            .bind(node_id)
            .execute(&mut *tx)
            .await?;
        append_op_tx(
            &mut tx,
            &sync::node_delete_op(
                Uuid::new_v4(),
                node_id,
                target_graph_id,
                &node_type,
                &title,
                &body.to_string(),
                &node_scope,
                owner_id,
                node_created_at,
                now,
                i64::from(version),
                device_id,
            ),
        )
        .await?;
        deleted_reef_ids.push(node_id);
    }

    // Commit-ambiguity is accepted here, identically to
    // `services::promotion` and `purge_one_graph` — see the module doc's
    // "what happens when Postgres and R2 disagree" section for the full
    // reasoning on why this is safe to just retry.
    tx.commit().await?;

    Ok(UnpublishOutcome {
        atom_was_present: true,
        deleted_reef_ids,
    })
}

/// **Revoke** — stop FUTURE corals flowing from `(source_id, source_graph_id)`
/// into `target_graph_id` under a live-synced source publication (§4.4, not
/// yet implemented — this is the data-layer capability §4.9 requires to
/// exist BEFORE live sync ships, per the plan's own ordering). Published
/// history is untouched by design: this updates exactly one column
/// (`source_publications.revoked_at`), never deletes anything.
///
/// Idempotent: revoking an already-revoked (or never-existing) publication
/// is a successful no-op ([`RevokeOutcome::AlreadyRevoked`] /
/// [`RevokeOutcome::NotPublished`]), not an error — matching
/// [`unpublish_atom`]'s "retracting something already retracted is success"
/// contract.
///
/// # Authorization
///
/// Same rule as [`unpublish_atom`], through the same
/// [`check_promoter`] decision function so the two verbs cannot drift: only
/// the person who CONSENTED to the publication may revoke it. The recorded
/// promoter here is `source_publications.consented_by` (§4.5, migration
/// `0111`) — the direct, purpose-built column, and this table's consent
/// record is written the same way regardless of which promotion path created
/// it, so there is no same-pool/cross-DB divergence to reconcile.
///
/// `consented_by` being NULL — either because the row predates a writer that
/// sets it, or because `ON DELETE SET NULL` fired when that user was deleted
/// — is [`RetractionError::ProvenanceUnavailable`], NOT a silent allow. Same
/// fail-closed reasoning as the module doc's.
///
/// A row that does not exist AT ALL is [`RevokeOutcome::NotPublished`] rather
/// than an authorization outcome: nothing was ever published under that
/// triple, so there is no protected object and no promoter to compare
/// against — and it is byte-identical to what the legitimate promoter gets on
/// a repeat call, so it is not an existence oracle either.
pub async fn revoke_source_publication(
    pool: &PgPool,
    source_id: &str,
    source_graph_id: Uuid,
    target_graph_id: Uuid,
    caller_user_id: Uuid,
) -> Result<RevokeOutcome, RetractionError> {
    // Scope the transaction to the account that owns the SOURCE graph, via
    // the same `resolve_graph_account_id` bootstrap `unpublish_atom` uses
    // (see its own doc comment above for the chicken-and-egg reasoning).
    // `source_graph_id`, not `target_graph_id`: this is the function's own
    // primary-key prefix (`source_id`, `source_graph_id`), and — same-pool
    // promotion being the only kind left after org-tenancy PR #414 deleted
    // the cross-DB promotion service (this module's own doc, "Reused from
    // §4.9" / `services::deletion`'s identical "Propagation" claim) — every
    // legitimate row this table holds has `source_graph_id` and
    // `target_graph_id` in the SAME account by construction, so either
    // anchor resolves to the same `account_id`. `source_publications`'
    // migration `0111` policies bear this out directly: `account_isolation_
    // insert`/`_update`'s `WITH CHECK` requires app.account_id to equal BOTH
    // anchors' accounts (a strict AND, not the SELECT policy's permissive
    // OR) — exactly 0083's `node_promotions` reasoning ("every legitimate
    // consumer-plane promotion is same-account"), applied to this table's
    // write side too. `None` (source graph gone) is `NotPublished`, same as
    // "no row" below — nothing to revoke either way.
    //
    // Own transaction (matching this module's other verb) so the consent read
    // and the revoke it authorizes cannot be split by a concurrent writer.
    // `FOR UPDATE` on the publication row is what makes that guarantee real
    // rather than nominal. `begin_account_scoped` is `pool.begin()` plus one
    // extra `set_config`, so this guarantee is unaffected by the scoping fix.
    let Some(account_id) = resolve_graph_account_id(pool, source_graph_id).await? else {
        return Ok(RevokeOutcome::NotPublished);
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id).await?;

    let row: Option<(Option<Uuid>, Option<chrono::DateTime<Utc>>)> = sqlx::query_as(
        "SELECT consented_by, revoked_at FROM source_publications \
         WHERE source_id = $1 AND source_graph_id = $2 AND target_graph_id = $3 \
         FOR UPDATE",
    )
    .bind(source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some((consented_by, revoked_at)) = row else {
        tx.rollback().await.ok();
        return Ok(RevokeOutcome::NotPublished);
    };

    let recorded: Vec<Uuid> = consented_by.into_iter().collect();
    let decision = check_promoter(&recorded, caller_user_id);
    if decision != PromoterCheck::Allowed {
        tx.rollback().await.ok();
        match decision {
            PromoterCheck::NotPromoter => tracing::warn!(
                source_id = %source_id,
                source_graph_id = %source_graph_id,
                target_graph_id = %target_graph_id,
                caller_user_id = %caller_user_id,
                consented_by = ?consented_by,
                "revoke_source_publication DENIED: caller did not consent to this publication \
                 (§4.9 authorization; there is deliberately no ocean-owner override)"
            ),
            PromoterCheck::ProvenanceUnavailable => tracing::warn!(
                source_id = %source_id,
                source_graph_id = %source_graph_id,
                target_graph_id = %target_graph_id,
                caller_user_id = %caller_user_id,
                "revoke_source_publication DENIED: source_publications.consented_by is NULL, so \
                 the consenting publisher cannot be established — failing closed. Either the row \
                 predates a writer that sets consented_by, or its user was deleted (the column's \
                 own ON DELETE SET NULL). This publication is currently UNREVOCABLE through the \
                 API; see services::retraction's module doc"
            ),
            PromoterCheck::Allowed => unreachable!("guarded above"),
        }
        return Err(decision.into_result().unwrap_err());
    }

    // Authorized. Already-revoked stays an idempotent no-op — and note it is
    // now an AUTHORIZED no-op: a non-consenter is refused above rather than
    // being handed the same "already done" answer, so the two cases stay
    // distinguishable in the logs.
    if revoked_at.is_some() {
        tx.rollback().await.ok();
        return Ok(RevokeOutcome::AlreadyRevoked);
    }

    sqlx::query(
        "UPDATE source_publications SET revoked_at = now() \
         WHERE source_id = $1 AND source_graph_id = $2 AND target_graph_id = $3 \
           AND revoked_at IS NULL",
    )
    .bind(source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(RevokeOutcome::Revoked)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three-way decision is the ONE piece of policy both §4.9 verbs
    /// share, so it gets pinned here independently of any database: an
    /// accidental `if recorded.is_empty() { Allowed }` (the "silently allow"
    /// default this change exists to rule out) fails right here, with no
    /// Postgres required.
    #[test]
    fn empty_provenance_is_unavailable_never_allowed() {
        let caller = Uuid::new_v4();
        assert_eq!(
            check_promoter(&[], caller),
            PromoterCheck::ProvenanceUnavailable,
            "no recorded promoter must NEVER resolve to Allowed — that is the silent \
             fall-through this change exists to remove"
        );
    }

    #[test]
    fn the_recorded_promoter_is_allowed() {
        let promoter = Uuid::new_v4();
        assert_eq!(
            check_promoter(&[promoter], promoter),
            PromoterCheck::Allowed
        );
    }

    #[test]
    fn a_different_user_is_not_the_promoter() {
        let promoter = Uuid::new_v4();
        let someone_else = Uuid::new_v4();
        assert_eq!(
            check_promoter(&[promoter], someone_else),
            PromoterCheck::NotPromoter,
            "reaching the ocean is not authority to retract someone else's disclosure"
        );
    }

    /// A coral can be linked to more than one promoted reef copy (a coral
    /// promoted twice, by two different people, into the same ocean). Each
    /// of those people published it, so each may retract it — but nobody
    /// else may.
    #[test]
    fn any_one_of_several_recorded_promoters_is_allowed() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let outsider = Uuid::new_v4();
        assert_eq!(check_promoter(&[a, b], a), PromoterCheck::Allowed);
        assert_eq!(check_promoter(&[a, b], b), PromoterCheck::Allowed);
        assert_eq!(
            check_promoter(&[a, b], outsider),
            PromoterCheck::NotPromoter
        );
    }

    /// `NotPromoter` and `ProvenanceUnavailable` must stay DISTINGUISHABLE
    /// all the way out to the error surface — they are a policy denial and a
    /// data gap respectively, and an operator needs to tell them apart.
    #[test]
    fn the_two_denials_map_to_distinct_errors() {
        let denied = PromoterCheck::NotPromoter.into_result().unwrap_err();
        let unavailable = PromoterCheck::ProvenanceUnavailable
            .into_result()
            .unwrap_err();
        assert!(matches!(denied, RetractionError::NotPromoter));
        assert!(matches!(
            unavailable,
            RetractionError::ProvenanceUnavailable
        ));
        assert_ne!(denied.to_string(), unavailable.to_string());
        assert!(PromoterCheck::Allowed.into_result().is_ok());
    }

    #[test]
    fn revoke_outcome_preserves_the_legacy_revoked_now_flag() {
        assert!(RevokeOutcome::Revoked.revoked_now());
        assert!(!RevokeOutcome::AlreadyRevoked.revoked_now());
        assert!(!RevokeOutcome::NotPublished.revoked_now());
        assert_eq!(RevokeOutcome::Revoked.as_str(), "revoked");
        assert_eq!(RevokeOutcome::AlreadyRevoked.as_str(), "already_revoked");
        assert_eq!(RevokeOutcome::NotPublished.as_str(), "not_published");
    }
}

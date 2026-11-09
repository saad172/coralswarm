//! Correction propagation (contract §8.4 / C15) — the tree-wide mechanism
//! that closes the gap `docs/connectors/slack.md:296`–`:299` and
//! `docs/connectors/CONNECTOR_CONTRACT.md` §8.4/§8.5's conformance test 12
//! both name: `transcript_corrections` and speaker relabelling have always
//! been a **read-time, display-only overlay** (`routes::meetings::
//! apply_corrections` / `apply_scoped_corrections`, whose own doc comments
//! say "the raw text is never mutated in storage") — nothing has EVER
//! touched `atoms`, re-embedded a rewritten atom, or invalidated a reef
//! derived from stale content. A fixed transcript with a stale reef quoting
//! the wrong speaker is worse than no correction at all (contract §8.4),
//! which is exactly the gap this module closes.
//!
//! **Not connector-specific.** This retroactively fixes Slack huddles and
//! native meeting recordings and is the prerequisite for any connector — Google Meet's Train G3
//! PR A included — ever declaring C15 honestly. `docs/connectors/slack.md`
//! (`:285`–`:299`) records the OLD posture this module deliberately
//! overrides: "corrections stay in the copy where they are made... they do
//! not propagate to other fanned-out copies". That was true because nothing
//! propagated ANYTHING; this module makes the shared source identity
//! (`source_type`, `source_id`) the propagation key for corrections exactly
//! as it already is for deletion (see below), so a correction now reaches
//! every fanned-out copy in the account, same as a delete does.
//!
//! # Deliberately shaped like `services::deletion::delete_source`
//!
//! Both start from one `(graph_id, source_id)`, resolve the account that
//! owns `graph_id` via the audited `resolve_graph_account_id` SECURITY
//! DEFINER bootstrap (migration `0084`, never a raw `graphs` read — see that
//! module's own doc comment for why), and expand to every atom account-wide
//! sharing the same source identity — deletion's "propagation is a lookup,
//! not a graph walk" argument applies unchanged here. The one shape
//! difference: deletion matches `source_id` EXACTLY (a coral's whole-atom
//! identity), while a meeting's `source_id` here is the un-chunked BASE id
//! (`"{base}:chunk:{n}"` is what actually appears on `atoms.source_id` —
//! `workers::extract::base_source_id` strips it) — so this module matches
//! the base id itself (an atom small enough to have never been chunked)
//! **or** any of its chunk children, the same `source_id = $2 OR source_id
//! LIKE $3` shape `workers::summary::resolve_meeting_attribution` already
//! uses for the identical reason (never accidentally matching an unrelated
//! sibling whose id merely starts with this one — see
//! [`chunk_or_base_pattern`]'s doc comment).
//!
//! # What propagation actually does, per correction event
//!
//! 1. **Rewrite.** Load this graph+source's `applied` corrections
//!    (`transcript_corrections`, unchanged table/columns — 0056/0070), then
//!    for every atom account-wide sharing the source identity, replace its
//!    STORED content with `routes::meetings::apply_scoped_corrections`
//!    applied to that atom's current content.
//! 2. **Re-embed.** Every atom whose stored content actually changed gets
//!    one `EmbedAtoms` job naming it.
//! 3. **Re-derive.** Any reef (`nodes` row) whose ONLY linked corals
//!    (`node_atoms`) are atoms this call just rewrote is deleted — the
//!    generalization of deletion's "lost its last coral" rule
//!    (`services::deletion::delete_atom_cascade`, `deleted_reef_ids`) to "every
//!    coral this reef cites just changed out from under it", not merely
//!    "every coral is gone". The rewritten atoms are NOT deleted (unlike
//!    deletion's cascade — a correction fixes content, it doesn't remove
//!    it), so a reef that also cites even ONE atom outside the rewritten set
//!    survives untouched: only a reef this call can fully explain gets
//!    cleared, matching §5's "delete the reefs whose only corals are the
//!    rewritten atoms and let extraction rebuild" instruction precisely (the
//!    cheaper, more surgical of the two options that design section names —
//!    see [`propagate`]'s doc comment for why re-derivation was chosen over
//!    the alternative). This module enqueues no rebuild: the per-atom
//!    `ExtractEntities`/`ExtractRelationships` re-derivation it used to
//!    enqueue is retired (it had been dead-lettered at admission since the
//!    managed-purpose fence), so a cleared reef comes back only when managed
//!    source extraction reads a version that still mentions it.
//! 4. **Leave `meeting_transcripts` and the overlay alone.** The verbatim raw
//!    transcript (`meeting_transcripts.raw_inline`/`raw_blob_key`) is never
//!    touched by this module — `RELABEL_SUMMARY_OWNER_SQL`
//!    (`store::mod::relabel_summary_owner[_scoped]`) stays exactly as it is,
//!    and `GET /v1/meetings/summary`'s `corrected_text` overlay keeps working
//!    unmodified, because it is computed from that untouched raw text, not
//!    from any atom this module rewrites. This is deliberate (§5 point 6):
//!    the overlay is what makes a correction visible **immediately**, while
//!    propagation (this module) runs asynchronously to the request that
//!    created the correction — removing the overlay would make corrections
//!    look broken for however long embed/extraction re-derivation takes.
//!
//! # Idempotency
//!
//! [`propagate`] is safe to call after every correction write, revert, and
//! restore (§5). Re-running it with an UNCHANGED `applied` correction set
//! converges to a no-op: [`rewrite_atom_content`] only ever replaces a
//! `surface_form` that is still literally present in an atom's current
//! content, so an atom already rewritten by a prior call simply doesn't
//! match again (no double-prepending, no drift) — the same "content-based
//! idempotency, not a separate progress ledger" argument
//! `workers::reembed::plan_reembed`'s doc comment makes for its own
//! header-injection idempotency.
//!
//! # Known, documented limitations: propagation is forward-only, and
//! occurrence-scoped corrections are not safe to reapply after a mutation
//!
//! Propagation still rewrites the retrieval atom's current content. Retained
//! source originals and superseded blobs are preserved, but propagation does
//! not reconstruct the retrieval atom from those originals on a later event.
//!
//! **Revert does not retroactively un-rewrite.** If a correction that
//! already propagated is later reverted, [`propagate`]'s next run sees a
//! SMALLER `applied` set and has no way to un-rewrite the atoms a prior run
//! already changed — a revert stops FUTURE propagate calls from picking up
//! that surface→canonical rewrite, but does not undo one already applied to
//! stored atom content.
//!
//! **An occurrence-scoped (this-segment, `Some(n)`) correction is safe to
//! apply exactly ONCE per atom, not safe to blindly reapply.**
//! [`rewrite_atom_content`]'s `Some(n)` semantics — inherited unchanged from
//! `apply_scoped_corrections`, which the read-time overlay relies on being
//! "the n-th occurrence of `surface_form` in the text handed to it" — are
//! positional over WHATEVER text they're given. The overlay is always safe
//! because it is always given the same pristine raw text. This module is
//! NOT always safe in the equivalent way: if propagate() runs a second time
//! while an occurrence-scoped correction is STILL `applied` (e.g. a caller
//! writes a SECOND, unrelated correction on the same source and this
//! module reloads the full `applied` set, which still includes the first
//! one), and that atom's content still contains the surface_form MORE THAN
//! once (an atom spanning multiple same-speaker turns), the "n-th
//! occurrence" the second call finds is n-th IN THE ALREADY-PARTIALLY-
//! MUTATED text — which is not necessarily the same original turn the first
//! call rewrote. Concretely: content `"Speaker 2: a. Speaker 2: b."` with
//! `occurrence = Some(1)` rewrites to `"Alice: a. Speaker 2: b."` on the
//! first call; a second call with the SAME correction still `applied` finds
//! exactly one remaining `"Speaker 2"` (now occurrence 1 of what's left) and
//! rewrites THAT one too, over-correcting to `"Alice: a. Alice: b."` — a
//! turn that was never meant to be touched. All-scope (`occurrence = None`)
//! corrections do NOT have this problem (a fully-replaced surface_form is
//! gone from the text, so a repeat pass is a true no-op — see the
//! "Idempotency" section above and this module's `idempotent_convergence_*`
//! unit tests). This is a real, understood gap in this PR, not silently
//! discovered: a correct fix needs either a persisted per-atom "which
//! corrections have already landed" ledger (a new column/table — a
//! migration, out of this PR's scope) or reconstructing each atom from its
//! meeting's pristine raw text through the SAME chunker ingest used (which
//! only exists for Pipeline A meetings — `meeting_transcripts` — and not at
//! all for Pipeline B connectors like Slack huddles, per §1.7 of the design
//! this closes). Both are follow-up work, reported as such rather than
//! silently risked.

use std::collections::HashSet;

use chrono::Utc;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::services::blob_store::PromotionBlobStore;
use crate::services::content_replacement::{self, ContentScope, ContentSnapshot};
use crate::store::{append_op_tx, sync};
use crate::tenancy::scope::begin_account_scoped;

use crate::routes::meetings::apply_scoped_corrections;

#[derive(Debug, thiserror::Error)]
pub enum CorrectionsError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
}

/// Outcome of one [`propagate`] call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PropagationOutcome {
    /// Every atom whose STORED content this call actually rewrote (its
    /// content differed from `rewrite_atom_content`'s output and the write
    /// succeeded) — labelled with the graph it lives in, since propagation
    /// can (and typically does) span more than one graph in the account.
    pub rewritten_atoms: Vec<(Uuid /* graph_id */, Uuid /* atom_id */)>,
    /// Every reef deleted because ALL of its linked corals were among
    /// [`Self::rewritten_atoms`] — see this module's doc comment, point 3.
    pub invalidated_reef_ids: Vec<(Uuid /* graph_id */, Uuid /* node_id */)>,
    pub embed_job_enqueued: bool,
}

// ── Account resolution (identical shape to services::deletion) ─────────────

/// Resolve the `account_id` that owns `graph_id` — authoritative, never
/// trusted from a caller. Same bootstrap, same reasoning as
/// `services::deletion::resolve_account_id_for_graph` (duplicated rather than
/// exported `pub(crate)` from that module purely to keep the two services
/// independent of each other's internals — both are thin wrappers over the
/// one audited SQL function, so there is no drift risk in having two call
/// sites).
async fn resolve_account_id_for_graph(
    pool: &PgPool,
    graph_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT resolve_graph_account_id($1)")
        .bind(graph_id)
        .fetch_one(pool)
        .await
}

// ── Pure logic (unit-tested without a database) ─────────────────────────────

/// Rewrite one atom's stored content by applying `corrections` to it —
/// a thin, named wrapper over `routes::meetings::apply_scoped_corrections`
/// (0070's occurrence-aware overlay function), reused verbatim rather than
/// re-implemented: the overlay and the storage rewrite must always agree on
/// what "apply this correction" means, and the overlay's own extensive unit
/// tests (`routes::meetings`'s `apply_scoped_corrections` suite) already
/// cover the replacement semantics (whole-word, case-insensitive,
/// `Some(n)` = only the n-th occurrence).
///
/// **Occurrence is counted PER ATOM, not per meeting.** The read-time
/// overlay counts occurrences across the WHOLE verbatim raw transcript
/// (`meeting_transcripts.raw_content`); an atom is one CHUNK of that
/// transcript (`routes::ingest`'s `"{base}:chunk:{n}"` convention), so "the
/// n-th occurrence within this atom's own text" is not generally the same
/// occurrence the overlay's global count would select. This is an accepted
/// divergence, not a bug: the overlay is unaffected by this module entirely
/// (see the module doc's point 4), so nothing requires the two counts to
/// agree — this function's only job is to converge each atom's STORED
/// content toward what a reader of that atom alone would expect the
/// correction to mean.
pub(crate) fn rewrite_atom_content(
    content: &str,
    corrections: &[(String, String, Option<i32>)],
) -> String {
    apply_scoped_corrections(content, corrections)
}

/// Build the `source_id LIKE` pattern matching a meeting's CHUNK atoms —
/// `"{base}:chunk:{n}"` (`routes::ingest`) — escaping LIKE metacharacters
/// (`\`, `%`, `_`) exactly as `workers::summary::chunk_like_pattern` does
/// (duplicated rather than exposed `pub(crate)` from that module, same
/// reasoning that module's own doc comment gives: one small, cheap-to-keep-
/// in-lockstep helper per caller beats widening either module's public
/// surface for a three-line function). The `:chunk:` delimiter is the whole
/// point — a bare `"<escaped>%"` would also match an unrelated sibling
/// whose `source_id` merely starts with this one (`"meeting-1%"` matching
/// `"meeting-10:chunk:0"`), silently pulling a stranger's atom into this
/// correction's rewrite set.
pub(crate) fn chunk_or_base_pattern(base_source_id: &str) -> String {
    let mut out = String::with_capacity(base_source_id.len() + 8);
    for c in base_source_id.chars() {
        if c == '\\' || c == '%' || c == '_' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push_str(":chunk:%");
    out
}

// ── Candidate atoms (account-wide, across every graph — module doc) ────────

struct CandidateAtom {
    graph_id: Uuid,
    atom_id: Uuid,
    content_blob_key: String,
    content_inline: Option<String>,
    content_revision: i64,
    header_len: Option<i32>,
    timestamp: chrono::DateTime<Utc>,
}

/// Runs on an already-`begin_account_scoped` transaction (CodeRabbit round 1
/// re-review self-catch, not a numbered finding — found while proving
/// finding 1's fix against the REAL `coralswarm_app` role rather than the
/// `probe.owner` BYPASSRLS pool every earlier test in this tree's own
/// `corrections_propagate_pg.rs` used): `atoms`/`graphs` are both on
/// migration `0083`'s RLS roster, exactly like `services::deletion::
/// expand_by_source_identity`'s own identical comment explains — a plain
/// `.fetch_all(pool)` here is not merely "unscoped", it can actively FAIL on
/// a pooled `coralswarm_app` connection that a PRIOR `begin_account_scoped`
/// call already touched: Postgres resets a custom GUC set via `SET LOCAL`
/// back to the empty string (not "unset") at that transaction's end, so the
/// NEXT unscoped read of `current_setting('app.account_id')::uuid` on the
/// SAME pooled connection raises `invalid input syntax for type uuid: ""`
/// rather than the intended `missing_ok`-free hard-fail this policy is
/// designed to produce. `services::deletion`'s own module doc calls this out
/// as "was a live, hard-failing bug" — this module had silently reintroduced
/// it, invisible to every `corrections_propagate_pg.rs` test because they
/// all run against the BYPASSRLS `probe.owner` pool, where `SET LOCAL`/RLS
/// are inert. `routes_b5a_rls_pg.rs`'s real-`coralswarm_app`-role harness is
/// what caught it.
async fn find_candidate_atoms(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    base_source_id: &str,
) -> Result<Vec<CandidateAtom>, sqlx::Error> {
    let chunk_pattern = chunk_or_base_pattern(base_source_id);
    let rows = sqlx::query(
        "SELECT a.graph_id, a.atom_id, a.content_blob_key, a.content_inline, a.content_revision, a.header_len, a.timestamp \
         FROM atoms a \
         JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND a.source_type = 'transcript' \
           AND (a.source_id = $2 OR a.source_id LIKE $3 ESCAPE '\\')",
    )
    .bind(account_id)
    .bind(base_source_id)
    .bind(&chunk_pattern)
    .fetch_all(&mut **tx)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| CandidateAtom {
            graph_id: r.get("graph_id"),
            atom_id: r.get("atom_id"),
            content_blob_key: r.get("content_blob_key"),
            content_inline: r.get("content_inline"),
            content_revision: r.get("content_revision"),
            header_len: r.get("header_len"),
            timestamp: r.get("timestamp"),
        })
        .collect())
}

/// Load this `(graph_id, source_id)`'s `applied` corrections, in the exact
/// `(surface_form, canonical, occurrence)` shape
/// [`rewrite_atom_content`]/`apply_scoped_corrections` need — same
/// `status = 'applied'` filter and column set
/// `routes::meetings::meeting_summary` uses to build its own overlay
/// (`list_transcript_corrections_scoped`'s underlying query), read directly
/// here rather than through the `Store` trait so this module stays as
/// self-contained (raw SQL, no `dyn Store`) as `services::deletion`.
///
/// Runs on an already-`begin_account_scoped` transaction — `transcript_corrections`
/// is on migration `0083`'s RLS roster too; see [`find_candidate_atoms`]'s doc
/// comment for why an unscoped `.fetch_all(pool)` here is actively unsafe on a
/// pooled `coralswarm_app` connection, not merely "less defensive".
async fn load_applied_corrections(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    source_id: &str,
) -> Result<Vec<(String, String, Option<i32>)>, sqlx::Error> {
    let rows: Vec<(String, String, Option<i32>)> = sqlx::query_as(
        "SELECT surface_form, canonical, occurrence FROM transcript_corrections \
         WHERE graph_id = $1 AND source_id = $2 AND status = 'applied' \
         ORDER BY created_at ASC",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}

// ── Persisting a rewritten atom (per-atom, own short transaction each) ─────

async fn persist_rewritten_content(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    dek: Option<&[u8; 32]>,
    device_id: &str,
    account_id: Uuid,
    atom: &CandidateAtom,
    new_content: &str,
    new_header_len: Option<i32>,
) -> Result<bool, String> {
    content_replacement::replace(
        ContentScope::Account { pool, account_id },
        &ContentSnapshot {
            graph_id: atom.graph_id,
            atom_id: atom.atom_id,
            revision: atom.content_revision,
            blob_key: atom.content_blob_key.clone(),
            inline: atom.content_inline.clone(),
            header_len: atom.header_len,
            timestamp: atom.timestamp,
        },
        new_content,
        new_header_len,
        device_id,
        blobs,
        dek,
    ).await
}

// ── Reef invalidation (module doc, point 3) ─────────────────────────────────

type NodeRow = (
    Uuid,                          // graph_id
    String,                        // type
    String,                        // title
    serde_json::Value,             // body
    String,                        // scope
    Option<Uuid>,                  // owner_id
    chrono::DateTime<chrono::Utc>, // created_at
    i32,                           // version
);

/// Delete every reef (`nodes` row) whose FULL set of linked corals
/// (`node_atoms`) is a subset of `rewritten_atom_ids` — i.e. every coral a
/// reader would need to re-derive that reef from just got rewritten. This is
/// STRICTLY narrower than "any reef touching a rewritten atom": a reef that
/// also cites an atom OUTSIDE the rewritten set survives untouched (its
/// characterization may still be accurate — nothing about a SIBLING atom's
/// correction invalidates it), exactly as `services::deletion::
/// delete_atom_cascade`'s "zero remaining corals" rule only fires once EVERY
/// linked coral is actually gone, never on a partial loss.
///
/// Returns the deleted `(graph_id, node_id)` pairs. Runs inside its own
/// short `begin_account_scoped` transaction — one write phase, no I/O other
/// than Postgres, so (unlike [`persist_rewritten_content`]) this needs no
/// per-node transaction split.
async fn invalidate_fully_rewritten_reefs(
    pool: &PgPool,
    account_id: Uuid,
    device_id: &str,
    rewritten_atom_ids: &[Uuid],
) -> Result<Vec<(Uuid, Uuid)>, CorrectionsError> {
    if rewritten_atom_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut tx = begin_account_scoped(pool, account_id).await?;

    // A node qualifies iff (a) it links at least one rewritten atom, and
    // (b) it links NO atom outside the rewritten set.
    let candidate_node_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT na.node_id FROM node_atoms na \
         WHERE na.node_id IN (SELECT node_id FROM node_atoms WHERE atom_id = ANY($1)) \
         GROUP BY na.node_id \
         HAVING bool_and(na.atom_id = ANY($1))",
    )
    .bind(rewritten_atom_ids)
    .fetch_all(&mut *tx)
    .await?;

    let now = Utc::now();
    let mut deleted = Vec::new();
    for node_id in candidate_node_ids {
        let node: Option<NodeRow> = sqlx::query_as(
            "SELECT graph_id, type, title, body, scope, owner_id, created_at, version \
             FROM nodes WHERE node_id = $1 FOR UPDATE",
        )
        .bind(node_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((
            node_graph_id,
            node_type,
            title,
            body,
            node_scope,
            owner_id,
            node_created_at,
            version,
        )) = node
        else {
            // Already gone (a concurrent delete/merge got there first) —
            // idempotent no-op for this node, same convention
            // `services::deletion::delete_atom_cascade` uses.
            continue;
        };
        // A rewrite invalidates only its retrieval chunks. Semantic evidence
        // belongs to immutable source passages and must keep its node reachable.
        let supported: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM node_semantic_atoms WHERE node_id=$1) OR EXISTS(SELECT 1 FROM node_atoms WHERE node_id=$1 AND NOT (atom_id=ANY($2)))",
        ).bind(node_id).bind(rewritten_atom_ids).fetch_one(&mut *tx).await?;
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
                node_graph_id,
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
        deleted.push((node_graph_id, node_id));
    }

    tx.commit().await?;
    Ok(deleted)
}

// ── The entry point ──────────────────────────────────────────────────────────

/// Propagate `(graph_id, source_id)`'s currently-`applied` corrections to
/// every atom account-wide sharing that source identity, then re-embed and
/// re-derive as needed (module doc). `source_id` is the BASE (un-chunked)
/// meeting id — the same value `routes::meetings::create_speaker_relabel`'s
/// `CreateRelabelBody::source_id` and `list_transcript_corrections[_scoped]`
/// already use.
///
/// Safe to call after every correction write, revert, and restore — see this
/// module's "Idempotency" and "Known, documented limitation" doc sections.
/// An unknown `graph_id` (race with graph deletion) is an idempotent no-op,
/// matching every `services::deletion` entry point's convention.
///
pub async fn propagate(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    dek: Option<[u8; 32]>,
    device_id: &str,
    graph_id: Uuid,
    source_id: &str,
) -> Result<PropagationOutcome, CorrectionsError> {
    let Some(account_id) = resolve_account_id_for_graph(pool, graph_id).await? else {
        return Ok(PropagationOutcome::default());
    };

    // Acquire the per-(account, source) lock BEFORE reading anything else —
    // everything from here down must observe state as of AFTER any
    // concurrent call has fully finished and released it.
    let mut lock_tx = pool.begin().await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtext('corrections_propagate:' || $1::text || ':' || $2))",
    )
    .bind(account_id.to_string())
    .bind(source_id)
    .execute(&mut *lock_tx)
    .await?;

    let result =
        propagate_locked(pool, blobs, dek, device_id, account_id, graph_id, source_id).await;

    // `pg_advisory_xact_lock` releases on `lock_tx` commit OR rollback —
    // rollback (not commit) since this transaction never held any real
    // writes of its own, matching `run_gmail_sync_for_email`'s identical
    // choice and its doc comment's reasoning (release promptly rather than
    // leaving the lock held for however long this connection idles in the
    // pool before reuse).
    lock_tx.rollback().await.ok();

    result
}

/// The actual propagation logic, run only once [`propagate`]'s advisory lock
/// is held. Split out purely so the lock-acquisition wrapper above stays
/// readable; not meant to be called directly (hence not `pub`).
async fn propagate_locked(
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    dek: Option<[u8; 32]>,
    device_id: &str,
    account_id: Uuid,
    graph_id: Uuid,
    source_id: &str,
) -> Result<PropagationOutcome, CorrectionsError> {
    // One scoped, read-only transaction for the whole setup phase (corrections
    // load + candidate-atom lookup) — same shape as `services::deletion`'s
    // entry points ("One scoped, read-only transaction for the whole setup
    // phase... Rolled back below"). `atoms`/`graphs`/`transcript_corrections`
    // are all on migration `0083`'s RLS roster; see [`find_candidate_atoms`]'s
    // doc comment for why this must never be a plain `.fetch_all(pool)`.
    let mut setup_tx = begin_account_scoped(pool, account_id).await?;
    let corrections = load_applied_corrections(&mut setup_tx, graph_id, source_id).await?;
    if corrections.is_empty() {
        // Nothing currently `applied` for this correction identity — a
        // legitimate no-op (never created, or every correction here was
        // reverted; see the module doc's "forward-only" limitation for why
        // this does not retroactively un-rewrite anything already applied).
        setup_tx.rollback().await.ok();
        return Ok(PropagationOutcome::default());
    }
    let candidates = find_candidate_atoms(&mut setup_tx, account_id, source_id).await?;
    setup_tx.rollback().await.ok();

    let mut outcome = PropagationOutcome::default();
    let mut rewritten_atom_ids: Vec<Uuid> = Vec::new();

    for atom in &candidates {
        let current_content = match content_replacement::current_text(
            &atom.content_blob_key, atom.content_inline.as_deref(), blobs, dek.as_ref(),
        ).await {
            Ok(content) => content,
            Err(error) => {
                tracing::warn!(atom_id=%atom.atom_id, %error, "correction skipped: current content unavailable");
                continue;
            }
        };

        let rewritten = rewrite_atom_content(&current_content, &corrections);
        if rewritten == current_content {
            continue;
        }

        let new_header_len = match atom.header_len {
            Some(length) if length > 0 => {
                let Some(header) = current_content.get(..length as usize) else {
                    tracing::warn!(atom_id=%atom.atom_id, "correction skipped: invalid stored header boundary");
                    continue;
                };
                let Ok(length) = i32::try_from(rewrite_atom_content(header, &corrections).len()) else {
                    tracing::warn!(atom_id=%atom.atom_id, "correction skipped: replacement header is too large");
                    continue;
                };
                Some(length)
            }
            length => length,
        };

        match persist_rewritten_content(
            pool,
            blobs,
            dek.as_ref(),
            device_id,
            account_id,
            atom,
            &rewritten,
            new_header_len,
        )
        .await
        {
            Ok(true) => {
                outcome.embed_job_enqueued = true;
                outcome.rewritten_atoms.push((atom.graph_id, atom.atom_id));
                rewritten_atom_ids.push(atom.atom_id);
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    atom_id = %atom.atom_id,
                    error = %e,
                    "corrections propagate: failed to persist rewritten content — atom stays a \
                     candidate for the next correction event"
                );
            }
        }
    }

    if !rewritten_atom_ids.is_empty() {
        match invalidate_fully_rewritten_reefs(pool, account_id, device_id, &rewritten_atom_ids)
            .await
        {
            Ok(deleted) => outcome.invalidated_reef_ids = deleted,
            Err(e) => tracing::warn!(
                error = %e,
                "corrections propagate: reef invalidation query failed — stale reefs left in place, \
                 next correction event on this source will retry"
            ),
        }
    }

    Ok(outcome)
}

/// De-duplicated convenience over [`PropagationOutcome::rewritten_atoms`] —
/// used by callers that only need the atom-id set (e.g. a caller already
/// tracking per-graph counts separately).
pub fn rewritten_atom_ids(outcome: &PropagationOutcome) -> Vec<Uuid> {
    outcome
        .rewritten_atoms
        .iter()
        .map(|(_, atom_id)| *atom_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn c(surface: &str, canonical: &str, occurrence: Option<i32>) -> (String, String, Option<i32>) {
        (surface.to_string(), canonical.to_string(), occurrence)
    }

    // ── rewrite_atom_content: idempotent convergence ───────────────────────

    #[test]
    fn idempotent_convergence_all_scope() {
        let content = "Speaker 2: hello everyone. Speaker 2: any questions?";
        let corrections = vec![c("Speaker 2", "Alice", None)];
        let once = rewrite_atom_content(content, &corrections);
        let twice = rewrite_atom_content(&once, &corrections);
        assert_eq!(
            once, twice,
            "re-running the SAME correction set must converge"
        );
        assert!(once.contains("Alice") && !once.contains("Speaker 2"));
    }

    /// An occurrence-scoped correction IS idempotent within a single
    /// application when the atom has only ONE occurrence of the surface
    /// form to begin with — the common case for a chunk atom scoped to one
    /// turn.
    #[test]
    fn idempotent_convergence_occurrence_scoped_single_occurrence_atom() {
        let content = "Speaker 2: only one turn in this atom.";
        let corrections = vec![c("Speaker 2", "Alice", Some(1))];
        let once = rewrite_atom_content(content, &corrections);
        let twice = rewrite_atom_content(&once, &corrections);
        assert_eq!(once, twice);
        assert_eq!(once, "Alice: only one turn in this atom.");
    }

    /// Known, documented limitation (module doc): when an atom's content
    /// contains MORE than one occurrence of an occurrence-scoped
    /// correction's surface form, reapplying the SAME still-`applied`
    /// correction against already-mutated content is NOT idempotent — the
    /// "n-th occurrence" search re-counts against whatever text it's
    /// currently given, so a second pass can over-correct a turn the first
    /// pass never touched. This test pins the CURRENT (accepted) behavior
    /// so a future change to it is a deliberate decision, not a silent
    /// regression — see the module doc's "Known, documented limitations"
    /// section for why this is out of scope to fix here (no migration
    /// budget for a per-atom "already applied" ledger).
    #[test]
    fn occurrence_scoped_reapplication_can_over_correct_a_multi_turn_atom_known_limitation() {
        let content = "Speaker 2: hello. Speaker 2: bye.";
        let corrections = vec![c("Speaker 2", "Alice", Some(1))];
        let once = rewrite_atom_content(content, &corrections);
        assert_eq!(
            once, "Alice: hello. Speaker 2: bye.",
            "first pass rewrites only occurrence 1"
        );

        let twice = rewrite_atom_content(&once, &corrections);
        assert_eq!(
            twice, "Alice: hello. Alice: bye.",
            "KNOWN LIMITATION: a second pass with the same still-applied occurrence-scoped \
             correction over-corrects the second turn too, because occurrence counting is \
             positional over CURRENT (already-mutated) text, not the original — see the module \
             doc. `services::corrections::propagate` callers must not rely on reapplying an \
             occurrence-scoped correction being a no-op on a multi-occurrence atom."
        );
    }

    #[test]
    fn idempotent_convergence_multiple_corrections() {
        let content = "Speaker 1: hi. Speaker 2: hi back.";
        let corrections = vec![c("Speaker 1", "Bob", None), c("Speaker 2", "Alice", None)];
        let once = rewrite_atom_content(content, &corrections);
        let twice = rewrite_atom_content(&once, &corrections);
        let thrice = rewrite_atom_content(&twice, &corrections);
        assert_eq!(once, twice);
        assert_eq!(twice, thrice);
        assert_eq!(once, "Bob: hi. Alice: hi back.");
    }

    // ── occurrence-scoped rewrite ────────────────────────────────────────────

    #[test]
    fn occurrence_scoped_rewrite_touches_only_the_nth_occurrence() {
        let content = "Speaker 2: first turn. Speaker 2: second turn. Speaker 2: third turn.";
        let corrections = vec![c("Speaker 2", "Alice", Some(2))];
        let out = rewrite_atom_content(content, &corrections);
        assert_eq!(
            out,
            "Speaker 2: first turn. Alice: second turn. Speaker 2: third turn."
        );
    }

    #[test]
    fn occurrence_scoped_rewrite_out_of_range_is_a_no_op() {
        let content = "Speaker 2: only one turn here.";
        let corrections = vec![c("Speaker 2", "Alice", Some(5))];
        assert_eq!(rewrite_atom_content(content, &corrections), content);
    }

    #[test]
    fn all_scope_rewrite_touches_every_occurrence() {
        let content = "Speaker 2: one. Speaker 2: two. Speaker 2: three.";
        let corrections = vec![c("Speaker 2", "Alice", None)];
        let out = rewrite_atom_content(content, &corrections);
        assert_eq!(out, "Alice: one. Alice: two. Alice: three.");
    }

    // ── no-op when no corrections apply ─────────────────────────────────────

    #[test]
    fn no_op_with_empty_corrections() {
        let content = "Nothing here should change.";
        assert_eq!(rewrite_atom_content(content, &[]), content);
    }

    #[test]
    fn no_op_when_surface_form_absent_from_content() {
        let content = "This atom never mentions the corrected speaker.";
        let corrections = vec![c("Speaker 9", "Nobody", None)];
        assert_eq!(rewrite_atom_content(content, &corrections), content);
    }

    #[test]
    fn no_op_preserves_content_exactly_byte_for_byte() {
        let content = "Emoji and unicode: café, naïve, 日本語, 🎉";
        let corrections = vec![c("nonexistent", "replacement", None)];
        assert_eq!(rewrite_atom_content(content, &corrections), content);
    }

    // ── chunk_or_base_pattern: LIKE-pattern escaping ────────────────────────

    #[test]
    fn chunk_or_base_pattern_appends_chunk_delimiter() {
        assert_eq!(chunk_or_base_pattern("meeting-1"), "meeting-1:chunk:%");
    }

    #[test]
    fn chunk_or_base_pattern_escapes_like_metacharacters() {
        assert_eq!(chunk_or_base_pattern("a%b"), "a\\%b:chunk:%");
        assert_eq!(chunk_or_base_pattern("a_b"), "a\\_b:chunk:%");
        assert_eq!(chunk_or_base_pattern("a\\b"), "a\\\\b:chunk:%");
    }

    #[test]
    fn chunk_or_base_pattern_never_bare_prefix_matches_a_sibling() {
        // "meeting-1" must not match "meeting-10:chunk:0" — the delimiter is
        // the whole point (module/function doc).
        let pattern = chunk_or_base_pattern("meeting-1");
        assert_eq!(pattern, "meeting-1:chunk:%");
        assert!(!"meeting-10:chunk:0".starts_with("meeting-1:chunk:"));
    }

    // ── rewritten_atom_ids: de-dup convenience ──────────────────────────────

    #[test]
    fn rewritten_atom_ids_deduplicates_across_graphs() {
        let a = Uuid::new_v4();
        let g1 = Uuid::new_v4();
        let g2 = Uuid::new_v4();
        let outcome = PropagationOutcome {
            rewritten_atoms: vec![(g1, a), (g2, a)],
            invalidated_reef_ids: vec![],
            embed_job_enqueued: true,
        };
        let ids = rewritten_atom_ids(&outcome);
        assert_eq!(ids, vec![a]);
    }
}

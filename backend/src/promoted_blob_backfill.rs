//! Backfill for the promote-content-loss bug: `Store::copy_atoms_to_graph`/
//! `_scoped` used to build a promoted coral's `content_blob_key` inline in
//! SQL (`{target_graph}/{new_atom_id}`) and never wrote an R2 object at that
//! key — see those methods' own doc comments (via [`crate::store::
//! AtomBlobCopyCtx`]) for the fix. Every atom promoted BEFORE that fix
//! landed still carries the broken placeholder key; this module finds those
//! rows, resolves each one's ORIGINAL (source) atom, re-uploads its content
//! under the CORRECT `blob_key_for` key, corrects the row, and re-enqueues
//! the derivation jobs that died reading it.
//!
//! # Run once per tenant database
//! Same convention as `bin::migrate_personal_to_member_ocean`/
//! `self_reef_backfill` (see either's module doc): the consumer/control
//! plane and each org's tenant database are separate Postgres instances.
//! This module only ever touches the ONE `pool` it is given — an operator
//! runs `bin/backfill_promoted_blobs` once per tenant DSN, plus once for the
//! consumer/control plane.
//!
//! # Where the org id comes from
//! [`resolve_org_id`] reads `tenant_meta.org_id` — the SAME single-row org
//! fingerprint `tenancy::registry::TenantPoolRegistry::connect_and_verify`
//! asserts against before ever publishing a tenant pool to request-serving
//! code (see `migrations_tenant/0002_tenant_meta_and_account_keys.sql`'s own
//! doc comment). A consumer/control-plane database has no `tenant_meta`
//! table at all, which this module reads as `org_id = None` — never a CLI
//! guess. An optional `--org-id` override is accepted (see
//! `bin/backfill_promoted_blobs.rs`) purely as a rehearsal/sanity check: it
//! must agree with `tenant_meta.org_id` (or the database's consumer-plane
//! absence) or the run refuses outright.
//!
//! # Candidate selection — TWO arms
//! [`CANDIDATES_SQL`]'s own doc comment has the exact signature each arm
//! keys on. In short:
//!  1. The pre-fix placeholder key still INTACT — deliberately narrower
//!     than "any 2-segment key", so it can never mis-select a legitimately
//!     2-segment CONSUMER-plane key belonging to a directly-ingested
//!     (non-promoted) team/org-graph atom.
//!  2. The placeholder key ERASED (`content_blob_key = ''`) — selected by
//!     PROVENANCE (the `promoted_from_graph_id` join both arms share)
//!     rather than by key shape, because `workers::atom_title_backfill`'s
//!     terminal-unfetchable sweep blanks exactly the marker arm 1 selects
//!     on, and on the org plane it did so to 1,191 of tenant `coralswarm`'s
//!     3,698 broken rows ~35 minutes before this backfill first ran. A
//!     repair keyed on a marker another worker may erase has to have a
//!     second way in; see [`CANDIDATES_SQL`] for the whole story and issue
//!     #852 for the sweep-side prevention (not this module's job).
//!
//! # What "fixed" means for one atom
//! 1. Resolve the SOURCE atom: every node the candidate is linked to that
//!    carries `promoted_from_graph_id` names a candidate source graph
//!    (`Candidate::candidate_source_graphs`) — the source atom is the row,
//!    in EXACTLY ONE of those graphs, sharing the candidate's
//!    `(source_type, source_id)` (the same dedup/identity key
//!    `copy_atoms_to_graph[_scoped]` copies under). Zero matching graphs is
//!    `source_not_found`; MORE than one is `ambiguous_source` — a commons
//!    atom reachable from promotions out of different member oceans — and
//!    is skipped, never guessed (see [`find_source_atom`]'s doc comment).
//! 2. Download + decrypt the source atom's OWN `content_blob_key` with the
//!    account's DEK (same-pool promotion never crosses accounts — the
//!    source and target graphs share one `graphs.account_id`, `0083`'s
//!    strict-AND `node_promotions` policy already assumes this).
//!
//!    **A real prod shape, found by the 2026-09-09 dry run against
//!    `coralswarm`/`testorg`/the consumer plane: the source has a
//!    `content_blob_key`, but no object ever exists at it.** These are
//!    pre-cutover rows — atoms promoted back when `content_inline` was
//!    still the primary content path, whose `content_blob_key` was
//!    stamped (same unconditional placeholder-key SQL as every other
//!    promoted row) but never actually written to R2, because the promote
//!    code of that era never tried. When the source GET comes back
//!    `NotFound`, this is NOT the same failure as a real PUT/GET error —
//!    it splits on whether the CANDIDATE (target) row itself has
//!    `content_inline` (`Candidate::has_inline`):
//!      - `has_inline = true` — the candidate is already readable as-is by
//!        any inline-preferring reader; there is nothing this backfill CAN
//!        repair (the source content is gone, not merely unreached) —
//!        counted `Totals::skipped_inline_only`, never an error, never
//!        `put_failed`.
//!      - `has_inline = false` — the candidate has NO fallback at all;
//!        this atom's content is genuinely, permanently lost unless
//!        recovered from a backup — counted
//!        `Totals::unrepairable_source_missing`, printed loudly (the atom
//!        id, so an operator can follow up), but STILL never `errors` or
//!        `put_failed` (this is not a transient condition retrying would
//!        fix, and it must never block `--apply` from repairing every
//!        OTHER, actually-repairable row).
//!
//!    Neither outcome comes from [`CANDIDATES_SQL`] filtering — that query
//!    deliberately has NO `content_inline IS NULL` clause (see its own doc
//!    comment for why: a candidate with real, fetchable source content
//!    must still be repaired regardless of its own `content_inline`). The
//!    split happens HERE, at GET time, driven by what R2 actually says.
//! 3. Read the target's stored pointer. Existing content is left untouched.
//!    Missing content is repaired at a fresh UUID key. A target inline
//!    fallback is preserved instead of replacing it with source bytes.
//! 4. Publish through the shared content replacement owner. Its full target
//!    snapshot CAS commits pointer, revision, vector invalidation, sync and
//!    embedding enqueue together. A concurrent edit wins without publication.
//!    Unknown COMMIT retains the uploaded object. No stored key is written.
//! 5. Enqueue `RegenerateDigest` for every node this atom is linked to.
//!    Existing `dead` per-atom extraction rows (`ExtractEntities`/
//!    `ExtractRelationships`, retired) for this atom are left alone (informational count only,
//!    `Totals::dead_jobs_seen`) — the same "never touch history, always add
//!    a fresh idempotent enqueue" convention `workers::promote` itself
//!    already uses; a `dead` row's `idempotency_key` no longer blocks a new
//!    enqueue (the partial unique index only dedupes `status = 'pending'`
//!    rows), so this is race-free.
//!
//! # Exit code
//! `0` iff `Totals::errors == 0 && Totals::put_failed == 0` — every OTHER
//! counter (`skipped_existing`, `skipped_inline_only`,
//! `unrepairable_source_missing`, `source_not_found`, `skipped`) describes
//! a row this run correctly decided NOT to touch, not a failure, and must
//! never flip the exit code. `errors` (transient transport/DB failures,
//! surviving one retry — see `Totals::errors`' own field doc) is the ONE
//! counter worth re-running for; a nonzero `unrepairable_source_missing`
//! is worth a human's attention but re-running this binary cannot fix it.
//!
//! # Concurrency
//! One batch's candidates ([`BATCH_SIZE`]) are processed up to
//! [`BLOB_IO_CONCURRENCY`] at a time (`futures::stream::buffer_unordered`
//! in `bin/backfill_promoted_blobs.rs`) — [`process_candidate`] takes no
//! `&mut` state (it RETURNS a [`CandidateOutcome`] the caller tallies
//! AFTER every concurrent future in the batch completes, never while any
//! are still in flight), and the shared per-account DEK cache is behind a
//! `tokio::sync::Mutex`, so this is safe under real concurrency, not just
//! sequential-in-disguise. One row's failure — transient or not — never
//! aborts any other row in the batch.
//!
//! A fresh pointer leaves the legacy candidate query. Replaying an already
//! selected candidate also refuses after its stored pointer changes. The
//! caller retains an exclusion list for candidates that cannot be repaired.
//!
//! # Prod sizing
//! **These numbers are AS OF a date and change every time the tool runs —
//! read them as a rehearsal aid, never as an assertion about what a fresh
//! run will select.** The `coralswarm` line below in particular is now
//! post-`--apply`.
//!
//! Measured 2026-09-10, tenant `coralswarm`, AFTER the 2026-09-09 `--apply`
//! run and the `atom_title_backfill` sweep that overlapped it: team graph
//! `32878a81…` holds 13,819 atoms, of which **1,191 carry a blank
//! `content_blob_key`** and ZERO still carry a 2-segment one. So arm 1 now
//! selects nothing there and arm 2 selects those 1,191 — the exact rows the
//! 2026-09-09 run could not see (it repaired 2,507 of 3,698 before the
//! sweep blanked the rest). All 1,191 have a sibling row in another graph
//! with the same `(source_type, source_id)` and a non-blank key, so all
//! 1,191 are repairable; no atom in that database carries `content_inline`,
//! so none of them can land in `skipped_inline_only`. Their user-visible
//! symptom is the "Archived coral (content unavailable)" title plus 2,211
//! `dead` `RegenerateDigest` jobs ("R2 fetch failed for N of 30 R2-only
//! coral(s)"), and there are ZERO pending jobs — nothing self-heals without
//! this run.
//!
//! Measured 2026-09-09 (dry run, BEFORE any `--apply`), for the timing
//! shape rather than the counts: tenant `coralswarm`'s then-3,698
//! candidates took 32m51s SERIAL (~1.9 rows/s). Concurrency
//! (`BLOB_IO_CONCURRENCY`, and `BACKFILL_BLOB_IO_CONCURRENCY` to override
//! it per run — see `bin/backfill_promoted_blobs.rs`) shifts that, but not
//! by its full factor: DB round trips (source lookup, DEK cache, the
//! UPDATE) don't parallelize as cleanly as the R2 calls do, and the
//! 2026-09-09 `--apply` run measured ~5.3 s/row at concurrency 8 (with R2
//! GET timeouts firing) against ~1 s/row at concurrency 1 — which is why
//! that override exists.
//! - Tenant `testorg`: 1 candidate, `has_inline = true`, source object
//!   missing → 1 `skipped_inline_only`, 0 `put_ok`, exit 0.
//! - Consumer plane: 1,721 candidates total — 405 repairable in team graph
//!   `36b23676…` (`put_ok`) + 2 MORE in that same graph whose source
//!   object is also missing but which carry `content_inline`
//!   (`skipped_inline_only`) + 1,314 in org graph `da62ad07…` (promoted
//!   2026-07-28, pre-R2-cutover — ALL `has_inline = true`, sources in graph
//!   `93cc446a…` have keys but no R2 objects, all `skipped_inline_only`).
//!   Expect 405 `put_ok` + 1,316 `skipped_inline_only` on `--apply`, exit
//!   0. The dry run that first found this shape also hit 3 transient
//!   fly-proxy errors ("pool timed out", "connection reset by peer") —
//!   see [`RETRY_BACKOFF`] and the "Concurrency" section above for the fix.

use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::crypto::resolve_dek_for_tenant;
use crate::jobs::queue;
use crate::jobs::JobKind;
use crate::tenancy::TenantId;

/// Preflight: refuse a role RLS would silently filter. This walks EVERY
/// candidate row in the database by design (`CANDIDATES_SQL` carries no
/// `account_id`/`app.account_id` scoping) — under the least-privilege
/// `coralswarm_app`/`coralswarm_tenant_app` role a live RLS policy on
/// `atoms`/`nodes` would hide rows and the run would report "nothing to do"
/// instead of refusing. Same preflight-not-trust check
/// `self_reef_backfill::assert_policy_bypass`/
/// `judgement_backfill::assert_policy_bypass` each
/// perform (own copy per module, same convention), checked against `atoms`
/// (the table every query here touches) instead of `nodes`.
pub async fn assert_policy_bypass(pool: &PgPool) -> Result<(), String> {
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass('atoms')::text")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("preflight query failed: {e}"))?;
    if table.is_none() {
        return Err("no `atoms` table in this database — migrate_control / migrate_tenants has \
                    not been applied here. Run the matching migrator first."
            .to_string());
    }
    let (bypasses, is_owner, forced): (bool, bool, bool) = sqlx::query_as(
        "SELECT \
            (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
            c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user), \
            c.relforcerowsecurity \
         FROM pg_class c WHERE c.oid = 'atoms'::regclass",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("preflight query failed: {e}"))?;
    if bypasses || (is_owner && !forced) {
        return Ok(());
    }
    Err(format!(
        "REFUSING: this connection's role does not bypass row-level security on `atoms` \
         (superuser/BYPASSRLS = {bypasses}, owns the table = {is_owner}, FORCE ROW LEVEL \
         SECURITY = {forced}). This tool walks every candidate row in the database by design; \
         under the least-privilege coralswarm_app/coralswarm_tenant_app role the RLS policy \
         would hide every row and the run would report nothing to do. Point \
         MIGRATOR_DATABASE_URL (control plane) or the tenant owner credential (tenant plane) \
         at the ELEVATED role and re-run."
    ))
}

/// Candidate rows fetched per batch — same order of magnitude as
/// `bin::migrate_personal_to_member_ocean::ATOM_BATCH_SIZE` (200): large
/// enough that per-batch overhead is negligible against prod's ~4,100
/// affected atoms (the largest single graph: 3,696), small enough that one
/// batch's blob I/O stays bounded.
pub const BATCH_SIZE: i64 = 200;

/// Bounded concurrency for the per-atom R2 get+put round trips within one
/// batch — same value and reasoning as `store::ATOM_BLOB_COPY_CONCURRENCY`.
/// Wired up in `bin/backfill_promoted_blobs.rs`'s main loop
/// (`futures::stream::iter(batch).map(process_candidate).buffer_unordered
/// (BLOB_IO_CONCURRENCY)`) — see this module's "Concurrency" doc section
/// for why [`process_candidate`] takes no `&mut` state, which is what
/// makes running this many at once safe.
pub const BLOB_IO_CONCURRENCY: usize = 8;

/// Per-call timeout for a single R2 get or put — a hung network call must
/// not stall the whole backfill. Matches
/// `bin::migrate_personal_to_member_ocean::BLOB_IO_TIMEOUT`'s order of
/// magnitude.
pub const BLOB_IO_TIMEOUT: Duration = Duration::from_secs(30);

/// Backoff before the SECOND (and last) attempt of a retried transport/DB
/// call (see [`process_candidate`]'s doc comment, step 2, for the prod
/// incident this exists for: real "pool timed out"/connection-reset
/// errors through a fly-proxy connection). Short and fixed, not
/// exponential — this is a low-frequency operator tool re-run by hand on
/// genuine failure, not a worker with its own backoff ladder, so one quick
/// retry is the right amount of self-healing.
pub const RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// One atom this backfill selected — either its row still carries the
/// pre-fix placeholder `{graph_id}/{atom_id}` key (arm 1) or its key was
/// blanked to `''` by another worker (arm 2). See [`CANDIDATES_SQL`] for
/// both arms.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Candidate {
    pub atom_id: Uuid,
    pub graph_id: Uuid,
    pub account_id: Uuid,
    pub source_type: String,
    pub source_id: String,
    /// This row's CURRENT key: arm 1's intact placeholder
    /// (`{graph_id}/{atom_id}`), or `''` for an arm-2 candidate — the key
    /// another worker erased (see [`CANDIDATES_SQL`]). Carried for the
    /// target snapshot CAS and report. A changed key refuses stale candidates.
    /// Report lines render blank keys through [`key_for_report`].
    pub content_blob_key: String,
    /// Every DISTINCT `promoted_from_graph_id` reachable from a node this
    /// atom is linked to — NOT a single pinned choice. A commons atom
    /// deduped by `(graph_id, source_type, source_id)` can be linked from
    /// reefs promoted out of SEVERAL different member oceans, so there can
    /// legitimately be more than one candidate source graph here — see
    /// `find_source_atom`'s doc comment for how that ambiguity is resolved
    /// (never guessed).
    pub candidate_source_graphs: Vec<Uuid>,
    /// `true` iff THIS candidate row (not the source) already carries
    /// `content_inline`. Decisive when the source's own blob turns out to
    /// be missing from R2 (a real, measured prod shape — pre-cutover rows
    /// promoted while `content_inline` was still the primary path, whose
    /// `content_blob_key` was always stamped but never actually written
    /// to): see [`process_candidate`]'s doc comment for the
    /// `skipped_inline_only` vs `unrepairable_source_missing` split this
    /// drives.
    pub has_inline: bool,
}

/// Two arms, one repair — both narrowed by the SAME promotion provenance
/// (`node_atoms`/`nodes` -> a node carrying `promoted_from_graph_id IS NOT
/// NULL`, in a `team`/`org` graph). What differs is how a broken row is
/// RECOGNIZED.
///
/// **Arm 1 — the pre-fix placeholder key, still intact.**
/// `Store::copy_atoms_to_graph`/`_scoped`'s pre-fix SQL always wrote EXACTLY
/// `{target_graph_id}/{new_atom_id}` as `content_blob_key` — a 2-segment,
/// SELF-referential key (both of ITS OWN segments equal THIS row's own
/// `graph_id`/`atom_id`), never the source's key, never an org-prefixed one.
/// Requiring BOTH segments to self-match (not merely "exactly 2 segments")
/// is what keeps this arm from ever mis-selecting a legitimately 2-segment
/// CONSUMER-plane key (`blob_key_for(None, ...)`) belonging to a
/// directly-ingested, non-promoted team/org-graph atom — its
/// `content_blob_key` also happens to look like `{its own graph}/{its own
/// atom}`.
///
/// **Arm 2 — the marker was ERASED (`content_blob_key = ''`), so selection
/// falls back to provenance.** A repair keyed on a marker that ANOTHER
/// worker can erase must not select on that marker alone, and this one
/// learned that the hard way: `workers::atom_title_backfill`'s
/// `TERMINAL_UNFETCHABLE_SQL` treats an `assert_blob_key_org_scope`
/// rejection as terminal and blanks `content_blob_key` to `''`
/// (`atoms.content_blob_key` is `NOT NULL`, so `''` is that table's "no
/// usable blob" value). On the ORG plane EVERY pre-fix placeholder key IS
/// such a rejection — a 2-segment key carries no org segment at all — and
/// that sweep shipped ~35 minutes BEFORE this backfill first ran: it blanked
/// 1,191 of tenant `coralswarm`'s 3,698 broken rows, which arm 1 then could
/// not see at all (`''` satisfies neither `split_part` equality), so only
/// 2,507 were repaired (measured 2026-09-10). Those 1,191 are the rows that
/// most need repairing — a blanked key is exactly what renders as "Archived
/// coral (content unavailable)" and dead-ends every `RegenerateDigest` for
/// the node it hangs off — and they are fully repairable:
/// [`find_source_atom`] resolves the source by `(source_type, source_id)`
/// across `candidate_source_graphs` and NEVER reads the candidate's own key,
/// and [`process_candidate`] computes the TARGET key from `blob_key_for` and
/// never from `Candidate::content_blob_key` either. So this arm selects by
/// the one property that sweep cannot erase: the promotion provenance the
/// join above already establishes. (Stopping the sweep from erasing it in
/// the first place is issue #852 — deliberately NOT this query's job. This
/// query has to find rows that were ALREADY blanked whether or not anything
/// ever blanks another one.)
///
/// `COALESCE(a.content_blob_key, '') = ''`, not `a.content_blob_key = ''`:
/// the column is `NOT NULL` on both planes
/// (`migrations_control/0003_atoms.sql`,
/// `migrations_tenant/0001_tenant_baseline.sql`), so today this can only
/// ever match `''`. The `COALESCE` costs nothing and keeps the arm working
/// (rather than silently selecting nothing) if that constraint is ever
/// relaxed.
///
/// **This is a HINT, not a proof — `process_candidate` never trusts it
/// alone.** Three known ways this query can still select a row that does NOT
/// actually need fixing:
/// * On the CONSUMER plane, EVERY correctly-keyed atom in a team graph has
///   arm 1's self-referential 2-segment shape (`blob_key_for(None, ...)`
///   IS `{graph}/{atom}` for a directly-ingested atom too, not only a
///   promoted copy) — the `promoted_from_graph_id IS NOT NULL` join is the
///   only thing narrowing this down there.
/// * That join itself is not airtight: node upsert's `(graph_id, type,
///   lower(title))` adoption means a directly-ingested atom can end up
///   linked to a node that ALSO carries `promoted_from_graph_id` (from an
///   unrelated promotion that happens to share a title), which would put
///   its atom in this result set too.
/// * Arm 2 selects a blank key whatever blanked it: an atom whose key was
///   erased for some reason OTHER than the org-scope rejection above (a
///   future terminal-unfetchable sweep over genuinely-lost content, say)
///   matches too, if it carries promotion provenance.
///
/// Because of all three, `process_candidate` NEVER overwrites a target key
/// blindly — it GETs the target key first and only PUTs on a genuine
/// `NotFound` (see that function's own doc comment). A live object at the
/// target key, however this row got selected, is left completely alone; and
/// a candidate with no resolvable source is reported and left untouched
/// (`source_not_found`), never guessed at.
///
/// No `content_inline IS NULL` filter (deliberately, unlike an earlier
/// revision of this query, and unlike what a prod-dry-run instinct might
/// suggest adding after the first real run — DON'T): `Store::
/// copy_atoms_to_graph_core`'s own R2-first re-upload gate dropped that
/// same condition (see its doc comment — `workers::embed`, sync-apply, and
/// `services::corrections` can all produce a row carrying both
/// `content_inline` and a live `content_blob_key`), so this backfill's
/// candidate set must not exclude that shape either — a candidate WITH
/// `content_inline` and a REAL, fetchable source object still gets its key
/// corrected and its object re-uploaded, exactly like one without. Only
/// when the SOURCE's own object turns out to be genuinely missing (a real
/// prod shape — pre-cutover rows) does `content_inline` change the
/// outcome, and that split happens at GET time in [`process_candidate`],
/// never here: `has_inline` is carried through as plain data for that
/// later decision, not used to filter candidates out.
///
/// **Why an explicit `exclude` list, not OFFSET pagination.** Neither arm
/// can rely on a repaired row dropping out of its own `WHERE` clause:
/// * Arm 1, CONSUMER plane: `blob_key_for(None, graph_id, atom_id)`
///   produces the SAME text as the pre-fix placeholder (the key-SHAPE half
///   of the bug is org-plane-only — see arm 1 above), so fixing a
///   consumer-plane row uploads the missing object but does NOT change its
///   key's TEXT, and a re-run of this exact query would select it again
///   forever.
/// * Arm 2, CONSUMER plane: a repaired blank key becomes `{graph}/{atom}`
///   — which is arm 1's shape, so the row moves from arm 2 to arm 1 rather
///   than leaving the result set. (On the ORG plane a repaired row gets a
///   3-segment key and does drop out of both arms — but that is a fact
///   about which plane it is on, not something the loop may depend on.)
/// * EITHER arm, EITHER plane, on the FAILURE path: a candidate this run
///   could NOT repair — no resolvable source, a blank source key, an
///   ambiguous source, a transient error — keeps the exact key text it came
///   in with, and therefore keeps matching the arm that selected it.
///
/// `list_candidates`' caller instead accumulates every `atom_id` it has
/// already ATTEMPTED (success or failure — a permanently-broken row must
/// not be retried in an infinite loop within one run either) and excludes
/// them here, guaranteeing forward progress: each call either returns at
/// least one never-before-seen atom_id or an empty page.
pub const CANDIDATES_SQL: &str = "
    SELECT a.atom_id, a.graph_id, g.account_id, a.source_type, a.source_id,
           a.content_blob_key,
           array_agg(DISTINCT n.promoted_from_graph_id) AS candidate_source_graphs,
           (a.content_inline IS NOT NULL) AS has_inline
    FROM atoms a
    JOIN node_atoms na ON na.atom_id = a.atom_id
    JOIN nodes n ON n.node_id = na.node_id AND n.promoted_from_graph_id IS NOT NULL
    JOIN graphs g ON g.id = a.graph_id
    WHERE g.kind IN ('team', 'org')
      AND (
            -- Arm 1: the pre-fix placeholder key, still intact.
            (    split_part(a.content_blob_key, '/', 1) = a.graph_id::text
             AND split_part(a.content_blob_key, '/', 2) = a.atom_id::text
             AND a.content_blob_key !~ '^[^/]+/[^/]+/')
            -- Arm 2: the key was blanked by another worker; the promotion
            -- provenance joined above identifies the row instead.
            OR COALESCE(a.content_blob_key, '') = ''
          )
      AND ($1::uuid IS NULL OR a.graph_id = $1)
      AND NOT (a.atom_id = ANY($2::uuid[]))
    GROUP BY a.atom_id, a.graph_id, g.account_id, a.source_type, a.source_id, a.content_blob_key,
             a.content_inline
    ORDER BY a.atom_id
    LIMIT $3
";

/// Fetch one page of candidates. `graph_filter` narrows to one target ocean
/// (a rehearsal/diagnosis aid, mirroring `self_reef_backfill`'s
/// `--graph-id`); `exclude` is every `atom_id` the caller has already
/// attempted THIS run (see [`CANDIDATES_SQL`]'s doc comment for why this,
/// not OFFSET, is what makes the caller's loop terminate).
pub async fn list_candidates(
    pool: &PgPool,
    graph_filter: Option<Uuid>,
    exclude: &[Uuid],
) -> Result<Vec<Candidate>, sqlx::Error> {
    sqlx::query_as(CANDIDATES_SQL)
        .bind(graph_filter)
        .bind(exclude)
        .bind(BATCH_SIZE)
        .fetch_all(pool)
        .await
}

/// `true` iff `e` is Postgres' `undefined_table` (42P01) — used by
/// [`resolve_org_id`] to tell "this database has no `tenant_meta` table at
/// all" (consumer/control plane) apart from a genuine connection/permission
/// failure, which must still surface as an error.
fn is_undefined_table(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|d| d.code())
        .map(|c| c == "42P01")
        .unwrap_or(false)
}

/// Resolve this database's org id the SAME way the app itself does — see
/// this module's doc comment. `override_org_id`, when given, is a pure
/// sanity check: it must agree with what the database itself says, or this
/// returns `Err` rather than silently trusting the CLI over the
/// fingerprint.
pub async fn resolve_org_id(
    pool: &PgPool,
    override_org_id: Option<&str>,
) -> Result<Option<String>, String> {
    let db_org_id: Option<String> =
        match sqlx::query_scalar("SELECT org_id FROM tenant_meta").fetch_optional(pool).await {
            Ok(v) => v,
            Err(e) if is_undefined_table(&e) => None,
            Err(e) => return Err(format!("failed to read tenant_meta.org_id: {e}")),
        };

    match (db_org_id, override_org_id) {
        (Some(db), Some(cli)) if db == cli => Ok(Some(db)),
        (Some(db), Some(cli)) => Err(format!(
            "--org-id {cli} does not match this database's tenant_meta.org_id {db} — refusing \
             to construct blob keys under the wrong org prefix"
        )),
        (Some(db), None) => Ok(Some(db)),
        (None, Some(cli)) => Err(format!(
            "--org-id {cli} given but this database has no tenant_meta row (looks like the \
             consumer/control plane) — refusing an org-id override with no fingerprint to check \
             it against"
        )),
        (None, None) => Ok(None),
    }
}

/// Running counts across a backfill invocation, printed once at the end —
/// same shape/purpose as `self_reef_backfill::Totals`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Totals {
    pub candidates: u64,
    pub source_not_found: u64,
    /// More than one `candidate_source_graphs` entry has a matching
    /// `(source_type, source_id)` atom — never guessed, always skipped
    /// (see [`find_source_atom`]'s doc comment). Counted in `errors` too
    /// (an ambiguous source is worth a human's attention, same as any
    /// other refusal-to-guess), but never in `put_failed`.
    pub ambiguous_source: u64,
    /// A REAL write — the target PUT and the row UPDATE both succeeded.
    /// Only ever incremented on `--apply`; a dry run's equivalent outcome
    /// is [`OutcomeKind::WouldRepair`] / `would_repair` instead, kept
    /// deliberately separate (see [`Totals::apply`]'s own doc comment).
    pub put_ok: u64,
    /// Dry-run-only twin of `put_ok`: every check passed and this
    /// candidate WOULD have been repaired had `--apply` been given.
    pub would_repair: u64,
    /// A GET at the CORRECT target key found an object already there —
    /// this candidate was left completely untouched (see
    /// [`process_candidate`]'s doc comment for why this check exists: the
    /// candidate query is a hint, never proof, that the target is actually
    /// missing content).
    pub skipped_existing: u64,
    /// The source's own blob is missing from R2 (a genuine `NotFound`, not
    /// a transient error) AND this candidate carries `content_inline` —
    /// already readable as-is, nothing this run can or needs to repair.
    /// See [`process_candidate`]'s doc comment (step 2) for the prod shape
    /// this counts.
    pub skipped_inline_only: u64,
    /// The source's own blob is missing from R2 AND this candidate has NO
    /// `content_inline` fallback — content is genuinely, permanently lost
    /// unless recovered from a backup. Printed loudly (the atom id) by
    /// [`process_candidate`]'s report line; never `errors` or
    /// `put_failed` (re-running this binary cannot fix it, and it must
    /// never block `--apply` from repairing every other row).
    pub unrepairable_source_missing: u64,
    /// A failure IN THE WRITE PATH ITSELF — the target-key PUT or the row
    /// UPDATE, after everything upstream (source resolution, source GET,
    /// existence check) already succeeded. NEVER set for a source-side
    /// outcome (`skipped_inline_only`/`unrepairable_source_missing`/
    /// `source_not_found` all have their own counters) — see this
    /// module's "Exit code" doc section for why that separation matters.
    pub put_failed: u64,
    /// The source atom itself has NO `content_blob_key` at all (its
    /// content lives ONLY inline) — nothing was ever in R2 to re-upload.
    /// Distinct from `skipped_inline_only`: that one means the source DID
    /// have a `content_blob_key` but the OBJECT is missing; this one means
    /// there was never a key to begin with.
    pub skipped: u64,
    /// The resolved SOURCE atom's `content_blob_key` is BLANK (`''` — the
    /// value `workers::atom_title_backfill`'s terminal-unfetchable write
    /// leaves behind; see [`CANDIDATES_SQL`]'s arm 2). Never `errors` and
    /// never `put_failed`: like `unrepairable_source_missing` this is a
    /// permanent condition a re-run cannot change, so it must not flip the
    /// exit code or block `--apply` from repairing every other row. Printed
    /// loudly (the atom id) all the same — it is the one shape that means a
    /// blanking sweep reached the SOURCE side too, which is worth a human's
    /// attention.
    pub source_key_blank: u64,
    pub jobs_reenqueued: u64,
    pub dead_jobs_seen: u64,
    /// Transient transport/DB failures that survived a retry (see
    /// [`RETRY_BACKOFF`]), PLUS `ambiguous_source` (a refusal to guess,
    /// not a transient condition, but equally worth a human's attention
    /// and equally worth re-running for — an ambiguous source today might
    /// resolve cleanly after an unrelated data fix). The ONLY counter
    /// (together with `put_failed`) that drives a nonzero exit code — see
    /// this module's "Exit code" doc section.
    pub errors: u64,
}

impl Totals {
    /// `true` iff this run should exit `0` — see the module doc's "Exit
    /// code" section for why every OTHER counter is deliberately excluded
    /// from this check.
    pub fn is_clean(&self) -> bool {
        self.errors == 0 && self.put_failed == 0
    }

    /// Fold one candidate's outcome into these running totals. The single
    /// place `candidates`/`dead_jobs_seen`/`jobs_reenqueued` and exactly
    /// one per-kind counter are incremented — called AFTER a batch's
    /// concurrent futures have all completed (see the module's
    /// "Concurrency" doc section), never from inside [`process_candidate`]
    /// itself, which is why that function takes no `&mut Totals`.
    pub fn apply(&mut self, outcome: &CandidateOutcome) {
        self.candidates += 1;
        self.dead_jobs_seen += outcome.dead_jobs_seen;
        self.jobs_reenqueued += outcome.jobs_reenqueued;
        match outcome.kind {
            OutcomeKind::PutOk => self.put_ok += 1,
            OutcomeKind::WouldRepair => self.would_repair += 1,
            OutcomeKind::SkippedExisting => self.skipped_existing += 1,
            OutcomeKind::SkippedInlineOnly => self.skipped_inline_only += 1,
            OutcomeKind::UnrepairableSourceMissing => self.unrepairable_source_missing += 1,
            OutcomeKind::SourceNotFound => self.source_not_found += 1,
            OutcomeKind::AmbiguousSource => {
                self.ambiguous_source += 1;
                self.errors += 1;
            }
            OutcomeKind::SkippedSourceInlineOnly => self.skipped += 1,
            OutcomeKind::SourceKeyBlank => self.source_key_blank += 1,
            OutcomeKind::PutFailed => self.put_failed += 1,
            OutcomeKind::Error => self.errors += 1,
        }
    }

    /// An unambiguous final summary line — the exit-code rule spelled out
    /// in words, plus an explicit call to action when there is one. Always
    /// printed alongside [`Totals`]'s own `Display` (the raw counters).
    pub fn summary_line(&self) -> String {
        let mut parts = Vec::new();
        if self.is_clean() {
            parts.push("clean run (errors=0, put_failed=0) — exit 0".to_string());
        } else {
            parts.push(format!(
                "NOT clean — {} error(s) + {} put_failed(s) — exit 1",
                self.errors, self.put_failed
            ));
        }
        if self.errors > 0 {
            parts.push(format!(
                "{} row(s) errored (transient failures that survived a retry, or an ambiguous \
                 source); re-run to retry them",
                self.errors
            ));
        }
        if self.unrepairable_source_missing > 0 {
            parts.push(format!(
                "{} row(s) UNREPAIRABLE (source content permanently missing from R2) — see the \
                 atom ids printed above; re-running this binary cannot fix them",
                self.unrepairable_source_missing
            ));
        }
        if self.source_key_blank > 0 {
            parts.push(format!(
                "{} row(s) UNREPAIRABLE (the SOURCE atom's own content_blob_key is BLANK, so \
                 there is nothing left to re-upload from) — see the atom ids printed above; \
                 re-running this binary cannot fix them",
                self.source_key_blank
            ));
        }
        parts.join(". ")
    }
}

impl std::fmt::Display for Totals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "candidates={} put_ok={} would_repair={} skipped_existing={} skipped_inline_only={} \
             unrepairable_source_missing={} source_not_found={} ambiguous_source={} \
             put_failed={} skipped={} source_key_blank={} jobs_reenqueued={} dead_jobs_seen={} \
             errors={}",
            self.candidates,
            self.put_ok,
            self.would_repair,
            self.skipped_existing,
            self.skipped_inline_only,
            self.unrepairable_source_missing,
            self.source_not_found,
            self.ambiguous_source,
            self.put_failed,
            self.skipped,
            self.source_key_blank,
            self.jobs_reenqueued,
            self.dead_jobs_seen,
            self.errors
        )
    }
}

/// [`process_candidate`]'s classification of what happened to one
/// candidate — see that function's own doc comment for the full decision
/// tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeKind {
    PutOk,
    /// Dry-run-only: every check up to (and including) the target-key
    /// existence check passed, so this candidate WOULD be repaired on
    /// `--apply` — but nothing was actually written. Deliberately NOT
    /// folded into `Totals::put_ok` (see [`Totals::apply`]'s own doc
    /// comment for why that distinction matters).
    WouldRepair,
    SkippedExisting,
    SkippedInlineOnly,
    UnrepairableSourceMissing,
    SourceNotFound,
    AmbiguousSource,
    SkippedSourceInlineOnly,
    /// The resolved SOURCE atom's own `content_blob_key` is blank — the
    /// mirror image of the blank TARGET key [`CANDIDATES_SQL`]'s arm 2
    /// admits, and the one case that is NOT repairable: a blank target is
    /// re-derivable from the source, a blank source leaves nothing to read
    /// the original content from. See [`process_candidate`]'s doc comment.
    SourceKeyBlank,
    PutFailed,
    Error,
}

/// [`process_candidate`]'s return value — a classification plus a
/// human-readable report line, carrying no `&mut` state (see the module's
/// "Concurrency" doc section for why that is what makes running many of
/// these at once safe).
pub struct CandidateOutcome {
    pub atom_id: Uuid,
    pub kind: OutcomeKind,
    pub dead_jobs_seen: u64,
    pub jobs_reenqueued: u64,
    pub report: String,
}

/// [`find_source_atom`]'s outcome.
enum SourceResolution {
    Found { atom_id: Uuid, content_blob_key: Option<String> },
    NotFound,
    /// More than one of `candidate_source_graphs` has a matching
    /// `(source_type, source_id)` atom — carries every graph id that
    /// matched, for the report line.
    Ambiguous(Vec<Uuid>),
}

/// Resolve a candidate's TRUE source atom across every graph
/// `candidate_source_graphs` names, rather than pinning to an arbitrary
/// one. A commons atom (deduped by `(graph_id, source_type, source_id)`
/// across every promotion that ever copied it) can be linked from reefs
/// promoted out of DIFFERENT member oceans, each with its own
/// `promoted_from_graph_id` — picking any single one and hoping it is the
/// real source would silently re-upload the WRONG content under this
/// atom's key if it guessed wrong. Instead: check every candidate graph
/// for a matching `(source_type, source_id)` row; exactly one match is a
/// real resolution, zero is [`SourceResolution::NotFound`], and more than
/// one is [`SourceResolution::Ambiguous`] — reported and skipped, never
/// guessed.
///
/// Every candidate graph is also required to share `cand.account_id` — a
/// `promoted_from_graph_id` can point at historical cross-account
/// promotion data (pre-dating whatever guard now prevents that), and
/// without this join a match in another account's graph would hand back
/// that account's `content_blob_key`. `process_candidate` would then fetch
/// it wrapped under THIS candidate's account's DEK, decrypt garbage, and
/// report an opaque `R2 get ... failed` instead of the true cause. A
/// same-account mismatch is indistinguishable from "no source exists" —
/// [`SourceResolution::NotFound`] — never a distinct error, since there is
/// nothing an operator can do about another account's row from here.
async fn find_source_atom(pool: &PgPool, cand: &Candidate) -> Result<SourceResolution, sqlx::Error> {
    let mut matches: Vec<(Uuid, Uuid, Option<String>)> = Vec::new();
    for &source_graph_id in &cand.candidate_source_graphs {
        let row: Option<(Uuid, Option<String>)> = sqlx::query_as(
            "SELECT a.atom_id, a.content_blob_key FROM atoms a \
             JOIN graphs g ON g.id = a.graph_id \
             WHERE a.graph_id = $1 AND a.source_type = $2 AND a.source_id = $3 \
               AND g.account_id = $4",
        )
        .bind(source_graph_id)
        .bind(&cand.source_type)
        .bind(&cand.source_id)
        .bind(cand.account_id)
        .fetch_optional(pool)
        .await?;
        if let Some((atom_id, key)) = row {
            matches.push((source_graph_id, atom_id, key));
        }
    }
    match matches.len() {
        0 => Ok(SourceResolution::NotFound),
        1 => {
            let (_, atom_id, content_blob_key) = matches.into_iter().next().expect("len == 1");
            Ok(SourceResolution::Found { atom_id, content_blob_key })
        }
        _ => Ok(SourceResolution::Ambiguous(matches.into_iter().map(|(g, _, _)| g).collect())),
    }
}

/// Every `(node_id, graph_id)` this candidate atom is linked to — what
/// `RegenerateDigest` is keyed on (step 5 of this module's doc comment).
async fn linked_nodes(pool: &PgPool, atom_id: Uuid) -> Result<Vec<(Uuid, Uuid)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT n.node_id, n.graph_id FROM node_atoms na \
         JOIN nodes n ON n.node_id = na.node_id \
         WHERE na.atom_id = $1",
    )
    .bind(atom_id)
    .fetch_all(pool)
    .await
}

/// How many `dead` derivation jobs already exist for this atom — purely
/// informational (see this module's doc comment on why old rows are left
/// alone rather than resurrected).
async fn dead_job_count(pool: &PgPool, atom_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs \
         WHERE status = 'dead' AND kind IN ('ExtractEntities', 'ExtractRelationships') \
           AND idempotency_key IN ($1, $2)",
    )
    .bind(format!("extract:{atom_id}"))
    .bind(format!("relextract:{atom_id}"))
    .fetch_one(pool)
    .await
    .unwrap_or(0)
}

/// A GET's outcome, timeout- and retry-aware: [`BlobGetOutcome::NotFound`]
/// is a distinct, semantic result (never retried as if transient — see
/// [`get_blob_retrying`]'s own doc comment), separate from
/// [`BlobGetOutcome::Failed`] (a real error or timeout, which WAS already
/// retried once before landing here).
enum BlobGetOutcome {
    Found(Vec<u8>),
    NotFound,
    Failed(String),
}

/// GET `key`, decrypting with `dek`, with [`BLOB_IO_TIMEOUT`] per attempt
/// and ONE retry (see [`RETRY_BACKOFF`]'s doc comment) — but ONLY for a
/// genuine error/timeout, never for [`coralswarm_core::r2::R2Error::
/// NotFound`], which both of this function's two call sites in
/// [`process_candidate`] treat as a real, expected, non-transient signal
/// (the source object genuinely doesn't exist; the target key is genuinely
/// free to write to) — retrying it would just waste a network round trip
/// finding the same true answer twice.
async fn get_blob_retrying(
    r2: &dyn crate::services::blob_store::PromotionBlobStore,
    key: &str,
    dek: &[u8; 32],
) -> BlobGetOutcome {
    for attempt in 0..2 {
        match tokio::time::timeout(BLOB_IO_TIMEOUT, r2.get_blob(key, dek)).await {
            Ok(Ok(bytes)) => return BlobGetOutcome::Found(bytes),
            Ok(Err(coralswarm_core::r2::R2Error::NotFound(_))) => return BlobGetOutcome::NotFound,
            Ok(Err(e)) if attempt == 0 => {
                tokio::time::sleep(RETRY_BACKOFF).await;
                continue;
            }
            Ok(Err(e)) => return BlobGetOutcome::Failed(e.to_string()),
            Err(_) if attempt == 0 => {
                tokio::time::sleep(RETRY_BACKOFF).await;
                continue;
            }
            Err(_) => {
                return BlobGetOutcome::Failed(format!("timed out after {BLOB_IO_TIMEOUT:?}"))
            }
        }
    }
    unreachable!("loop always returns within 2 attempts")
}

/// Await `$fut` (a fresh expression, re-evaluated on retry — NOT a
/// pre-built `Future` value); on `Err`, sleep [`RETRY_BACKOFF`] and try
/// ONE more time. See [`RETRY_BACKOFF`]'s own doc comment for why one
/// retry, not a loop.
macro_rules! retry_once {
    ($fut:expr) => {{
        match $fut.await {
            Ok(v) => Ok(v),
            Err(_first_err) => {
                tokio::time::sleep(RETRY_BACKOFF).await;
                $fut.await
            }
        }
    }};
}

/// `true` iff a stored `content_blob_key` carries no usable key at all.
/// `atoms.content_blob_key` is `NOT NULL` on both planes, so `''` is the
/// "no usable blob" value (`workers::atom_title_backfill`'s
/// `TERMINAL_UNFETCHABLE_SQL` is what writes it — see [`CANDIDATES_SQL`]'s
/// arm 2). Deliberately `trim()`-tolerant, which makes it very slightly
/// WIDER than arm 2's own `= ''` predicate: this is only ever used to
/// classify a value already in hand (a resolved SOURCE key, or a key being
/// printed), never to select rows, and an all-whitespace key is just as
/// unreadable as an empty one.
fn is_blank(key: &str) -> bool {
    key.trim().is_empty()
}

/// How a `content_blob_key` reads in a report line: itself, or an explicit
/// `<blank>` marker. An empty string interpolated raw would print as a
/// confusing gap (`... — -> org/graph/atom`) in the one line an operator
/// reads to confirm what this run actually did to an arm-2 row.
fn key_for_report(key: &str) -> &str {
    if is_blank(key) {
        "<blank>"
    } else {
        key
    }
}

/// Build a [`CandidateOutcome`] — the one place every `process_candidate`
/// return path constructs its result, so `dead_jobs_seen`/`jobs_reenqueued`
/// can never be forgotten on one arm and not another.
fn outcome(
    atom_id: Uuid,
    kind: OutcomeKind,
    dead_jobs_seen: u64,
    jobs_reenqueued: u64,
    report: String,
) -> CandidateOutcome {
    CandidateOutcome { atom_id, kind, dead_jobs_seen, jobs_reenqueued, report }
}

/// Process one candidate: resolve its source, re-upload+correct (when
/// `execute`), and re-enqueue derivation. Returns a [`CandidateOutcome`]
/// (classification + report line) rather than mutating shared state — see
/// the module's "Concurrency" doc section for why: this function is safe
/// to run many-at-once via `buffer_unordered`, with the caller folding
/// every outcome into one [`Totals`] AFTER a batch's futures all complete.
/// Never panics — every failure is classified and reported, never fatal to
/// the batch (matches `self_reef_backfill::process_graph`'s per-row error
/// containment).
///
/// **Never overwrites a live object.** [`CANDIDATES_SQL`]'s own doc comment
/// explains why candidate selection is a HINT, not proof, that the target
/// key is actually missing content (consumer-plane key-shape ambiguity;
/// node-adoption false positives on the `promoted_from_graph_id` join).
/// It snapshots the target before source I/O and reads its stored pointer.
/// Repair uploads to a fresh key and uses the shared full-snapshot CAS owner.
/// Existing target bytes and a concurrent target edit are preserved.
///
/// **Source-missing classification** (step 2 of the module doc — the real
/// prod shape the 2026-09-09 dry run found): a `NotFound` on the SOURCE
/// GET is not the same failure as a `NotFound` anywhere else in this
/// function. It splits on `cand.has_inline` into
/// [`OutcomeKind::SkippedInlineOnly`] (readable as-is, nothing to repair)
/// or [`OutcomeKind::UnrepairableSourceMissing`] (content permanently
/// lost) — NEVER [`OutcomeKind::PutFailed`], which is reserved for a
/// failure in the actual write path (the target PUT or the row UPDATE).
///
/// **A blank key means opposite things on the two sides, and this function
/// is where that asymmetry lives.**
/// * A blank TARGET key is repairable. The fresh key uses the verified
///   organization and target graph, while CAS still compares the blank key.
/// * A blank SOURCE key is NOT repairable — there is nothing left to read
///   the original content from — and is classified
///   [`OutcomeKind::SourceKeyBlank`] BEFORE the source GET, because
///   `R2Client::get_atom_blob("")` fails `validate_blob_key` with
///   `InvalidKey` rather than `NotFound`: left to reach the GET it would
///   burn a retry and count as `Totals::errors`, i.e. demand a re-run for a
///   condition no re-run can change.
///
/// **Dry run vs apply.** The existence check above (and the source GET
/// before it) run REGARDLESS of `execute` — a dry run proves R2
/// reachability, the source key's validity, and the DEK all actually work,
/// not merely that the candidate query matched a row. Only the PUT, the row
/// UPDATE, and the derivation re-enqueue are skipped when `execute` is
/// `false`.
pub async fn process_candidate(
    pool: &PgPool,
    r2: &dyn crate::services::blob_store::PromotionBlobStore,
    org_id: Option<&str>,
    tenant: &TenantId,
    dek_cache: &tokio::sync::Mutex<std::collections::HashMap<Uuid, [u8; 32]>>,
    cand: &Candidate,
    execute: bool,
) -> CandidateOutcome {
    if !matches!((tenant, org_id), (TenantId::Consumer, None))
        && !matches!((tenant, org_id), (TenantId::Org(expected), Some(actual)) if expected == actual)
    {
        return outcome(cand.atom_id, OutcomeKind::Error, 0, 0,
            format!("atom {}: organization does not match verified tenant", cand.atom_id));
    }
    let dead = dead_job_count(pool, cand.atom_id).await as u64;
    let target: Result<
        Option<(
            i64,
            String,
            Option<String>,
            Option<i32>,
            chrono::DateTime<chrono::Utc>,
        )>,
        _,
    > = sqlx::query_as(
        "SELECT a.content_revision,a.content_blob_key,a.content_inline,a.header_len,a.timestamp \
             FROM atoms a JOIN graphs g ON g.id=a.graph_id \
             WHERE a.atom_id=$1 AND a.graph_id=$2 AND g.account_id=$3 \
               AND a.source_type=$4 AND a.source_id=$5 AND a.content_blob_key=$6",
    )
    .bind(cand.atom_id)
    .bind(cand.graph_id)
    .bind(cand.account_id)
    .bind(&cand.source_type)
    .bind(&cand.source_id)
    .bind(&cand.content_blob_key)
    .fetch_optional(pool)
    .await;
    let snapshot = match target {
        Ok(Some((revision, blob_key, inline, header_len, timestamp))) => {
            crate::services::content_replacement::ContentSnapshot {
                atom_id: cand.atom_id,
                graph_id: cand.graph_id,
                revision,
                blob_key,
                inline,
                header_len,
                timestamp,
            }
        }
        Ok(None) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::SkippedExisting,
                dead,
                0,
                format!(
                    "atom {}: target changed since selection; left untouched",
                    cand.atom_id
                ),
            )
        }
        Err(error) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::Error,
                dead,
                0,
                format!(
                    "atom {}: target snapshot unavailable: {error}",
                    cand.atom_id
                ),
            )
        }
    };

    let source = retry_once!(find_source_atom(pool, cand));
    let source = match source {
        Ok(SourceResolution::Found { atom_id, content_blob_key }) => (atom_id, content_blob_key),
        Ok(SourceResolution::NotFound) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::SourceNotFound,
                dead,
                0,
                format!(
                    "atom {} (graph {}): SOURCE NOT FOUND in any of {:?} for ({}, {}) — skipped",
                    cand.atom_id,
                    cand.graph_id,
                    cand.candidate_source_graphs,
                    cand.source_type,
                    cand.source_id
                ),
            );
        }
        Ok(SourceResolution::Ambiguous(graphs)) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::AmbiguousSource,
                dead,
                0,
                format!(
                    "atom {} (graph {}): AMBIGUOUS SOURCE — {} candidate graphs {:?} each have a \
                     matching ({}, {}) atom; refusing to guess — skipped",
                    cand.atom_id, cand.graph_id, graphs.len(), graphs, cand.source_type, cand.source_id
                ),
            );
        }
        Err(e) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::Error,
                dead,
                0,
                format!("atom {}: source lookup failed (after 1 retry): {e}", cand.atom_id),
            );
        }
    };
    let (source_atom_id, source_blob_key) = source;
    // A BLANK source key — the mirror image of the blank TARGET key
    // `CANDIDATES_SQL`'s arm 2 admits, and the one direction that is NOT
    // repairable: this backfill rebuilds a target key from `blob_key_for`,
    // but it can only rebuild target CONTENT by reading the source's own
    // key, and there is none left to read. Classified HERE, before the GET,
    // for a concrete reason: `R2Client::get_atom_blob("")` fails
    // `validate_blob_key` with `InvalidKey`, NOT `NotFound`, so without this
    // branch it would burn a pointless retry and land in `Totals::errors` —
    // a nonzero exit code demanding a re-run, for a permanent condition no
    // re-run can change.
    let source_had_blank_key = matches!(source_blob_key.as_deref(), Some(k) if is_blank(k));
    let source_blob_key = source_blob_key.filter(|k| !is_blank(k));
    let Some(source_blob_key) = source_blob_key else {
        // The source itself has NO usable content_blob_key — nothing is in
        // R2 to re-upload. Distinct from the has_inline split below, which
        // is about the source HAVING a key that turns out to point at
        // nothing. Two sub-cases, deliberately counted apart: a NULL key
        // (the source's content lives only inline, and always did) versus a
        // BLANK one (`''` — a key that was ERASED, see this function's own
        // doc comment and `CANDIDATES_SQL`'s arm 2).
        let (kind, why) = if source_had_blank_key {
            (
                OutcomeKind::SourceKeyBlank,
                "its content_blob_key is BLANK (''), i.e. erased — nothing left to re-upload \
                 from; content is unrecoverable from here",
            )
        } else {
            (OutcomeKind::SkippedSourceInlineOnly, "it has no R2 content at all (inline-only)")
        };
        return outcome(
            cand.atom_id,
            kind,
            dead,
            0,
            format!(
                "atom {} (graph {}): source atom {source_atom_id} {why} — skipped",
                cand.atom_id, cand.graph_id
            ),
        );
    };

    let dek = {
        let cached = dek_cache.lock().await.get(&cand.account_id).copied();
        match cached {
            Some(d) => d,
            None => {
                let resolved = retry_once!(resolve_dek_for_tenant(tenant, pool, cand.account_id));
                match resolved {
                    Ok(Some(d)) => {
                        dek_cache.lock().await.insert(cand.account_id, d);
                        d
                    }
                    Ok(None) => {
                        return outcome(
                            cand.atom_id,
                            OutcomeKind::Error,
                            dead,
                            0,
                            format!("atom {}: account {} has no DEK", cand.atom_id, cand.account_id),
                        );
                    }
                    Err(e) => {
                        return outcome(
                            cand.atom_id,
                            OutcomeKind::Error,
                            dead,
                            0,
                            format!(
                                "atom {}: DEK resolution failed (after 1 retry): {e}",
                                cand.atom_id
                            ),
                        );
                    }
                }
            }
        }
    };

    // The source GET's NotFound is a semantic result (see this function's
    // own doc comment), never retried as if transient; any OTHER failure
    // (timeout, a genuine R2/network error) IS retried once.
    let plaintext = match get_blob_retrying(r2, &source_blob_key, &dek).await {
        BlobGetOutcome::Found(bytes) => bytes,
        BlobGetOutcome::NotFound => {
            return if snapshot.inline.is_some() {
                outcome(
                    cand.atom_id,
                    OutcomeKind::SkippedInlineOnly,
                    dead,
                    0,
                    format!(
                        "atom {} (graph {}): source blob {source_blob_key} missing in R2, but this \
                         atom has content_inline — readable as-is, nothing to repair \
                         (skipped_inline_only)",
                        cand.atom_id, cand.graph_id
                    ),
                )
            } else {
                outcome(
                    cand.atom_id,
                    OutcomeKind::UnrepairableSourceMissing,
                    dead,
                    0,
                    format!(
                        "atom {} (graph {}): UNREPAIRABLE — source blob {source_blob_key} missing \
                         in R2 and this atom has NO content_inline fallback; content is \
                         permanently lost unless recovered from a backup \
                         (unrepairable_source_missing)",
                        cand.atom_id, cand.graph_id
                    ),
                )
            };
        }
        BlobGetOutcome::Failed(msg) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::Error,
                dead,
                0,
                format!("atom {}: R2 get {source_blob_key} failed (after 1 retry): {msg}", cand.atom_id),
            );
        }
    };

    let key_prefix = match coralswarm_core::r2::blob_key_for(org_id, cand.graph_id, cand.atom_id) {
        Ok(key) => key.rsplit_once('/').unwrap().0.to_owned(),
        Err(e) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::Error,
                dead,
                0,
                format!("atom {}: blob key construction failed: {e}", cand.atom_id),
            );
        }
    };

    let target_blob = if is_blank(&snapshot.blob_key) {
        BlobGetOutcome::NotFound
    } else {
        get_blob_retrying(r2, &snapshot.blob_key, &dek).await
    };
    match target_blob {
        BlobGetOutcome::Found(_) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::SkippedExisting,
                dead,
                0,
                format!(
                    "atom {} (graph {}): target key {} ALREADY has content — left untouched \
                     (skipped_existing)",
                    cand.atom_id, cand.graph_id, snapshot.blob_key
                ),
            );
        }
        BlobGetOutcome::NotFound => {}
        BlobGetOutcome::Failed(msg) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::Error,
                dead,
                0,
                format!(
                    "atom {}: existence check at {} failed (after 1 retry, not a clean \
                     NotFound): {msg} — refusing to guess whether it is safe to write",
                    cand.atom_id, snapshot.blob_key
                ),
            );
        }
    }

    if !execute {
        // NOT `OutcomeKind::PutOk` — nothing was actually written (see this
        // function's own "Dry run vs apply" doc section), and folding a
        // dry-run "would repair" into the SAME counter as a real write
        // would silently overcount `put_ok` on every dry run.
        return outcome(
            cand.atom_id,
            OutcomeKind::WouldRepair,
            dead,
            0,
            format!(
                "atom {} (graph {}): would re-upload from source atom {source_atom_id} \
                 ({source_blob_key}) under {key_prefix} with a fresh key and re-enqueue derivation [dry run — R2 \
                 reachability, source key, target-key existence check, and DEK all verified above]",
                cand.atom_id, cand.graph_id
            ),
        );
    }

    let content = match snapshot.inline.as_deref() {
        Some(inline) => inline,
        None => match std::str::from_utf8(&plaintext) {
            Ok(content) => content,
            Err(error) => {
                return outcome(
                    cand.atom_id,
                    OutcomeKind::Error,
                    dead,
                    0,
                    format!(
                        "atom {}: source content is not UTF-8: {error}",
                        cand.atom_id
                    ),
                )
            }
        },
    };
    use crate::services::content_replacement::{repair_missing_blob, ContentScope};
    let scope = match tenant {
        TenantId::Consumer => ContentScope::Account {
            pool,
            account_id: cand.account_id,
        },
        TenantId::Org(_) => ContentScope::Org {
            pool,
            account_id: cand.account_id,
        },
    };
    match repair_missing_blob(scope, &snapshot, content, org_id, r2, &dek).await {
        Ok(true) => {}
        Ok(false) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::SkippedExisting,
                dead,
                0,
                format!(
                    "atom {}: concurrent target edit won; left untouched",
                    cand.atom_id
                ),
            )
        }
        Err(error) => {
            return outcome(
                cand.atom_id,
                OutcomeKind::PutFailed,
                dead,
                0,
                format!(
                    "atom {}: fresh blob publication failed: {error}",
                    cand.atom_id
                ),
            )
        }
    }

    // Best-effort (a failure never unwinds the committed content fix).
    let mut jobs_reenqueued = 0u64;

    let mut digest_count = 0u64;
    match linked_nodes(pool, cand.atom_id).await {
        Ok(nodes) => {
            for (node_id, graph_id) in nodes {
                let kind = JobKind::RegenerateDigest { node_id, graph_id };
                match queue::enqueue(
                    pool,
                    &kind,
                    Some(cand.account_id),
                    Some(&format!("digest:{node_id}")),
                    0,
                )
                .await
                {
                    Ok(_) => digest_count += 1,
                    Err(e) => tracing::warn!(
                        atom_id = %cand.atom_id, %node_id, error = %e,
                        "promoted_blob_backfill: failed to re-enqueue RegenerateDigest"
                    ),
                }
            }
        }
        Err(e) => tracing::warn!(
            atom_id = %cand.atom_id, error = %e,
            "promoted_blob_backfill: failed to look up linked nodes for RegenerateDigest"
        ),
    }
    jobs_reenqueued += digest_count;

    outcome(
        cand.atom_id,
        OutcomeKind::PutOk,
        dead,
        jobs_reenqueued,
        format!(
            "atom {} (graph {}): re-uploaded from source atom {source_atom_id} — {} -> {} \
             ({dead} pre-existing dead job(s) for this atom; re-enqueued {} derivation + \
             {digest_count} digest job(s))",
            cand.atom_id,
            cand.graph_id,
            key_for_report(&cand.content_blob_key),
            "fresh blob key",
            jobs_reenqueued - digest_count,
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_display_includes_every_counter() {
        let t = Totals { candidates: 3, put_ok: 1, source_not_found: 1, put_failed: 1, ..Default::default() };
        let s = t.to_string();
        assert!(s.contains("candidates=3"));
        assert!(s.contains("put_ok=1"));
        assert!(s.contains("source_not_found=1"));
        assert!(s.contains("put_failed=1"));
    }

    #[test]
    fn is_undefined_table_matches_only_42p01() {
        // Constructed via a round trip through sqlx's own error decode path
        // would need a live connection; this pure-logic test instead pins
        // the non-database-error arm (`unwrap_or(false)`), which is what
        // protects a genuine connection failure from being silently
        // swallowed as "consumer plane, no tenant_meta".
        let e = sqlx::Error::PoolClosed;
        assert!(!is_undefined_table(&e));
    }

    fn sample_outcome(kind: OutcomeKind) -> CandidateOutcome {
        outcome(Uuid::new_v4(), kind, 0, 0, String::new())
    }

    #[test]
    fn skipped_and_unrepairable_never_flip_exit_code() {
        let mut t = Totals::default();
        t.apply(&sample_outcome(OutcomeKind::SkippedInlineOnly));
        t.apply(&sample_outcome(OutcomeKind::UnrepairableSourceMissing));
        t.apply(&sample_outcome(OutcomeKind::SkippedExisting));
        t.apply(&sample_outcome(OutcomeKind::SourceNotFound));
        t.apply(&sample_outcome(OutcomeKind::SkippedSourceInlineOnly));
        assert_eq!(t.skipped_inline_only, 1);
        assert_eq!(t.unrepairable_source_missing, 1);
        assert!(t.is_clean(), "none of these outcomes are failures: {t}");
    }

    #[test]
    fn put_failed_and_errors_are_the_only_things_that_flip_exit_code() {
        let mut t = Totals::default();
        t.apply(&sample_outcome(OutcomeKind::PutFailed));
        assert!(!t.is_clean());
        let mut t = Totals::default();
        t.apply(&sample_outcome(OutcomeKind::Error));
        assert!(!t.is_clean());
        let mut t = Totals::default();
        t.apply(&sample_outcome(OutcomeKind::AmbiguousSource));
        assert!(!t.is_clean(), "ambiguous source counts as errors, per its own field doc");
    }

    #[test]
    fn summary_line_names_unrepairable_atoms_and_retry_advice() {
        let mut t = Totals::default();
        t.apply(&sample_outcome(OutcomeKind::Error));
        t.apply(&sample_outcome(OutcomeKind::UnrepairableSourceMissing));
        let s = t.summary_line();
        assert!(s.contains("re-run to retry"), "{s}");
        assert!(s.contains("UNREPAIRABLE"), "{s}");
        assert!(s.contains("exit 1"), "{s}");
    }

    #[test]
    fn clean_run_summary_says_exit_0() {
        let mut t = Totals::default();
        t.apply(&sample_outcome(OutcomeKind::PutOk));
        t.apply(&sample_outcome(OutcomeKind::SkippedInlineOnly));
        assert!(t.summary_line().contains("exit 0"));
    }

    #[test]
    fn blank_keys_are_recognized_and_printed_readably() {
        assert!(is_blank(""), "'' is the NOT NULL column's 'no usable blob' value");
        assert!(is_blank("   "), "an all-whitespace key is just as unreadable as an empty one");
        let real = format!("{}/{}", Uuid::new_v4(), Uuid::new_v4());
        assert!(!is_blank(&real));
        assert_eq!(
            key_for_report(""),
            "<blank>",
            "a blank key must never print as a gap in the operator's report line"
        );
        assert_eq!(key_for_report(&real), real);
    }

    #[test]
    fn a_blank_source_key_is_reported_but_never_flips_the_exit_code() {
        let mut t = Totals::default();
        t.apply(&sample_outcome(OutcomeKind::SourceKeyBlank));
        assert_eq!(t.source_key_blank, 1);
        assert!(
            t.is_clean(),
            "a blank SOURCE key is permanent, not a retryable failure — re-running cannot fix \
             it, so it must not demand a re-run: {t}"
        );
        assert!(t.to_string().contains("source_key_blank=1"), "{t}");
        let summary = t.summary_line();
        assert!(summary.contains("BLANK"), "{summary}");
        assert!(summary.contains("exit 0"), "{summary}");
    }

    /// A no-database TRIPWIRE, not proof of behaviour: it only shows arm 2
    /// is still present in the query text. The real, behavioural proof that
    /// a blank-key row is SELECTED (and that provenance still narrows) is
    /// `backend/tests/promoted_blob_backfill_pg.rs`'s
    /// `blank_key_candidate_is_selected_and_repaired_end_to_end` /
    /// `blank_key_without_promoted_provenance_is_not_selected`, which need a
    /// live Postgres. This test exists because a plain `cargo test` (no DSN
    /// configured) runs it anyway, so deleting arm 2 cannot pass CI quietly.
    #[test]
    fn candidates_sql_still_carries_both_arms() {
        assert!(
            CANDIDATES_SQL.contains("split_part(a.content_blob_key, '/', 1) = a.graph_id::text"),
            "arm 1 (intact placeholder key) is gone from CANDIDATES_SQL"
        );
        assert!(
            CANDIDATES_SQL.contains("COALESCE(a.content_blob_key, '') = ''"),
            "arm 2 (erased key, selected by provenance) is gone from CANDIDATES_SQL — the 1,191 \
             blanked prod rows would be invisible again"
        );
        assert!(
            CANDIDATES_SQL.contains("n.promoted_from_graph_id IS NOT NULL"),
            "the provenance join is what makes arm 2 safe; it must never be dropped"
        );
    }
}

//! `migrate_personal_to_member_ocean` — one-shot, resumable orchestrator that
//! copies the CORALS (atoms, plus their small coding-session/project
//! metadata) of ONE ocean living on the shared CONTROL-PLANE (consumer)
//! database into ONE named member-private ocean inside ONE org's Neon
//! TENANT database.
//!
//! **Why this exists, and why it is narrower than a generic copy primitive.**
//! `scripts/migrate_shared_db_org.sh`'s header ("WHAT STEP 5 ACTUALLY
//! REQUIRES") is the documented spec for cross-plane content movement of this
//! shape: decrypt every atom with the SOURCE plane's DEK, re-encrypt under
//! the TARGET tenant's DEK, re-upload every blob under the org-namespaced R2
//! prefix, and copy every content row into the tenant tree — while dropping
//! and rebuilding the tenant's HNSW index around the bulk load and chunking
//! with a resumable cursor rather than one all-or-nothing transaction. That
//! script's own step 5 hard-refuses because the orchestrator was never
//! built; this binary is that orchestrator, narrowed to the ONE shape this
//! repo actually needs today — a single named consumer/personal ocean going
//! into a single named member ocean — rather than "every graph an account
//! owns" (the shared-DB-org case that script's header also describes).
//!
//! The header's own citation of `backend/src/services/promotion.rs` is
//! STALE — that module was deleted (see
//! `scripts/check_no_cross_plane_promotion.sh`'s header for why: it was
//! deleted specifically because a cross-database copy reachable from
//! request-serving code is a tenancy-boundary bug, however carefully
//! written). The primitives it names still exist, just relocated:
//!   * DEK resolution for both planes: [`coralswarm_backend::crypto::resolve_dek_for_tenant`]
//!     (`backend/src/crypto.rs:188`) — `TenantId::Consumer` for the source
//!     account's `accounts.dek_wrapped` (control plane), `TenantId::Org(_)`
//!     for the target account's `account_keys.dek_wrapped` (tenant plane).
//!   * Blob re-encrypt + org-namespaced re-upload:
//!     [`coralswarm_core::r2::R2Client::put_atom_blob`]
//!     (`packages/core/src/r2.rs`), which derives the key via
//!     [`coralswarm_core::r2::blob_key_for`]`(Some(org_id), graph_id, atom_id)`
//!     internally — this binary never reconstructs a key by hand.
//!   * The DSN-swap / fingerprint guard: the same `SELECT org_id FROM
//!     tenant_meta WHERE id = true` check `tenancy::registry::TenantPoolRegistry::connect_and_verify`
//!     uses in the request path and `bin/export_member_ocean.rs::connect_tenant`
//!     uses in this same family of concierge tools — reimplemented here
//!     (this binary cannot reuse either directly: the registry's version
//!     lives behind request-serving machinery this bin deliberately does not
//!     pull in, and `export_member_ocean`'s is a private fn in a different
//!     binary crate).
//!   * The `TENANT_DB_<SLUG>` secret-ref convention: same
//!     `tenancy::secrets::OperatorDsnResolver` as `migrate_tenants`/
//!     `export_member_ocean`.
//! This is deliberately a `backend/src/bin/` binary and holds a
//! consumer-plane pool handle directly — this is the one place in the repo
//! that is *supposed* to, and `check_no_cross_plane_promotion.sh` only scans
//! `backend/src/{routes,services,mcp}` for exactly that reason (see its own
//! header). No helper from this file is exported for use by anything under
//! those trees.
//!
//! # Refuses, before writing a single row (in this order)
//!   1. `--target-slug` has no `tenant_databases` routing row, or its
//!      `status` isn't `active`, or its `clerk_org_id` doesn't equal
//!      `--target-clerk-org` — the CLI's own belt-and-braces: naming both
//!      independently means a copy-pasted mismatch is caught here rather
//!      than silently routing by whichever one the code trusted.
//!   2. Connecting the resolved tenant DSN, the `tenant_meta` fingerprint
//!      doesn't say `--target-clerk-org` (missing row OR mismatch) — the
//!      DSN-swap guard from `tenancy::registry::connect_and_verify`.
//!   3. `--source-graph` names no `graphs` row on the control plane, or that
//!      row's `kind` isn't `'personal'` — this binary's name and purpose is
//!      "personal → member", not a generic ocean mover; a team/org source
//!      is refused rather than silently accepted.
//!   3b. The source graph has retained `source_versions`. This legacy
//!      atoms-only copier cannot preserve version history or semantic evidence;
//!      upgrade it before migrating that graph (also refused in dry-run mode).
//!   4. No LIVE (`archived_at IS NULL`) `kind='member' AND owner_user_id =
//!      --target-user` graph exists in the tenant DB (after applying
//!      `--target-graph` and preferring `is_primary = true` when it is not
//!      given) — refused with a message pointing at the real cause: the
//!      member ocean is provisioned lazily on the member's first sight by
//!      the org host (`tenancy::registry`'s `ensure_org_ocean` /
//!      first-request self-heal), so the operator must have that person
//!      authenticate against the org once before this can run.
//!   5. The source account's DEK is missing (`accounts.dek_wrapped IS NULL`
//!      on the control plane) OR the target account's DEK is missing
//!      (no `account_keys` row on the tenant plane). **Runs in `--dry-run`
//!      too** — same reasoning as `export_member_ocean`'s gate 5: a
//!      rehearsal that skips a refusal gate is a rehearsal of the wrong
//!      thing, and this check needs no KEK material, only a row's presence.
//!
//! # What is copied, and what is not
//! Copied (idempotently, see below): `atoms` (every column except the ones
//! listed below), `coding_sessions`, and `projects` for the source graph.
//! **Deliberately NOT copied**: `nodes`, `node_atoms`, `relationship_edge`,
//! `supersession_edge`, `reef_identifiers`, `evaluation_ledger`, or any other
//! derived/reef table. Nothing re-derives them from copied atoms: the
//! per-atom extractor (and its `reextract` bin) is retired, and managed
//! source extraction reads `source_versions`, which this binary does not copy.
//!
//! ## `atoms` column handling
//! Every column is carried **verbatim** from the source row except:
//!   * `atom_id` — a FRESH `uuid_v4`, never the source atom_id (the two
//!     planes' atom ids are not coordinated, and the tenant table has no
//!     reason to match the control-plane one).
//!   * `graph_id` — the resolved TARGET member graph.
//!   * `content_blob_key` — see "Blob handling" below; either a freshly
//!     re-uploaded org-prefixed key, or the empty string (dev/no-R2 inline
//!     case, byte-identical to the source row's own empty-key convention).
//!   * `embed_attempts` (reset to `0`) and `embed_failed_at` (reset to
//!     `NULL`) — these are per-plane embedding-pipeline bookkeeping, not
//!     content; a freshly-landed row has made no attempts *on this plane*.
//!     `embedding` itself is copied **verbatim** (both planes use the same
//!     384-dim BAAI bge-small-en-v1.5 model) — this binary never re-embeds
//!     and never enqueues `EmbedAtoms`.
//!   * `content_tsv` — recomputed as `to_tsvector('simple', <plaintext>)`
//!     over the SAME plaintext this binary already has in hand (decrypted
//!     blob, or `content_inline`), using the SAME `'simple'` regconfig
//!     `workers::embed::update_content_tsv_and_clear_failure` writes at
//!     normal ingest time (see that function's own doc comment for why it is
//!     always `'simple'` regardless of `fts_language`). This is an
//!     addition beyond the literal migration spec, made deliberately: no
//!     ambient worker in this repo backfills `content_tsv` for rows that
//!     already carry a non-NULL `embedding` (the normal write path only
//!     populates it as a side effect of the embed worker's own write, which
//!     this binary intentionally never triggers) — leaving it NULL would
//!     silently and permanently break `Store::keyword_search` for every
//!     migrated atom. See `backend/src/eval.rs:531-549` for the same
//!     construction used by the eval-fixture loader.
//!   * `ingested_by` — **REATTRIBUTED** to the target ocean's own
//!     `owner_user_id`, not carried over from the source row. The source and
//!     target ocean are not necessarily owned by the same identity (the
//!     motivating case: a personal ocean owned by a consumer identity being
//!     copied into an org member ocean owned by that person's WORK identity),
//!     and a coral whose `ingested_by` disagrees with the owner of the ocean
//!     it lives in is inconsistent on its face and misattributes per-user
//!     billing (`llm_usage.attributed_user_id`). The value is read off the
//!     resolved target `graphs` row itself — never off a separate CLI
//!     argument — so it cannot disagree with the destination by construction.
//!     `--preserve-ingested-by` opts out and keeps each source row's value.
//!     No uuid TRANSLATION is involved either way: neither tenant table has
//!     an FK to `users` (`users` is control-plane only), so a user uuid is
//!     equally meaningful in both databases.
//!
//!     DELIBERATE ASYMMETRY: `author_id`/`author_name`/`participants` (and
//!     `channel_id`/`channel_name`/`workspace_id`) are left VERBATIM. Those
//!     are human-readable display/provenance strings rather than identity
//!     keys, and the same human is behind both accounts in the motivating
//!     case — rewriting them would corrupt provenance without buying any
//!     consistency. This asymmetry is intentional, not an oversight.
//!   * The task brief that seeded this binary's spec also names
//!     `atoms.content_hash` as a verbatim-copy column. **No such column
//!     exists** — checked against every `migrations_control/` and
//!     `migrations_tenant/` DDL file for `atoms`. `content_hash` in this
//!     codebase is always a transient, in-process value computed at ingest
//!     time (`hex::encode(Sha256::digest(content))`) for a job-queue
//!     idempotency KEY, never a stored atom column — see
//!     `backend/src/routes/ingest.rs:1527` for the canonical example. There
//!     is nothing to read or write here; this is a documented gap between
//!     the brief and the actual schema, not an oversight.
//!
//! ## Blob handling (source decrypt → target re-encrypt)
//! For each atom whose `content_blob_key` is non-empty: fetch + decrypt with
//! the SOURCE (consumer) DEK (`R2Client::get_atom_blob`), then re-encrypt and
//! upload under the target org's namespaced key with the TARGET DEK
//! (`R2Client::put_atom_blob(Some(target_clerk_org_id), target_graph_id,
//! <fresh atom_id>, ..)`), and store the returned key as the new row's
//! `content_blob_key`.
//!
//! **A non-empty `content_blob_key` does NOT mean the content is in R2.**
//! Before the 2026-08-04 R2 cutover, atoms were written with a
//! `content_blob_key` set AND their plaintext in `content_inline`, with no
//! object ever uploaded — 5,277 of this migration's 9,407 source corals are
//! in that state (everything up to 2026-08-04), and an R2 read for any of
//! them returns "blob not found". A 25-atom pilot skipped 13 rows for exactly
//! this reason before the source-selection below was corrected. So
//! `content_inline` is consulted FIRST and is authoritative wherever present
//! (it holds full content — the truncated preview is `excerpt`), and R2 is
//! read only for rows with no inline copy.
//!
//! Whichever side the plaintext came from, the TARGET row is normalised to
//! R2-backed with its own `content_inline` left NULL: carrying pre-cutover
//! inline plaintext into the tenant would write unencrypted content into the
//! tenant database, which is what the R2 cutover exists to avoid.
//!
//! **Per-atom failure policy — two classes, two policies, and NEITHER exits
//! zero.** A per-atom failure is classified at the stage that produced it
//! (`AtomFailure`), never guessed from an error string afterwards:
//!
//!   * **Blob failure** (source fetch/decrypt timed out, 404'd, or failed to
//!     decrypt; or the target upload failed) — counted under `blob_failures`,
//!     logged by source `atom_id`, and **the atom is skipped: no row is
//!     inserted for it.** The alternative (a row pointing at
//!     unreachable/never-written content) is exactly the outcome the brief
//!     forbids. A multi-hour migration of a real ocean must not abort
//!     entirely for one flaky R2 read; the failed atom is simply retried on
//!     the next invocation (nothing was written for it, so it is
//!     indistinguishable from "not yet examined" on a re-run).
//!   * **Insert failure** (the tenant `INSERT` itself errored) — counted
//!     under `insert_failures` and **the run ABORTS.** This is the
//!     destination database refusing writes, not a per-atom content fault, so
//!     it will almost certainly repeat for every atom left; grinding on would
//!     burn hours re-uploading blobs whose rows will never exist (one
//!     orphaned R2 object per atom) and bury the real error under thousands
//!     of identical lines.
//!
//! Both classes are ROW LOSS, and any non-zero loss makes the process exit
//! **5** (`EXIT_INCOMPLETE_COPY`), never 0. This is a fix, not a nicety: the
//! two used to be one bucket that only printed a count, and `main` returned
//! success unless the whole atoms phase returned `Err` — so a run in which
//! 1,019 atoms failed with a Postgres `relation "atoms" does not exist`
//! (which reads as an R2 fault when it is filed under "blob failures")
//! exited 0, and the operator went straight on to `reextract.rs` against a
//! graph missing a thousand corals.
//!
//! ## Idempotency and resumability
//! `atoms_graph_id_source_type_source_id_key` (`UNIQUE (graph_id,
//! source_type, source_id)`, present on both trees) is the correctness
//! backstop: every insert is `ON CONFLICT (graph_id, source_type, source_id)
//! DO NOTHING`. On top of that, this binary loads the target graph's
//! existing `(source_type, source_id)` pairs into memory ONCE up front and
//! skips any atom already present **before** touching R2 — so a re-run after
//! an interruption does not re-pay the decrypt/re-encrypt/upload cost for
//! already-migrated atoms, only for whatever is left. Source atoms are read
//! in **keyset** pages ordered by `atom_id` (`atom_id > $cursor ORDER BY
//! atom_id LIMIT N`), never `OFFSET` — the source ocean can be actively
//! written to by its owner while this runs, and offset pagination would skip
//! or duplicate rows as new atoms land mid-run.
//!
//! There is no separate persisted watermark: a from-scratch re-run is always
//! safe (idempotent via the unique index) but will re-examine every source
//! atom, paying the in-memory existing-keys lookup (cheap) for each. This is
//! a known, deliberate simplification — a future version could persist a
//! high-water `atom_id` if migrations of a much larger ocean make the full
//! rescan itself too slow; today's target ocean does not warrant the extra
//! state.
//!
//! `coding_sessions` (`UNIQUE (graph_id, session_id)`) and `projects` (`PRIMARY
//! KEY (graph_id, project_key)`) copy the same way: read-all-then-`ON
//! CONFLICT ... DO NOTHING` insert, remapped to the target graph. Both tables
//! are small (session/project metadata, not content), so no batching.
//! `coding_sessions.id` gets a fresh uuid per row for the same reason atoms
//! do; `projects` has no surrogate id at all (its PK is the natural key).
//!
//! ## `coding_sessions` raw content is NOT carried across
//! `raw_inline` and `raw_blob_key` are written **NULL** on every target row,
//! whatever the source held — see `target_raw_content` for the full argument.
//! In short: `raw_inline` is plaintext transcript and copying it puts
//! unencrypted content in the tenant database (the same rule the atoms path
//! already enforces by pinning the target's `content_inline` to NULL), and
//! `raw_blob_key` is a consumer-namespaced key naming an object encrypted
//! under the SOURCE account's DEK — unreadable under the tenant's org DEK,
//! and actively harmful, because `workers::member_ocean_reconciler`'s purge
//! org-scope-asserts every `coding_sessions.raw_blob_key` it finds for a
//! graph and aborts the whole purge on one that isn't the org's. The repo's
//! own graph-to-graph session copy (`store::copy_coding_session_tx`) omits
//! both columns for the same reason. The run reports
//! `raw_content_dropped=<n>` so this is visible, not silent; the session's
//! actual content lives in the copied `atoms`.
//!
//! ## The HNSW index
//! A bulk insert against a live `atoms_embedding_hnsw_idx` serializes (every
//! insert takes a lock on the graph structure). This binary drops it
//! **lazily** — only immediately before the FIRST atom it actually inserts
//! this run, so a fully-resumed no-op run (everything already migrated)
//! never touches the index at all — and rebuilds it **once**, after the bulk
//! atoms loop finishes (success or failure; see below), using `CONCURRENTLY`
//! both ways so a shared tenant DB serving OTHER members' oceans is never
//! blocked by an ACCESS EXCLUSIVE lock on this binary's account.
//!
//! **Exact statements** (safe to run by hand if this process is killed after
//! the drop but before the rebuild — both are `IF [NOT] EXISTS` and need no
//! transaction wrapper):
//! ```sql
//! DROP INDEX CONCURRENTLY IF EXISTS atoms_embedding_hnsw_idx;
//! -- ... bulk atom inserts happen here ...
//! CREATE INDEX CONCURRENTLY IF NOT EXISTS atoms_embedding_hnsw_idx
//!     ON public.atoms USING hnsw (embedding public.vector_cosine_ops)
//!     WITH (m='16', ef_construction='64')
//!     WHERE (embedding IS NOT NULL);
//! ```
//! This binary always attempts the rebuild if it performed the drop, even if
//! the atom-copy loop itself later fails — the alternative (an org's shared
//! tenant DB permanently missing its vector index because one migration run
//! died) is worse than a rebuild racing a hard failure. If the process is
//! killed hard enough that even that cleanup does not run, the statement
//! above is exactly what to run by hand; `CREATE INDEX CONCURRENTLY` can
//! leave an `INVALID` index on a previously-failed attempt, in which case
//! `DROP INDEX CONCURRENTLY IF EXISTS atoms_embedding_hnsw_idx;` once more
//! before retrying the `CREATE` clears it.
//!
//! # Usage
//! ```text
//! migrate_personal_to_member_ocean \
//!     --source-graph <uuid> --target-slug <slug> --target-clerk-org <org_id> \
//!     --target-user <uuid> [--target-graph <uuid>] \
//!     [--since <rfc3339>] [--until <rfc3339>] [--limit <n>] [--project <name>] \
//!     [--execute] [--dry-run] [--yes]
//! ```
//!
//! ## `--project` — narrowing to one `atoms.project` tag
//! Absent a `--project` value, this binary copies **every** atom in
//! `--source-graph` that also matches `--since`/`--until` — the WHOLE
//! personal ocean, not just some caller-assumed subset of it. There is no
//! implicit narrowing anywhere in this file: `--since`/`--until` bound
//! `atoms.timestamp`, which is not a proxy for topical relevance, and prior
//! to this flag existing there was **no** way to ask for "only the atoms
//! tagged for project X" at all.
//!
//! `--project <name>` adds `AND atoms.project = <name>` to both the count
//! preview and the batch-copy query (exact string match, not `ILIKE` — this
//! repo's `atoms.project` values are written by the capture pipeline as
//! exact repo-alias or workspace-key strings, e.g. `'coralswarm'` vs. the
//! unnormalised `'github.com/CoralSwarm/coralswarm'` the same repo can also
//! appear under; see `projects.aliases` if those need reconciling before a
//! run — this binary does not consult `projects` to expand aliases itself).
//! Passing it is optional and changes nothing about the refusal gates, the
//! idempotency key (`(graph_id, source_type, source_id)` is unaffected by
//! which atoms happen to be selected), or `coding_sessions`/`projects`
//! copying (those two tables have no `project`-shaped column to filter by
//! and are copied in full regardless — see their own sections above; a
//! `--project`-scoped run intentionally still carries every session/project
//! ROW, since those are small metadata tables, not content, and a session
//! with zero atoms surviving the filter is harmless dead metadata rather
//! than a disclosure).
//! (Example values for a concrete run this binary was built for: `--source-graph
//! 93cc446a-3d5e-497e-ac36-cbccc42d1828 --target-slug coralswarm
//! --target-clerk-org org_2ExampleTargetOrg0000000000 --target-user
//! <the member's users.id uuid>` — **every one of these is a required CLI
//! argument with no default or hardcoded fallback anywhere in this file**;
//! the example is documentation, not a default.)
//!
//! `--dry-run` **is the default** — every refusal gate above runs (including
//! gate 5, the DEK-presence check) and this binary prints the exact counts a
//! real run would produce, but performs zero writes: no `atoms`/
//! `coding_sessions`/`projects` rows, no R2 objects, no index drop. Passing
//! `--dry-run` explicitly is accepted (shell-history legibility, same
//! convention as `backfill_unattributed_job_accounts`/
//! `backfill_reef_judgement_from_history`) and is redundant with the
//! default. A real run requires **both** `--execute` (the repo-wide
//! flip-to-real-run flag every sibling one-shot in this directory uses) AND
//! `--yes` (an extra, explicit confirmation this binary alone requires,
//! because unlike a backfill that only touches rows already in one
//! database, a real run here permanently copies one person's decrypted
//! content across a tenancy boundary into a second database — a mistake is
//! not merely re-runnable, it is a disclosure). `--execute` and `--dry-run`
//! together, or `--execute` without `--yes`, are both usage errors.
//!
//! Note on the brief this binary was built from: its literal CLI surface
//! listed a bare `--dry-run` (said to default to `true`) as the ONLY
//! opt-out-of-dry-run mechanism alongside `--yes`. Every existing one-shot in
//! `backend/src/bin/` that defaults to dry-run
//! (`backfill_unattributed_job_accounts`, `backfill_reef_judgement_from_history`)
//! instead uses a dedicated `--execute` flag, mutually exclusive with an
//! explicit `--dry-run`. This binary follows that established, tested repo
//! convention (`--execute` flips it) rather than inventing a new
//! `--dry-run=<bool>` value-flag idiom with no precedent anywhere else in
//! this directory, and additionally keeps `--yes` from the brief as an extra
//! confirmation gate on top of `--execute` — the union of both, not a
//! straight substitution.
//!
//! # Env
//!   DATABASE_URL / MIGRATOR_DATABASE_URL   control-plane DSN, same
//!       precedence as every sibling tool (`migrator_database_url()`). This
//!       binary performs no control-plane WRITES (it only reads `graphs`,
//!       `accounts.dek_wrapped`, and `tenant_databases`), so a plain
//!       `DATABASE_URL` under the least-privilege role is sufficient; the
//!       elevated DSN is accepted (and preferred when set) purely for
//!       operator consistency with the other concierge tools in this
//!       directory.
//!   TENANT_DB_<SLUG>   the target tenant's DSN, resolved from the routing
//!       row's `db_secret_ref` via `OperatorDsnResolver` — same convention as
//!       every other tool in this directory. This is the tenant's
//!       OWNER-role credential (RLS does not restrict it — see
//!       `tenancy::registry::TenantPoolRegistry::connect_and_verify`'s own
//!       comment on why the table owner is exempt), so it can DROP/CREATE
//!       the HNSW index and write `atoms`/`coding_sessions`/`projects`
//!       directly.
//!   MASTER_KEK_HEX / MASTER_KEK_HEX_NEXT   required for a REAL run only
//!       (DEK unwrap on both planes) — never read during `--dry-run`.
//!   R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET_NAME
//!       required for a REAL run only, unconditionally (even if every atom
//!       in this particular ocean turns out to be dev/no-R2 inline content —
//!       same posture as `export_member_ocean`, which builds its R2 client
//!       before it has looked at a single atom row).
//!
//! # Exit codes
//!   0  success — every examined atom either landed or was already present.
//!      Includes a `--dry-run` preview with zero refusals. **This is the only
//!      code that means "safe to run `reextract.rs` now".**
//!   1  an unexpected runtime failure (a DB connection, a query, an R2 call
//!      outside the per-atom blob-failure policy above, e.g. the HNSW
//!      rebuild itself failing) — INCLUDING the abort on a tenant `INSERT`
//!      failure.
//!   2  a usage/argument error, or no control-plane DSN in the environment —
//!      caught before any database connection is attempted.
//!   3  a refusal gate rejected the run — nothing written.
//!   4  every gate passed but a real-run precondition (R2 credentials
//!       missing) prevented starting the write — nothing written.
//!   5  the run finished every phase but did NOT copy every atom it examined
//!      — the target graph is INCOMPLETE. Re-run (idempotent) after fixing
//!      the logged failures. See `EXIT_INCOMPLETE_COPY`.
//!
//! Prints one `progress:` line to stderr every [`PROGRESS_INTERVAL`] atoms
//! examined, and a final summary of examined / inserted / skipped-duplicate /
//! blob-failures / insert-failures / not-copied for atoms (plus small
//! inserted/skipped tallies for `coding_sessions` — with its own
//! `raw_content_dropped` count — and `projects`).

use std::collections::HashSet;
use std::process::ExitCode;
use std::time::Duration as StdDuration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use pgvector::Vector;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_backend::crypto::resolve_dek_for_tenant;
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_core::r2::R2Client;

/// Source atoms are read in keyset pages of this size, ordered by `atom_id`.
/// Chosen to keep each page's R2 round-trips (one GET + one PUT per non-inline
/// atom, real-run only) bounded without adding per-batch transaction
/// machinery — see the module doc's "Idempotency and resumability" section.
const ATOM_BATCH_SIZE: i64 = 200;

/// Print a `progress:` line to stderr every this-many atoms EXAMINED
/// (matched by the source query, whether inserted, skipped, or failed).
const PROGRESS_INTERVAL: u64 = 200;

/// Bound on a single R2 round-trip (fetch+decrypt OR encrypt+upload). Mirrors
/// `export_member_ocean::BLOB_FETCH_TIMEOUT` — a one-shot migration must not
/// hang indefinitely on a stalled connection; a named, bounded timeout error
/// is far more useful than a process that never returns.
const BLOB_IO_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// Exit code for a real-run environment/precondition problem surfacing AFTER
/// every refusal gate has passed (R2 credentials missing) — see
/// `export_member_ocean::EXIT_POST_GATE_SETUP_ERROR`, same convention.
const EXIT_POST_GATE_SETUP_ERROR: u8 = 4;

/// Exit code for a run that reached the end of its work but did NOT copy
/// every source atom it examined — the target graph is INCOMPLETE.
///
/// This code exists because its absence caused a real incident. Per-atom
/// failures were counted and printed, but `main` returned success unless the
/// whole atoms phase returned `Err`, so a run that silently dropped 1,019
/// corals exited `0` and the operator (and any wrapper script) went straight
/// on to `reextract.rs` against a graph missing a thousand rows. A count of
/// lost rows is a failed migration, not a footnote in a successful one — see
/// [`final_exit_code`].
const EXIT_INCOMPLETE_COPY: u8 = 5;

/// The tenant tree's vector index — see the module doc's "The HNSW index"
/// section for the exact statements and why `CONCURRENTLY` is used both ways.
const HNSW_INDEX_NAME: &str = "atoms_embedding_hnsw_idx";
const HNSW_DROP_SQL: &str = "DROP INDEX CONCURRENTLY IF EXISTS atoms_embedding_hnsw_idx";
const HNSW_CREATE_SQL: &str = "CREATE INDEX CONCURRENTLY IF NOT EXISTS atoms_embedding_hnsw_idx \
     ON public.atoms USING hnsw (embedding public.vector_cosine_ops) \
     WITH (m='16', ef_construction='64') WHERE (embedding IS NOT NULL)";

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Args {
    source_graph: Uuid,
    target_slug: String,
    target_clerk_org: String,
    target_user: Uuid,
    target_graph: Option<Uuid>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    limit: Option<u64>,
    /// Exact-match filter on `atoms.project` — see the module doc's
    /// `--project` section. `None` means unfiltered (every atom in the
    /// source graph matching `--since`/`--until`).
    project: Option<String>,
    execute: bool,
    /// Only consulted inside `parse_args`'s own `--execute requires --yes`
    /// check; kept on `Args` (rather than a bare local) so `{args:?}` (never
    /// currently printed, but available for an operator's own debugging)
    /// reflects every flag actually passed, not just the ones read again
    /// later.
    #[allow(dead_code)]
    yes: bool,
    /// Keep each source row's own `ingested_by` instead of reattributing it
    /// to the target ocean's owner. OFF by default — see the module doc's
    /// `ingested_by` bullet for why reattribution is the default.
    preserve_ingested_by: bool,
}

const USAGE: &str = "usage: migrate_personal_to_member_ocean --source-graph <uuid> \
--target-slug <slug> --target-clerk-org <org_id> --target-user <uuid> \
[--target-graph <uuid>] [--since <rfc3339>] [--until <rfc3339>] [--limit <n>] \
[--project <atoms.project value, exact match>] \
[--execute] [--dry-run] [--yes] [--preserve-ingested-by]";

fn parse_rfc3339(flag: &str, s: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| format!("{flag}: not a valid RFC3339 timestamp ({s}): {e}"))
}

/// The outcome of parsing an argv: either a fully-resolved [`Args`], or "the
/// operator asked for `--help`".
///
/// Help is returned as a VALUE rather than short-circuited with a
/// `std::process::exit(0)` inside the parser, because a parse arm that kills
/// the process is unreachable from a test — and an untestable arm is exactly
/// how [`parse_argv`]'s former test-only mirror drifted (it never grew the
/// `-h`/`--help` arm at all).
#[derive(Debug)]
enum ParsedArgs {
    Run(Args),
    Help,
}

/// The one and only argv parser. [`parse_args`] is a thin adapter that feeds
/// it `std::env::args()`; the tests at the bottom of this file call it
/// directly with a literal argv.
///
/// There is deliberately **no second copy of this logic**. There used to be:
/// a `parse_args_from(&[&str])` in `mod tests` whose own doc comment promised
/// it was "kept in exact lockstep" with the production parser. It was not —
/// it never had the `-h`/`--help` arm, and its `--execute requires --yes`
/// message was a different, shorter string. Every CLI test in this file was
/// therefore asserting against a parser no operator has ever run. Taking an
/// explicit `&[&str]` (instead of reading `std::env::args()` internally) is
/// what removes the reason the mirror existed: these tests still depend on no
/// process-global state, they just exercise the real code path now.
fn parse_argv(argv: &[&str]) -> Result<ParsedArgs, String> {
    let mut source_graph: Option<Uuid> = None;
    let mut target_slug: Option<String> = None;
    let mut target_clerk_org: Option<String> = None;
    let mut target_user: Option<Uuid> = None;
    let mut target_graph: Option<Uuid> = None;
    let mut since: Option<DateTime<Utc>> = None;
    let mut until: Option<DateTime<Utc>> = None;
    let mut limit: Option<u64> = None;
    let mut project: Option<String> = None;
    let mut execute = false;
    let mut explicit_dry_run = false;
    let mut yes = false;
    let mut preserve_ingested_by = false;

    let mut args = argv.iter().skip(1).map(|s| (*s).to_string());
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source-graph" => {
                let v = args.next().ok_or("--source-graph requires a value")?;
                source_graph = Some(
                    Uuid::parse_str(&v).map_err(|_| format!("--source-graph: not a UUID: {v}"))?,
                );
            }
            "--target-slug" => {
                target_slug = Some(args.next().ok_or("--target-slug requires a value")?)
            }
            "--target-clerk-org" => {
                target_clerk_org = Some(args.next().ok_or("--target-clerk-org requires a value")?)
            }
            "--target-user" => {
                let v = args.next().ok_or("--target-user requires a value")?;
                target_user =
                    Some(Uuid::parse_str(&v).map_err(|_| format!("--target-user: not a UUID: {v}"))?);
            }
            "--target-graph" => {
                let v = args.next().ok_or("--target-graph requires a value")?;
                target_graph = Some(
                    Uuid::parse_str(&v).map_err(|_| format!("--target-graph: not a UUID: {v}"))?,
                );
            }
            "--since" => {
                let v = args.next().ok_or("--since requires a value")?;
                since = Some(parse_rfc3339("--since", &v)?);
            }
            "--until" => {
                let v = args.next().ok_or("--until requires a value")?;
                until = Some(parse_rfc3339("--until", &v)?);
            }
            "--limit" => {
                let v = args.next().ok_or("--limit requires a value")?;
                limit = Some(v.parse::<u64>().map_err(|_| format!("--limit: not a number: {v}"))?);
            }
            "--project" => {
                let v = args.next().ok_or("--project requires a value")?;
                if v.trim().is_empty() {
                    return Err("--project: value must not be empty".to_string());
                }
                project = Some(v);
            }
            "--execute" => execute = true,
            "--dry-run" => explicit_dry_run = true,
            "--yes" => yes = true,
            "--preserve-ingested-by" => preserve_ingested_by = true,
            // Short-circuits everything, exactly as the previous
            // `std::process::exit(0)` in this arm did: `--help` anywhere on
            // the line wins, even ahead of a missing required argument or a
            // typo later in the same argv.
            "-h" | "--help" => return Ok(ParsedArgs::Help),
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if execute && explicit_dry_run {
        return Err("--execute and --dry-run are mutually exclusive".to_string());
    }
    if execute && !yes {
        return Err(
            "--execute requires --yes — this is a one-way copy of decrypted content across a \
             tenancy boundary, not merely a re-runnable row update; pass --yes to confirm you \
             mean it"
                .to_string(),
        );
    }
    if let (Some(s), Some(u)) = (since, until) {
        if u < s {
            return Err(format!("--until ({u}) is before --since ({s})"));
        }
    }

    Ok(ParsedArgs::Run(Args {
        source_graph: source_graph.ok_or("--source-graph is required")?,
        target_slug: target_slug.ok_or("--target-slug is required")?,
        target_clerk_org: target_clerk_org.ok_or("--target-clerk-org is required")?,
        target_user: target_user.ok_or("--target-user is required")?,
        target_graph,
        since,
        until,
        limit,
        project,
        execute,
        yes,
        preserve_ingested_by,
    }))
}

/// Adapter: the process's real argv into [`parse_argv`]. Holds no parsing
/// logic of its own — that is the whole point (see [`parse_argv`]'s doc).
fn parse_args() -> Result<ParsedArgs, String> {
    let owned: Vec<String> = std::env::args().collect();
    let argv: Vec<&str> = owned.iter().map(String::as_str).collect();
    parse_argv(&argv)
}

impl Args {
    fn dry_run(&self) -> bool {
        !self.execute
    }
}

// ── Row shapes ───────────────────────────────────────────────────────────────

/// The source `graphs` row (control plane) — just enough to gate on `kind`
/// and to resolve the source account for DEK lookups.
#[derive(Debug, Clone, sqlx::FromRow)]
struct SourceGraphRow {
    /// Selected (matches the `WHERE id = $1` this was looked up by) purely
    /// so the struct documents the full row shape; not re-read after the
    /// lookup — the caller already holds `args.source_graph`.
    #[allow(dead_code)]
    id: Uuid,
    account_id: Uuid,
    kind: String,
}

/// The target `graphs` row (tenant plane) — mirrors
/// `export_member_ocean::GraphRow`.
#[derive(Debug, Clone, sqlx::FromRow)]
struct TargetGraphRow {
    id: Uuid,
    /// Selected (matches the `WHERE account_id = $1` this was looked up by)
    /// purely so the struct documents the full row shape; not re-read after
    /// the lookup — the caller already holds the routing row's account_id.
    #[allow(dead_code)]
    account_id: Uuid,
    owner_user_id: Option<Uuid>,
    is_primary: bool,
    /// Selected (matches the `WHERE archived_at IS NULL` this was looked up
    /// by, so this is always `None`) purely so the struct documents the full
    /// row shape, same reasoning as `account_id` above.
    #[allow(dead_code)]
    archived_at: Option<DateTime<Utc>>,
}

/// One source `atoms` row, every column this binary needs to carry over or
/// transform. See the module doc's "atoms column handling" section for
/// exactly which columns are verbatim vs. rewritten.
#[derive(Debug, Clone, sqlx::FromRow)]
struct SourceAtomRow {
    atom_id: Uuid,
    source_type: String,
    source_id: String,
    content_blob_key: String,
    embedding: Option<Vector>,
    participants: Vec<String>,
    event_timestamp: DateTime<Utc>,
    ingested_by: Option<Uuid>,
    scope: String,
    created_at: DateTime<Utc>,
    content_inline: Option<String>,
    fts_language: String,
    channel_id: Option<String>,
    channel_name: Option<String>,
    author_id: Option<String>,
    author_name: Option<String>,
    workspace_id: Option<String>,
    embed_model: Option<String>,
    chunker_version: Option<i16>,
    header_len: Option<i32>,
    project: Option<String>,
    mcp_client_name: Option<String>,
    title: Option<String>,
    excerpt: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct ProjectRow {
    project_key: String,
    display_name: Option<String>,
    aliases: Vec<String>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct CodingSessionRow {
    session_id: String,
    project_key: Option<String>,
    title: Option<String>,
    repo_path: Option<String>,
    remote_sanitized: Option<String>,
    hostname: Option<String>,
    harness_version: Option<String>,
    model: Option<String>,
    started_at: DateTime<Utc>,
    last_activity_at: DateTime<Utc>,
    raw_blob_key: Option<String>,
    raw_inline: Option<String>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, Copy)]
struct AtomCounts {
    examined: u64,
    inserted: u64,
    skipped_duplicate: u64,
    /// Atoms whose CONTENT could not be moved — a source R2 fetch/decrypt or
    /// a target encrypt/upload failed. Skip-and-continue (see
    /// [`record_atom_failure`]).
    blob_failures: u64,
    /// Atoms whose content moved fine but whose tenant `INSERT` failed — a
    /// DATABASE fault, counted separately because it is a different class
    /// with a different policy. Conflating the two is what sent the original
    /// investigation after R2 while the real error was Postgres saying
    /// `relation "atoms" does not exist`.
    insert_failures: u64,
}

impl AtomCounts {
    /// Source atoms this run examined but did not land in the target, for any
    /// reason. This — not `Err` from the atoms phase — is the measure of
    /// whether a migration is complete: `examined == inserted +
    /// skipped_duplicate + lost()` always holds.
    fn lost(&self) -> u64 {
        self.blob_failures + self.insert_failures
    }
}

/// Why one atom did not land. The two variants are not cosmetic labels: they
/// carry different policies (see [`record_atom_failure`]) because they are
/// different failures. A blob fault is a per-atom content-transport problem
/// that the next invocation retries; an INSERT fault is the destination
/// database itself refusing writes, which will almost certainly repeat for
/// every remaining atom.
#[derive(Debug)]
enum AtomFailure {
    /// Source fetch/decrypt, or target encrypt/upload, failed. No row was
    /// inserted (deliberately: a row pointing at unreachable content is worse
    /// than no row).
    Blob(String),
    /// The tenant `INSERT` itself failed.
    Insert(String),
}

impl std::fmt::Display for AtomFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Blob(msg) => write!(f, "BLOB FAILURE: {msg}"),
            Self::Insert(msg) => write!(f, "INSERT FAILURE: {msg}"),
        }
    }
}

/// Record one per-atom failure against `counts` and decide what the run does
/// next. Returns `true` if the run must ABORT, `false` if it may continue to
/// the next atom.
///
/// * [`AtomFailure::Blob`] → **continue.** The module doc's blob-failure
///   policy, unchanged: a multi-hour migration must not abort because one R2
///   read was flaky, and nothing was written for the atom, so the next
///   invocation retries it as if it had never been examined.
/// * [`AtomFailure::Insert`] → **abort.** The tenant database refusing a
///   write is not a per-atom content fault, and grinding through the rest of
///   the ocean re-uploading blobs whose rows will never exist wastes hours
///   and leaves an orphaned R2 object per atom. Abort surfaces the real error
///   at the top of the output on the FIRST occurrence instead of burying it
///   under thousands of identical lines — which is exactly what happened when
///   1,019 atoms "failed" with `relation "atoms" does not exist`.
///
/// Split out as a pure function so both policies are pinned by a test that
/// needs no database — see `mod tests`.
fn record_atom_failure(counts: &mut AtomCounts, failure: &AtomFailure) -> bool {
    match failure {
        AtomFailure::Blob(_) => {
            counts.blob_failures += 1;
            false
        }
        AtomFailure::Insert(_) => {
            counts.insert_failures += 1;
            true
        }
    }
}

/// The exit code a run that reached the end of its work must produce.
///
/// `0` **only** when every examined atom landed (inserted) or was already
/// there (skipped as a duplicate). Any loss at all is
/// [`EXIT_INCOMPLETE_COPY`] — a non-zero code an operator or wrapper script
/// cannot mistake for "safe to run `reextract.rs` now".
///
/// Pure, so the rule that made this whole finding — *a run that dropped rows
/// must never exit 0* — is testable without a database.
fn final_exit_code(counts: &AtomCounts) -> u8 {
    if counts.lost() > 0 {
        EXIT_INCOMPLETE_COPY
    } else {
        0
    }
}

// ── Connections + gates ──────────────────────────────────────────────────────

async fn connect(dsn: &str, max_connections: u32) -> Result<PgPool, String> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(StdDuration::from_secs(15))
        .connect(dsn)
        .await
        .map_err(|e| format!("connect: {e}"))
}

/// Gate 1: `--target-slug` names an `active` routing row whose `clerk_org_id`
/// equals `--target-clerk-org`. Naming both independently on the CLI (rather
/// than trusting whichever one is easier to look up) means a stale/copy-pasted
/// argument mismatch is caught HERE, not by writing into the wrong org.
async fn resolve_routing_row(
    control: &PgPool,
    target_slug: &str,
    target_clerk_org: &str,
) -> Result<TenantDatabase, String> {
    let row = routing::lookup_by_slug(control, target_slug)
        .await
        .map_err(|e| format!("routing lookup for slug '{target_slug}': {e}"))?
        .ok_or_else(|| format!("no tenant_databases routing row for slug '{target_slug}'"))?;
    if !row.is_servable() {
        return Err(format!(
            "tenant routing row for slug '{target_slug}' (org {}) has status '{}', not 'active'",
            row.clerk_org_id, row.status
        ));
    }
    if row.clerk_org_id != target_clerk_org {
        return Err(format!(
            "--target-clerk-org ({target_clerk_org}) does not match the routing row's \
             clerk_org_id ({}) for slug '{target_slug}' — refusing (argument mismatch guard)",
            row.clerk_org_id
        ));
    }
    Ok(row)
}

/// Gate 2: connect the resolved tenant DSN and verify the `tenant_meta`
/// fingerprint — the same DSN-swap guard as
/// `tenancy::registry::TenantPoolRegistry::connect_and_verify` and
/// `export_member_ocean::connect_tenant`. A missing fingerprint fails closed
/// exactly like a mismatched one; there is no "trust it anyway" path.
async fn connect_tenant_verified(resolver: &OperatorDsnResolver, row: &TenantDatabase) -> Result<PgPool, String> {
    // Environment first, then the KEK-encrypted tenant_db_credentials row
    // (self-serve tenants have no Fly secret; issue #1514). Labelled by slug:
    // the error never carries a DSN.
    let dsn = resolver
        .resolve(&row.db_secret_ref)
        .await
        .map_err(|e| format!("resolving tenant DSN for {}: {e}", row.slug))?;
    let pool = connect(&dsn, 4).await.map_err(|e| format!("tenant DB: {e}"))?;

    let fingerprint: Option<String> = sqlx::query_scalar("SELECT org_id FROM public.tenant_meta WHERE id = true")
        .fetch_optional(&pool)
        .await
        .map_err(|e| format!("tenant_meta fingerprint read: {e}"))?;
    match fingerprint {
        Some(org) if org == row.clerk_org_id => Ok(pool),
        Some(org) => {
            pool.close().await;
            Err(format!(
                "tenant_meta fingerprint mismatch: routing row says {}, tenant DB says {org} — \
                 refusing (DSN-swap guard)",
                row.clerk_org_id
            ))
        }
        None => {
            pool.close().await;
            Err(format!(
                "tenant_meta has no fingerprint row for org {} — refusing (DSN-swap guard)",
                row.clerk_org_id
            ))
        }
    }
}

/// Gate 3b: refuse content this legacy copier cannot preserve.
async fn refuse_preserved_meeting_evidence(pool: &PgPool, source_graph: Uuid) -> Result<(), String> {
    let has_versions: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM public.source_items WHERE graph_id=$1)",
    )
    .bind(source_graph)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("cannot verify meeting history for source graph {source_graph}: {e}; nothing migrated"))?;
    if has_versions {
        return Err(format!("source graph {source_graph} contains preserved source versions and may contain semantic evidence; this legacy migration tool does not support copying them. Upgrade the tool before moving this graph. Nothing migrated."));
    }
    Ok(())
}

/// Gate 3: `--source-graph` exists on the control plane and is `kind = 'personal'`.
async fn resolve_source_graph(control: &PgPool, source_graph: Uuid) -> Result<SourceGraphRow, String> {
    let row: Option<SourceGraphRow> =
        sqlx::query_as("SELECT id, account_id, kind FROM public.graphs WHERE id = $1")
            .bind(source_graph)
            .fetch_optional(control)
            .await
            .map_err(|e| format!("source graph lookup: {e}"))?;
    let row = row.ok_or_else(|| format!("no graphs row for --source-graph {source_graph}"))?;
    if row.kind != "personal" {
        return Err(format!(
            "--source-graph {source_graph} has kind='{}', not 'personal' — this binary migrates a \
             personal ocean into a member ocean, not an arbitrary graph; refusing",
            row.kind
        ));
    }
    Ok(row)
}

/// Gate 4: resolve the TARGET member ocean. See the module doc's refusal-gate
/// list (item 4) for the "provisioned lazily" refusal message, and
/// `export_member_ocean::resolve_member_graph`'s doc for the sibling
/// reasoning this mirrors (there: prefer/require disambiguating ARCHIVED
/// primaries; here: prefer/require disambiguating LIVE ones).
async fn resolve_target_member_graph(
    tenant: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    graph_id: Option<Uuid>,
) -> Result<TargetGraphRow, String> {
    if let Some(gid) = graph_id {
        return sqlx::query_as::<_, TargetGraphRow>(
            "SELECT id, account_id, owner_user_id, is_primary, archived_at FROM public.graphs \
             WHERE id = $1 AND account_id = $2 AND owner_user_id = $3 AND kind = 'member' \
               AND archived_at IS NULL",
        )
        .bind(gid)
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_optional(tenant)
        .await
        .map_err(|e| format!("target graph lookup: {e}"))?
        .ok_or_else(|| {
            format!(
                "no LIVE member ocean found matching --target-graph {gid} for owner \
                 {owner_user_id} — wrong id, wrong owner, not kind='member', or archived"
            )
        });
    }

    let mut rows = sqlx::query_as::<_, TargetGraphRow>(
        "SELECT id, account_id, owner_user_id, is_primary, archived_at FROM public.graphs \
         WHERE account_id = $1 AND owner_user_id = $2 AND kind = 'member' AND archived_at IS NULL \
         ORDER BY is_primary DESC, created_at ASC",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_all(tenant)
    .await
    .map_err(|e| format!("target graph lookup: {e}"))?;

    if rows.is_empty() {
        return Err(format!(
            "no LIVE member ocean (kind='member', archived_at IS NULL) exists for --target-user \
             {owner_user_id} in this tenant — member oceans are provisioned lazily on the \
             member's first authenticated sight of the org host; have that person visit the org \
             host once, then re-run this migration"
        ));
    }
    if rows[0].is_primary {
        return Ok(rows.remove(0));
    }
    if rows.len() == 1 {
        return Ok(rows.remove(0));
    }
    let candidates = rows
        .iter()
        .map(|g| format!("\n    {}", g.id))
        .collect::<String>();
    Err(format!(
        "{} LIVE, non-primary member oceans exist for --target-user {owner_user_id} and none is \
         primary:{candidates}\nrefusing to guess which one you meant — re-run with --target-graph \
         naming exactly one of the ids above",
        rows.len()
    ))
}

/// Gate 5 (source half): does the source account have a wrapped DEK on the
/// control plane at all? Mirrors `export_member_ocean::dek_row_exists` —
/// deliberately a presence check only (no KEK material touched), so it costs
/// a `--dry-run` nothing.
async fn source_dek_exists(control: &PgPool, account_id: Uuid) -> Result<bool, String> {
    let found: Option<(bool,)> =
        sqlx::query_as("SELECT true FROM public.accounts WHERE id = $1 AND dek_wrapped IS NOT NULL")
            .bind(account_id)
            .fetch_optional(control)
            .await
            .map_err(|e| format!("accounts.dek_wrapped lookup: {e}"))?;
    Ok(found.is_some())
}

/// Gate 5 (target half): does the target account have an `account_keys` row
/// on the tenant plane? Same presence-only shape as [`source_dek_exists`].
async fn target_dek_exists(tenant: &PgPool, account_id: Uuid) -> Result<bool, String> {
    let found: Option<(bool,)> = sqlx::query_as("SELECT true FROM public.account_keys WHERE account_id = $1")
        .bind(account_id)
        .fetch_optional(tenant)
        .await
        .map_err(|e| format!("account_keys lookup: {e}"))?;
    Ok(found.is_some())
}

// ── Preview (dry-run) ────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn count_source_atoms(
    control: &PgPool,
    source_graph: Uuid,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    project: Option<&str>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM public.atoms WHERE graph_id = $1
             AND ($2::timestamptz IS NULL OR "timestamp" >= $2)
             AND ($3::timestamptz IS NULL OR "timestamp" <= $3)
             AND ($4::text IS NULL OR project = $4)"#,
    )
    .bind(source_graph)
    .bind(since)
    .bind(until)
    .bind(project)
    .fetch_one(control)
    .await
}

async fn existing_atom_keys(
    tenant: &PgPool,
    target_graph: Uuid,
) -> Result<HashSet<(String, String)>, sqlx::Error> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT source_type, source_id FROM public.atoms WHERE graph_id = $1")
            .bind(target_graph)
            .fetch_all(tenant)
            .await?;
    Ok(rows.into_iter().collect())
}

// ── Blob I/O (real run only) ─────────────────────────────────────────────────

/// Both blob helpers return [`AtomFailure::Blob`] rather than a bare `String`
/// so a caller cannot accidentally file a content-transport fault under the
/// database class (or vice versa) — the classification is made here, at the
/// stage that actually knows which stage it is, not reconstructed later from
/// an error message.
async fn fetch_and_decrypt(
    r2: &R2Client,
    dek: &[u8; 32],
    blob_key: &str,
    source_atom_id: Uuid,
) -> Result<Vec<u8>, AtomFailure> {
    tokio::time::timeout(BLOB_IO_TIMEOUT, r2.get_atom_blob(blob_key, dek))
        .await
        .map_err(|_| {
            AtomFailure::Blob(format!("timed out after {BLOB_IO_TIMEOUT:?} fetching/decrypting blob for source atom {source_atom_id}"))
        })?
        .map_err(|e| AtomFailure::Blob(format!("fetching/decrypting blob for source atom {source_atom_id}: {e}")))
}

async fn reencrypt_and_upload(
    r2: &R2Client,
    dek: &[u8; 32],
    target_org_id: &str,
    target_graph: Uuid,
    new_atom_id: Uuid,
    plaintext: &[u8],
) -> Result<String, AtomFailure> {
    tokio::time::timeout(
        BLOB_IO_TIMEOUT,
        r2.put_atom_blob(Some(target_org_id), target_graph, new_atom_id, plaintext, dek),
    )
    .await
    .map_err(|_| AtomFailure::Blob(format!("timed out after {BLOB_IO_TIMEOUT:?} uploading re-encrypted blob for new atom {new_atom_id}")))?
    .map_err(|e| AtomFailure::Blob(format!("uploading re-encrypted blob for new atom {new_atom_id}: {e}")))
}

/// Migrate one atom: resolve its plaintext, re-encrypt/upload if it lives in
/// R2, and INSERT the new row. Returns `Ok(true)` if this call's INSERT
/// actually landed a row, `Ok(false)` if `ON CONFLICT` found it already
/// present (a race with the existing-keys pre-check — see the module doc).
///
/// The `Err` half is a typed [`AtomFailure`], not a string, precisely so the
/// caller can tell the two failure classes apart: this function's blob I/O
/// yields [`AtomFailure::Blob`] and its `INSERT` yields
/// [`AtomFailure::Insert`]. No row is inserted in either error case.
#[allow(clippy::too_many_arguments)]
async fn migrate_one_atom(
    tenant: &PgPool,
    r2: &R2Client,
    source_dek: &[u8; 32],
    target_dek: &[u8; 32],
    target_org_id: &str,
    target_graph: Uuid,
    target_ingested_by: Option<Uuid>,
    atom: &SourceAtomRow,
) -> Result<bool, AtomFailure> {
    let new_atom_id = Uuid::new_v4();

    // Content lives in ONE of two places, and a non-empty `content_blob_key`
    // does NOT tell you which. Before the 2026-08-04 R2 cutover, atoms were
    // written with a `content_blob_key` set AND their plaintext in
    // `content_inline`, and no R2 object was ever uploaded for them. In this
    // migration's own source ocean that is 5,277 of 9,407 corals, and reading
    // R2 for any of them fails with "blob not found" — measured, not
    // hypothesised: an early 25-atom pilot skipped 13 rows for exactly this
    // reason before this branch existed.
    //
    // So `content_inline` is checked FIRST and treated as authoritative
    // wherever present (it is full content, not a truncated preview — that is
    // `excerpt`); R2 is consulted only for rows with no inline copy, i.e.
    // everything ingested after the cutover.
    //
    // Either way the TARGET row is normalised to R2-backed with its own
    // `content_inline` left NULL — see the `$12` bind below. Carrying inline
    // plaintext into the tenant would put unencrypted content in the tenant
    // database, which is precisely what the R2 cutover moved away from.
    let (new_blob_key, plaintext) = match atom.content_inline.as_deref() {
        Some(inline) => {
            let plaintext = inline.as_bytes().to_vec();
            let new_key = reencrypt_and_upload(
                r2,
                target_dek,
                target_org_id,
                target_graph,
                new_atom_id,
                &plaintext,
            )
            .await?;
            (new_key, plaintext)
        }
        // No inline copy and no blob key: nothing to carry. Leaves the target
        // row content-less rather than failing, matching the previous shape.
        None if atom.content_blob_key.trim().is_empty() => (String::new(), Vec::new()),
        None => {
            let plaintext =
                fetch_and_decrypt(r2, source_dek, &atom.content_blob_key, atom.atom_id).await?;
            let new_key = reencrypt_and_upload(
                r2,
                target_dek,
                target_org_id,
                target_graph,
                new_atom_id,
                &plaintext,
            )
            .await?;
            (new_key, plaintext)
        }
    };

    // content_tsv — see the module doc's "atoms column handling" section for
    // why this binary computes it itself rather than leaving it NULL.
    let tsv_text = String::from_utf8_lossy(&plaintext).into_owned();

    let inserted: Option<Uuid> = sqlx::query_scalar(
        r#"
        INSERT INTO public.atoms (
            atom_id, graph_id, source_type, source_id, content_blob_key, embedding,
            participants, "timestamp", ingested_by, scope, created_at, content_inline,
            fts_language, channel_id, channel_name, author_id, author_name, workspace_id,
            embed_model, chunker_version, embed_attempts, header_len, project,
            mcp_client_name, title, excerpt, content_tsv
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
            $19, $20, 0, $21, $22, $23, $24, $25, to_tsvector('simple', $26)
        )
        ON CONFLICT (graph_id, source_type, source_id) DO NOTHING
        RETURNING atom_id
        "#,
    )
    .bind(new_atom_id)
    .bind(target_graph)
    .bind(&atom.source_type)
    .bind(&atom.source_id)
    .bind(&new_blob_key)
    .bind(&atom.embedding)
    .bind(&atom.participants)
    .bind(atom.event_timestamp)
    // Reattribution, not a carry-over: `target_ingested_by` is derived from
    // the TARGET ocean's own `owner_user_id` (see `main`), so it can never
    // disagree with the ocean this row is being written into. `None` (only
    // under `--preserve-ingested-by`) falls back to the source row's value.
    .bind(target_ingested_by.or(atom.ingested_by))
    .bind(&atom.scope)
    .bind(atom.created_at)
    // $12 content_inline — deliberately NULL on the target regardless of how
    // the source stored its content. Every migrated coral is R2-backed in the
    // tenant (see the plaintext resolution above); copying a pre-cutover row's
    // inline plaintext across would write unencrypted content into the tenant
    // database.
    .bind(None::<String>)
    .bind(&atom.fts_language)
    .bind(&atom.channel_id)
    .bind(&atom.channel_name)
    .bind(&atom.author_id)
    .bind(&atom.author_name)
    .bind(&atom.workspace_id)
    .bind(&atom.embed_model)
    .bind(atom.chunker_version)
    .bind(atom.header_len)
    .bind(&atom.project)
    .bind(&atom.mcp_client_name)
    .bind(&atom.title)
    .bind(&atom.excerpt)
    .bind(&tsv_text)
    .fetch_optional(tenant)
    .await
    // The DATABASE class, not the blob class. This one line is the whole
    // finding: the previous code returned a bare `String` here, so the
    // caller filed a Postgres error under `blob_failures` and printed it
    // beside genuine R2 faults.
    .map_err(|e| {
        AtomFailure::Insert(format!(
            "inserting migrated atom (source atom_id={}): {e}",
            atom.atom_id
        ))
    })?;

    Ok(inserted.is_some())
}

#[allow(clippy::too_many_arguments)]
async fn fetch_atom_batch(
    control: &PgPool,
    source_graph: Uuid,
    cursor: Option<Uuid>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    project: Option<&str>,
    take: i64,
) -> Result<Vec<SourceAtomRow>, sqlx::Error> {
    sqlx::query_as::<_, SourceAtomRow>(
        r#"
        SELECT atom_id, source_type, source_id, content_blob_key, embedding, participants,
               "timestamp" AS event_timestamp, ingested_by, scope, created_at, content_inline,
               fts_language, channel_id, channel_name, author_id, author_name, workspace_id,
               embed_model, chunker_version, header_len, project, mcp_client_name, title, excerpt
        FROM public.atoms
        WHERE graph_id = $1
          AND ($2::uuid IS NULL OR atom_id > $2)
          AND ($3::timestamptz IS NULL OR "timestamp" >= $3)
          AND ($4::timestamptz IS NULL OR "timestamp" <= $4)
          AND ($5::text IS NULL OR project = $5)
        ORDER BY atom_id
        LIMIT $6
        "#,
    )
    .bind(source_graph)
    .bind(cursor)
    .bind(since)
    .bind(until)
    .bind(project)
    .bind(take)
    .fetch_all(control)
    .await
}

async fn drop_hnsw_index(tenant: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(HNSW_DROP_SQL).execute(tenant).await.map(|_| ())
}

async fn rebuild_hnsw_index(tenant: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(HNSW_CREATE_SQL).execute(tenant).await.map(|_| ())
}

/// The bulk atoms copy. Drops [`HNSW_INDEX_NAME`] lazily (only before the
/// first row this run actually inserts) and leaves the caller to rebuild it
/// — see [`run_atoms_migration`], which always attempts the rebuild whether
/// this returns `Ok` or `Err`.
#[allow(clippy::too_many_arguments)]
async fn migrate_atoms(
    control: &PgPool,
    tenant: &PgPool,
    r2: &R2Client,
    source_dek: &[u8; 32],
    target_dek: &[u8; 32],
    target_org_id: &str,
    source_graph: Uuid,
    target_graph: Uuid,
    target_ingested_by: Option<Uuid>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    project: Option<&str>,
    limit: Option<u64>,
    hnsw_dropped: &mut bool,
    counts: &mut AtomCounts,
) -> anyhow::Result<()> {
    let mut existing = existing_atom_keys(tenant, target_graph)
        .await
        .context("reading target graph's existing (source_type, source_id) keys")?;

    let mut cursor: Option<Uuid> = None;
    let mut remaining = limit;

    loop {
        if remaining == Some(0) {
            break;
        }
        let take = remaining.map(|n| n.min(ATOM_BATCH_SIZE as u64) as i64).unwrap_or(ATOM_BATCH_SIZE);

        let batch = fetch_atom_batch(control, source_graph, cursor, since, until, project, take)
            .await
            .context("reading source atom batch")?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();

        for atom in &batch {
            cursor = Some(atom.atom_id);
            counts.examined += 1;
            if let Some(n) = remaining.as_mut() {
                *n = n.saturating_sub(1);
            }

            let key = (atom.source_type.clone(), atom.source_id.clone());
            if existing.contains(&key) {
                counts.skipped_duplicate += 1;
            } else {
                if !*hnsw_dropped {
                    drop_hnsw_index(tenant)
                        .await
                        .context("dropping atoms_embedding_hnsw_idx before bulk load")?;
                    *hnsw_dropped = true;
                    eprintln!("dropped {HNSW_INDEX_NAME} for the bulk load (will rebuild after)");
                }
                match migrate_one_atom(
                    tenant,
                    r2,
                    source_dek,
                    target_dek,
                    target_org_id,
                    target_graph,
                    target_ingested_by,
                    atom,
                )
                .await
                {
                    Ok(true) => {
                        counts.inserted += 1;
                        existing.insert(key);
                    }
                    Ok(false) => {
                        // ON CONFLICT raced us — someone else (or a prior,
                        // interrupted run) inserted it between our
                        // existing-keys snapshot and this INSERT.
                        counts.skipped_duplicate += 1;
                        existing.insert(key);
                    }
                    // The failure is CLASSIFIED at the stage that produced it
                    // (see `AtomFailure`), so this arm no longer has to guess
                    // from an error string what went wrong. `record_atom_failure`
                    // owns both the tally and the continue-vs-abort policy.
                    Err(failure) => {
                        let must_abort = record_atom_failure(counts, &failure);
                        eprintln!(
                            "{failure} — source atom {} ({}/{}): no row inserted",
                            atom.atom_id, atom.source_type, atom.source_id
                        );
                        if must_abort {
                            return Err(anyhow::anyhow!(
                                "aborting after a tenant INSERT failure on source atom {} — this \
                                 is the destination database refusing writes, not a per-atom \
                                 content fault, so it will almost certainly repeat for every \
                                 atom left. {failure}",
                                atom.atom_id
                            ));
                        }
                    }
                }
            }

            if counts.examined % PROGRESS_INTERVAL == 0 {
                eprintln!(
                    "progress: examined={} inserted={} skipped_duplicate={} blob_failures={} insert_failures={}",
                    counts.examined,
                    counts.inserted,
                    counts.skipped_duplicate,
                    counts.blob_failures,
                    counts.insert_failures
                );
            }
        }

        if (batch_len as i64) < take {
            break; // source exhausted
        }
    }

    Ok(())
}

/// Wraps [`migrate_atoms`] with the lazy-drop/always-rebuild HNSW discipline
/// described in the module doc: the rebuild is attempted regardless of
/// whether the copy loop itself succeeded, and a rebuild failure is reported
/// but does not mask an underlying copy failure (both are combined into one
/// error when both occur).
#[allow(clippy::too_many_arguments)]
async fn run_atoms_migration(
    control: &PgPool,
    tenant: &PgPool,
    r2: &R2Client,
    source_dek: &[u8; 32],
    target_dek: &[u8; 32],
    target_org_id: &str,
    source_graph: Uuid,
    target_graph: Uuid,
    target_ingested_by: Option<Uuid>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    project: Option<&str>,
    limit: Option<u64>,
) -> (AtomCounts, anyhow::Result<()>) {
    let mut counts = AtomCounts::default();
    let mut hnsw_dropped = false;

    let copy_result = migrate_atoms(
        control,
        tenant,
        r2,
        source_dek,
        target_dek,
        target_org_id,
        source_graph,
        target_graph,
        target_ingested_by,
        since,
        until,
        project,
        limit,
        &mut hnsw_dropped,
        &mut counts,
    )
    .await;

    if hnsw_dropped {
        eprintln!("rebuilding {HNSW_INDEX_NAME} (CONCURRENTLY)...");
        if let Err(rebuild_err) = rebuild_hnsw_index(tenant).await {
            let rebuild_msg = format!(
                "rebuilding {HNSW_INDEX_NAME} after the bulk load FAILED: {rebuild_err} — run by \
                 hand: `{HNSW_CREATE_SQL}` (if it reports the index already exists as INVALID, \
                 first run `{HNSW_DROP_SQL}`, then retry the CREATE)"
            );
            let combined = match copy_result {
                Ok(()) => anyhow::anyhow!(rebuild_msg),
                Err(copy_err) => copy_err.context(rebuild_msg),
            };
            return (counts, Err(combined));
        }
        eprintln!("rebuilt {HNSW_INDEX_NAME}");
    }

    (counts, copy_result)
}

// ── coding_sessions / projects (small, no batching) ─────────────────────────

async fn copy_projects(
    control: &PgPool,
    tenant: &PgPool,
    source_graph: Uuid,
    target_graph: Uuid,
) -> anyhow::Result<(u64, u64)> {
    let rows: Vec<ProjectRow> = sqlx::query_as(
        "SELECT project_key, display_name, aliases, created_at FROM public.projects WHERE graph_id = $1",
    )
    .bind(source_graph)
    .fetch_all(control)
    .await
    .context("reading source projects")?;

    let (mut inserted, mut skipped) = (0u64, 0u64);
    for row in &rows {
        let result = sqlx::query(
            "INSERT INTO public.projects (graph_id, project_key, display_name, aliases, created_at) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (graph_id, project_key) DO NOTHING",
        )
        .bind(target_graph)
        .bind(&row.project_key)
        .bind(&row.display_name)
        .bind(&row.aliases)
        .bind(row.created_at)
        .execute(tenant)
        .await
        .with_context(|| format!("inserting project '{}'", row.project_key))?;
        if result.rows_affected() > 0 {
            inserted += 1;
        } else {
            skipped += 1;
        }
    }
    Ok((inserted, skipped))
}

/// What the TARGET `coding_sessions` row carries in its two raw-content
/// columns: **always `(None, None)`**, whatever the source row held.
///
/// Neither column may be carried verbatim across this plane boundary, for two
/// separate reasons:
///
/// * `raw_inline` is the session's transcript **in plaintext**. Copying it
///   would write unencrypted content into the tenant database — the exact
///   thing the atoms path already refuses (see the module doc's "Blob
///   handling" section and the `$12 content_inline` bind in
///   [`migrate_one_atom`], which pins the target's `content_inline` to NULL
///   for the same reason).
/// * `raw_blob_key` is a CONSUMER-plane key: 2-segment `{graph}/{uuid}`,
///   naming an object encrypted under the SOURCE account's DEK. The tenant
///   resolves `TenantId::Org(..)` → the ORG DEK, so the copied key names an
///   object this database's key material cannot decrypt even if it could
///   reach it. Worse, it is not inert: `workers::member_ocean_reconciler`'s
///   purge reads `coding_sessions.raw_blob_key` for the graph and runs
///   `assert_blob_key_org_scope(key, Some(org_id))` over every key before
///   deleting anything — a 2-segment consumer key fails that assert, and the
///   purge ABORTS "for operator review" every tick, permanently, for that
///   ocean. A dangling pointer here breaks a background invariant, it does
///   not merely fail to resolve.
///
/// This is not a new policy invented for this binary. The repo's own
/// graph-to-graph session copy — `store::copy_coding_session_tx`
/// (`backend/src/store/mod.rs`), the promote/sync path — omits both columns
/// from its column list for the same reason: a session copy carries the
/// session's METADATA, never its raw capture.
///
/// The cost is stated rather than hidden: migrated sessions land in the
/// tenant without their raw transcript, and [`copy_coding_sessions`] reports
/// how many. The session's actual content is not lost — that is what the
/// `atoms` copy above moves.
fn target_raw_content(source: &CodingSessionRow) -> (Option<String>, Option<String>) {
    // The source row is taken and deliberately NOT read: the point of routing
    // through a named function is that the call site reads as a per-row
    // decision with a rationale attached, rather than two bare `None` binds a
    // later edit could quietly "fix" back into `&row.raw_blob_key`.
    let _ = source;
    (None, None)
}

/// Returns `(inserted, skipped_duplicate, raw_content_dropped)` — see
/// [`target_raw_content`] for what "dropped" means and why.
async fn copy_coding_sessions(
    control: &PgPool,
    tenant: &PgPool,
    source_graph: Uuid,
    target_graph: Uuid,
) -> anyhow::Result<(u64, u64, u64)> {
    let rows: Vec<CodingSessionRow> = sqlx::query_as(
        "SELECT session_id, project_key, title, repo_path, remote_sanitized, hostname, \
                harness_version, model, started_at, last_activity_at, raw_blob_key, raw_inline, \
                created_at \
         FROM public.coding_sessions WHERE graph_id = $1",
    )
    .bind(source_graph)
    .fetch_all(control)
    .await
    .context("reading source coding_sessions")?;

    let (mut inserted, mut skipped, mut raw_dropped) = (0u64, 0u64, 0u64);
    for row in &rows {
        let new_id = Uuid::new_v4();
        let (raw_blob_key, raw_inline) = target_raw_content(row);
        if row.raw_blob_key.is_some() || row.raw_inline.is_some() {
            raw_dropped += 1;
        }
        let result = sqlx::query(
            "INSERT INTO public.coding_sessions (id, graph_id, session_id, project_key, title, \
                repo_path, remote_sanitized, hostname, harness_version, model, started_at, \
                last_activity_at, raw_blob_key, raw_inline, created_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
             ON CONFLICT (graph_id, session_id) DO NOTHING",
        )
        .bind(new_id)
        .bind(target_graph)
        .bind(&row.session_id)
        .bind(&row.project_key)
        .bind(&row.title)
        .bind(&row.repo_path)
        .bind(&row.remote_sanitized)
        .bind(&row.hostname)
        .bind(&row.harness_version)
        .bind(&row.model)
        .bind(row.started_at)
        .bind(row.last_activity_at)
        // $13 raw_blob_key / $14 raw_inline — always NULL on the target. See
        // `target_raw_content`: the source key is consumer-namespaced and
        // encrypted under the SOURCE DEK (unreadable here, and it wedges the
        // member-ocean purge), and the inline copy is plaintext transcript
        // that must not land in a tenant database.
        .bind(&raw_blob_key)
        .bind(&raw_inline)
        .bind(row.created_at)
        .execute(tenant)
        .await
        .with_context(|| format!("inserting coding_session '{}'", row.session_id))?;
        if result.rows_affected() > 0 {
            inserted += 1;
        } else {
            skipped += 1;
        }
    }
    Ok((inserted, skipped, raw_dropped))
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(ParsedArgs::Run(a)) => a,
        Ok(ParsedArgs::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let control_url = match migrator_database_url() {
        Some(u) => u,
        None => {
            eprintln!("DATABASE_URL (or MIGRATOR_DATABASE_URL) must be set to the control-plane DSN");
            return ExitCode::from(2);
        }
    };
    let control = match connect(&control_url, 4).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect control-plane database: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "migrate_personal_to_member_ocean — mode={} source_graph={} target_slug={} \
         target_clerk_org={} target_user={} project={}",
        if args.dry_run() { "DRY RUN" } else { "EXECUTE" },
        args.source_graph,
        args.target_slug,
        args.target_clerk_org,
        args.target_user,
        args.project.as_deref().unwrap_or("<unfiltered — every atom in the source graph>")
    );

    // ── Gate 1: routing row active + clerk_org_id matches ────────────────────
    let row = match resolve_routing_row(&control, &args.target_slug, &args.target_clerk_org).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("REFUSED: {e}");
            return ExitCode::from(3);
        }
    };

    // ── Gate 2: tenant DSN resolves + fingerprint matches ────────────────────
    let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "migrate_personal_to_member_ocean");
    let tenant = match connect_tenant_verified(&resolver, &row).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("REFUSED: {e}");
            return ExitCode::from(3);
        }
    };

    // ── Gate 3: source graph exists and is kind='personal' ───────────────────
    let source_graph_row = match resolve_source_graph(&control, args.source_graph).await {
        Ok(g) => g,
        Err(e) => {
            eprintln!("REFUSED: {e}");
            return ExitCode::from(3);
        }
    };

    // This tool cannot copy source versions, exact passages, or their node
    // links. Refuse before any mutation rather than silently exporting only
    // legacy chunks and claiming that meeting memory was migrated.
    if let Err(e) = refuse_preserved_meeting_evidence(&control, args.source_graph).await {
        eprintln!("REFUSED: {e}");
        return ExitCode::from(3);
    }

    // ── Gate 4: target member ocean resolves ─────────────────────────────────
    let target_graph_row = match resolve_target_member_graph(
        &tenant,
        row.account_id,
        args.target_user,
        args.target_graph,
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            eprintln!("REFUSED: {e}");
            return ExitCode::from(3);
        }
    };

    // ── Gate 5: both DEKs are provisioned — runs in --dry-run too ────────────
    match source_dek_exists(&control, source_graph_row.account_id).await {
        Ok(true) => {}
        Ok(false) => {
            eprintln!(
                "REFUSED: source account {} has no DEK provisioned (accounts.dek_wrapped IS NULL \
                 on the control plane) — nothing migrated",
                source_graph_row.account_id
            );
            return ExitCode::from(3);
        }
        Err(e) => {
            eprintln!("ERROR: {e}");
            return ExitCode::FAILURE;
        }
    }
    match target_dek_exists(&tenant, row.account_id).await {
        Ok(true) => {}
        Ok(false) => {
            eprintln!(
                "REFUSED: target account {} has no account_keys row on the tenant plane — nothing \
                 migrated",
                row.account_id
            );
            return ExitCode::from(3);
        }
        Err(e) => {
            eprintln!("ERROR: {e}");
            return ExitCode::FAILURE;
        }
    }

    // ── Preview counts (cheap; safe for both dry-run and real run) ──────────
    let source_atom_total = match count_source_atoms(
        &control,
        args.source_graph,
        args.since,
        args.until,
        args.project.as_deref(),
    )
    .await
    {
        Ok(n) => n,
        Err(e) => {
            eprintln!("ERROR: counting source atoms: {e}");
            return ExitCode::FAILURE;
        }
    };
    let already_present = match existing_atom_keys(&tenant, target_graph_row.id).await {
        Ok(set) => set.len(),
        Err(e) => {
            eprintln!("ERROR: reading target graph's existing atom keys: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "target member ocean: graph={} owner_user_id={:?} is_primary={}",
        target_graph_row.id, target_graph_row.owner_user_id, target_graph_row.is_primary
    );
    println!(
        "source atoms matching filters: {source_atom_total} (target graph already has \
         {already_present} atom(s) by (source_type, source_id) — an unknown number of which \
         belong to THIS source, the rest to any prior partial run)"
    );

    if args.dry_run() {
        println!(
            "DRY RUN — no rows written, no R2 objects written, no index touched. Blob \
             fetch/decrypt is NOT attempted in a dry run (same posture as export_member_ocean: \
             MASTER_KEK_HEX/R2 credentials are not dry-run prerequisites), so blob_failures \
             cannot be predicted here — only discoverable on a real (--execute) run."
        );
        println!("Re-run with --execute --yes to perform the migration.");
        return ExitCode::SUCCESS;
    }

    // ── Real run only: unwrap both DEKs ──────────────────────────────────────
    let source_dek = match resolve_dek_for_tenant(&TenantId::Consumer, &control, source_graph_row.account_id)
        .await
    {
        Ok(Some(k)) => k,
        Ok(None) => {
            eprintln!("REFUSED: source account DEK disappeared between the gate check and now");
            return ExitCode::from(3);
        }
        Err(e) => {
            eprintln!("ERROR: source DEK resolution failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let target_dek = match resolve_dek_for_tenant(
        &TenantId::Org(row.clerk_org_id.clone()),
        &tenant,
        row.account_id,
    )
    .await
    {
        Ok(Some(k)) => k,
        Ok(None) => {
            eprintln!("REFUSED: target account DEK disappeared between the gate check and now");
            return ExitCode::from(3);
        }
        Err(e) => {
            eprintln!("ERROR: target DEK resolution failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    let r2 = match (
        std::env::var("R2_ACCOUNT_ID"),
        std::env::var("R2_ACCESS_KEY_ID"),
        std::env::var("R2_SECRET_ACCESS_KEY"),
        std::env::var("R2_BUCKET_NAME"),
    ) {
        (Ok(account_id), Ok(access_key_id), Ok(secret_access_key), Ok(bucket)) => {
            R2Client::new(&account_id, &access_key_id, &secret_access_key, &bucket).await
        }
        _ => {
            eprintln!(
                "ERROR: R2 credentials not fully set (R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / \
                 R2_SECRET_ACCESS_KEY / R2_BUCKET_NAME) — required for a real (--execute) run"
            );
            return ExitCode::from(EXIT_POST_GATE_SETUP_ERROR);
        }
    };

    // Reattribution of `ingested_by`, derived from the TARGET ocean's own
    // resolved row rather than from `--target-user`, so the value written can
    // never disagree with the ocean it is written into even if a future caller
    // passes `--target-graph` and `--target-user` that drift apart (they can't
    // today — `resolve_target_member_graph` requires both to match — but the
    // invariant should hold by construction, not by that gate's continued
    // existence). `--preserve-ingested-by` opts out, keeping each source row's
    // own value.
    let target_ingested_by = if args.preserve_ingested_by {
        None
    } else {
        target_graph_row.owner_user_id
    };
    println!(
        "ingested_by: {}",
        match target_ingested_by {
            Some(u) => format!("reattributing every copied coral to target ocean owner {u}"),
            None => "preserving each source row's own value (--preserve-ingested-by)".to_string(),
        }
    );

    let (atom_counts, atoms_result) = run_atoms_migration(
        &control,
        &tenant,
        &r2,
        &source_dek,
        &target_dek,
        &row.clerk_org_id,
        args.source_graph,
        target_graph_row.id,
        target_ingested_by,
        args.since,
        args.until,
        args.project.as_deref(),
        args.limit,
    )
    .await;

    println!(
        "atoms: examined={} inserted={} skipped_duplicate={} blob_failures={} insert_failures={} \
         (not_copied={})",
        atom_counts.examined,
        atom_counts.inserted,
        atom_counts.skipped_duplicate,
        atom_counts.blob_failures,
        atom_counts.insert_failures,
        atom_counts.lost()
    );
    if let Err(e) = atoms_result {
        eprintln!("ERROR during atom migration: {e:#}");
        eprintln!(
            "the counts above reflect everything that landed before this failure — every \
             inserted atom is durably committed (ON CONFLICT DO NOTHING makes re-running safe); \
             fix the underlying problem and re-run to pick up where this left off"
        );
        return ExitCode::FAILURE;
    }

    let (projects_inserted, projects_skipped) =
        match copy_projects(&control, &tenant, args.source_graph, target_graph_row.id).await {
            Ok(counts) => counts,
            Err(e) => {
                eprintln!("ERROR during projects copy: {e:#}");
                eprintln!("atoms already migrated above are unaffected; re-run to retry projects/coding_sessions");
                return ExitCode::FAILURE;
            }
        };
    println!("projects: inserted={projects_inserted} skipped_duplicate={projects_skipped}");

    let (sessions_inserted, sessions_skipped, sessions_raw_dropped) =
        match copy_coding_sessions(&control, &tenant, args.source_graph, target_graph_row.id).await {
            Ok(counts) => counts,
            Err(e) => {
                eprintln!("ERROR during coding_sessions copy: {e:#}");
                eprintln!("atoms/projects already migrated above are unaffected; re-run to retry coding_sessions");
                return ExitCode::FAILURE;
            }
        };
    println!(
        "coding_sessions: inserted={sessions_inserted} skipped_duplicate={sessions_skipped} \
         raw_content_dropped={sessions_raw_dropped}"
    );
    if sessions_raw_dropped > 0 {
        println!(
            "  ({sessions_raw_dropped} source session(s) carried a raw transcript — \
             raw_inline/raw_blob_key — that was deliberately NOT copied: inline transcript is \
             plaintext and must not land in a tenant database, and the source blob key is \
             consumer-namespaced and encrypted under the SOURCE account's DEK, so it would be \
             both unreadable here and a permanent abort for the member-ocean purge. The session \
             CONTENT is in the atoms copied above.)"
        );
    }

    // Row loss is a FAILED migration, not a footnote in a successful one.
    // Everything above ran to completion, so the counts are final — but if any
    // examined atom did not land, the target graph is incomplete and this must
    // not hand an operator (or a wrapper script) a zero.
    if atom_counts.lost() > 0 {
        eprintln!(
            "INCOMPLETE: {} of {} examined source atom(s) were NOT copied ({} blob failure(s), \
             {} insert failure(s)) — target graph {} is MISSING those corals. Each was logged \
             above with its source atom_id. Do NOT treat this as a finished migration: fix the \
             underlying failures and re-run this binary (it is idempotent and will retry exactly \
             the missing rows) until a run reports zero not_copied.",
            atom_counts.lost(),
            atom_counts.examined,
            atom_counts.blob_failures,
            atom_counts.insert_failures,
            target_graph_row.id
        );
        return ExitCode::from(final_exit_code(&atom_counts));
    }

    println!(
        "migration complete. Derived tables (nodes, node_atoms, relationship_edge, \
         supersession_edge, reef_identifiers, evaluation_ledger, ...) were NOT touched in \
         target graph {}. Nothing re-derives them from copied atoms: the per-atom extractor \
         is retired, and managed source extraction reads source_versions, which this binary \
         does not copy.",
        target_graph_row.id
    );

    ExitCode::from(final_exit_code(&atom_counts))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> Vec<&'static str> {
        vec![
            "migrate_personal_to_member_ocean",
            "--source-graph",
            "93cc446a-3d5e-497e-ac36-cbccc42d1828",
            "--target-slug",
            "coralswarm",
            "--target-clerk-org",
            "org_2ExampleTargetOrg0000000000",
            "--target-user",
            "1981e2c9-2f76-4d26-a6f2-5aae61290c3d",
        ]
    }

    /// Drive the REAL parser ([`parse_argv`]) and unwrap the `Run` case.
    ///
    /// This used to be a 90-line hand-copy of `parse_args()` living in this
    /// module, whose own doc comment promised it was "kept in exact lockstep".
    /// It was not: it never had the `-h`/`--help` arm, and its
    /// `--execute requires --yes` message was a different string — so every
    /// test below was green against a parser no operator ever ran.
    /// `help_is_a_value_not_a_process_exit` and
    /// `execute_without_yes_names_the_disclosure_risk` below both fail
    /// against that old mirror, which is the point.
    fn parse_run(argv: &[&str]) -> Result<Args, String> {
        match parse_argv(argv)? {
            ParsedArgs::Run(a) => Ok(a),
            ParsedArgs::Help => Err("parsed as --help, not a run".to_string()),
        }
    }

    /// Reattributing `ingested_by` to the target ocean's owner is the DEFAULT;
    /// preserving the source value is the opt-in. Asserted explicitly because
    /// a silent flip of this default would misattribute every copied coral
    /// (and its per-user billing) without failing anything else.
    #[test]
    fn reattributes_ingested_by_unless_opted_out() {
        let args = parse_run(&base_args()).expect("parses");
        assert!(!args.preserve_ingested_by, "reattribution must be the default");

        let mut argv = base_args();
        argv.push("--preserve-ingested-by");
        let args = parse_run(&argv).expect("parses");
        assert!(args.preserve_ingested_by);
    }

    #[tokio::test]
    async fn migration_refuses_preserved_meeting_evidence_only_for_selected_graph() {
        use std::str::FromStr;
        use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
        let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "RELINFER_TEST_DATABASE_URL is required");
            eprintln!("skipping meeting evidence migration guard test: set RELINFER_TEST_DATABASE_URL");
            return;
        };
        let opts = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
        assert_eq!(opts.get_database(), Some("memory_provenance_test"));
        let pool = PgPoolOptions::new().max_connections(2).connect_with(opts).await.unwrap();
        let user: Uuid = sqlx::query_scalar("INSERT INTO public.users(email) VALUES($1) RETURNING id")
            .bind(format!("migration-evidence-{}@test.local", Uuid::new_v4())).fetch_one(&pool).await.unwrap();
        let account: Uuid = sqlx::query_scalar("INSERT INTO public.accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user).fetch_one(&pool).await.unwrap();
        let graph: Uuid = sqlx::query_scalar("INSERT INTO public.graphs(account_id,name) VALUES($1,'legacy migration guard') RETURNING id")
            .bind(account).fetch_one(&pool).await.unwrap();
        refuse_preserved_meeting_evidence(&pool, graph).await.unwrap();
        sqlx::query("INSERT INTO public.meeting_transcripts(graph_id,source_id,raw_inline) VALUES($1,'guard-meeting','Original')")
            .bind(graph).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO public.source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting','guard-meeting')").bind(graph).execute(&pool).await.unwrap();
        let version: Uuid = sqlx::query_scalar("INSERT INTO public.source_versions(graph_id,source_id,source_item_id,raw_inline) VALUES($1,'guard-meeting',(SELECT source_item_id FROM public.source_items WHERE graph_id=$1),'Original') RETURNING version_id")
            .bind(graph).fetch_one(&pool).await.unwrap();
        let error = refuse_preserved_meeting_evidence(&pool, graph).await.unwrap_err();
        assert!(error.contains("Upgrade the tool before moving this graph"));
        refuse_preserved_meeting_evidence(&pool, Uuid::new_v4()).await.unwrap();
        let original: String = sqlx::query_scalar("SELECT raw_inline FROM public.source_versions WHERE version_id=$1")
            .bind(version).fetch_one(&pool).await.unwrap();
        assert_eq!(original, "Original", "refusal preserves source history");
        sqlx::query("DELETE FROM public.meeting_transcripts WHERE graph_id=$1").bind(graph).execute(&pool).await.unwrap();
        let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
        sqlx::query("DELETE FROM public.accounts WHERE id=$1").bind(account).execute(&mut *financial).await.unwrap();
        financial.commit().await.unwrap();
        sqlx::query("DELETE FROM public.users WHERE id=$1").bind(user).execute(&pool).await.unwrap();
        pool.close().await;
    }

    #[test]
    fn defaults_to_dry_run() {
        let args = parse_run(&base_args()).expect("parses");
        assert!(args.dry_run());
        assert!(!args.execute);
    }

    #[test]
    fn execute_requires_yes() {
        let mut argv = base_args();
        argv.push("--execute");
        let err = parse_run(&argv).expect_err("must refuse --execute without --yes");
        assert!(err.contains("--yes"), "{err}");
    }

    /// The message an operator actually sees. Pinned because it is one of the
    /// two places the deleted test-only parser copy had silently DRIFTED from
    /// production (its version was the bare "--execute requires --yes"), so
    /// this assertion is the proof that these tests now drive the real
    /// parser rather than a mirror of it.
    #[test]
    fn execute_without_yes_names_the_disclosure_risk() {
        let mut argv = base_args();
        argv.push("--execute");
        let err = parse_run(&argv).expect_err("must refuse --execute without --yes");
        assert!(
            err.contains("tenancy boundary"),
            "the refusal must explain WHY --yes exists, not just demand it: {err}"
        );
    }

    /// The other place the deleted mirror had drifted: it had no `-h`/`--help`
    /// arm at all, so `--help` was an "unknown argument" to every test while
    /// being a clean exit for every operator. Help is now a parse RESULT
    /// rather than a `std::process::exit(0)` buried in a match arm, which is
    /// what makes it reachable from a test in the first place.
    #[test]
    fn help_is_a_value_not_a_process_exit() {
        assert!(matches!(parse_argv(&["bin", "-h"]), Ok(ParsedArgs::Help)));
        assert!(matches!(parse_argv(&["bin", "--help"]), Ok(ParsedArgs::Help)));
        // Short-circuits: --help wins over a missing required argument AND
        // over a typo later on the same line, exactly as the previous
        // in-parser `exit(0)` did.
        assert!(matches!(
            parse_argv(&["bin", "--help", "--bogus"]),
            Ok(ParsedArgs::Help)
        ));
        let mut argv = base_args();
        argv.push("--help");
        assert!(matches!(parse_argv(&argv), Ok(ParsedArgs::Help)));
    }

    #[test]
    fn execute_and_yes_together_disables_dry_run() {
        let mut argv = base_args();
        argv.push("--execute");
        argv.push("--yes");
        let args = parse_run(&argv).expect("parses");
        assert!(!args.dry_run());
    }

    #[test]
    fn execute_and_explicit_dry_run_are_mutually_exclusive() {
        let mut argv = base_args();
        argv.push("--execute");
        argv.push("--yes");
        argv.push("--dry-run");
        let err = parse_run(&argv).expect_err("must refuse");
        assert!(err.contains("mutually exclusive"), "{err}");
    }

    #[test]
    fn explicit_dry_run_alone_is_accepted_and_is_a_no_op_on_the_default() {
        let mut argv = base_args();
        argv.push("--dry-run");
        let args = parse_run(&argv).expect("parses");
        assert!(args.dry_run());
    }

    #[test]
    fn missing_required_args_are_rejected_one_at_a_time() {
        assert!(parse_run(&["bin"]).is_err());
        assert!(parse_run(&["bin", "--source-graph", "not-a-uuid"]).is_err());
    }

    #[test]
    fn since_after_until_is_rejected() {
        let mut argv = base_args();
        argv.push("--since");
        argv.push("2026-02-01T00:00:00Z");
        argv.push("--until");
        argv.push("2026-01-01T00:00:00Z");
        let err = parse_run(&argv).expect_err("must refuse since > until");
        assert!(err.contains("--until"), "{err}");
    }

    #[test]
    fn since_and_until_parse_as_utc() {
        let mut argv = base_args();
        argv.push("--since");
        argv.push("2026-01-01T00:00:00Z");
        argv.push("--until");
        argv.push("2026-02-01T00:00:00Z");
        let args = parse_run(&argv).expect("parses");
        assert!(args.since.unwrap() < args.until.unwrap());
    }

    #[test]
    fn limit_parses_as_u64() {
        let mut argv = base_args();
        argv.push("--limit");
        argv.push("500");
        let args = parse_run(&argv).expect("parses");
        assert_eq!(args.limit, Some(500));
    }

    /// `--project` is OFF by default (unfiltered — every atom in the source
    /// graph, bounded only by `--since`/`--until`), and is an exact-match
    /// string when given. See the module doc's `--project` section for why
    /// this flag exists at all: absent it, there was no way to ask for
    /// "only the atoms tagged for project X" — the historical gap this test
    /// pins closed.
    #[test]
    fn project_is_unfiltered_by_default_and_exact_match_when_given() {
        let args = parse_run(&base_args()).expect("parses");
        assert_eq!(args.project, None, "no --project must mean unfiltered, not empty-string");

        let mut argv = base_args();
        argv.push("--project");
        argv.push("coralswarm");
        let args = parse_run(&argv).expect("parses");
        assert_eq!(args.project.as_deref(), Some("coralswarm"));
    }

    /// An empty `--project` value is almost certainly a shell-quoting mistake
    /// (`--project ""` or a dropped positional) rather than a deliberate
    /// "match atoms whose project is the empty string" request — atoms with
    /// no project tag store `NULL`, never `''` (see `atoms.project`'s own
    /// column, `Option<String>` throughout this file), so an empty-string
    /// filter would silently match zero rows forever. Refusing it here is
    /// cheaper than debugging a "0 atoms matched" report later.
    #[test]
    fn empty_project_value_is_rejected() {
        let mut argv = base_args();
        argv.push("--project");
        argv.push("");
        let err = parse_run(&argv).expect_err("must refuse an empty --project value");
        assert!(err.contains("--project"), "{err}");
    }

    #[test]
    fn unknown_flag_is_rejected() {
        let mut argv = base_args();
        argv.push("--bogus");
        assert!(parse_run(&argv).is_err());
    }

    // ── Pure logic: target-graph selection preference ───────────────────────
    // `resolve_target_member_graph`'s query does the actual DB-side ordering
    // (`is_primary DESC, created_at ASC`); this test locks in the in-process
    // decision made from that already-ordered result set (see the function's
    // body: "if rows[0].is_primary { pick it }, else if exactly one
    // candidate remains { pick it }, else refuse").
    fn pick_target<'a>(ordered_candidates: &'a [(&'a str, bool)]) -> Result<&'a str, String> {
        if ordered_candidates.is_empty() {
            return Err("none".to_string());
        }
        if ordered_candidates[0].1 {
            return Ok(ordered_candidates[0].0);
        }
        if ordered_candidates.len() == 1 {
            return Ok(ordered_candidates[0].0);
        }
        Err("ambiguous".to_string())
    }

    #[test]
    fn prefers_primary_when_present() {
        let candidates = [("primary-id", true), ("secondary-id", false)];
        assert_eq!(pick_target(&candidates), Ok("primary-id"));
    }

    #[test]
    fn single_non_primary_is_accepted() {
        let candidates = [("only-id", false)];
        assert_eq!(pick_target(&candidates), Ok("only-id"));
    }

    #[test]
    fn multiple_non_primary_is_ambiguous() {
        let candidates = [("a", false), ("b", false)];
        assert!(pick_target(&candidates).is_err());
    }

    #[test]
    fn no_candidates_is_refused() {
        let candidates: [(&str, bool); 0] = [];
        assert!(pick_target(&candidates).is_err());
    }

    // ── Pure logic: batch-size clamping under --limit ───────────────────────
    fn take_for(remaining: Option<u64>, batch_size: i64) -> i64 {
        remaining.map(|n| n.min(batch_size as u64) as i64).unwrap_or(batch_size)
    }

    #[test]
    fn take_is_batch_size_when_no_limit() {
        assert_eq!(take_for(None, ATOM_BATCH_SIZE), ATOM_BATCH_SIZE);
    }

    #[test]
    fn take_is_clamped_to_remaining_limit() {
        assert_eq!(take_for(Some(3), ATOM_BATCH_SIZE), 3);
    }

    #[test]
    fn take_is_batch_size_when_limit_exceeds_it() {
        assert_eq!(take_for(Some(10_000), ATOM_BATCH_SIZE), ATOM_BATCH_SIZE);
    }

    // ── Pure logic: AtomCounts accumulate independently ─────────────────────
    #[test]
    fn atom_counts_default_is_all_zero() {
        let c = AtomCounts::default();
        assert_eq!(c.examined, 0);
        assert_eq!(c.inserted, 0);
        assert_eq!(c.skipped_duplicate, 0);
        assert_eq!(c.blob_failures, 0);
        assert_eq!(c.insert_failures, 0);
        assert_eq!(c.lost(), 0);
    }

    // ── Row loss must never exit 0 ──────────────────────────────────────────

    /// The incident this binary's exit code was changed for: 1,019 atoms did
    /// not land, the run printed the count, and `main` still returned success
    /// — so the operator's next step was `reextract.rs` over a graph missing
    /// a thousand corals.
    #[test]
    fn a_run_that_dropped_rows_never_exits_zero() {
        let clean = AtomCounts {
            examined: 10,
            inserted: 7,
            skipped_duplicate: 3,
            ..Default::default()
        };
        assert_eq!(
            final_exit_code(&clean),
            0,
            "a run where every examined atom landed or was already present is a success"
        );

        let incident = AtomCounts {
            examined: 1019,
            inserted: 0,
            skipped_duplicate: 0,
            blob_failures: 0,
            insert_failures: 1019,
        };
        assert_ne!(
            final_exit_code(&incident),
            0,
            "1,019 atoms lost must not look like success to a wrapper script"
        );
        assert_eq!(final_exit_code(&incident), EXIT_INCOMPLETE_COPY);

        let one_flaky_blob = AtomCounts {
            examined: 5,
            inserted: 4,
            skipped_duplicate: 0,
            blob_failures: 1,
            insert_failures: 0,
        };
        assert_eq!(
            final_exit_code(&one_flaky_blob),
            EXIT_INCOMPLETE_COPY,
            "even ONE skipped atom leaves the target incomplete"
        );
    }

    /// Blob faults and database faults are different classes with different
    /// policies — and both are row loss.
    #[test]
    fn blob_and_insert_failures_are_separate_classes_with_separate_policies() {
        let mut counts = AtomCounts::default();

        let blob = AtomFailure::Blob("blob not found".to_string());
        assert!(
            !record_atom_failure(&mut counts, &blob),
            "a flaky R2 read must NOT abort a multi-hour migration"
        );
        assert_eq!(
            (counts.blob_failures, counts.insert_failures),
            (1, 0),
            "a content-transport fault belongs in blob_failures"
        );

        let insert = AtomFailure::Insert("relation \"atoms\" does not exist".to_string());
        assert!(
            record_atom_failure(&mut counts, &insert),
            "the destination database refusing writes must abort the run, not be absorbed \
             1,019 times"
        );
        assert_eq!(
            (counts.blob_failures, counts.insert_failures),
            (1, 1),
            "a Postgres error must NOT be filed under blob_failures — that mislabelling sent a \
             real investigation after R2"
        );

        assert_eq!(counts.lost(), 2, "both classes are lost rows");
        assert_ne!(final_exit_code(&counts), 0);
    }

    /// The operator-facing labels say which stage failed, so a Postgres error
    /// never reads as an R2 fault in the log again.
    #[test]
    fn failure_display_names_the_stage() {
        assert!(AtomFailure::Blob("x".into()).to_string().starts_with("BLOB FAILURE"));
        assert!(AtomFailure::Insert("x".into())
            .to_string()
            .starts_with("INSERT FAILURE"));
    }

    /// `examined == inserted + skipped_duplicate + lost()` is what makes
    /// `lost()` a trustworthy completeness signal rather than one more tally.
    #[test]
    fn counts_partition_everything_examined() {
        let c = AtomCounts {
            examined: 100,
            inserted: 60,
            skipped_duplicate: 30,
            blob_failures: 7,
            insert_failures: 3,
        };
        assert_eq!(c.examined, c.inserted + c.skipped_duplicate + c.lost());
    }

    // ── coding_sessions raw content never crosses the plane boundary ────────

    fn sample_source_session(
        raw_blob_key: Option<&str>,
        raw_inline: Option<&str>,
    ) -> CodingSessionRow {
        let now = Utc::now();
        CodingSessionRow {
            session_id: "cc-abc123".to_string(),
            project_key: Some("coralswarm".to_string()),
            title: Some("a session".to_string()),
            repo_path: Some("/home/dev/dev/coralswarm".to_string()),
            remote_sanitized: Some("github.com/x/y".to_string()),
            hostname: Some("laptop".to_string()),
            harness_version: Some("1.0".to_string()),
            model: Some("claude-sonnet-5".to_string()),
            started_at: now,
            last_activity_at: now,
            raw_blob_key: raw_blob_key.map(str::to_string),
            raw_inline: raw_inline.map(str::to_string),
            created_at: now,
        }
    }

    /// A source `raw_blob_key` is a CONSUMER-plane key. Copying it into the
    /// tenant is not merely useless (wrong DEK) — it is a live hazard, which
    /// this test's first half demonstrates against the REAL functions the
    /// member-ocean purge uses.
    #[test]
    fn source_raw_content_is_never_carried_into_the_tenant() {
        use coralswarm_core::r2::{assert_blob_key_org_scope, validate_blob_key};

        const TARGET_ORG: &str = "org_2ExampleTargetOrg0000000000";
        // The shape a consumer/personal-plane blob key actually has:
        // `{graph_id}/{atom_id}`, no org segment (see `r2::personal_blob_key`).
        let consumer_key = "93cc446a-3d5e-497e-ac36-cbccc42d1828/6d0ee5e2-4f5b-4a1e-8f9a-1c2d3e4f5061";

        // NEGATIVE CONTROL — what copying the key verbatim would do.
        // It is not malformed, so nothing catches it at write time...
        assert!(
            validate_blob_key(consumer_key).is_ok(),
            "the source key is perfectly well-formed — shape is not what makes it wrong"
        );
        // ...but `workers::member_ocean_reconciler` org-scope-asserts every
        // `coding_sessions.raw_blob_key` it finds for a graph before deleting
        // any blob, and ABORTS the whole purge "for operator review" on a key
        // that isn't this org's. A copied consumer key fails that forever.
        assert!(
            assert_blob_key_org_scope(consumer_key, Some(TARGET_ORG)).is_err(),
            "if a 2-segment consumer key ever passed the tenant's org-scope assert, the purge \
             argument above would be void and this policy would need re-deriving"
        );

        // THE FIX: neither column crosses, whatever the source held.
        for source in [
            sample_source_session(Some(consumer_key), None),
            sample_source_session(None, Some("full plaintext session transcript")),
            sample_source_session(Some(consumer_key), Some("full plaintext session transcript")),
        ] {
            let (blob_key, inline) = target_raw_content(&source);
            assert!(
                blob_key.is_none(),
                "raw_blob_key must be NULL on the target: a consumer-plane key is undecryptable \
                 under the org DEK and wedges the member-ocean purge"
            );
            assert!(
                inline.is_none(),
                "raw_inline must be NULL on the target: it is plaintext transcript, and writing \
                 it would put unencrypted content in the tenant database — the same rule the \
                 atoms path enforces by pinning content_inline to NULL"
            );
        }
    }

    /// A session with no raw content at all is unchanged by the policy — the
    /// rule drops raw content, it does not depend on there being any.
    #[test]
    fn a_session_with_no_raw_content_is_unaffected() {
        let (blob_key, inline) = target_raw_content(&sample_source_session(None, None));
        assert!(blob_key.is_none());
        assert!(inline.is_none());
    }

    // ── Every table reference is schema-qualified ───────────────────────────

    /// Commit d164a626 ("schema-qualify every table reference") did that by
    /// hand and missed exactly one line — the `accounts` lookup in
    /// [`source_dek_exists`], where every sibling query says `public.`. A
    /// one-word omission is precisely what a second hand-pass misses too, so
    /// this pins it mechanically over this file's own source.
    ///
    /// Why it matters here specifically: this binary runs against TWO
    /// databases via operator-supplied DSNs. An unqualified name resolves
    /// through whatever `search_path` that connection's role happens to have,
    /// so it can silently resolve a different table — or none at all, the
    /// same `relation "atoms" does not exist` class of failure that cost this
    /// migration 1,019 corals.
    ///
    /// Comment lines are skipped: SQL lives in string literals, and prose is
    /// allowed to wrap a keyword however it likes.
    #[test]
    fn every_sql_table_reference_is_schema_qualified() {
        const SRC: &str = include_str!("migrate_personal_to_member_ocean.rs");
        const KEYWORDS: [&str; 4] = ["FROM ", "INTO ", "UPDATE ", "JOIN "];

        let mut offenders: Vec<String> = Vec::new();
        for (i, line) in SRC.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            for kw in KEYWORDS {
                let mut rest = line;
                while let Some(at) = rest.find(kw) {
                    let tail = &rest[at + kw.len()..];
                    rest = tail;
                    let ident: String = tail
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
                        .collect();
                    // Anything that isn't a bare lowercase identifier is not a
                    // table reference (a quote, a paren, a bind placeholder,
                    // end of line) — nothing to qualify.
                    let is_bare_ident = ident
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_lowercase() || c == '_');
                    if !is_bare_ident {
                        continue;
                    }
                    if !ident.starts_with("public.") {
                        offenders.push(format!("line {}: `{kw}{ident}`", i + 1));
                    }
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "these table references resolve through search_path instead of being pinned to the \
             public schema — prefix each with the schema: {offenders:?}"
        );
    }
}

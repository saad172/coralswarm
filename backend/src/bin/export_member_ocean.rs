//! `export_member_ocean` — concierge-only export of ONE member's private
//! ocean out of an org's tenant database (member-private-oceans PR 9,
//! `docs/plans/member-private-oceans.md` §2.7 + §5's decisions table).
//!
//! **Why a Rust binary, and not pure shell:** atom (and meeting-transcript)
//! content is AES-GCM ciphertext under the org's DEK, itself KEK-wrapped
//! (`MASTER_KEK_HEX`, `crypto.rs:118-156`) and stored in the tenant's own
//! `account_keys.dek_wrapped`. Bash cannot unwrap that — this binary does the
//! crypto and the dump. `scripts/export_member_ocean.sh` is the
//! operator-facing wrapper: slug parity with Clerk, the typed confirmation
//! (this produces **decrypted plaintext of a person's private work**), and
//! resolving the operator's free-form `--email` input to a `users.id` UUID in
//! ONE guarded lookup — this binary never accepts an email at all, only the
//! already-resolved `--user-id` UUID, so it has no raw-SQL-interpolation
//! surface to guard in the first place (every query here is a bound sqlx
//! parameter).
//!
//! **CONCIERGE ONLY — no HTTP endpoint, no UI.** Self-serve export is
//! explicitly deferred (plan §2.7/§2.8): this is an operator running a
//! script, on demand, for one member at a time.
//!
//! **Refuses, before writing anything** (plan §5/§2.7):
//!   * no `tenant_databases` routing row for `--org`, or its status isn't
//!     `active`;
//!   * the org is in `org_revocations`;
//!   * no matching `kind='member'` graph for `--user-id` (+ the routing row's
//!     `account_id`, + `--graph-id` if given) — covers "already purged" and
//!     "wrong id" alike, since both look identical from here (zero rows);
//!   * more than one PRIMARY member ocean matches (possible for ARCHIVED rows
//!     — the one-primary-per-owner unique index is partial on `archived_at IS
//!     NULL`): the candidates are printed and the operator must name one with
//!     `--graph-id`, because exporting an arbitrary one would silently hand
//!     over half of a person's work as if it were all of it;
//!   * the ocean is not archived (`archived_at IS NULL`) — **a live member's
//!     ocean is never exportable.** Continuity for a sitting employee is the
//!     M-of-N escrow ceremony (ADR-0007 D6), not this tool;
//!   * the ocean was archived more than 60 days ago (the export window has
//!     closed; it may already be purge-eligible);
//!   * the org's DEK is not provisioned (`account_keys` row missing).
//!
//! All of the above happen before a `member_ocean_lifecycle_log` row is
//! written and before the output directory is created — a refusal never
//! touches disk or the control-plane audit table (there is nothing to log:
//! `member_ocean_lifecycle_log.graph_id` is `NOT NULL`, and these gates run
//! before a graph is definitively resolved as exportable).
//!
//! **Audit.** Once every gate passes, one `member_ocean_lifecycle_log` row is
//! written `started` (control-plane, migration `0092`), then updated to
//! `completed` (with a metadata-only manifest — the table's own CHECK
//! constraint enforces "identifiers, counts and digests, never content") or
//! `failed` (with an `error` string) once the dump finishes or errors. This
//! binary writes both phases directly — `DATABASE_URL` alone is sufficient
//! (unlike `migrate_control`/`migrate_tenants`, which need the elevated
//! `MIGRATOR_DATABASE_URL` role): `member_ocean_desired_state` and
//! `member_ocean_lifecycle_log` (migration 0092) were created AFTER
//! `0083_consumer_rls`'s `ALTER DEFAULT PRIVILEGES ... GRANT SELECT, INSERT,
//! UPDATE, DELETE ON TABLES TO coralswarm_app`, so the least-privilege role
//! already has read/write on them by default — neither table is on 0083's
//! narrow "revoke the write" list (`tenant_databases`, `escrow_shares`,
//! `_sqlx_migrations`). `MIGRATOR_DATABASE_URL` is still accepted (and
//! preferred when set) purely for operator consistency with the sibling
//! concierge tools, not because it is required here.
//!
//! **Bundle shape** (written under `<--out>/<org_slug>_<user_id>_<graph_id>_<utc>/`):
//!   * `manifest.json` — schema_version, org, user, graph, archived_at,
//!     counts, per-file sha256, exported_by, exported_at, tool version,
//!     audit_id, and the re-import non-promise (below).
//!   * `ocean.json` — the `graphs` row itself.
//!   * `atoms.ndjson` — every column **except** `embedding` (model-bound and
//!     regenerable — `embed_model`/`chunker_version` are recorded per row for
//!     a deterministic re-embed) and `content_tsv` (derived, FTS-only).
//!   * `nodes.ndjson`, `node_atoms.ndjson`, `relationship_edges.ndjson`,
//!     `node_promotions.ndjson` (source-side only — `kind='member'` is never
//!     a promotion target), `meeting_transcripts.ndjson`,
//!     `source_versions.ndjson`, `semantic_atoms.ndjson`,
//!     `semantic_atom_passages.ndjson`, `node_semantic_atoms.ndjson`,
//!     `semantic_atom_windows.ndjson`, `transcript_corrections.ndjson`, `conversations.ndjson`,
//!     `conversation_messages.ndjson`.
//!   * `blobs/<atom_id>` — DECRYPTED plaintext, one file per atom whose
//!     `content_blob_key` is set. Meeting-transcript raw blobs are
//!     deliberately NOT decrypted into `blobs/` in this version — see the PR
//!     description for the scope call (transcript rows still carry
//!     `raw_inline` when present, and `raw_blob_key` as a reference, including
//!     every retained source version). The manifest explicitly records this limitation.
//!
//! **Re-import is NOT guaranteed and NOT precluded.** No importer exists for
//! this bundle format as of this tool's version — that is stated once here
//! and again, verbatim, in every `manifest.json` this binary writes, so
//! nobody later claims a promise that was never made.
//!
//! Usage:
//!   export_member_ocean --org <slug|clerk_org_id> --user-id <uuid>
//!       [--user-email <email>] [--graph-id <uuid>] --out <dir>
//!       [--actor <label>] [--dry-run]
//!
//! `--dry-run` runs every refusal gate — including the DEK-provisioned one,
//! which is why that gate's presence check is deliberately ordered ABOVE the
//! dry-run return — and prints the row counts that WOULD be exported, but
//! writes nothing at all: no files, no audit row. A rehearsal that skipped a
//! gate would be worthless (clean preview, exit 3 on the real run). A dry run
//! still needs neither `MASTER_KEK_HEX` nor the R2 credentials: the gate asks
//! the tenant DB whether an `account_keys` row exists, and only a REAL run
//! unwraps the DEK or opens an R2 client.
//!
//! Env:
//!   DATABASE_URL / MIGRATOR_DATABASE_URL   control-plane DSN (see the audit
//!                                          note above for why either works).
//!   R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET_NAME
//!                                          required for a real (non
//!                                          --dry-run) run.
//!   MASTER_KEK_HEX / MASTER_KEK_HEX_NEXT   required for a real run (DEK
//!                                          unwrap — see `coralswarm_core::kek`).
//!   TENANT_DB_<SLUG>   the tenant DSN, resolved via `OperatorDsnResolver` from
//!                      the routing row's `db_secret_ref` — same convention
//!                      as `migrate_tenants`/`onboard_tenant.sh`.
//!
//! Exit codes: 0 success (including a `--dry-run` preview); 2 a usage/argument
//! error, or the control-plane DSN is unset — both caught before any gate has
//! run, and before any database connection is attempted; 3 a refusal gate
//! (routing inactive, org revoked, no matching member graph, not archived,
//! window expired, DEK not provisioned) rejected the export — nothing
//! written, no audit row; 4 every gate passed but a real-run
//! precondition/environment problem prevented starting the write (R2
//! credentials missing, the output path already exists, or a `mkdir`
//! failure) — nothing written, no audit row; 1 an unexpected runtime failure
//! — the control-plane or tenant DB connection, a count query, or (once a
//! `started` audit row exists) the dump itself failing, in which case that
//! row is updated to `failed` before this process exits.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use coralswarm_backend::crypto::resolve_dek_for_tenant;
use coralswarm_backend::migrator_database_url;
use coralswarm_backend::tenancy::routing::{self, TenantDatabase};
use coralswarm_backend::tenancy::secrets::OperatorDsnResolver;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_core::r2::{assert_blob_key_org_scope, R2Client};

/// The export window (plan §2.6/§5): archived, then exportable for exactly
/// this long before it is purge-eligible.
const EXPORT_WINDOW: Duration = Duration::days(60);

const TOOL_VERSION: &str = concat!("export_member_ocean/", env!("CARGO_PKG_VERSION"));

/// Exit code for a real-run environment/precondition problem that surfaces
/// AFTER every refusal gate has already passed (R2 credentials missing, the
/// output path already exists, or a `mkdir` failure) — distinct from `2`
/// (a usage/argument error or unset control-plane DSN, both caught BEFORE any
/// gate runs) so a caller scripting around this tool's exit code can tell
/// "you passed bad arguments" apart from "your gates passed but the
/// environment isn't ready to actually write."
const EXIT_POST_GATE_SETUP_ERROR: u8 = 4;

/// Bound on a single atom's `R2Client::get_atom_blob` call (network fetch +
/// AES-GCM decrypt). A one-shot concierge tool must not hang indefinitely on
/// a stalled connection to R2 — a bounded, atom-id-naming timeout error is
/// far more useful to an operator than a process that never returns.
const BLOB_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Manifest keys `member_ocean_lifecycle_log.manifest` accepts — MUST mirror
/// migration `0092`'s `member_ocean_lifecycle_log_manifest_metadata_only`
/// CHECK constraint exactly, or every write from this binary fails at the
/// database. Kept as an explicit constant (rather than "just don't add a bad
/// key") so a future column-set change in 0092 has one obvious place to
/// cross-check against, and ENFORCED by [`assert_control_manifest_keys`]
/// (called from both [`audit_started`] and [`audit_finish`]) rather than left
/// as a comment nobody re-checks — a caller gets a clear, named error here
/// instead of an opaque Postgres constraint violation.
const CONTROL_MANIFEST_ALLOWED_KEYS: &[&str] = &[
    "atoms",
    "nodes",
    "node_atoms",
    "blobs",
    "bytes",
    "relationship_edges",
    "transcripts",
    "conversations",
    "manifest_sha256",
    "export_id",
    "schema_version",
    "offboarded_at",
    "archived_at",
    "drift_seconds",
    "error",
];

/// Validate that every top-level key of `manifest` is in
/// [`CONTROL_MANIFEST_ALLOWED_KEYS`] — the same allowlist migration `0092`'s
/// `member_ocean_lifecycle_log_manifest_metadata_only` CHECK constraint
/// enforces at the database. Called before every `member_ocean_lifecycle_log`
/// write in this binary so a stray/renamed key fails loudly and locally
/// (naming the offending key) instead of as a bare constraint-violation error
/// from Postgres.
fn assert_control_manifest_keys(manifest: &Value) -> Result<(), String> {
    let obj = manifest
        .as_object()
        .ok_or_else(|| "control manifest must be a JSON object".to_string())?;
    for key in obj.keys() {
        if !CONTROL_MANIFEST_ALLOWED_KEYS.contains(&key.as_str()) {
            return Err(format!(
                "control manifest key '{key}' is not in CONTROL_MANIFEST_ALLOWED_KEYS — \
                 migration 0092's manifest CHECK constraint would reject this row. Add the key to \
                 both places (deliberately, and only for identifiers/counts/digests, never \
                 content), or remove it here."
            ));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Args {
    org: String,
    user_id: Uuid,
    user_email: Option<String>,
    graph_id: Option<Uuid>,
    out: PathBuf,
    actor: String,
    dry_run: bool,
}

const USAGE: &str = "usage: export_member_ocean --org <slug|clerk_org_id> --user-id <uuid> \
[--user-email <email>] [--graph-id <uuid>] --out <dir> [--actor <label>] [--dry-run]";

fn parse_args() -> Result<Args, String> {
    let mut org: Option<String> = None;
    let mut user_id: Option<Uuid> = None;
    let mut user_email: Option<String> = None;
    let mut graph_id: Option<Uuid> = None;
    let mut out: Option<PathBuf> = None;
    let mut actor = "operator:cli".to_string();
    let mut dry_run = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--org" => org = Some(args.next().ok_or("--org requires a value")?),
            "--user-id" => {
                let v = args.next().ok_or("--user-id requires a value")?;
                user_id =
                    Some(Uuid::parse_str(&v).map_err(|_| format!("--user-id: not a UUID: {v}"))?);
            }
            "--user-email" => {
                user_email = Some(args.next().ok_or("--user-email requires a value")?)
            }
            "--graph-id" => {
                let v = args.next().ok_or("--graph-id requires a value")?;
                graph_id =
                    Some(Uuid::parse_str(&v).map_err(|_| format!("--graph-id: not a UUID: {v}"))?);
            }
            "--out" => out = Some(PathBuf::from(args.next().ok_or("--out requires a value")?)),
            "--actor" => actor = args.next().ok_or("--actor requires a value")?,
            "--dry-run" => dry_run = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    Ok(Args {
        org: org.ok_or("--org is required")?,
        user_id: user_id.ok_or("--user-id is required")?,
        user_email,
        graph_id,
        out: out.ok_or("--out is required")?,
        actor,
        dry_run,
    })
}

/// A member-ocean `graphs` row, as read directly from the tenant DB. Mirrors
/// `migrations_tenant/0001_tenant_baseline.sql` + `0005_member_ocean_columns.sql`.
#[derive(Debug, Clone, Serialize, FromRow)]
struct GraphRow {
    id: Uuid,
    account_id: Uuid,
    name: String,
    created_at: DateTime<Utc>,
    kind: String,
    is_primary: bool,
    self_node_id: Option<Uuid>,
    owner_user_id: Option<Uuid>,
    archived_at: Option<DateTime<Utc>>,
}

/// `atoms` row, every column EXCEPT `embedding` (model-bound, regenerable)
/// and `content_tsv` (derived, FTS-only) — plan §2.7's explicit exclusion.
#[derive(Debug, Clone, Serialize, FromRow)]
struct AtomRow {
    atom_id: Uuid,
    graph_id: Uuid,
    source_type: String,
    source_id: String,
    content_blob_key: String,
    participants: Vec<String>,
    event_timestamp: DateTime<Utc>,
    ingested_by: Option<Uuid>,
    scope: String,
    created_at: DateTime<Utc>,
    embed_failed_at: Option<DateTime<Utc>>,
    content_inline: Option<String>,
    fts_language: String,
    channel_id: Option<String>,
    channel_name: Option<String>,
    author_id: Option<String>,
    author_name: Option<String>,
    workspace_id: Option<String>,
    embed_model: Option<String>,
    chunker_version: Option<i16>,
    embed_attempts: i32,
    header_len: Option<i32>,
    project: Option<String>,
    mcp_client_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct NodeRow {
    node_id: Uuid,
    graph_id: Uuid,
    node_type: String,
    title: String,
    body: Value,
    scope: String,
    owner_id: Option<Uuid>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    version: i32,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct NodeAtomRow {
    node_id: Uuid,
    atom_id: Uuid,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct RelationshipEdgeRow {
    edge_id: Uuid,
    graph_id: Uuid,
    src_node_id: Uuid,
    predicate: String,
    dst_node_id: Uuid,
    origin: String,
    status: String,
    confidence: f32,
    provenance: Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    version: i32,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct NodePromotionRow {
    id: Uuid,
    source_node_id: Uuid,
    target_graph_id: Uuid,
    target_node_id: Uuid,
    promoted_by: Option<Uuid>,
    from_version: i32,
    promoted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct MeetingTranscriptRow {
    current_version_id: Option<Uuid>,
    id: Uuid,
    graph_id: Uuid,
    source_id: String,
    raw_blob_key: Option<String>,
    raw_inline: Option<String>,
    duration_ms: Option<i64>,
    created_at: DateTime<Utc>,
    segments: Option<Value>,
    summary: Option<Value>,
    summary_status: String,
    summary_model: Option<String>,
    summary_error: Option<String>,
    summary_updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct TranscriptCorrectionRow {
    correction_id: Uuid,
    graph_id: Uuid,
    atom_id: Uuid,
    source_id: String,
    surface_form: String,
    canonical: String,
    node_id: Option<Uuid>,
    confidence: Option<f32>,
    status: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    kind: String,
    origin: String,
    occurrence: Option<i32>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct ConversationRow {
    id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
    user_id: Uuid,
    title: String,
    pinned: bool,
    shared: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    mode: String,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct ConversationMessageRow {
    id: Uuid,
    conversation_id: Uuid,
    seq: i32,
    role: String,
    text: String,
    citations: Value,
    sources: Value,
    mode: Option<String>,
    timing: Value,
    created_at: DateTime<Utc>,
    scope_graph_ids: Value,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Write one JSON value (pretty-printed) to a file that is created at mode
/// `0600` (see [`write_private_file`]) and return its sha256.
fn write_json_file<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<String> {
    let buf = serde_json::to_vec_pretty(value)
        .with_context(|| format!("serialize {}", path.display()))?;
    write_private_file(path, &buf)?;
    Ok(sha256_hex(&buf))
}

/// Write one newline-delimited-JSON file (one compact object per row) at mode
/// `0600` and return `(row_count, sha256)`. Same create-at-0600 reasoning as
/// [`write_private_file`].
fn write_ndjson_file<T: Serialize>(path: &Path, rows: &[T]) -> anyhow::Result<(usize, String)> {
    let mut buf = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut buf, row)
            .with_context(|| format!("serialize {}", path.display()))?;
        buf.push(b'\n');
    }
    write_private_file(path, &buf)?;
    Ok((rows.len(), sha256_hex(&buf)))
}

/// Create a DIRECTORY that has never existed at a mode wider than `0700`, and
/// that is guaranteed to be OURS.
///
/// Two properties, both load-bearing for a tool whose output is one named
/// person's decrypted private work:
///
/// 1. **Non-recursive, so it fails closed on collision.** `create_dir_all`
///    returns `Ok(())` for a directory that already exists, which would make
///    this binary silently ADOPT whatever is already sitting at the bundle
///    path (a concurrent second run, or a pre-planted `0777` directory a
///    local attacker created while waiting for a predictable bundle name) —
///    defeating the "refusing to overwrite" guarantee. `DirBuilder::create`
///    is one `mkdir(2)`: it returns `ErrorKind::AlreadyExists` instead. An
///    `exists()` pre-check followed by a create is a TOCTOU; this is the
///    check.
/// 2. **Mode set by the same syscall.** `mkdir(2)` applies the mode at
///    creation, so the directory is never briefly traversable by anyone else.
///    (`umask` can only narrow this further, never widen it.)
///
/// Parents are deliberately NOT created here — the caller creates `--out`
/// itself (ordinary modes: it is an operator-chosen parent, not the secret),
/// so this only ever creates the leaf it is asked for.
#[cfg(unix)]
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().mode(0o700).create(path)
}
#[cfg(not(unix))]
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new().create(path)?;
    eprintln!(
        "WARNING: non-Unix target — restrict {} to this user by hand before the export leaves \
         this machine",
        path.display()
    );
    Ok(())
}

/// Create a FILE at mode `0600` in a single `open(2)`, failing if anything is
/// already at `path`.
///
/// The mode is set **by the create call**, not by a `chmod` after the write:
/// `fs::write` would create the file at the umask default (typically `0644`,
/// i.e. group/world READABLE) and only narrow it afterward, leaving a window
/// in which decrypted plaintext of a person's private work exists on disk at
/// a readable mode. The parent directory being `0700` shrinks that window's
/// reachability but does not close it (a group-readable `--out` ancestor, a
/// process that already holds a descriptor on the directory, or a later move
/// of the bundle all defeat it). Creating with the mode set removes the
/// window entirely.
///
/// `create_new(true)` additionally means this never follows a pre-existing
/// symlink or truncates someone else's file at that path.
fn create_private_file(path: &Path) -> anyhow::Result<fs::File> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    #[cfg(not(unix))]
    eprintln!(
        "WARNING: non-Unix target — restrict {} to this user by hand before the export leaves \
         this machine",
        path.display()
    );
    Ok(file)
}

/// Write `bytes` to a file created at mode `0600` (see
/// [`create_private_file`]) and `fsync` it before returning.
///
/// Every byte this binary puts on disk — `ocean.json`, every `*.ndjson`,
/// `manifest.json`, and each `blobs/<atom_id>` — goes through here, so this
/// one function is where both of the bundle's on-disk guarantees live: no
/// file ever exists at a wider mode than `0600`, and no file is reported as
/// written until the kernel says it is durable.
///
/// **Why the explicit `sync_all`:** `File`'s `Drop` closes the descriptor and
/// *discards* the `close(2)` result. On a buffered filesystem the real write
/// error — ENOSPC is the realistic one here, since a bundle of decrypted blobs
/// can be large and this runs on an operator's laptop — can surface at flush
/// time, after `write_all` has already returned `Ok`. The caller would then
/// record a sha256 of the in-memory buffer in `manifest.json` for a file that
/// is truncated or absent on disk, and the runbook's own `shasum -a 256 -c`
/// step would fail on an export that reported success. Since the manifest's
/// digests are the integrity contract for this bundle, a silent divergence
/// between "what we hashed" and "what landed" is the one failure this helper
/// must not allow: `sync_all` forces the flush and propagates the error, so
/// the export fails loudly (and the failure path removes the partial bundle
/// and marks the audit row `failed`) instead of shipping a bundle that cannot
/// verify.
fn write_private_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let mut file = create_private_file(path)?;
    file.write_all(bytes)
        .with_context(|| format!("write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("fsync {}", path.display()))?;
    Ok(())
}

/// Try `--org` as a Clerk org id first (the common case — the shell wrapper
/// already verified slug parity), falling back to slug. Same two-step
/// `routing::lookup_by_org` / `lookup_by_slug` order `tenancy::migrate`'s
/// `select_tenants` uses for its own `--only` flag.
async fn resolve_routing_row(
    control: &PgPool,
    org_or_slug: &str,
) -> Result<TenantDatabase, String> {
    if let Some(row) = routing::lookup_by_org(control, org_or_slug)
        .await
        .map_err(|e| format!("routing lookup (by org): {e}"))?
    {
        return Ok(row);
    }
    routing::lookup_by_slug(control, org_or_slug)
        .await
        .map_err(|e| format!("routing lookup (by slug): {e}"))?
        .ok_or_else(|| format!("no tenant_databases routing row for '{org_or_slug}'"))
}

async fn assert_not_revoked(control: &PgPool, clerk_org_id: &str) -> Result<(), String> {
    let revoked: Option<(bool,)> =
        sqlx::query_as("SELECT true FROM org_revocations WHERE clerk_org_id = $1")
            .bind(clerk_org_id)
            .fetch_optional(control)
            .await
            .map_err(|e| format!("org_revocations lookup: {e}"))?;
    if revoked.is_some() {
        return Err(format!(
            "org {clerk_org_id} is revoked (org_revocations) — refusing to export any member \
             ocean for a revoked org"
        ));
    }
    Ok(())
}

async fn connect_tenant(resolver: &OperatorDsnResolver, row: &TenantDatabase) -> Result<PgPool, String> {
    // Environment first, then the KEK-encrypted tenant_db_credentials row
    // (self-serve tenants have no Fly secret; issue #1514). Labelled by slug:
    // the error never carries a DSN.
    let dsn = resolver
        .resolve(&row.db_secret_ref)
        .await
        .map_err(|e| format!("resolving tenant DSN for {}: {e}", row.slug))?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(15))
        .connect(&dsn)
        .await
        .map_err(|e| format!("connecting tenant DB for {}: {e}", row.slug))?;

    // Fingerprint check — same defense as `tenancy::migrate::run_migrate` and
    // `orgt_tenant_set_fingerprint`: refuse a DSN/routing-row mismatch, AND
    // refuse an entirely UNFINGERPRINTED database, rather than ever exporting
    // the WRONG (or an unverified) org's data under the right label.
    //
    // `WHERE id = true` (not `LIMIT 1`): matches `tenant_meta`'s own
    // singleton-row convention (`tenant_meta_singleton CHECK (id = true)`,
    // `migrations_tenant/0002_tenant_meta_and_account_keys.sql`) explicitly,
    // rather than an arbitrary `LIMIT 1` that would pick unpredictably if a
    // stray extra row ever existed.
    //
    // A missing fingerprint (`None`) MUST fail closed here, not fall through
    // to `Ok(pool)` — `orgt_tenant_set_fingerprint` already treats
    // missing-or-mismatched as fatal for exactly this reason (see that
    // function's own doc in `orgt_tenant_lib.sh`), and this binary must not
    // be laxer than the onboarding script it's meant to trust: an
    // unfingerprinted DSN gives no proof at all that it points at the org the
    // routing row claims.
    let fingerprint: Option<String> =
        sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
            .fetch_optional(&pool)
            .await
            .map_err(|e| format!("tenant_meta fingerprint read: {e}"))?;
    match fingerprint {
        Some(org) if org == row.clerk_org_id => Ok(pool),
        Some(org) => Err(format!(
            "tenant_meta fingerprint mismatch: routing row says {}, tenant DB says {org} — \
             refusing (DSN-swap guard)",
            row.clerk_org_id
        )),
        None => Err(format!(
            "tenant_meta has no fingerprint row for org {} — refusing (DSN-swap guard): a tenant \
             database must be fingerprinted (onboard_tenant.sh step 6 / \
             orgt_tenant_set_fingerprint) before anything can be exported from it",
            row.clerk_org_id
        )),
    }
}

/// Resolve the target member-ocean graph. `graph_id` filters to exactly one
/// graph id (must still be this owner's `kind='member'` row); its absence
/// defaults to the owner's PRIMARY member ocean. Both shapes return zero rows
/// identically for "wrong id", "not this owner's", "not kind='member'", and
/// "already purged" — callers cannot and need not distinguish those from here.
///
/// **The `is_primary` default REFUSES on more than one match rather than
/// picking.** `uq_graphs_one_primary_per_owner`
/// (`migrations_tenant/0005_member_ocean_columns.sql:74`) is a PARTIAL unique
/// index — `WHERE is_primary AND owner_user_id IS NOT NULL AND archived_at IS
/// NULL`. This tool only ever exports ARCHIVED oceans, i.e. exactly the rows
/// that index does NOT constrain: a member who was archived, re-added with a
/// fresh primary ocean, and archived again legitimately has two archived
/// `is_primary` rows. A `fetch_optional`/`LIMIT 1` there would hand an admin
/// one arbitrary half of a person's private work and call it the export, with
/// nothing on the bundle or in the audit row saying another ocean existed.
/// So: list the candidates and make the operator name one with `--graph-id`.
async fn resolve_member_graph(
    tenant: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    graph_id: Option<Uuid>,
) -> Result<GraphRow, String> {
    if let Some(gid) = graph_id {
        // `id` is the primary key — this branch can match at most one row.
        return sqlx::query_as::<_, GraphRow>(
            "SELECT id, account_id, name, created_at, kind, is_primary, self_node_id, \
                    owner_user_id, archived_at \
             FROM graphs \
             WHERE id = $1 AND account_id = $2 AND owner_user_id = $3 AND kind = 'member'",
        )
        .bind(gid)
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_optional(tenant)
        .await
        .map_err(|e| format!("graph lookup: {e}"))?
        .ok_or_else(|| {
            "no member ocean found matching --graph-id for this owner/account — wrong id, wrong \
             owner, not kind='member', or already purged"
                .to_string()
        });
    }

    let mut rows = sqlx::query_as::<_, GraphRow>(
        "SELECT id, account_id, name, created_at, kind, is_primary, self_node_id, \
                owner_user_id, archived_at \
         FROM graphs \
         WHERE account_id = $1 AND owner_user_id = $2 AND kind = 'member' AND is_primary \
         ORDER BY archived_at DESC NULLS FIRST, created_at DESC",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_all(tenant)
    .await
    .map_err(|e| format!("graph lookup: {e}"))?;

    match rows.len() {
        0 => Err(
            "no PRIMARY member ocean found for this owner — if you intend to export a SECONDARY \
             member ocean, pass --graph-id explicitly"
                .to_string(),
        ),
        1 => Ok(rows.remove(0)),
        n => {
            let candidates = rows
                .iter()
                .map(|g| {
                    format!(
                        "\n    {} (name={:?}, created_at={}, archived_at={})",
                        g.id,
                        g.name,
                        g.created_at,
                        g.archived_at
                            .map(|a| a.to_rfc3339())
                            .unwrap_or_else(|| "NULL (live)".into())
                    )
                })
                .collect::<String>();
            Err(format!(
                "{n} PRIMARY member oceans exist for this owner (the one-primary-per-owner unique \
                 index only covers NON-archived rows, so archived duplicates are possible):\
                 {candidates}\nrefusing to guess which one you meant — re-run with --graph-id \
                 naming exactly one of the ids above (and consider exporting each in turn: \
                 exporting one of them is NOT this person's complete ocean)"
            ))
        }
    }
}

/// The archived/window gate (plan §5/§2.7) — a live member's ocean is never
/// exportable, and the window is a fixed 60 days from `archived_at`.
fn check_archived_and_window(graph: &GraphRow) -> Result<DateTime<Utc>, String> {
    let Some(archived_at) = graph.archived_at else {
        return Err(
            "ocean is not archived — a LIVE member's ocean can never be exported. Continuity \
             for a current employee is the M-of-N escrow ceremony (ADR-0007 D6), not this tool"
                .to_string(),
        );
    };
    let deadline = archived_at + EXPORT_WINDOW;
    if Utc::now() > deadline {
        return Err(format!(
            "the 60-day export window has closed (archived_at = {archived_at}, deadline was \
             {deadline}) — this ocean may already be purge-eligible"
        ));
    }
    Ok(archived_at)
}

struct Counts {
    atoms: i64,
    nodes: i64,
    node_atoms: i64,
    relationship_edges: i64,
    node_promotions: i64,
    meeting_transcripts: i64,
    meeting_evidence: std::collections::BTreeMap<&'static str, i64>,
    transcript_corrections: i64,
    conversations: i64,
    conversation_messages: i64,
}

async fn preview_counts(tenant: &PgPool, graph_id: Uuid) -> Result<Counts, String> {
    async fn one(pool: &PgPool, sql: &str, graph_id: Uuid) -> Result<i64, String> {
        sqlx::query_scalar(sql)
            .bind(graph_id)
            .fetch_one(pool)
            .await
            .map_err(|e| format!("count query failed ({sql}): {e}"))
    }
    let mut meeting_evidence = std::collections::BTreeMap::new();
    for (table, _) in MEETING_EVIDENCE_EXPORTS {
        let count = one(tenant, &format!("SELECT COUNT(*) FROM {table} WHERE graph_id = $1"), graph_id).await?;
        meeting_evidence.insert(*table, count);
    }
    Ok(Counts {
        meeting_evidence,
        atoms: one(
            tenant,
            "SELECT COUNT(*) FROM atoms WHERE graph_id = $1",
            graph_id,
        )
        .await?,
        nodes: one(
            tenant,
            "SELECT COUNT(*) FROM nodes WHERE graph_id = $1",
            graph_id,
        )
        .await?,
        node_atoms: one(
            tenant,
            "SELECT COUNT(*) FROM node_atoms na JOIN nodes n ON n.node_id = na.node_id \
             WHERE n.graph_id = $1",
            graph_id,
        )
        .await?,
        relationship_edges: one(
            tenant,
            "SELECT COUNT(*) FROM relationship_edge WHERE graph_id = $1",
            graph_id,
        )
        .await?,
        node_promotions: one(
            tenant,
            "SELECT COUNT(*) FROM node_promotions p JOIN nodes n ON n.node_id = p.source_node_id \
             WHERE n.graph_id = $1",
            graph_id,
        )
        .await?,
        meeting_transcripts: one(
            tenant,
            "SELECT COUNT(*) FROM meeting_transcripts WHERE graph_id = $1",
            graph_id,
        )
        .await?,
        transcript_corrections: one(
            tenant,
            "SELECT COUNT(*) FROM transcript_corrections WHERE graph_id = $1",
            graph_id,
        )
        .await?,
        conversations: one(
            tenant,
            "SELECT COUNT(*) FROM conversations WHERE graph_id = $1",
            graph_id,
        )
        .await?,
        conversation_messages: one(
            tenant,
            "SELECT COUNT(*) FROM conversation_messages cm \
             JOIN conversations c ON c.id = cm.conversation_id WHERE c.graph_id = $1",
            graph_id,
        )
        .await?,
    })
}

/// Gate 5's dry-run-safe half: does this org have a provisioned DEK at all?
///
/// Mirrors the `TenantId::Org(_)` branch of `crypto::fetch_wrapped_dek`'s
/// predicate (`account_keys` on the TENANT plane, keyed by the routing row's
/// `account_id`) but deliberately selects a constant rather than
/// `dek_wrapped`: the answer this gate needs is "is provisioning complete",
/// and a preview has no business pulling wrapped key material into the
/// process at all. The real unwrap (`resolve_dek_for_tenant`, which needs
/// `MASTER_KEK_HEX`) still happens on the real-run path.
async fn dek_row_exists(tenant: &PgPool, account_id: Uuid) -> Result<bool, String> {
    let found: Option<(bool,)> =
        sqlx::query_as("SELECT true FROM account_keys WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(tenant)
            .await
            .map_err(|e| format!("account_keys lookup: {e}"))?;
    Ok(found.is_some())
}

/// Insert the `started` audit row. Only ever called once every refusal gate
/// has passed and we are certain we are about to write a real bundle.
///
/// `export_id` is the SAME `Uuid` value carried in `manifest.json`'s own
/// `export_id` field and in the `completed`/`failed` manifests
/// [`audit_finish`] writes later — it is the one identifier meant to
/// correlate a bundle on disk with its control-plane audit trail across all
/// three phases. (An earlier version of this function took the bundle
/// DIRECTORY NAME here instead, which meant a `completed` row's manifest —
/// which `audit_finish` replaces wholesale — carried a different `export_id`
/// value than the `started` row that preceded it, defeating the two-phase
/// audit's whole point. The directory name still appears in `manifest.json`
/// on disk; it has no allowed key of its own in the control-plane manifest,
/// see [`CONTROL_MANIFEST_ALLOWED_KEYS`].)
#[allow(clippy::too_many_arguments)]
async fn audit_started(
    control: &PgPool,
    clerk_org_id: &str,
    subject_user_id: Uuid,
    graph_id: Uuid,
    actor: &str,
    export_id: Uuid,
    schema_version: Option<i64>,
) -> Result<Uuid, String> {
    let manifest = json!({ "export_id": export_id, "schema_version": schema_version });
    assert_control_manifest_keys(&manifest)?;
    sqlx::query_scalar(
        "INSERT INTO member_ocean_lifecycle_log \
            (action, status, clerk_org_id, subject_user_id, graph_id, actor, manifest) \
         VALUES ('export', 'started', $1, $2, $3, $4, $5) \
         RETURNING id",
    )
    .bind(clerk_org_id)
    .bind(subject_user_id)
    .bind(graph_id)
    .bind(actor)
    .bind(manifest)
    .fetch_one(control)
    .await
    .map_err(|e| format!("writing 'started' audit row: {e}"))
}

async fn audit_finish(
    control: &PgPool,
    audit_id: Uuid,
    status: &str,
    manifest: Value,
) -> Result<(), String> {
    assert_control_manifest_keys(&manifest)?;
    sqlx::query(
        "UPDATE member_ocean_lifecycle_log SET status = $1, manifest = $2, finished_at = now() \
         WHERE id = $3",
    )
    .bind(status)
    .bind(manifest)
    .bind(audit_id)
    .execute(control)
    .await
    .map(|_| ())
    .map_err(|e| format!("writing '{status}' audit row (id={audit_id}): {e}"))
}

/// Everything `run_export` produces that the caller (`main`, which owns the
/// control-plane pool) needs to finish the audit trail and report to the
/// operator.
struct ExportOutcome {
    manifest_path: PathBuf,
    /// Metadata-only subset for `member_ocean_lifecycle_log.manifest` — see
    /// [`CONTROL_MANIFEST_ALLOWED_KEYS`].
    control_manifest: Value,
}

#[tokio::main]
async fn main() -> ExitCode {
    // No tracing subscriber: nothing this binary calls (crypto/tenancy/R2)
    // emits a `tracing::` event, so initializing one here would configure a
    // dispatcher for events that never arrive — dead setup, not observability.
    // Every operator-facing message below is plain stdout/stderr, which is
    // the right UX for a rare, human-run concierge tool (unlike
    // `migrate_control`/`migrate_tenants`, which keep a subscriber because
    // the LIBRARY code they call, e.g. `tenancy::migrate::process_one`, does
    // emit real `tracing::warn!` events).
    let args = match parse_args() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let control_url = match migrator_database_url() {
        Some(u) => u,
        None => {
            eprintln!(
                "DATABASE_URL (or MIGRATOR_DATABASE_URL) must be set to the control-plane DSN"
            );
            return ExitCode::from(2);
        }
    };
    let control = match PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(15))
        .connect(&control_url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect control-plane database: {e}");
            return ExitCode::FAILURE;
        }
    };

    // ── Gate 1: routing row exists and is active ─────────────────────────────
    let row = match resolve_routing_row(&control, &args.org).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("REFUSED: {e}");
            return ExitCode::from(3);
        }
    };
    if !row.is_servable() {
        eprintln!(
            "REFUSED: tenant routing row for {} has status '{}', not 'active'",
            row.clerk_org_id, row.status
        );
        return ExitCode::from(3);
    }

    // ── Gate 2: org not revoked ───────────────────────────────────────────────
    if let Err(e) = assert_not_revoked(&control, &row.clerk_org_id).await {
        eprintln!("REFUSED: {e}");
        return ExitCode::from(3);
    }

    // ── Connect to the tenant DB (fingerprint-verified) ──────────────────────
    let resolver = OperatorDsnResolver::from_env_or_env_only(control.clone(), "export_member_ocean");
    let tenant = match connect_tenant(&resolver, &row).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return ExitCode::FAILURE;
        }
    };

    // ── Gate 3: a matching kind='member' graph exists for this owner ────────
    let graph =
        match resolve_member_graph(&tenant, row.account_id, args.user_id, args.graph_id).await {
            Ok(g) => g,
            Err(e) => {
                eprintln!("REFUSED: {e}");
                return ExitCode::from(3);
            }
        };

    // ── Gate 4: archived, and inside the 60-day window ───────────────────────
    let archived_at = match check_archived_and_window(&graph) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("REFUSED: {e}");
            return ExitCode::from(3);
        }
    };

    let counts = match preview_counts(&tenant, graph.id).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return ExitCode::FAILURE;
        }
    };

    // ── Gate 5: the org's DEK is provisioned ─────────────────────────────────
    // Deliberately ABOVE the dry-run return: a rehearsal that skips a refusal
    // gate is a rehearsal of the wrong thing — the operator would get a clean
    // preview and then eat exit 3 on the real run. This half only asks the
    // tenant DB "is there an account_keys row", which needs no KEK and pulls
    // no key material into this process, so it costs a dry run nothing (in
    // particular it does NOT make MASTER_KEK_HEX a dry-run prerequisite). The
    // real unwrap happens below, real-run only.
    match dek_row_exists(&tenant, row.account_id).await {
        Ok(true) => {}
        Ok(false) => {
            eprintln!("REFUSED: org DEK not provisioned (no account_keys row) — nothing exported");
            return ExitCode::from(3);
        }
        Err(e) => {
            eprintln!("ERROR: {e}");
            return ExitCode::FAILURE;
        }
    }

    if args.dry_run {
        println!(
            "DRY RUN — would export org={} user={} graph={} (archived_at={archived_at}):",
            row.clerk_org_id, args.user_id, graph.id
        );
        println!("  atoms:                  {}", counts.atoms);
        println!("  nodes:                  {}", counts.nodes);
        println!("  node_atoms:             {}", counts.node_atoms);
        println!("  relationship_edges:     {}", counts.relationship_edges);
        println!("  node_promotions:        {}", counts.node_promotions);
        println!("  meeting_transcripts:    {}", counts.meeting_transcripts);
        for (table, count) in &counts.meeting_evidence {
            println!("  {table}: {count}");
        }
        println!(
            "  transcript_corrections: {}",
            counts.transcript_corrections
        );
        println!("  conversations:          {}", counts.conversations);
        println!("  conversation_messages:  {}", counts.conversation_messages);
        println!("no files written (dry run); no audit row written.");
        return ExitCode::SUCCESS;
    }

    // ── Real run only: actually unwrap the DEK (needs MASTER_KEK_HEX) ────────
    // Gate 5's *presence* half already ran above (before the dry-run return).
    // `Ok(None)` here means the `account_keys` row disappeared between the two
    // — still a refusal, never a fallback.
    let dek = match resolve_dek_for_tenant(
        &TenantId::Org(row.clerk_org_id.clone()),
        &tenant,
        row.account_id,
    )
    .await
    {
        Ok(Some(k)) => k,
        Ok(None) => {
            eprintln!("REFUSED: org DEK not provisioned (no account_keys row) — nothing exported");
            return ExitCode::from(3);
        }
        Err(e) => {
            eprintln!("ERROR: DEK resolution failed: {e}");
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
                 R2_SECRET_ACCESS_KEY / R2_BUCKET_NAME) — required for a real (non-dry-run) export"
            );
            return ExitCode::from(EXIT_POST_GATE_SETUP_ERROR);
        }
    };

    // ── Everything past this point is the real, file-writing export ─────────
    // `export_id` is generated ONCE here and is the SAME value used as the
    // "export_id" key in every manifest this run writes (control-plane
    // started/completed/failed AND the on-disk manifest.json) — the
    // correlating identifier across the whole export's lifecycle. It is
    // deliberately distinct from `bundle_name` (the directory name, which has
    // no allowed key of its own on the control plane — see
    // CONTROL_MANIFEST_ALLOWED_KEYS).
    let export_id = Uuid::new_v4();
    let utc_stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let bundle_name = format!("{}_{}_{}_{}", row.slug, args.user_id, graph.id, utc_stamp);
    let bundle_path = args.out.join(&bundle_name);

    // The operator-chosen parent (`--out`) is created recursively and with
    // ordinary modes — it is a container, not the secret. The BUNDLE itself is
    // created non-recursively at 0700 in one syscall, so "already exists" is a
    // hard refusal rather than a silent adoption of someone else's directory,
    // and the mode is never briefly wider (see `create_private_dir`).
    if let Err(e) = fs::create_dir_all(&args.out) {
        eprintln!("ERROR: creating {}: {e}", args.out.display());
        return ExitCode::from(EXIT_POST_GATE_SETUP_ERROR);
    }
    if let Err(e) = create_private_dir(&bundle_path) {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            eprintln!(
                "ERROR: output path already exists: {} — refusing to overwrite",
                bundle_path.display()
            );
        } else {
            eprintln!("ERROR: creating {}: {e}", bundle_path.display());
        }
        return ExitCode::from(EXIT_POST_GATE_SETUP_ERROR);
    }
    let blobs_dir = bundle_path.join("blobs");
    if let Err(e) = create_private_dir(&blobs_dir) {
        eprintln!("ERROR: creating {}: {e}", blobs_dir.display());
        return ExitCode::from(EXIT_POST_GATE_SETUP_ERROR);
    }

    let audit_id = match audit_started(
        &control,
        &row.clerk_org_id,
        args.user_id,
        graph.id,
        &args.actor,
        export_id,
        row.schema_version,
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return ExitCode::FAILURE;
        }
    };

    match run_export(
        &tenant,
        &r2,
        &dek,
        &row,
        &graph,
        &args,
        &bundle_path,
        &blobs_dir,
        audit_id,
        export_id,
        archived_at,
    )
    .await
    {
        Ok(outcome) => {
            if let Err(e) =
                audit_finish(&control, audit_id, "completed", outcome.control_manifest).await
            {
                eprintln!(
                    "WARNING: export succeeded but the 'completed' audit write failed: {e} — the \
                     lifecycle log row for audit_id={audit_id} is stuck at 'started'; verify by \
                     hand"
                );
            }
            println!("export complete: {}", bundle_path.display());
            println!("manifest: {}", outcome.manifest_path.display());
            println!(
                "REMINDER: this directory contains DECRYPTED plaintext of a person's private \
                 work. Its directories were created 0700 and its files 0600, but you must encrypt \
                 it at rest and delete the plaintext before it leaves this machine (same posture \
                 as offboard_tenant.sh step 2)."
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            // The failure mode here is DISCLOSURE, not inconvenience: a
            // partial run may already have decrypted one or more
            // `blobs/<atom_id>` files to disk before hitting whatever failed.
            // Try to remove the whole partial bundle; if that itself fails,
            // print the SAME strength warning the success path prints
            // (naming the path) so an operator is never left unaware that
            // decrypted plaintext might be sitting under `--out`.
            eprintln!("ERROR during export: {e:#}");
            match fs::remove_dir_all(&bundle_path) {
                Ok(()) => eprintln!(
                    "the partial bundle at {} (which may have contained decrypted plaintext) was \
                     removed after the failure above.",
                    bundle_path.display()
                ),
                Err(cleanup_err) => eprintln!(
                    "WARNING: failed to remove the partial bundle at {} after the failure above \
                     ({cleanup_err}) — it may contain DECRYPTED plaintext of a person's private \
                     work left behind by the failed run. Delete it by hand, or encrypt it at rest \
                     before it leaves this machine (same posture as the success-path reminder / \
                     offboard_tenant.sh step 2).",
                    bundle_path.display()
                ),
            }
            let manifest = json!({
                "error": format!("{e:#}"),
                "export_id": export_id,
                "schema_version": row.schema_version,
            });
            if let Err(audit_err) = audit_finish(&control, audit_id, "failed", manifest).await {
                eprintln!("ERROR: additionally failed to record the audit failure: {audit_err}");
            }
            ExitCode::FAILURE
        }
    }
}

// Static table/query allowlist: never interpolate a user-provided identifier.
// Each evidence relation carries graph_id and its own foreign keys, so exporting
// one graph preserves the source/version/passage/node chain without reading a
// neighboring member's content. Column names are explicit so a later schema
// addition cannot silently widen the private-data export contract.
const MEETING_EVIDENCE_EXPORTS: &[(&str, &str)] = &[
    ("source_items", "SELECT to_jsonb(s) FROM (SELECT source_item_id,graph_id,source_type,provider_namespace,source_id,current_version_id,created_at,updated_at,origin FROM source_items WHERE graph_id=$1 ORDER BY source_item_id) s"),
    ("source_version_chunks", "SELECT to_jsonb(c) FROM (SELECT graph_id,source_version_id,atom_id FROM source_version_chunks WHERE graph_id=$1 ORDER BY source_version_id,atom_id) c"),
    ("source_versions", "SELECT to_jsonb(v) FROM (SELECT version_id, graph_id, source_item_id, source_id, source_type, provider_namespace, representation, source_metadata, origin, occurred_at, participants, ingested_by, scope, fingerprint_sha256, extraction_version, captured_at, is_legacy_baseline, raw_blob_key, raw_inline, content_sha256, duration_ms, segments, extraction_status, extraction_error, next_start_char FROM source_versions WHERE graph_id=$1 ORDER BY captured_at,version_id) v"),
    ("semantic_atoms", "SELECT to_jsonb(a) FROM (SELECT semantic_atom_id, graph_id, source_version_id, ordinal, kind, statement, confidence, extraction_version, model, entities, created_at FROM semantic_atoms WHERE graph_id=$1 ORDER BY source_version_id,ordinal,semantic_atom_id) a"),
    ("semantic_atom_passages", "SELECT to_jsonb(p) FROM (SELECT passage_id, semantic_atom_id, source_version_id, graph_id, start_char, end_char, quote, start_ms, end_ms, speaker FROM semantic_atom_passages WHERE graph_id=$1 ORDER BY semantic_atom_id,start_char,end_char) p"),
    ("node_semantic_atoms", "SELECT to_jsonb(n) FROM (SELECT node_id, semantic_atom_id, graph_id FROM node_semantic_atoms WHERE graph_id=$1 ORDER BY node_id,semantic_atom_id) n"),
    ("semantic_atom_windows", "SELECT to_jsonb(w) FROM (SELECT source_version_id, graph_id, extraction_version, start_char, end_char, completed_at, model, job_id FROM semantic_atom_windows WHERE graph_id=$1 ORDER BY source_version_id,extraction_version,start_char) w"),
];

async fn export_meeting_evidence(
    tenant: &PgPool,
    graph_id: Uuid,
    bundle_path: &Path,
    files_sha256: &mut serde_json::Map<String, Value>,
) -> anyhow::Result<serde_json::Map<String, Value>> {
    let mut counts = serde_json::Map::new();
    for (table, sql) in MEETING_EVIDENCE_EXPORTS {
        let rows: Vec<Value> = sqlx::query_scalar(sql)
            .bind(graph_id)
            .fetch_all(tenant)
            .await
            .with_context(|| format!("reading {table}"))?;
        let filename = format!("{table}.ndjson");
        let (count, digest) = write_ndjson_file(&bundle_path.join(&filename), &rows)?;
        counts.insert((*table).to_string(), json!(count));
        files_sha256.insert(filename, json!(digest));
    }
    Ok(counts)
}

#[allow(clippy::too_many_arguments)]
async fn run_export(
    tenant: &PgPool,
    r2: &R2Client,
    dek: &[u8; 32],
    row: &TenantDatabase,
    graph: &GraphRow,
    args: &Args,
    bundle_path: &Path,
    blobs_dir: &Path,
    audit_id: Uuid,
    export_id: Uuid,
    archived_at: DateTime<Utc>,
) -> anyhow::Result<ExportOutcome> {
    let graph_id = graph.id;
    let mut files_sha256 = serde_json::Map::new();

    let ocean_sha = write_json_file(&bundle_path.join("ocean.json"), graph)?;
    files_sha256.insert("ocean.json".into(), json!(ocean_sha));

    let atoms: Vec<AtomRow> = sqlx::query_as(
        "SELECT atom_id, graph_id, source_type, source_id, content_blob_key, participants, \
                \"timestamp\" AS event_timestamp, ingested_by, scope, created_at, embed_failed_at, \
                content_inline, fts_language, channel_id, channel_name, author_id, author_name, \
                workspace_id, embed_model, chunker_version, embed_attempts, header_len, project, \
                mcp_client_name \
         FROM atoms WHERE graph_id = $1 ORDER BY created_at",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading atoms")?;

    // Decrypt each atom's blob (if any) BEFORE writing atoms.ndjson, so a
    // mid-loop R2/crypto failure leaves no partially-written ndjson claiming
    // more blobs exist than actually got decrypted.
    let mut blob_count: u64 = 0;
    let mut blob_bytes_total: u64 = 0;
    let mut blobs_sha256 = serde_json::Map::new();
    for atom in &atoms {
        let key = atom.content_blob_key.trim();
        if key.is_empty() {
            continue;
        }
        assert_blob_key_org_scope(key, Some(&row.clerk_org_id))
            .with_context(|| format!("atom {} blob key failed org-scope assert", atom.atom_id))?;
        // Bounded: a hung R2 connection must not wedge this one-shot tool
        // indefinitely — it should fail loudly (naming the atom) so the
        // failure path below can clean up and record it, not hang forever.
        let plaintext = tokio::time::timeout(BLOB_FETCH_TIMEOUT, r2.get_atom_blob(key, dek))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "timed out after {BLOB_FETCH_TIMEOUT:?} fetching/decrypting blob for atom {}",
                    atom.atom_id
                )
            })?
            .with_context(|| format!("fetching/decrypting blob for atom {}", atom.atom_id))?;
        let blob_path = blobs_dir.join(atom.atom_id.to_string());
        // Created at 0600 by the same syscall that creates it — decrypted
        // plaintext never exists on disk at a readable mode, not even
        // momentarily (see `create_private_file`).
        write_private_file(&blob_path, &plaintext)?;
        blob_count += 1;
        blob_bytes_total += plaintext.len() as u64;
        blobs_sha256.insert(atom.atom_id.to_string(), json!(sha256_hex(&plaintext)));
    }

    let (atoms_n, atoms_sha) = write_ndjson_file(&bundle_path.join("atoms.ndjson"), &atoms)?;
    files_sha256.insert("atoms.ndjson".into(), json!(atoms_sha));

    let nodes: Vec<NodeRow> = sqlx::query_as(
        "SELECT node_id, graph_id, type AS node_type, title, body, scope, owner_id, created_at, \
                updated_at, version \
         FROM nodes WHERE graph_id = $1 ORDER BY created_at",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading nodes")?;
    let (nodes_n, nodes_sha) = write_ndjson_file(&bundle_path.join("nodes.ndjson"), &nodes)?;
    files_sha256.insert("nodes.ndjson".into(), json!(nodes_sha));

    let node_atoms: Vec<NodeAtomRow> = sqlx::query_as(
        "SELECT na.node_id, na.atom_id FROM node_atoms na \
         JOIN nodes n ON n.node_id = na.node_id WHERE n.graph_id = $1",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading node_atoms")?;
    let (node_atoms_n, node_atoms_sha) =
        write_ndjson_file(&bundle_path.join("node_atoms.ndjson"), &node_atoms)?;
    files_sha256.insert("node_atoms.ndjson".into(), json!(node_atoms_sha));

    let edges: Vec<RelationshipEdgeRow> = sqlx::query_as(
        "SELECT edge_id, graph_id, src_node_id, predicate, dst_node_id, origin, status, \
                confidence, provenance, created_at, updated_at, version \
         FROM relationship_edge WHERE graph_id = $1 ORDER BY created_at",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading relationship_edge")?;
    let (edges_n, edges_sha) =
        write_ndjson_file(&bundle_path.join("relationship_edges.ndjson"), &edges)?;
    files_sha256.insert("relationship_edges.ndjson".into(), json!(edges_sha));

    // Source-side only — `kind='member'` is never a promotion TARGET (plan §2.5).
    let promotions: Vec<NodePromotionRow> = sqlx::query_as(
        "SELECT p.id, p.source_node_id, p.target_graph_id, p.target_node_id, p.promoted_by, \
                p.from_version, p.promoted_at \
         FROM node_promotions p JOIN nodes n ON n.node_id = p.source_node_id \
         WHERE n.graph_id = $1 ORDER BY p.promoted_at",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading node_promotions")?;
    let (promotions_n, promotions_sha) =
        write_ndjson_file(&bundle_path.join("node_promotions.ndjson"), &promotions)?;
    files_sha256.insert("node_promotions.ndjson".into(), json!(promotions_sha));

    let transcripts: Vec<MeetingTranscriptRow> = sqlx::query_as(
        "SELECT id, graph_id, source_id, current_version_id, raw_blob_key, raw_inline, duration_ms, created_at, \
                segments, summary, summary_status, summary_model, summary_error, \
                summary_updated_at \
         FROM meeting_transcripts WHERE graph_id = $1 ORDER BY created_at",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading meeting_transcripts")?;
    let (transcripts_n, transcripts_sha) = write_ndjson_file(
        &bundle_path.join("meeting_transcripts.ndjson"),
        &transcripts,
    )?;
    files_sha256.insert("meeting_transcripts.ndjson".into(), json!(transcripts_sha));

    let meeting_evidence_counts = export_meeting_evidence(tenant, graph_id, bundle_path, &mut files_sha256).await?;

    let corrections: Vec<TranscriptCorrectionRow> = sqlx::query_as(
        "SELECT correction_id, graph_id, atom_id, source_id, surface_form, canonical, node_id, \
                confidence, status, created_at, updated_at, kind, origin, occurrence \
         FROM transcript_corrections WHERE graph_id = $1 ORDER BY created_at",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading transcript_corrections")?;
    let (corrections_n, corrections_sha) = write_ndjson_file(
        &bundle_path.join("transcript_corrections.ndjson"),
        &corrections,
    )?;
    files_sha256.insert(
        "transcript_corrections.ndjson".into(),
        json!(corrections_sha),
    );

    let conversations: Vec<ConversationRow> = sqlx::query_as(
        "SELECT id, account_id, graph_id, user_id, title, pinned, shared, created_at, \
                updated_at, mode \
         FROM conversations WHERE graph_id = $1 ORDER BY created_at",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading conversations")?;
    let (conversations_n, conversations_sha) =
        write_ndjson_file(&bundle_path.join("conversations.ndjson"), &conversations)?;
    files_sha256.insert("conversations.ndjson".into(), json!(conversations_sha));

    let messages: Vec<ConversationMessageRow> = sqlx::query_as(
        "SELECT cm.id, cm.conversation_id, cm.seq, cm.role, cm.text, cm.citations, cm.sources, \
                cm.mode, cm.timing, cm.created_at, cm.scope_graph_ids \
         FROM conversation_messages cm JOIN conversations c ON c.id = cm.conversation_id \
         WHERE c.graph_id = $1 ORDER BY cm.conversation_id, cm.seq",
    )
    .bind(graph_id)
    .fetch_all(tenant)
    .await
    .context("reading conversation_messages")?;
    let (messages_n, messages_sha) =
        write_ndjson_file(&bundle_path.join("conversation_messages.ndjson"), &messages)?;
    files_sha256.insert("conversation_messages.ndjson".into(), json!(messages_sha));

    let manifest = json!({
        "schema_version": row.schema_version,
        "tool_version": TOOL_VERSION,
        "audit_id": audit_id,
        "export_id": export_id,
        "exported_by": args.actor,
        "exported_at": Utc::now(),
        "org": { "clerk_org_id": row.clerk_org_id, "slug": row.slug },
        "user": { "id": args.user_id, "email": args.user_email },
        "graph": {
            "id": graph.id,
            "name": graph.name,
            "kind": graph.kind,
            "is_primary": graph.is_primary,
            "created_at": graph.created_at,
            "archived_at": archived_at,
        },
        "counts": {
            "atoms": atoms_n,
            "nodes": nodes_n,
            "node_atoms": node_atoms_n,
            "relationship_edges": edges_n,
            "node_promotions": promotions_n,
            "meeting_transcripts": transcripts_n,
            "source_items": meeting_evidence_counts["source_items"],
            "source_version_chunks": meeting_evidence_counts["source_version_chunks"],
            "source_versions": meeting_evidence_counts["source_versions"],
            "semantic_atoms": meeting_evidence_counts["semantic_atoms"],
            "semantic_atom_passages": meeting_evidence_counts["semantic_atom_passages"],
            "node_semantic_atoms": meeting_evidence_counts["node_semantic_atoms"],
            "semantic_atom_windows": meeting_evidence_counts["semantic_atom_windows"],
            "transcript_corrections": corrections_n,
            "conversations": conversations_n,
            "conversation_messages": messages_n,
            "blobs": blob_count,
        },
        "bytes": { "blobs_plaintext_total": blob_bytes_total },
        "files_sha256": Value::Object(files_sha256),
        "blobs_sha256": Value::Object(blobs_sha256),
        "excluded_fields": {
            "atoms": ["embedding", "content_tsv"],
            "meeting_raw_blobs": "Raw meeting blobs are referenced by raw_blob_key but are not downloaded or decrypted into this bundle. raw_inline is included for each retained source version when available.",
            "note": "embedding is model-bound and regenerable — embed_model/chunker_version are \
                     recorded per atom row for a deterministic re-embed. content_tsv is derived \
                     (full-text search only).",
        },
        "reimport_note": "Re-import is NOT guaranteed and NOT precluded — no importer exists for \
                          this bundle format as of this tool's version.",
    });

    // ORDERING IS LOAD-BEARING: manifest.json is written LAST, after every
    // file it describes has already been written AND fsynced by
    // `write_private_file`. The manifest is the thing that asserts "these
    // digests describe these files", so it must never become durable before
    // its subjects — a manifest that outlives a data file it names is a
    // bundle that fails `shasum -c` on an otherwise-good export, which is
    // exactly the direction that makes a verification failure look like
    // tampering. Data first, then the claim about the data. (The
    // control-plane `completed` row, whose manifest carries `manifest_sha256`,
    // is written later still — see `audit_finish` in `main` — so the audit
    // trail never claims success ahead of the bytes either.)
    let manifest_path = bundle_path.join("manifest.json");
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("serializing manifest.json")?;
    write_private_file(&manifest_path, &manifest_bytes)?;
    let manifest_sha = sha256_hex(&manifest_bytes);

    // No belt-and-braces `chmod` pass here on purpose: every directory in the
    // bundle was created at 0700 by `mkdir(2)` and every file at 0600 by
    // `open(2)`, so there has never been a moment at which anything under
    // `bundle_path` was wider than intended. A trailing chmod would only be
    // able to fix a window this code no longer has.

    let control_manifest = json!({
        "atoms": atoms_n,
        "nodes": nodes_n,
        "node_atoms": node_atoms_n,
        "blobs": blob_count,
        "bytes": blob_bytes_total,
        "relationship_edges": edges_n,
        "transcripts": transcripts_n,
        "conversations": conversations_n,
        "manifest_sha256": manifest_sha,
        "export_id": export_id,
        "schema_version": row.schema_version,
        "archived_at": archived_at,
    });
    assert_control_manifest_keys(&control_manifest).map_err(|e| anyhow::anyhow!(e))?;

    Ok(ExportOutcome {
        manifest_path,
        control_manifest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch directory that cleans itself up. Deliberately created with
    /// [`create_private_dir`] (0700) — these tests handle no real data, but
    /// the helper under test is the one that matters here anyway.
    struct Scratch(PathBuf);
    impl Scratch {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("export_member_ocean_test_{}", Uuid::new_v4()));
            create_private_dir(&path).expect("scratch mkdir");
            Self(path)
        }
        fn join(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path).expect("stat").permissions().mode() & 0o777
    }

    #[test]
    #[cfg(unix)]
    fn private_file_is_never_group_or_world_readable() {
        let scratch = Scratch::new();
        let path = scratch.join("secret");
        write_private_file(&path, b"decrypted plaintext").expect("write");
        // The security property: no group/other bits, at any point — the mode
        // came from `open(2)` itself, not from a later chmod.
        assert_eq!(
            mode_of(&path) & 0o077,
            0,
            "bundle file must not be group/world accessible"
        );
        assert_eq!(fs::read(&path).expect("read"), b"decrypted plaintext");
    }

    #[test]
    fn private_file_refuses_to_clobber_an_existing_path() {
        let scratch = Scratch::new();
        let path = scratch.join("secret");
        write_private_file(&path, b"first").expect("write");
        // `create_new(true)`: a second write must fail rather than truncate
        // whatever (or whoever's) file is already sitting there.
        let err = write_private_file(&path, b"second").expect_err("must refuse to clobber");
        let io_err = err
            .downcast_ref::<std::io::Error>()
            .expect("io error underneath");
        assert_eq!(io_err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&path).expect("read"), b"first");
    }

    #[test]
    #[cfg(unix)]
    fn private_dir_is_0700_and_refuses_an_existing_directory() {
        let scratch = Scratch::new();
        let path = scratch.join("bundle");
        create_private_dir(&path).expect("mkdir");
        assert_eq!(
            mode_of(&path) & 0o077,
            0,
            "bundle dir must not be group/world accessible"
        );
        // The whole point of the non-recursive create: unlike
        // `create_dir_all`, a second call must NOT silently adopt the
        // directory that is already there.
        let err = create_private_dir(&path).expect_err("must refuse an existing directory");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(
            fs::create_dir_all(&path).is_ok(),
            "sanity: create_dir_all is the permissive behaviour we are avoiding"
        );
    }

    #[test]
    #[cfg(unix)]
    fn json_and_ndjson_writers_go_through_the_private_path() {
        let scratch = Scratch::new();
        let json_path = scratch.join("ocean.json");
        let sha = write_json_file(&json_path, &json!({ "k": "v" })).expect("write json");
        assert_eq!(mode_of(&json_path) & 0o077, 0);
        assert_eq!(sha, sha256_hex(&fs::read(&json_path).expect("read")));

        let ndjson_path = scratch.join("atoms.ndjson");
        let (n, sha) = write_ndjson_file(&ndjson_path, &[json!({ "a": 1 }), json!({ "a": 2 })])
            .expect("write ndjson");
        assert_eq!(n, 2);
        assert_eq!(mode_of(&ndjson_path) & 0o077, 0);
        let bytes = fs::read(&ndjson_path).expect("read");
        assert_eq!(sha, sha256_hex(&bytes));
        assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 2);
    }

    #[tokio::test]
    async fn meeting_evidence_export_preserves_history_links_and_scope() {
        use std::str::FromStr;
        use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
        let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "RELINFER_TEST_DATABASE_URL is required");
            eprintln!("skipping meeting evidence export test: set RELINFER_TEST_DATABASE_URL");
            return;
        };
        let opts = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
        assert_eq!(opts.get_database(), Some("memory_provenance_test"));
        let pool = PgPoolOptions::new().max_connections(2).connect_with(opts).await.unwrap();
        let user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
            .bind(format!("export-evidence-{}@test.local", Uuid::new_v4())).fetch_one(&pool).await.unwrap();
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user).fetch_one(&pool).await.unwrap();
        let origin = json!({"service_id":"fixture","transport":"native","content_kind":"transcript",
            "container":{"kind":"meeting","id":"M1","provider_namespace":"fixture:workspace:W1","label":"Meeting"}});
        let mut fixtures = vec![];
        for _ in 0..2 {
            let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'private export fixture') RETURNING id")
                .bind(account).fetch_one(&pool).await.unwrap();
            sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,raw_inline) VALUES($1,'same-source','New transcript')")
                .bind(graph).execute(&pool).await.unwrap();
            let source_item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting','same-source') RETURNING source_item_id").bind(graph).fetch_one(&pool).await.unwrap();
            sqlx::query("UPDATE meeting_transcripts SET source_item_id=$2 WHERE graph_id=$1").bind(graph).bind(source_item).execute(&pool).await.unwrap();
            let key = coralswarm_core::r2::blob_key_for(Some("org_export_history"), graph, Uuid::new_v4()).unwrap();
            let old: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_id,source_item_id,raw_blob_key,raw_inline,content_sha256,is_legacy_baseline) VALUES($1,'same-source',(SELECT source_item_id FROM source_items WHERE graph_id=$1),$2,'Ship 🚀 Friday','old-hash',true) RETURNING version_id")
                .bind(graph).bind(&key).fetch_one(&pool).await.unwrap();
            let current: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_id,source_item_id,raw_inline) VALUES($1,'same-source',(SELECT source_item_id FROM source_items WHERE graph_id=$1),'New transcript') RETURNING version_id")
                .bind(graph).fetch_one(&pool).await.unwrap();
            sqlx::query("UPDATE meeting_transcripts SET current_version_id=$2 WHERE graph_id=$1 AND source_id='same-source'")
                .bind(graph).bind(current).execute(&pool).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2,origin=$3 WHERE source_item_id=$1").bind(source_item).bind(current).bind(&origin).execute(&pool).await.unwrap();
            sqlx::query("UPDATE source_versions SET origin=$2 WHERE version_id=$1").bind(current).bind(&origin).execute(&pool).await.unwrap();
            let chunk:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'transcript','same-source:chunk:0',$2,'Ship 🚀 Friday',now()) RETURNING atom_id").bind(graph).bind(&key).fetch_one(&pool).await.unwrap();
            sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(graph).bind(old).bind(chunk).execute(&pool).await.unwrap();
            let atom: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'decision','Ship Friday','fixture-v1','fixture-model') RETURNING semantic_atom_id")
                .bind(graph).bind(old).fetch_one(&pool).await.unwrap();
            sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,13,'Ship 🚀 Friday')")
                .bind(graph).bind(old).bind(atom).execute(&pool).await.unwrap();
            let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Launch') RETURNING node_id")
                .bind(graph).fetch_one(&pool).await.unwrap();
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
                .bind(graph).bind(node).bind(atom).execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO semantic_atom_windows(graph_id,source_version_id,extraction_version,start_char,end_char,model,job_id) VALUES($1,$2,'fixture-v1',0,13,'fixture-model',$3)")
                .bind(graph).bind(old).bind(Uuid::new_v4()).execute(&pool).await.unwrap();
            fixtures.push((graph, old, current, atom, node, key));
        }
        let (graph, old, current, atom, node, key) = &fixtures[0];
        let scratch = Scratch::new();
        let mut digests = serde_json::Map::new();
        let counts = export_meeting_evidence(&pool, *graph, &scratch.0, &mut digests).await.unwrap();
        for (table, _) in MEETING_EVIDENCE_EXPORTS {
            let filename = format!("{table}.ndjson");
            let path = scratch.join(&filename);
            let bytes = fs::read(&path).unwrap();
            assert_eq!(digests[&filename], sha256_hex(&bytes));
            #[cfg(unix)]
            assert_eq!(mode_of(&path) & 0o077, 0);
            let rows: Vec<Value> = String::from_utf8(bytes).unwrap().lines().map(|line| serde_json::from_str(line).unwrap()).collect();
            assert_eq!(counts[*table], if *table == "source_versions" { 2 } else { 1 });
            assert!(rows.iter().all(|row| row["graph_id"] == graph.to_string()), "{table} leaked neighbor metadata");
            match *table {
                "source_items" => {
                    assert_eq!(rows[0]["provider_namespace"],"legacy:meeting");
                    assert_eq!(rows[0]["current_version_id"],current.to_string());
                    assert_eq!(rows[0]["origin"],origin);
                }
                "source_version_chunks" => {
                    assert_eq!(rows[0]["source_version_id"],old.to_string());
                    assert!(rows[0]["atom_id"].is_string());
                }
                "source_versions" => {
                    let old_row = rows.iter().find(|row| row["version_id"] == old.to_string()).unwrap();
                    assert_eq!(old_row["raw_blob_key"], *key);
                    assert_eq!(old_row["raw_inline"], "Ship 🚀 Friday");
                    assert_eq!(old_row["content_sha256"], "old-hash");
                    assert_eq!(old_row["is_legacy_baseline"], true);
                    assert!(old_row.get("origin").unwrap().is_null(), "legacy unknown attribution survives export");
                    let current_row=rows.iter().find(|row| row["version_id"] == current.to_string()).unwrap();
                    assert_eq!(current_row["origin"],origin);
                }
                "semantic_atoms" => {
                    assert_eq!(rows[0]["semantic_atom_id"], atom.to_string());
                    assert_eq!(rows[0]["source_version_id"], old.to_string());
                    assert_eq!(rows[0]["extraction_version"], "fixture-v1");
                    assert_eq!(rows[0]["model"], "fixture-model");
                }
                "semantic_atom_passages" => {
                    assert_eq!(rows[0]["semantic_atom_id"], atom.to_string());
                    assert_eq!(rows[0]["source_version_id"], old.to_string());
                    assert_eq!(rows[0]["quote"], "Ship 🚀 Friday");
                    assert_eq!(rows[0]["end_char"], 13);
                }
                "node_semantic_atoms" => {
                    assert_eq!(rows[0]["node_id"], node.to_string());
                    assert_eq!(rows[0]["semantic_atom_id"], atom.to_string());
                }
                "semantic_atom_windows" => {
                    assert_eq!(rows[0]["source_version_id"], old.to_string());
                    assert_eq!(rows[0]["model"], "fixture-model");
                    assert!(rows[0]["job_id"].is_string());
                }
                _ => unreachable!(),
            }
        }
        let graph_ids: Vec<Uuid> = fixtures.iter().map(|row| row.0).collect();
        sqlx::query("DELETE FROM meeting_transcripts WHERE graph_id=ANY($1)").bind(&graph_ids).execute(&pool).await.unwrap();
        let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
        sqlx::query("DELETE FROM accounts WHERE id=$1").bind(account).execute(&mut *financial).await.unwrap();
        financial.commit().await.unwrap();
        sqlx::query("DELETE FROM users WHERE id=$1").bind(user).execute(&pool).await.unwrap();
        pool.close().await;
    }

    #[test]
    fn control_manifest_keys_are_allowlisted() {
        assert!(assert_control_manifest_keys(&json!({ "atoms": 1, "export_id": "x" })).is_ok());
        let err = assert_control_manifest_keys(&json!({ "content": "leaked" }))
            .expect_err("content is not an allowed key");
        assert!(
            err.contains("content"),
            "error names the offending key: {err}"
        );
        assert!(assert_control_manifest_keys(&json!("not an object")).is_err());
    }
}

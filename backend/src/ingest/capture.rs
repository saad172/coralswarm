//! The capture core: the ONE code path from an admitted source to the
//! knowledge graph's capture stages.
//!
//! [`super::submit`] hands every source to [`store_capture_tx`] inside the
//! door's own authorized transaction. The function is private to `ingest`
//! (ADR-0016 §5), so nothing outside the door can reach it:
//!
//! 1. **Version.** `source_evidence::capture_source_version_tracked_tx` writes
//!    the `source_items`/`source_versions` snapshot and, with it, the
//!    standing-admission row (`corpus_capture_admissions`) and the extraction
//!    enqueue (`queue_extraction_tx`). Unchanged content re-adopts the current
//!    version instead of minting one.
//! 2. **Chunks.** One INSERT writes every new positional chunk atom with the
//!    columns enrichment and the Library read: fallback title, journal
//!    excerpt, code symbols, provenance (channel, author, workspace, project,
//!    MCP client), header length and content hash.
//! 3. **Bodies.** Chunk text goes to the object store under the key the row
//!    names (or inline in dev mode). An upload failure aborts the capture
//!    (ADR-0016 §4): nothing commits and [`FunnelError::storage`] says the
//!    same capture succeeds once the store is back.
//! 4. **Reconcile.** `chunk_reconcile::reconcile_tx` adopts unchanged
//!    positions, replaces edited ones, retires surplus ones and links every
//!    settled position to the version.
//! 5. **Embed.** Every atom whose content this capture wrote gets an
//!    `EmbedAtoms` job in the same transaction, so it commits or rolls back
//!    with the rows it embeds (ADR-0016 §4).
//!
//! What stays with each caller is what is genuinely its own: destination
//! resolution (the door's D7 and routing-grant checks, Slack's channel
//! fan-out), identity
//! (Slack author/self reef, Google Chat space identity), the contextual header
//! text, and side effects such as activity events or anchors.
//!
//! Chunking and titles are decided here too: [`chunk_strategy`] is the single
//! strategy decision (source type, code path, or a trusted connector's
//! [`CaptureHints`]), and [`Prepared::new`] derives every chunk's text and
//! write-time columns the same way for every source. The coverage guard
//! (`tests/capture_funnel_guard.rs` and [`pg_tests`]) fails if a connector
//! writes atoms, versions or chunk links anywhere else.

use chrono::{DateTime, Utc};
use coralswarm_core::chunker::{Chunk, ChunkStrategy};
use coralswarm_core::pg_text::{self, PgText};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::chunk_reconcile::{ChunkColumns, PositionalChunk, Reconciled};
use crate::source_evidence::{CaptureFailure, CaptureSource};
use crate::source_origin::SourceOrigin;

/// Object-store uploads in flight per capture.
const UPLOAD_CONCURRENCY: usize = 8;

/// Capture hints a TRUSTED connector supplies for one source. Never read from
/// a client payload (`IngestAtomRequest::capture_hints` is `#[serde(skip)]`);
/// the Composio recipes are the source today.
#[derive(Debug, Clone, Default)]
pub(crate) struct CaptureHints {
    /// Overrides the source type's chunk strategy.
    pub chunk: Option<ChunkStrategy>,
    /// Title for every chunk of this source, instead of one derived from the
    /// chunk text. Stored as `title_kind = 'source'` (the provider's own
    /// title, e.g. a Composio recipe render), which the managed title sweep
    /// never overwrites.
    pub title: Option<String>,
    /// The source's display key (a Linear or Jira issue key such as
    /// `COR-65`; a comment carries its issue's). Recorded, with `title` and
    /// `url`, in `source_versions.source_metadata` under the anchor keys
    /// source extraction reads, so atoms can say which source they came from.
    pub identifier: Option<String>,
    /// The source's web link (a Slack message permalink).
    pub url: Option<String>,
}

impl CaptureHints {
    /// Write the anchor hints (`title`, `identifier`, `url`) into a source
    /// version's metadata. Blank values are skipped and a key the caller
    /// already set is kept. Metadata only: no source text.
    pub(crate) fn record_anchor(&self, metadata: &mut serde_json::Value) {
        let Some(object) = metadata.as_object_mut() else { return };
        for (key, value) in [("title", &self.title), ("identifier", &self.identifier), ("url", &self.url)] {
            let Some(value) = value.as_deref().map(str::trim).filter(|v| !v.is_empty()) else { continue };
            if object.get(key).is_none_or(serde_json::Value::is_null) {
                object.insert(key.to_owned(), serde_json::Value::String(value.to_owned()));
            }
        }
    }
}

/// The one chunk-strategy decision: a trusted hint wins, then a GitHub code
/// path (the one input `for_source_type` cannot see), then the source type.
pub(crate) fn chunk_strategy(
    source_type: &str,
    code_path: Option<&str>,
    hint: Option<&ChunkStrategy>,
) -> ChunkStrategy {
    if let Some(hint) = hint {
        return hint.clone();
    }
    match (source_type, code_path) {
        ("github_code", Some(path)) if !path.is_empty() => ChunkStrategy::code_for_path(path),
        _ => ChunkStrategy::for_source_type(source_type),
    }
}

/// How a contextual header sits on every chunk of a capture.
#[derive(Debug, Clone)]
pub(crate) enum Header {
    None,
    /// Prepended and recorded in `atoms.header_len`, so readers, titles,
    /// excerpts and chunk hashes strip it (meetings, sessions, huddles).
    Recorded(String),
    /// Prepended with no `header_len` (the historical Slack and Google Chat
    /// shape). The chunk hash covers it, exactly what those writers stored;
    /// titles and excerpts are still derived from the body.
    Inline(String),
}

/// One chunk ready to write: positional identity, persisted text and the
/// write-time columns.
///
/// `text` is a [`PgText`], which cannot carry a NUL, so no chunk body (sealed
/// in the object store, later bound into `content_tsv` by the embed worker)
/// can carry one. [`Prepared::new`] derives the columns from the same cleaned
/// text.
#[derive(Debug, Clone)]
pub(super) struct PreparedChunk {
    pub(super) source_id: String,
    pub(super) text: PgText,
    pub(super) header_len: Option<i32>,
    pub(super) columns: ChunkColumns,
}

/// Every chunk of one capture. Graph-independent, so a fan-out connector
/// prepares once and stores per destination.
#[derive(Debug, Clone, Default)]
pub(crate) struct Prepared {
    pub(super) chunks: Vec<PreparedChunk>,
    /// `atoms.header_len` for a [`Header::Recorded`] header.
    pub(super) header_len: Option<i32>,
}

impl Prepared {
    /// Text and columns for `chunks` of the source `base_source_id`.
    /// `content` is the whole source, used for calendar titles (the summary
    /// line can fall into an earlier chunk under generic chunking).
    pub(crate) fn new(
        base_source_id: &str,
        source_type: &str,
        content: &str,
        chunks: &[Chunk],
        header: &Header,
        title: Option<&str>,
    ) -> Self {
        // The one place capture text is cleaned for Postgres: every chunk's
        // body, header, title and columns below derive from these.
        let content = pg_text::sanitize(content);
        let content = content.as_ref();
        let (header_text, recorded) = match header {
            Header::None => (None, false),
            Header::Recorded(h) => (Some(pg_text::sanitize(h)), true),
            Header::Inline(h) => (Some(pg_text::sanitize(h)), false),
        };
        let header_text = header_text.as_deref();
        let header_len = header_text.filter(|_| recorded).map(|h| (h.len() + 2) as i32);
        let title = title.map(pg_text::sanitize);
        let hinted = title
            .as_deref()
            .map(crate::services::atom_title::truncate_title)
            .filter(|t| !t.is_empty());
        let hinted_kind = hinted.is_some();
        let source_title = hinted
            .or_else(|| match source_type {
                "google_calendar" | "microsoft_calendar" => {
                    crate::services::atom_title::fallback_title_for_source(source_type, content)
                }
                _ => None,
            });
        let chunks = chunks
            .iter()
            .map(|chunk| {
                let body = pg_text::sanitized(chunk.text_with_speaker());
                let text = match header_text {
                    Some(h) => format!("{h}\n\n{body}"),
                    None => body.clone(),
                };
                let title = source_title.clone().or_else(|| {
                    crate::services::atom_title::fallback_title_for_source(source_type, &body)
                });
                PreparedChunk {
                    source_id: crate::chunk_reconcile::chunk_source_id(base_source_id, chunk.position),
                    header_len,
                    columns: ChunkColumns {
                        excerpt: crate::store::derive_journal_excerpt(Some(&body), None),
                        // A trusted hint is the provider's own title: the
                        // title sweep (candidates `title_kind = 'fallback'`)
                        // must not replace it with a model headline.
                        title_kind: title
                            .as_ref()
                            .map(|_| if hinted_kind { "source" } else { "fallback" }.to_string()),
                        title,
                        symbols: (!chunk.symbols.is_empty()).then(|| pg_text::sanitized(chunk.symbols.join("\n"))),
                    },
                    text: PgText::new(text),
                }
            })
            .collect();
        Self { chunks, header_len }
    }

    pub(crate) fn len(&self) -> usize {
        self.chunks.len()
    }

    fn positional(&self) -> Vec<PositionalChunk> {
        self.chunks
            .iter()
            .map(|c| PositionalChunk {
                source_id: c.source_id.clone(),
                text: c.text.as_str().to_owned(),
                header_len: c.header_len,
                columns: Some(c.columns.clone()),
            })
            .collect()
    }
}

/// Row-level columns shared by every chunk atom of one capture.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AtomFields<'a> {
    pub(crate) source_type: &'a str,
    pub(crate) participants: &'a [String],
    /// Source event time.
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) ingested_by: Option<Uuid>,
    pub(crate) scope: &'a str,
    pub(crate) fts_language: &'a str,
    /// Whole-source hash, written to `atoms.content_hash` (the re-fetch edit
    /// detector reads it) and to replaced chunks.
    pub(crate) content_hash: Option<&'a str>,
    pub(crate) channel_id: Option<&'a str>,
    pub(crate) channel_name: Option<&'a str>,
    pub(crate) author_id: Option<&'a str>,
    pub(crate) author_name: Option<&'a str>,
    pub(crate) workspace_id: Option<&'a str>,
    pub(crate) project: Option<&'a str>,
    pub(crate) mcp_client_name: Option<&'a str>,
}

impl<'a> AtomFields<'a> {
    /// Defaults: private scope, English full-text search, no provenance.
    pub(crate) fn new(
        source_type: &'a str,
        participants: &'a [String],
        timestamp: DateTime<Utc>,
        ingested_by: Option<Uuid>,
    ) -> Self {
        Self {
            source_type,
            participants,
            timestamp,
            ingested_by,
            scope: "private",
            fts_language: "english",
            content_hash: None,
            channel_id: None,
            channel_name: None,
            author_id: None,
            author_name: None,
            workspace_id: None,
            project: None,
            mcp_client_name: None,
        }
    }
}

/// `values` with NUL replaced (`pg_text`); borrowed when there is none.
fn clean_all(values: &[String]) -> std::borrow::Cow<'_, [String]> {
    if values.iter().any(|v| v.contains('\0')) {
        std::borrow::Cow::Owned(values.iter().map(|v| pg_text::sanitized(v.as_str())).collect())
    } else {
        std::borrow::Cow::Borrowed(values)
    }
}

/// Clean copies of the external text an [`AtomFields`] carries (participant
/// and author names, channel, project, MCP client), so a NUL in a provider's
/// display name cannot fail the atom insert. Ids and enum-like columns
/// (scope, language, hashes) are the caller's own and are left alone.
struct CleanAtomText<'a> {
    participants: std::borrow::Cow<'a, [String]>,
    channel_name: Option<std::borrow::Cow<'a, str>>,
    author_name: Option<std::borrow::Cow<'a, str>>,
    project: Option<std::borrow::Cow<'a, str>>,
    mcp_client_name: Option<std::borrow::Cow<'a, str>>,
}

impl<'a> CleanAtomText<'a> {
    fn of(atoms: &AtomFields<'a>) -> Self {
        Self {
            participants: clean_all(atoms.participants),
            channel_name: pg_text::sanitize_opt(atoms.channel_name),
            author_name: pg_text::sanitize_opt(atoms.author_name),
            project: pg_text::sanitize_opt(atoms.project),
            mcp_client_name: pg_text::sanitize_opt(atoms.mcp_client_name),
        }
    }

    fn apply<'b>(&'b self, atoms: AtomFields<'b>) -> AtomFields<'b> {
        AtomFields {
            participants: &self.participants,
            channel_name: self.channel_name.as_deref(),
            author_name: self.author_name.as_deref(),
            project: self.project.as_deref(),
            mcp_client_name: self.mcp_client_name.as_deref(),
            ..atoms
        }
    }
}

/// The usual embed idempotency key: one per destination, source and content.
///
/// The source is part of the key on purpose. The key dedupes against PENDING
/// jobs only (`ON CONFLICT (idempotency_key) WHERE status='pending' DO
/// NOTHING`), and the earlier `embed:{graph}:{content_hash}` dropped the embed
/// job of every other source in the graph whose text was byte-identical while
/// the first job waited (two Slack "thanks" messages): those chunks were never
/// embedded. A retry of the same source still dedupes.
pub(crate) fn embed_key(graph_id: Uuid, source_type: &str, source_id: &str, content_hash: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut source = Sha256::new();
    source.update(source_type.as_bytes());
    source.update([0]);
    source.update(source_id.as_bytes());
    let source = hex::encode(source.finalize());
    format!("embed:{graph_id}:{}:{content_hash}", &source[..16])
}

/// Where the capture lands and how its bodies are sealed. Built only by the
/// door (`ingest.rs`), from a checked destination.
#[derive(Clone, Copy)]
pub(super) struct Destination<'a> {
    pub(super) graph_id: Uuid,
    /// Owner account of `graph_id` (the DEK's account).
    pub(super) account_id: Uuid,
    /// Blob-key org segment (`None` on the consumer plane).
    pub(super) org_id: Option<&'a str>,
    pub(super) dek: &'a [u8; 32],
    /// `None` = dev/inline mode.
    pub(super) r2: Option<&'a coralswarm_core::r2::R2Client>,
}

/// One source capture.
pub(super) struct CaptureRequest<'a> {
    /// The version snapshot (identity, normalized text, metadata).
    pub source: CaptureSource<'a>,
    pub origin: Option<&'a SourceOrigin>,
    pub chunks: &'a Prepared,
    pub atoms: AtomFields<'a>,
    /// The `EmbedAtoms` job's idempotency key, usually [`embed_key`].
    pub embed_key: String,
    /// Record `op_log` operations for the atoms this capture creates and
    /// inlines, through this store. The HTTP funnel and GitHub always did;
    /// the Slack, huddle and Google Chat writers never did.
    pub sync_ops: Option<&'a dyn crate::store::Store>,
}

/// What one capture did.
#[derive(Debug, Clone)]
pub(super) struct Stored {
    /// Read by the capture suite; the door needs only the version.
    #[cfg_attr(not(test), allow(dead_code))]
    pub source_item_id: Uuid,
    pub version_id: Uuid,
    /// A new version was minted (the content changed).
    pub version_changed: bool,
    pub meeting_projection: bool,
    /// Rows this capture inserted, with their bodies durable.
    pub inserted: Vec<Uuid>,
    pub reconciled: Reconciled,
    /// `EmbedAtoms` jobs this capture enqueued (0 or 1): none when nothing
    /// was written, or when an identical job was already pending.
    pub enqueued_in_transaction: usize,
}

impl Stored {
    /// Atoms whose content this capture wrote (inserted or replaced).
    pub fn written(&self) -> Vec<Uuid> {
        self.reconciled.written()
    }

    /// Existing positions this capture did not rewrite.
    pub fn duplicates(&self) -> usize {
        self.reconciled.adopted.len() + self.reconciled.conflicts.len() + self.reconciled.unverified.len()
    }
}

/// Why a capture failed. Nothing it wrote is committed; uploads it made are in
/// the caller's `uploads` list for cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FunnelError {
    pub(crate) stage: &'static str,
    pub(crate) message: String,
    /// Retrying cannot help (a malformed blob-key segment).
    pub(crate) permanent: bool,
    /// Postgres refused the atom insert under row-level security.
    pub(crate) rls_refused: bool,
    /// The object store refused a body. Nothing committed, and the same
    /// capture succeeds once the store is back. The door answers it as
    /// `IngestError::StorageUnavailable`.
    pub(crate) storage: bool,
}

impl FunnelError {
    fn at(stage: &'static str) -> impl Fn(String) -> Self {
        move |message| Self { stage, message, permanent: false, rls_refused: false, storage: false }
    }

    /// A failed stage whose cause is already classified: the source version
    /// or the chunk reconcile. [`CaptureFailure::Storage`] is the object
    /// store's refusal.
    fn failed(stage: &'static str) -> impl Fn(CaptureFailure) -> Self {
        move |e| Self { stage, storage: e.is_storage(), message: e.to_string(), permanent: false, rls_refused: false }
    }

    /// A failed object-store write at `stage`, classified by the one storage
    /// rule (`R2Error::is_store_unavailable`): a key or encryption error is
    /// not a storage failure.
    fn upload(stage: &'static str) -> impl Fn(coralswarm_core::r2::R2Error) -> Self {
        move |e| Self::failed(stage)(e.into())
    }
}

impl std::fmt::Display for FunnelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.stage, self.message)
    }
}

impl std::error::Error for FunnelError {}

/// Store one admitted source in the caller's transaction: version, chunk
/// atoms, bodies, reconcile/links and embed. See the module doc. Every object
/// this uploads is appended to `uploads`, so the caller's rollback cleanup
/// covers it; the caller owns the commit.
pub(super) async fn store_capture_tx(
    tx: &mut Transaction<'_, Postgres>,
    dest: Destination<'_>,
    req: &CaptureRequest<'_>,
    uploads: &mut Vec<String>,
) -> Result<Stored, FunnelError> {
    // Capture text is cleaned for Postgres here, once (`pg_text`): the
    // version snapshot (its text is sealed in the object store and read back
    // by extraction; its metadata and segments are jsonb) and the row's
    // external text columns. Chunk text is a `PgText` already. Identities
    // (source ids, namespaces) are not rewritten: a NUL there fails the
    // capture loudly instead of aliasing two sources.
    let text = pg_text::sanitize(req.source.normalized_text);
    let metadata = pg_text::sanitize_json(req.source.metadata);
    let segments = req.source.segments.map(pg_text::sanitize_json);
    let source_participants = clean_all(req.source.participants);
    let source = CaptureSource {
        normalized_text: &text,
        metadata: &metadata,
        segments: segments.as_deref(),
        participants: &source_participants,
        ..req.source
    };
    let atom_text = CleanAtomText::of(&req.atoms);
    let atom_fields = atom_text.apply(req.atoms);
    let req = &CaptureRequest {
        source,
        origin: req.origin,
        chunks: req.chunks,
        atoms: atom_fields,
        embed_key: req.embed_key.clone(),
        sync_ops: req.sync_ops,
    };
    // Meet's native attendee adapter declares transcripts first-write-wins.
    // The lock must precede the read and remain held through the caller's
    // commit: a connector-side precheck races another delivery. Do this before
    // versioning as well as reconciliation, so discarded bytes cannot change
    // the raw snapshot, projection, chunk family or downstream jobs.
    if req.source.source_type == crate::connectors::google_meet::SOURCE_TYPE {
        if let Some(origin) = req.origin {
            origin.validate().map_err(FunnelError::at("source version"))?;
        }
        crate::source_evidence::lock_identity_tx(tx, dest.graph_id, req.source.source_id)
            .await
            .map_err(|e| FunnelError::at("source version")(e.to_string()))?;
        if is_immutable_google_meet(req) {
            if let Some(stored) = reuse_google_meet_capture_tx(tx, dest.graph_id, &req.source, req.origin.unwrap()).await? {
                return Ok(stored);
            }
        } else if has_native_google_meet_tx(tx, dest.graph_id, req.source.source_id).await? {
            // A generic caller cannot turn an already-native artifact into a
            // mutable source by omitting/changing attribution or namespace.
            return Err(FunnelError::at("source version")("native Meet capture cannot be replaced by another capture origin".into()));
        }
    }
    let captured = crate::source_evidence::capture_source_version_tracked_tx(
        tx,
        dest.r2,
        dest.graph_id,
        dest.account_id,
        &req.source,
        dest.org_id,
        dest.dek,
        req.origin,
        uploads,
    )
    .await
    .map_err(FunnelError::failed("source version"))?;

    let atoms = &req.atoms;
    let chunks = &req.chunks.chunks;
    let ids: Vec<Uuid> = chunks.iter().map(|_| Uuid::new_v4()).collect();
    let keys = ids
        .iter()
        .map(|id| coralswarm_core::r2::blob_key_for(dest.org_id, dest.graph_id, *id))
        .collect::<Result<Vec<String>, _>>()
        .map_err(|e| FunnelError { stage: "blob key", message: e.to_string(), permanent: true, rls_refused: false, storage: false })?;
    let inserted = insert_chunks_tx(tx, dest, &req.atoms, req.chunks, &ids, &keys).await?;
    if let Some(store) = req.sync_ops {
        append_create_ops_tx(tx, store, dest.graph_id, atoms, chunks, &ids, &keys, &inserted).await?;
    }

    let position: std::collections::HashMap<Uuid, usize> = ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
    if let Some(r2) = dest.r2 {
        let pending: Vec<(Uuid, String, Vec<u8>)> = inserted
            .iter()
            .map(|id| {
                let i = position[id];
                (*id, keys[i].clone(), chunks[i].text.as_bytes().to_vec())
            })
            .collect();
        let mut first_error = None;
        for batch in pending.chunks(UPLOAD_CONCURRENCY) {
            let results = futures::future::join_all(
                batch.iter().cloned().map(|(id, key, bytes)| put_body(r2, id, key, bytes, *dest.dek)),
            )
            .await;
            // Every landed body goes on `uploads` before the capture fails, so
            // the caller's rollback cleanup reaches all of them.
            for (id, result) in results {
                match result {
                    Ok(key) => uploads.push(key),
                    Err(e) => {
                        tracing::warn!(error = %e, atom_id = %id, graph_id = %dest.graph_id, "chunk body upload failed");
                        first_error.get_or_insert(e);
                    }
                }
            }
            // The capture aborts on the first failure: a later batch would
            // only be uploaded to be deleted again.
            if first_error.is_some() {
                break;
            }
        }
        if let Some(e) = first_error {
            return Err(FunnelError::upload("chunk upload")(e));
        }
    } else if !inserted.is_empty() {
        // Dev mode: the body lives inline so embedding and reads work without
        // an object store.
        let texts: Vec<String> = inserted.iter().map(|id| chunks[position[id]].text.as_str().to_owned()).collect();
        match req.sync_ops {
            Some(store) => store
                .set_atoms_inline_content_scoped(tx, &inserted, &texts)
                .await
                .map_err(|e| e.to_string()),
            None => sqlx::query(
                "UPDATE atoms a SET content_inline=v.t FROM unnest($2::uuid[],$3::text[]) AS v(id,t)
                  WHERE a.graph_id=$1 AND a.atom_id=v.id",
            )
            .bind(dest.graph_id)
            .bind(&inserted)
            .bind(&texts)
            .execute(&mut **tx)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        }
        .map_err(FunnelError::at("inline chunk content"))?;
    }

    let reconciled = crate::chunk_reconcile::reconcile_tx(
        tx,
        &crate::chunk_reconcile::ChunkTarget {
            graph_id: dest.graph_id,
            account_id: dest.account_id,
            source_item_id: captured.source_item_id,
            version_id: captured.version_id,
            source_type: atoms.source_type,
            base_source_id: req.source.source_id,
            content_hash: atoms.content_hash,
            participants: Some(atoms.participants),
            org_id: dest.org_id,
            dek: dest.dek,
            r2: dest.r2,
            device_id: req.sync_ops.map_or(crate::store::CLOUD_DEVICE_ID, |s| s.device_id()),
        },
        &req.chunks.positional(),
        &inserted,
        uploads,
    )
    .await
    .map_err(FunnelError::failed("chunk reconcile"))?;

    let written = reconciled.written();
    let mut enqueued_in_transaction = 0;
    if !written.is_empty() {
        let job = crate::jobs::queue::enqueue_tx(
            tx,
            &crate::jobs::JobKind::EmbedAtoms { atom_ids: written },
            Some(dest.account_id),
            Some(&req.embed_key),
            0,
        )
        .await
        .map_err(|e| FunnelError::at("embed enqueue")(e.to_string()))?;
        enqueued_in_transaction = usize::from(job.is_some());
    }

    Ok(Stored {
        source_item_id: captured.source_item_id,
        version_id: captured.version_id,
        version_changed: captured.changed,
        meeting_projection: captured.meeting_projection,
        inserted,
        reconciled,
        enqueued_in_transaction,
    })
}

/// Test seam: a capture through [`store_capture_tx`] for a test outside
/// `ingest` that needs landed rows without a door (the embed workers' NUL
/// capture). Compiled into test builds only, so production code still cannot
/// reach the core. Dev mode: no object store, bodies inline. Returns the
/// version and the atoms the capture wrote; their embed job is enqueued in
/// `tx`, as for every capture.
#[cfg(test)]
pub(crate) async fn store_for_test(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    account_id: Uuid,
    source: CaptureSource<'_>,
    chunks: &Prepared,
    atoms: AtomFields<'_>,
) -> Result<(Uuid, Vec<Uuid>), FunnelError> {
    let dest = Destination { graph_id, account_id, org_id: None, dek: &[0; 32], r2: None };
    let req = CaptureRequest {
        embed_key: embed_key(dest.graph_id, source.source_type, source.source_id, "test"),
        source,
        origin: None,
        chunks,
        atoms,
        sync_ops: None,
    };
    let stored = store_capture_tx(tx, dest, &req, &mut Vec::new()).await?;
    Ok((stored.version_id, stored.written()))
}

/// Only the trusted native attendee adapter has this contract. A caller's
/// `source_type = "meet"` alone must keep the generic mutable-source behavior.
fn is_immutable_google_meet(req: &CaptureRequest<'_>) -> bool {
    req.source.source_type == crate::connectors::google_meet::SOURCE_TYPE
        && req.atoms.source_type == req.source.source_type
        && req.origin.is_some_and(|origin| is_native_google_meet_origin(req.source.source_id, origin))
}

fn is_native_google_meet_origin(source_id: &str, origin: &SourceOrigin) -> bool {
    origin.transport == crate::source_origin::CaptureTransport::Native
                && origin.service_id.as_str() == "google_meet"
                && origin.content_kind.as_str() == "transcript"
                && origin.capture_surface.as_ref().is_some_and(|surface| surface.as_str() == "google_meet_attendee_capture")
                && origin.container.as_ref().is_some_and(|container| {
                    container.kind.as_str() == "conference_record"
                        && container.provider_namespace.as_str() == "google_meet:conference_records"
                        && source_id.strip_prefix("meet:") == Some(container.id.as_str())
                })
}

/// Native family protection survives a changed incoming origin/namespace.
/// Inspect only this graph/type/native ID, and never return foreign IDs to
/// the caller. Historical native attribution also fails closed if an older
/// buggy capture already moved the current pointer; this does not repair it.
async fn has_native_google_meet_tx(
    tx: &mut Transaction<'_, Postgres>, graph_id: Uuid, source_id: &str,
) -> Result<bool, FunnelError> {
    let origins: Vec<Option<sqlx::types::Json<SourceOrigin>>> = sqlx::query_scalar(
        "SELECT s.origin FROM source_items s WHERE s.graph_id=$1 AND s.source_type='meet' AND s.source_id=$2
         UNION ALL
         SELECT v.origin FROM source_items s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id
          WHERE s.graph_id=$1 AND s.source_type='meet' AND s.source_id=$2",
    ).bind(graph_id).bind(source_id).fetch_all(&mut **tx).await
        .map_err(|e| FunnelError::at("source version")(e.to_string()))?;
    Ok(origins.into_iter().flatten().any(|origin| is_native_google_meet_origin(source_id, &origin.0)))
}

/// Reuse exactly the previously committed identity/version/chunk snapshot.
/// Partial captures stay partial: any repair must use the retained original
/// bytes, never a later delivery's possibly changed transcript.
async fn reuse_google_meet_capture_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    source: &CaptureSource<'_>,
    origin: &SourceOrigin,
) -> Result<Option<Stored>, FunnelError> {
    let prior: Option<(Uuid, Uuid, bool)> = sqlx::query_as(
        "SELECT s.source_item_id,v.version_id,
                EXISTS(SELECT 1 FROM meeting_transcripts m WHERE m.graph_id=s.graph_id AND m.source_id=s.source_id AND m.source_item_id=s.source_item_id)
           FROM source_items s JOIN source_versions v
             ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.version_id=s.current_version_id
          WHERE s.graph_id=$1 AND s.source_type=$2 AND s.provider_namespace=$3 AND s.source_id=$4
            AND s.origin=$5 AND v.origin=$5",
    )
    .bind(graph_id).bind(source.source_type).bind(source.provider_namespace).bind(source.source_id)
    .bind(sqlx::types::Json(origin))
    .fetch_optional(&mut **tx).await
    .map_err(|e| FunnelError::at("source version")(e.to_string()))?;
    let Some((source_item_id, version_id, meeting_projection)) = prior else {
        // Legacy/unbound/foreign holdings are not proof of this exact source
        // version. Refuse to adopt or overwrite them from a new delivery.
        // `left` uses a literal prefix, so provider IDs containing SQL LIKE
        // metacharacters cannot accidentally match another meeting family.
        let occupied: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM source_items WHERE graph_id=$1 AND source_type=$2 AND source_id=$3)
                 OR EXISTS(SELECT 1 FROM atoms WHERE graph_id=$1 AND source_type=$2
                            AND (source_id=$3 OR left(source_id,length($3)+7)=$3||':chunk:'))
                 OR EXISTS(SELECT 1 FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$3)",
        ).bind(graph_id).bind(source.source_type).bind(source.source_id)
            .fetch_one(&mut **tx).await
            .map_err(|e| FunnelError::at("source version")(e.to_string()))?;
        if occupied {
            return Err(FunnelError::at("source version")("native Meet identity has no reusable committed capture".into()));
        }
        return Ok(None);
    };
    let linked: Vec<Uuid> = sqlx::query_scalar(
        "SELECT c.atom_id FROM source_version_chunks c JOIN atoms a ON a.graph_id=c.graph_id AND a.atom_id=c.atom_id
          WHERE c.graph_id=$1 AND c.source_version_id=$2 ORDER BY a.source_id,c.atom_id",
    )
    .bind(graph_id).bind(version_id).fetch_all(&mut **tx).await
    .map_err(|e| FunnelError::at("chunk reconcile")(e.to_string()))?;
    Ok(Some(Stored {
        source_item_id, version_id, version_changed: false, meeting_projection,
        inserted: Vec::new(),
        reconciled: Reconciled { adopted: linked.clone(), linked, ..Reconciled::default() },
        enqueued_in_transaction: 0,
    }))
}

/// Re-chunk an existing source version in place: the recovery for chunks the
/// chunker of the day let through over the embedder's input limit
/// (`coralswarm_core::chunker::MAX_CHUNK_BYTES`). No new version is minted
/// and no extraction is queued: the version's text is unchanged, only its
/// chunking is. New positions are inserted, existing ones adopted, replaced
/// or retired by [`crate::chunk_reconcile::reconcile_tx`] exactly as a
/// re-capture would, and every atom whose content this wrote gets an
/// `EmbedAtoms` job in the transaction. An upload failure aborts. The caller
/// holds the source's identity lock and owns the commit; `uploads` lists
/// every object written, for its rollback cleanup.
///
/// Private to `ingest` like [`store_capture_tx`]: it inserts atoms. The
/// door's `ingest::rechunk_existing_version_tx` is the maintenance entry the
/// oversized-chunk backfill calls.
pub(super) async fn rechunk_version_tx(
    tx: &mut Transaction<'_, Postgres>,
    dest: Destination<'_>,
    source_item_id: Uuid,
    version_id: Uuid,
    base_source_id: &str,
    prepared: &Prepared,
    atoms: &AtomFields<'_>,
    uploads: &mut Vec<String>,
) -> Result<Reconciled, FunnelError> {
    let atom_text = CleanAtomText::of(atoms);
    let atoms = &atom_text.apply(*atoms);
    let chunks = &prepared.chunks;
    let ids: Vec<Uuid> = chunks.iter().map(|_| Uuid::new_v4()).collect();
    let keys = ids
        .iter()
        .map(|id| coralswarm_core::r2::blob_key_for(dest.org_id, dest.graph_id, *id))
        .collect::<Result<Vec<String>, _>>()
        .map_err(|e| FunnelError { stage: "blob key", message: e.to_string(), permanent: true, rls_refused: false, storage: false })?;
    let inserted = insert_chunks_tx(tx, dest, atoms, prepared, &ids, &keys).await?;
    let position: std::collections::HashMap<Uuid, usize> = ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
    if let Some(r2) = dest.r2 {
        for id in &inserted {
            let i = position[id];
            let (_, result) = put_body(r2, *id, keys[i].clone(), chunks[i].text.as_bytes().to_vec(), *dest.dek).await;
            uploads.push(result.map_err(FunnelError::upload("chunk upload"))?);
        }
    } else if !inserted.is_empty() {
        let texts: Vec<String> = inserted.iter().map(|id| chunks[position[id]].text.as_str().to_owned()).collect();
        sqlx::query(
            "UPDATE atoms a SET content_inline=v.t FROM unnest($2::uuid[],$3::text[]) AS v(id,t)
              WHERE a.graph_id=$1 AND a.atom_id=v.id",
        )
        .bind(dest.graph_id)
        .bind(&inserted)
        .bind(&texts)
        .execute(&mut **tx)
        .await
        .map_err(|e| FunnelError::at("inline chunk content")(e.to_string()))?;
    }
    let reconciled = crate::chunk_reconcile::reconcile_tx(
        tx,
        &crate::chunk_reconcile::ChunkTarget {
            graph_id: dest.graph_id,
            account_id: dest.account_id,
            source_item_id,
            version_id,
            source_type: atoms.source_type,
            base_source_id,
            content_hash: atoms.content_hash,
            participants: Some(atoms.participants),
            org_id: dest.org_id,
            dek: dest.dek,
            r2: dest.r2,
            device_id: crate::store::CLOUD_DEVICE_ID,
        },
        &prepared.positional(),
        &inserted,
        uploads,
    )
    .await
    .map_err(FunnelError::failed("chunk reconcile"))?;
    let written = reconciled.written();
    if !written.is_empty() {
        crate::jobs::queue::enqueue_tx(
            tx,
            &crate::jobs::JobKind::EmbedAtoms { atom_ids: written },
            Some(dest.account_id),
            None,
            0,
        )
        .await
        .map_err(|e| FunnelError::at("embed enqueue")(e.to_string()))?;
    }
    Ok(reconciled)
}

/// Upload one chunk body to the key its row names.
async fn put_body(
    r2: &coralswarm_core::r2::R2Client,
    id: Uuid,
    key: String,
    bytes: Vec<u8>,
    dek: [u8; 32],
) -> (Uuid, Result<String, coralswarm_core::r2::R2Error>) {
    (id, r2.put_atom_blob_at(&key, &bytes, &dek).await.map(|()| key))
}

/// One INSERT for every new position; existing positions are left to
/// reconcile (`ON CONFLICT DO NOTHING`). Returns the ids actually inserted.
async fn insert_chunks_tx(
    tx: &mut Transaction<'_, Postgres>,
    dest: Destination<'_>,
    a: &AtomFields<'_>,
    prepared: &Prepared,
    ids: &[Uuid],
    keys: &[String],
) -> Result<Vec<Uuid>, FunnelError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let chunks = &prepared.chunks;
    let source_ids: Vec<&str> = chunks.iter().map(|c| c.source_id.as_str()).collect();
    let excerpts: Vec<Option<&str>> = chunks.iter().map(|c| c.columns.excerpt.as_deref()).collect();
    let titles: Vec<Option<&str>> = chunks.iter().map(|c| c.columns.title.as_deref()).collect();
    let title_kinds: Vec<Option<&str>> = chunks.iter().map(|c| c.columns.title_kind.as_deref()).collect();
    let symbols: Vec<Option<&str>> = chunks.iter().map(|c| c.columns.symbols.as_deref()).collect();
    sqlx::query_scalar(
        "INSERT INTO atoms
            (atom_id,graph_id,source_type,source_id,content_blob_key,participants,timestamp,ingested_by,
             scope,fts_language,created_at,excerpt,title,title_kind,symbols,content_hash,header_len,
             channel_id,channel_name,author_id,author_name,workspace_id,project,mcp_client_name)
         SELECT u.id,$2,$3,u.sid,u.key,$6,$7,$8,$9,$10,now(),u.excerpt,u.title,u.title_kind,u.symbols,
                $15,$16,$17,$18,$19,$20,$21,$22,$23
           FROM unnest($1::uuid[],$4::text[],$5::text[],$11::text[],$12::text[],$13::text[],$14::text[])
                AS u(id,sid,key,excerpt,title,title_kind,symbols)
         ON CONFLICT (graph_id,source_type,source_id) DO NOTHING
         RETURNING atom_id",
    )
    .bind(ids)
    .bind(dest.graph_id)
    .bind(a.source_type)
    .bind(&source_ids)
    .bind(keys)
    .bind(a.participants)
    .bind(a.timestamp)
    .bind(a.ingested_by)
    .bind(a.scope)
    .bind(a.fts_language)
    .bind(&excerpts)
    .bind(&titles)
    .bind(&title_kinds)
    .bind(&symbols)
    .bind(a.content_hash)
    .bind(prepared.header_len)
    .bind(a.channel_id)
    .bind(a.channel_name)
    .bind(a.author_id)
    .bind(a.author_name)
    .bind(a.workspace_id)
    .bind(a.project)
    .bind(a.mcp_client_name)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| {
        let message = e.to_string();
        FunnelError {
            stage: "chunk insert",
            // SQLSTATE 42501: the RETURNING row is invisible under RLS (wrong
            // ocean, or a timestamp before the actor's history window).
            rls_refused: message.contains("violates row-level security"),
            message,
            permanent: false,
            storage: false,
        }
    })
}

/// `op_log` create ops for the inserted rows, exactly what
/// `Store::insert_atoms_scoped` appends.
#[allow(clippy::too_many_arguments)]
async fn append_create_ops_tx(
    tx: &mut Transaction<'_, Postgres>,
    store: &dyn crate::store::Store,
    graph_id: Uuid,
    atoms: &AtomFields<'_>,
    chunks: &[PreparedChunk],
    ids: &[Uuid],
    keys: &[String],
    inserted: &[Uuid],
) -> Result<(), FunnelError> {
    let now = Utc::now();
    for atom_id in inserted {
        let Some(i) = ids.iter().position(|id| id == atom_id) else { continue };
        let op = crate::store::sync::atom_create_op(
            Uuid::new_v4(),
            *atom_id,
            graph_id,
            atoms.source_type,
            &chunks[i].source_id,
            &keys[i],
            atoms.participants,
            atoms.timestamp,
            atoms.ingested_by,
            atoms.scope,
            atoms.fts_language,
            now,
            store.device_id(),
        );
        crate::store::append_op_tx(tx, &op)
            .await
            .map_err(|e| FunnelError::at("sync op")(e.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_exact_native_meet_identity_is_immutable() {
        let origin = SourceOrigin::native("google_meet", "transcript", "google_meet_attendee_capture").unwrap()
            .with_container("conference_record", "conferenceRecords/a", "google_meet:conference_records", None).unwrap();
        let metadata = serde_json::json!({});
        let prepared = Prepared::default();
        let selected = |origin: Option<&SourceOrigin>, source_id: &str| is_immutable_google_meet(&CaptureRequest {
            source: CaptureSource {
                source_type: "meet", provider_namespace: "declared:user:x", source_id,
                normalized_text: "Alice: First", representation: "normalized_text", metadata: &metadata,
                occurred_at: None, participants: &[], ingested_by: None, scope: "private", duration_ms: None, segments: None,
            },
            origin, chunks: &prepared,
            atoms: AtomFields::new("meet", &[], Utc::now(), None),
            embed_key: "unused".into(), sync_ops: None,
        });
        assert!(selected(Some(&origin), "meet:conferenceRecords/a"));
        assert!(!selected(None, "meet:conferenceRecords/a"), "public source type is insufficient");
        assert!(!selected(Some(&origin), "meet:conferenceRecords/other"), "container must bind the native ID");
        for change in 0..5 {
            let mut other = origin.clone();
            match change {
                0 => other.transport = crate::source_origin::CaptureTransport::Composio,
                1 => other.service_id = crate::source_origin::ServiceId::new("other").unwrap(),
                2 => other.content_kind = crate::source_origin::ContentKind::new("message").unwrap(),
                3 => other.capture_surface = None,
                _ => other.container.as_mut().unwrap().provider_namespace = crate::source_origin::ProviderNamespace::new("other:namespace").unwrap(),
            }
            assert!(!selected(Some(&other), "meet:conferenceRecords/a"));
        }
    }
    use coralswarm_core::chunker::chunk;

    #[test]
    fn a_hint_wins_then_the_code_path_then_the_source_type() {
        assert!(matches!(chunk_strategy("linear_issue", None, None), ChunkStrategy::Generic));
        assert!(matches!(chunk_strategy("slack", None, None), ChunkStrategy::Slack));
        assert!(matches!(chunk_strategy("transcript", None, Some(&ChunkStrategy::Generic)), ChunkStrategy::Generic));
        assert!(matches!(
            chunk_strategy("github_code", Some("src/lib.rs"), None),
            ChunkStrategy::Code { ref path, .. } if path == "src/lib.rs"
        ));
        assert!(matches!(chunk_strategy("github_code", Some(""), None), ChunkStrategy::Code { ref path, .. } if path.is_empty()));
    }

    #[test]
    fn identical_text_from_two_sources_gets_two_embed_keys() {
        let g = Uuid::nil();
        assert_ne!(embed_key(g, "slack", "C1/1.1", "h"), embed_key(g, "slack", "C1/2.2", "h"));
        assert_ne!(embed_key(g, "slack", "x", "h"), embed_key(g, "google_chat", "x", "h"));
        assert_eq!(embed_key(g, "slack", "C1/1.1", "h"), embed_key(g, "slack", "C1/1.1", "h"), "a retry dedupes");
    }

    #[test]
    fn titles_and_excerpts_come_from_the_body_whatever_the_header() {
        let chunks = chunk("Ship the pilot on Friday. Morgan owns it.", ChunkStrategy::Slack);
        for header in [Header::None, Header::Recorded("Alice in #eng".into()), Header::Inline("Alice in #eng".into())] {
            let prepared = Prepared::new("m1", "slack", "", &chunks, &header, None);
            let c = &prepared.chunks[0];
            assert_eq!(c.source_id, "m1:chunk:0");
            assert_eq!(c.columns.title.as_deref(), Some("Ship the pilot on Friday"));
            assert_eq!(c.columns.title_kind.as_deref(), Some("fallback"));
            assert!(c.columns.excerpt.as_deref().is_some_and(|e| e.starts_with("Ship the pilot")), "{header:?}");
            match &header {
                Header::None => assert_eq!((c.text.as_str(), c.header_len), ("Ship the pilot on Friday. Morgan owns it.", None)),
                Header::Recorded(_) => assert_eq!(c.header_len, Some(("Alice in #eng".len() + 2) as i32)),
                Header::Inline(_) => assert!(c.text.starts_with("Alice in #eng\n\n") && c.header_len.is_none()),
            }
        }
    }

    #[test]
    fn a_hinted_title_titles_every_chunk() {
        let text = (0..400).map(|i| format!("Sentence number {i} about the rollout.")).collect::<Vec<_>>().join(" ");
        let chunks = chunk(&text, ChunkStrategy::Generic);
        assert!(chunks.len() > 1);
        let prepared = Prepared::new("i", "linear_issue", &text, &chunks, &Header::None, Some("  COR-1: Fresh decision  "));
        assert!(prepared.chunks.iter().all(|c| c.columns.title.as_deref() == Some("COR-1: Fresh decision")));
        assert!(prepared.chunks.iter().all(|c| c.columns.title_kind.as_deref() == Some("source")),
            "a recipe title is the provider's own; the title sweep must not overwrite it");
        let blank = Prepared::new("i", "linear_issue", &text, &chunks, &Header::None, Some("   "));
        assert_eq!(blank.chunks[0].columns.title.as_deref(), Some("Sentence number 0 about the rollout"));
        assert_eq!(blank.chunks[0].columns.title_kind.as_deref(), Some("fallback"));
    }

    #[test]
    fn calendar_titles_are_the_summary_line_for_every_chunk() {
        let content = "Weekly sync\n\nJoin: https://meet.example.com/abc";
        let chunks = chunk(content, ChunkStrategy::Generic);
        let prepared = Prepared::new("e", "google_calendar", content, &chunks, &Header::None, None);
        assert!(prepared.chunks.iter().all(|c| c.columns.title.as_deref() == Some("Weekly sync")));
    }
}

#[cfg(test)]
mod pg_tests;

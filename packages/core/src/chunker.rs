/// Source-type-aware text chunker for CoralSwarm.
///
/// Each piece of ingested content (transcript, email, Slack thread, generic doc) is
/// split into `Chunk`s before embedding.  The target embedding model is
/// `bge-small-en-v1.5` with a 512-token context window.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Versioning
// ---------------------------------------------------------------------------

/// Current chunking-strategy version, stamped onto every atom's
/// `atoms.chunker_version` column at embed time
/// (`Store::set_atom_embeddings`). Bump when the chunking algorithm changes
/// in a way that alters chunk boundaries or the text that gets embedded.
///
/// v2 (contextual chunk headers, cs-phase0-chunk-headers): meeting/transcript
/// atoms now get a `Meeting: {title}, {date}. Participants: {names}.` header
/// prepended to their persisted content before embedding (see
/// `build_meeting_header` below), so chunk boundaries are unchanged but the
/// embedded/tsvectored TEXT differs from v1 — mixed-version corpora should be
/// re-embedded to pick up the header (tracked as follow-up; existing v1 atoms
/// keep working, just without header context).
pub const CHUNKER_VERSION: i16 = 2;

// ---------------------------------------------------------------------------
// Contextual chunk headers (cs-phase0-chunk-headers)
// ---------------------------------------------------------------------------

/// Build a short "Meeting: {title}, {date}. Participants: {names}." context
/// header to prepend to a meeting/transcript atom's text before embedding and
/// before building `content_tsv`, so semantically similar snippets from
/// different meetings are distinguishable and queries mentioning meeting /
/// participant context can match (Anthropic "contextual retrieval" pattern).
///
/// Every clause is independently optional — the caller passes whatever
/// metadata it actually has, and this gracefully omits what's missing:
/// - `title` only known when the raw transcript's first line looks like one
///   (see `meeting_title_from_raw` in `routes/meetings.rs`) — often absent
///   (e.g. Slack Huddles have no calendar subject).
/// - `occurred_at` is `atoms.timestamp`, which is always present in practice.
/// - `participants` may be empty (e.g. a not-yet-diarized transcript).
///
/// Returns `None` only when there is truly nothing to say (no title, no date,
/// no participants) — callers should skip prepending anything in that case
/// rather than emit an empty/near-empty header.
///
/// Scope: only called for meeting-shaped sources (`ChunkStrategy::Transcript`
/// — "transcript"/"zoom"/"meet"/"teams_transcript"). Chat sources (Slack,
/// Teams channel/chat) already carry their own "{sender} in #{channel} ·
/// {when}" provenance header (`routes/slack.rs`), which is intentionally
/// user-visible and NOT built by this function; generic/email sources have no
/// well-defined "meeting" to describe and get no header at all.
pub fn build_meeting_header(
    title: Option<&str>,
    occurred_at: Option<DateTime<Utc>>,
    participants: &[String],
) -> Option<String> {
    let title = title.map(str::trim).filter(|t| !t.is_empty());
    let date = occurred_at.map(|dt| dt.format("%Y-%m-%d").to_string());

    let meeting_clause = match (title, &date) {
        (Some(t), Some(d)) => Some(format!("Meeting: {t}, {d}.")),
        (Some(t), None) => Some(format!("Meeting: {t}.")),
        (None, Some(d)) => Some(format!("Meeting on {d}.")),
        (None, None) => None,
    };

    let names: Vec<&str> = participants
        .iter()
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect();
    let participants_clause = if names.is_empty() {
        None
    } else {
        Some(format!("Participants: {}.", names.join(", ")))
    };

    match (meeting_clause, participants_clause) {
        (Some(m), Some(p)) => Some(format!("{m} {p}")),
        (Some(m), None) => Some(m),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    }
}

/// Build a short "Session: {title}. Repo: {repo_path}. Branch: {branch}."
/// context header to prepend to a coding-session checkpoint atom's text
/// before embedding and before building `content_tsv` (coding-session
/// capture Phase 1, audit amendment A3) — the same "Anthropic contextual
/// retrieval" pattern [`build_meeting_header`] uses.
///
/// This is the ONLY place per-checkpoint branch/repo context lives: the
/// audit rejected new `atoms` columns for this (D4 reject) in favor of
/// embedding it directly into the persisted/embedded/tsvectored text, exactly
/// like the meeting header — it's strippable on display via the SAME
/// `header_len`-offset mechanism (migration 0066), so `atoms` gains zero new
/// columns for this.
///
/// Every clause is independently optional and gracefully omitted when
/// missing; `None` only when there is truly nothing to say.
pub fn build_coding_session_header(
    title: Option<&str>,
    repo_path: Option<&str>,
    branch: Option<&str>,
) -> Option<String> {
    let title = title.map(str::trim).filter(|t| !t.is_empty());
    let repo_path = repo_path.map(str::trim).filter(|r| !r.is_empty());
    let branch = branch.map(str::trim).filter(|b| !b.is_empty());

    let session_clause = title.map(|t| format!("Session: {t}."));
    let repo_clause = repo_path.map(|r| format!("Repo: {r}."));
    let branch_clause = branch.map(|b| format!("Branch: {b}."));

    let clauses: Vec<String> = [session_clause, repo_clause, branch_clause]
        .into_iter()
        .flatten()
        .collect();

    if clauses.is_empty() {
        None
    } else {
        Some(clauses.join(" "))
    }
}

/// Email, notes, and documents get one provenance line. Meetings, chat, and
/// coding sessions already carry their own header and are not in this set.
///
/// The clauses are the same for every company: title (an email subject is
/// stored as the title), date, project. Empty clauses are omitted. There is
/// no per-company rule and no model inventing the line.
pub fn provenance_source(source_type: &str) -> bool {
    matches!(
        source_type.to_ascii_lowercase().as_str(),
        "email"
            | "gmail"
            | "outlook"
            | "note"
            | "document"
            | "notion"
            | "gdrive"
            | "google_doc"
            | "file"
    )
}

/// `Title: {title}. Date: {date}. Project: {project}.` — each clause optional.
/// `None` when every clause is empty.
pub fn build_provenance_header(
    title: Option<&str>,
    occurred_at: Option<DateTime<Utc>>,
    project: Option<&str>,
) -> Option<String> {
    let title = title.map(str::trim).filter(|t| !t.is_empty());
    let project = project.map(str::trim).filter(|p| !p.is_empty());
    let date = occurred_at.map(|dt| dt.format("%Y-%m-%d").to_string());

    let clauses: Vec<String> = [
        title.map(|t| format!("Title: {t}.")),
        date.map(|d| format!("Date: {d}.")),
        project.map(|p| format!("Project: {p}.")),
    ]
    .into_iter()
    .flatten()
    .collect();

    if clauses.is_empty() {
        None
    } else {
        Some(clauses.join(" "))
    }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    /// Chunk text content.
    pub text: String,
    /// 0-based index of this chunk within its source document.
    pub position: usize,
    /// Speaker name, populated for `Transcript` strategy.
    pub speaker: Option<String>,
    /// Thread parent message ID, populated for `Slack` strategy.
    pub parent_message_id: Option<String>,
    /// Approximate token count (`words × 4/3`).
    pub token_count: usize,
    /// The UNCAPPED `"{kind} {name}"` spellings of every declaration this
    /// chunk contains — the sibling of the `items …` header
    /// [`ts::combined_label`] builds for `ChunkStrategy::Code`, but never
    /// truncated to "+N more" the way the header's display text is.
    ///
    /// Populated ONLY by the code-chunking path (`ts::flush_group` /
    /// `ts::push_split_item`); every other strategy (Transcript, Email,
    /// Slack, Generic) leaves this empty — there is no "declaration" concept
    /// for those, and an empty `Vec` is the honest answer, not a gap.
    ///
    /// Backend insert sites persist this to the new `atoms.symbols` column
    /// (migrations `0247`/`0100`) so the atom keyword leg's symbol candidate
    /// (`store::keyword_search_pg`) can admit a chunk by a declaration that
    /// never made it into the header — and therefore never made it into
    /// `atoms.excerpt`'s first `JOURNAL_EXCERPT_MAX_CHARS` characters —
    /// because `combined_label` had to cut the header's display list short.
    /// The header text itself is UNCHANGED by this field: it stays exactly
    /// the truncated, human-readable string it always was.
    ///
    /// This closes "declared but hidden behind `+N more`", not "used but
    /// never declared" — a symbol that only ever appears at a CALL site, with
    /// no declaration in any chunk header this repo produces, stays
    /// unreachable by design (see `store::keyword_search_pg`'s doc comment
    /// for the affordability argument that rules out indexing every mention).
    #[serde(default)]
    pub symbols: Vec<String>,
}

impl Chunk {
    fn new(text: String, position: usize) -> Self {
        let token_count = count_tokens(&text);
        Chunk {
            text,
            position,
            speaker: None,
            parent_message_id: None,
            token_count,
            symbols: Vec::new(),
        }
    }

    /// Re-prepend a stripped speaker label to this chunk's text.
    ///
    /// `ChunkStrategy::Transcript` splits `"Alice: hello"` into `speaker="Alice"`,
    /// `text="hello"` for speaker-turn merging (see `chunk_transcript` below), but
    /// nothing downstream reads `chunk.speaker` — extraction and synthesis only
    /// ever read the persisted content text. Without this, speaker attribution
    /// (including from diarized transcripts, CS-422) is silently
    /// destroyed on ingest. Backend insert sites (`routes/ingest.rs`,
    /// `workers/huddle.rs`) call this before persisting chunk text.
    pub fn text_with_speaker(&self) -> String {
        match &self.speaker {
            Some(speaker) => format!("{speaker}: {}", self.text),
            None => self.text.clone(),
        }
    }
}

/// Distinct chunk speakers, in first-seen order.
///
/// Shared by both backend transcript-ingest call sites so speaker
/// deduplication stays in lockstep (see `transcript_speaker_is_separate_from_text_by_design`
/// below, which documents the precondition these callers rely on).
pub fn distinct_chunk_speakers(chunks: &[Chunk]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut list = Vec::new();
    for ci in chunks {
        if let Some(speaker) = &ci.speaker {
            if seen.insert(speaker.clone()) {
                list.push(speaker.clone());
            }
        }
    }
    list
}

// ---------------------------------------------------------------------------
// Strategy enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ChunkStrategy {
    /// Zoom / Meet / Teams transcripts – split on speaker-turn boundaries.
    Transcript,
    /// Emails – split on `"> "` quoted-reply boundaries.
    Email,
    /// Slack messages – one message per chunk, merge short consecutive ones.
    Slack,
    /// Fallback – paragraph/sentence-aware 512-token target with 64-token overlap.
    Generic,
    /// GitHub raw code tier (docs/connectors/github.md §3.9, PR B) —
    /// syntax-aware chunking on item boundaries (functions, impls/classes,
    /// modules, types) for the seven recognised languages, merging small
    /// adjacent items up to [`TOKEN_TARGET`] and splitting any single
    /// oversized item into overlapping line windows; a fixed line-window
    /// fallback for every other language, or when this crate was built
    /// without the `code-chunking` feature. `path` is the repo-relative file
    /// path, embedded verbatim into every produced chunk's one-line header
    /// (`// {path} — {item kind} {item name}`, language-appropriate comment
    /// syntax) so a retrieved chunk is self-describing without a join back
    /// to `atoms.source_id`.
    ///
    /// See [`ChunkStrategy::code_for_path`] — that is the constructor a
    /// caller who actually knows the path should use.
    /// [`ChunkStrategy::for_source_type`] alone cannot know the path (it
    /// only ever sees the bare `source_type` string), so it hands back
    /// `Code { language: CodeLanguage::Unknown, path: String::new() }` —
    /// still a real, working strategy (the line-window fallback), just
    /// without a language or a useful header, which is why the backend's
    /// ingest route (`IngestAtomRequest::code_path`) calls `code_for_path`
    /// directly instead of going through `for_source_type` for this one
    /// source type.
    Code { language: CodeLanguage, path: String },
}

/// A source language [`ChunkStrategy::Code`] recognises for syntax-aware
/// chunking (docs/connectors/github.md §3.9). Selected from a file's
/// extension by [`CodeLanguage::from_path`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeLanguage {
    Rust,
    /// `.ts` / `.mts` / `.cts` — NOT `.tsx` (see [`CodeLanguage::Tsx`]): the
    /// underlying grammar (`tree-sitter-typescript`) ships TypeScript and
    /// TSX as two distinct parsers (`LANGUAGE_TYPESCRIPT` / `LANGUAGE_TSX`),
    /// so this crate mirrors that split even though the design doc's own
    /// language list writes them as one entry ("TypeScript/TSX").
    TypeScript,
    Tsx,
    JavaScript,
    Python,
    Go,
    Java,
    CSharp,
    /// Extension not recognised (or [`ChunkStrategy::for_source_type`] was
    /// used without a real path — see that variant's own doc). Always uses
    /// the fixed-line-window fallback, `code-chunking` feature or no.
    Unknown,
}

impl CodeLanguage {
    /// Classify a repo-relative (or absolute — only the extension matters)
    /// path by its extension. Case-insensitive; unrecognised or missing
    /// extensions map to [`CodeLanguage::Unknown`].
    pub fn from_path(path: &str) -> Self {
        let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
        match ext.as_str() {
            "rs" => CodeLanguage::Rust,
            "ts" | "mts" | "cts" => CodeLanguage::TypeScript,
            "tsx" => CodeLanguage::Tsx,
            "js" | "jsx" | "mjs" | "cjs" => CodeLanguage::JavaScript,
            "py" | "pyi" => CodeLanguage::Python,
            "go" => CodeLanguage::Go,
            "java" => CodeLanguage::Java,
            "cs" => CodeLanguage::CSharp,
            _ => CodeLanguage::Unknown,
        }
    }

    /// The line-comment prefix used for this language's chunk headers.
    /// Python is the one recognised language here with a non-`//` line
    /// comment; every other recognised language (and the `Unknown`
    /// fallback, since a header is retrieval-context prose, not code that
    /// must actually compile) uses `//`.
    fn comment_prefix(self) -> &'static str {
        match self {
            CodeLanguage::Python => "#",
            _ => "//",
        }
    }

    /// The separator this language conventionally uses between an outer
    /// container's name and a nested item's own name in a chunk header
    /// (`Outer::inner` for Rust, `Outer.inner` everywhere else) — cosmetic
    /// only, never parsed back. Only called from the tree-sitter walk
    /// (nested containers are a `code-chunking`-only concept — the
    /// line-window fallback has no notion of nesting).
    #[cfg(feature = "code-chunking")]
    fn nesting_separator(self) -> &'static str {
        match self {
            CodeLanguage::Rust => "::",
            _ => ".",
        }
    }
}

impl ChunkStrategy {
    /// Select the appropriate strategy for a known source type string.
    pub fn for_source_type(source_type: &str) -> Self {
        match source_type.to_lowercase().as_str() {
            "transcript" | "zoom" | "meet" | "teams_transcript" => ChunkStrategy::Transcript,
            "email" | "gmail" | "outlook" => ChunkStrategy::Email,
            // Teams channel/chat messages, and Google Chat spaces/DMs
            // (gsuite Train G4 PR 2 — design doc §3.6), are chat-shaped like
            // Slack.
            "slack" | "teams_channel" | "teams_chat" | "google_chat" => ChunkStrategy::Slack,
            // GitHub connector PR A (docs/connectors/github.md §3.5/§3.9).
            // The four narrative types this PR registers are prose-shaped —
            // same paragraph/sentence-aware path every other prose source
            // falls through to `_` for, made an EXPLICIT arm here (rather
            // than left implicit in the wildcard) so the six github source
            // types are visibly registered at this switch site, per the
            // connector doc's own instruction. `github_code_summary` (a
            // GENERATED prose summary, never raw code) stays on `Generic`.
            "github_pr" | "github_issue" | "github_release" | "github_commit" | "github_code_summary" => {
                ChunkStrategy::Generic
            }
            // PR B: real syntax-aware chunking. `for_source_type` only ever
            // sees the bare string, never the file path, so this always
            // hands back `Unknown` (still real — see `Code`'s own doc); the
            // backend calls `code_for_path` directly once it knows the path.
            "github_code" => ChunkStrategy::Code { language: CodeLanguage::Unknown, path: String::new() },
            _ => ChunkStrategy::Generic,
        }
    }

    /// Build a [`ChunkStrategy::Code`] for a known repo-relative file path —
    /// the constructor the backend's ingest route uses once it knows the
    /// path (`IngestAtomRequest::code_path`), giving syntax-aware chunking
    /// when the extension is recognised (and this crate was built with the
    /// `code-chunking` feature); the same fixed-line-window fallback as
    /// every other unrecognised language otherwise, always with the same
    /// self-describing header.
    pub fn code_for_path(path: &str) -> Self {
        ChunkStrategy::Code { language: CodeLanguage::from_path(path), path: path.to_string() }
    }
}

// ---------------------------------------------------------------------------
// Token counting
// ---------------------------------------------------------------------------

/// Approximate token count using the word × 4/3 heuristic.
///
/// For English text, `bge-small-en-v1.5`'s BPE tokeniser produces roughly
/// 1.2–1.4 tokens per whitespace-delimited word.  We use 4/3 ≈ 1.33 as a
/// conservative middle estimate.  Exact counts are not required for chunking.
pub fn count_tokens(text: &str) -> usize {
    let words = text.split_whitespace().count();
    words * 4 / 3
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Split `text` into chunks according to `strategy`.
///
/// Every strategy's output then passes through [`bound_chunk_bytes`], so no
/// chunk is ever longer than [`MAX_CHUNK_BYTES`] whatever its shape.
pub fn chunk(text: &str, strategy: ChunkStrategy) -> Vec<Chunk> {
    let chunks = match strategy {
        ChunkStrategy::Generic => chunk_generic(text),
        ChunkStrategy::Transcript => chunk_transcript(text),
        ChunkStrategy::Email => chunk_email(text),
        ChunkStrategy::Slack => chunk_slack(text, None),
        ChunkStrategy::Code { language, path } => code_boundaries::chunk_code(text, &path, language),
    };
    bound_chunk_bytes(chunks)
}

// ---------------------------------------------------------------------------
// Hard size ceiling
// ---------------------------------------------------------------------------

/// Hard ceiling on one chunk's persisted body ([`Chunk::text_with_speaker`]),
/// in bytes.
///
/// The strategies size chunks with [`count_tokens`], which counts words, so a
/// run of text with no whitespace or sentence punctuation (a URL dump, a
/// table, base64, a long email body before the email strategy bounded its
/// groups) used to come out as ONE chunk of any length. The cloud embedder
/// (`openai/text-embedding-3-small`) rejects an input over 8192 tokens with
/// an HTTP 400, and such an atom never gets a vector (prod, 2026-10-23: a
/// Gmail body over the limit was re-sent every ten minutes).
///
/// Bytes, not estimated tokens: a byte-level BPE token is at least one byte,
/// so a body of at most this many bytes is at most this many tokens, for any
/// text. 6000 leaves 2192 bytes of the 8192 for the contextual header a
/// writer prepends (`ingest::capture::Header`). Ordinary prose chunks are far
/// below it ([`TOKEN_TARGET`] is about 2 KB of English), so this only ever
/// splits the shapes above.
pub const MAX_CHUNK_BYTES: usize = 6000;

/// Split every chunk whose body is over [`MAX_CHUNK_BYTES`] into consecutive
/// pieces that fit, then renumber positions. Nothing is dropped: the pieces
/// concatenate back to the original text, less the whitespace at each cut.
/// A piece keeps its chunk's speaker, parent and symbols.
fn bound_chunk_bytes(chunks: Vec<Chunk>) -> Vec<Chunk> {
    if chunks.iter().all(|c| c.text_with_speaker().len() <= MAX_CHUNK_BYTES) {
        return chunks;
    }
    let mut out: Vec<Chunk> = Vec::with_capacity(chunks.len() + 1);
    for chunk in chunks {
        // `text_with_speaker` prepends "{speaker}: ", which counts too.
        let prefix = chunk.speaker.as_ref().map_or(0, |s| s.len() + 2);
        let limit = MAX_CHUNK_BYTES.saturating_sub(prefix).max(MAX_CHUNK_BYTES / 2);
        if chunk.text.len() <= limit {
            out.push(chunk);
            continue;
        }
        for piece in split_to_bytes(&chunk.text, limit) {
            let mut c = Chunk::new(piece, 0);
            c.speaker = chunk.speaker.clone();
            c.parent_message_id = chunk.parent_message_id.clone();
            c.symbols = chunk.symbols.clone();
            out.push(c);
        }
    }
    for (position, c) in out.iter_mut().enumerate() {
        c.position = position;
    }
    out
}

/// Cut `text` into pieces of at most `limit` bytes, each cut at the last
/// paragraph break, line break, sentence end or space in the back half of
/// the window, else at the last character boundary. Pieces are trimmed and
/// never empty.
pub(crate) fn split_to_bytes(text: &str, limit: usize) -> Vec<String> {
    let mut pieces = Vec::new();
    let mut rest = text.trim();
    while rest.len() > limit {
        let mut end = limit;
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        let window = &rest[..end];
        let floor = end / 2;
        let cut = ["\n\n", "\n", ". ", "? ", "! ", " ", "\t"]
            .iter()
            .find_map(|sep| {
                window
                    .rfind(sep)
                    .map(|at| at + sep.len())
                    .filter(|at| *at > floor)
            })
            .unwrap_or(end);
        let piece = rest[..cut].trim();
        if !piece.is_empty() {
            pieces.push(piece.to_string());
        }
        rest = rest[cut..].trim_start();
    }
    if !rest.is_empty() {
        pieces.push(rest.to_string());
    }
    pieces
}

// ---------------------------------------------------------------------------
// Generic strategy
// ---------------------------------------------------------------------------

/// Target chunk size, in *estimated* tokens (see [`count_tokens`]).
///
/// The embedder's actual hard cap is 512 BPE tokens (`embed_ort::DEFAULT_MAX_LENGTH`)
/// — but `count_tokens`'s words×4/3 heuristic only approximates the real BPE
/// tokenizer, and it *undercounts* on plenty of real content: punctuation-heavy
/// text, URLs/code, contractions, and non-English text all split into more
/// subword tokens per word than 4/3 assumes. Targeting the embedder's exact
/// cap (512) meant a chunk this heuristic judged "right at the limit" could
/// silently exceed it once actually tokenized, truncating the tail before
/// embedding with no signal anywhere (see `embed_ort::tokenize_batch`, which
/// now at least logs when that happens). 400 leaves ~22% headroom versus 512
/// so the estimate would have to be off by more than that — a much wider
/// margin than 4/3 undercounts by in practice — before truncation kicks in.
/// This only changes size accounting, not chunk *boundaries* (same
/// paragraph/sentence-aware splitting); if it changes produced chunks for a
/// given input, that's an argument for bumping `chunker_version` alongside
/// this change wherever that column lands.
const TOKEN_TARGET: usize = 400;
const TOKEN_OVERLAP: usize = 64;

/// Split `text` into sentences at `. `, `? `, `! ` boundaries.
fn split_sentences(text: &str) -> Vec<&str> {
    // We walk the string manually to avoid allocating an intermediate String.
    let mut sentences: Vec<&str> = Vec::new();
    let mut start = 0;
    let bytes = text.as_bytes();
    let len = bytes.len();

    let mut i = 0;
    while i < len {
        if i + 1 < len
            && (bytes[i] == b'.' || bytes[i] == b'?' || bytes[i] == b'!')
            && bytes[i + 1] == b' '
        {
            // Include the punctuation and the trailing space in the sentence.
            let end = i + 2;
            let s = text.get(start..end).unwrap_or("").trim();
            if !s.is_empty() {
                sentences.push(text.get(start..end).unwrap_or(""));
            }
            start = end;
            i = end;
        } else {
            i += 1;
        }
    }
    // Trailing fragment that has no sentence-ending punctuation.
    if start < len {
        let s = text[start..].trim();
        if !s.is_empty() {
            sentences.push(&text[start..]);
        }
    }
    sentences
}

/// Collect the last sentence(s) of `sentences` that together amount to at most
/// `overlap_tokens` tokens.  Returns them as a single overlap string.
fn build_overlap(sentences: &[String]) -> String {
    let mut overlap_tokens = 0usize;
    let mut overlap_sentences: Vec<&str> = Vec::new();
    for s in sentences.iter().rev() {
        let t = count_tokens(s);
        if overlap_tokens + t > TOKEN_OVERLAP {
            break;
        }
        overlap_tokens += t;
        overlap_sentences.push(s);
        if overlap_tokens >= TOKEN_OVERLAP {
            break;
        }
    }
    overlap_sentences.reverse();
    overlap_sentences.join(" ")
}

fn chunk_generic(text: &str) -> Vec<Chunk> {
    let paragraphs: Vec<&str> = text.split("\n\n").collect();
    let mut chunks: Vec<Chunk> = Vec::new();
    // Accumulator for the current window.
    let mut current_sentences: Vec<String> = Vec::new();
    let mut current_tokens: usize = 0;

    let flush = |current_sentences: &mut Vec<String>,
                 current_tokens: &mut usize,
                 chunks: &mut Vec<Chunk>| {
        if current_sentences.is_empty() {
            return;
        }
        let text = current_sentences.join(" ");
        let position = chunks.len();
        chunks.push(Chunk::new(text, position));

        // Build overlap for the *next* chunk.
        let overlap = build_overlap(current_sentences);
        *current_tokens = count_tokens(&overlap);
        current_sentences.clear();
        if !overlap.is_empty() {
            current_sentences.push(overlap);
        }
    };

    for para in paragraphs {
        let para = para.trim();
        if para.is_empty() {
            continue;
        }
        let para_tokens = count_tokens(para);

        if para_tokens <= TOKEN_TARGET {
            // Whole paragraph fits; check if it still fits in the current window.
            if current_tokens + para_tokens > TOKEN_TARGET && !current_sentences.is_empty() {
                flush(&mut current_sentences, &mut current_tokens, &mut chunks);
            }
            current_sentences.push(para.to_string());
            current_tokens += para_tokens;
        } else {
            // Paragraph is too large – split on sentence boundaries.
            let sentences = split_sentences(para);
            for sentence in sentences {
                let sentence = sentence.trim();
                if sentence.is_empty() {
                    continue;
                }
                let st = count_tokens(sentence);
                if current_tokens + st > TOKEN_TARGET && !current_sentences.is_empty() {
                    flush(&mut current_sentences, &mut current_tokens, &mut chunks);
                }
                current_sentences.push(sentence.to_string());
                current_tokens += st;
            }
        }
    }

    // Flush any remaining text.
    if !current_sentences.is_empty() {
        let text = current_sentences.join(" ");
        let position = chunks.len();
        chunks.push(Chunk::new(text, position));
    }

    chunks
}

// ---------------------------------------------------------------------------
// Transcript strategy
// ---------------------------------------------------------------------------

struct Turn {
    speaker: String,
    text: String,
}

/// Parse a line as a speaker turn.
///
/// Supported formats:
/// - `Speaker Name: text`
/// - `[Speaker Name] text`
fn parse_turn(line: &str) -> Option<Turn> {
    // Try `[Speaker Name] text`
    if line.starts_with('[') {
        if let Some(close) = line.find(']') {
            let speaker = line[1..close].trim().to_string();
            let text = line[close + 1..].trim().to_string();
            if !speaker.is_empty() {
                return Some(Turn { speaker, text });
            }
        }
    }
    // Try `Speaker Name: text`
    if let Some(colon_pos) = line.find(':') {
        let candidate = &line[..colon_pos];
        // A speaker label should not span more than ~4 words and must not
        // contain newlines.  This heuristic avoids treating URLs as speakers.
        if candidate.split_whitespace().count() <= 4
            && !candidate.is_empty()
            && !candidate.contains("://")
        {
            let speaker = candidate.trim().to_string();
            let text = line[colon_pos + 1..].trim().to_string();
            return Some(Turn { speaker, text });
        }
    }
    None
}

fn chunk_transcript(text: &str) -> Vec<Chunk> {
    // Step 1: group lines into turns.
    let mut turns: Vec<Turn> = Vec::new();
    let mut current_turn: Option<Turn> = None;

    for line in text.lines() {
        if let Some(turn) = parse_turn(line) {
            if let Some(prev) = current_turn.take() {
                turns.push(prev);
            }
            current_turn = Some(turn);
        } else if let Some(ref mut t) = current_turn {
            // Continuation of the current turn.
            if !line.trim().is_empty() {
                t.text.push(' ');
                t.text.push_str(line.trim());
            }
        }
        // Lines before the first speaker are ignored.
    }
    if let Some(t) = current_turn {
        turns.push(t);
    }

    // No speaker turns parsed — e.g. raw Whisper output or a pasted
    // transcript without "Name:" labels. Fall back to prose chunking so the
    // content is captured instead of silently dropped (previously yielded 0
    // chunks → nothing ingested).
    if turns.is_empty() {
        return chunk_generic(text);
    }

    // Step 2: merge consecutive same-speaker turns if combined < 256 tokens.
    const MERGE_THRESHOLD: usize = 256;
    let mut merged: Vec<Turn> = Vec::new();
    for turn in turns {
        if let Some(last) = merged.last_mut() {
            if last.speaker == turn.speaker
                && count_tokens(&last.text) + count_tokens(&turn.text) < MERGE_THRESHOLD
            {
                last.text.push(' ');
                last.text.push_str(&turn.text);
                continue;
            }
        }
        merged.push(turn);
    }

    // Step 3: build chunks, splitting oversized turns on sentence boundaries.
    let mut chunks: Vec<Chunk> = Vec::new();
    for turn in merged {
        let speaker = turn.speaker.clone();
        if count_tokens(&turn.text) <= TOKEN_TARGET {
            let mut c = Chunk::new(turn.text, chunks.len());
            c.speaker = Some(speaker);
            chunks.push(c);
        } else {
            // Split on sentences.
            let sentences = split_sentences(&turn.text);
            let mut current: Vec<String> = Vec::new();
            let mut current_tokens = 0usize;
            for s in sentences {
                let s = s.trim();
                if s.is_empty() {
                    continue;
                }
                let st = count_tokens(s);
                if current_tokens + st > TOKEN_TARGET && !current.is_empty() {
                    let text = current.join(" ");
                    let mut c = Chunk::new(text, chunks.len());
                    c.speaker = Some(speaker.clone());
                    chunks.push(c);
                    current.clear();
                    current_tokens = 0;
                }
                current.push(s.to_string());
                current_tokens += st;
            }
            if !current.is_empty() {
                let text = current.join(" ");
                let mut c = Chunk::new(text, chunks.len());
                c.speaker = Some(speaker);
                chunks.push(c);
            }
        }
    }
    chunks
}

// ---------------------------------------------------------------------------
// Email strategy
// ---------------------------------------------------------------------------

/// Determine the quote depth of a line (number of leading `"> "` or `">"` prefixes).
fn quote_depth(line: &str) -> usize {
    let mut depth = 0;
    let mut rest = line;
    loop {
        let trimmed = rest.trim_start();
        if let Some(after) = trimmed.strip_prefix("> ") {
            depth += 1;
            rest = after;
        } else if let Some(after) = trimmed.strip_prefix('>') {
            depth += 1;
            rest = after;
        } else {
            break;
        }
    }
    depth
}

/// Strip one level of `"> "` prefixes from each line.
fn strip_one_quote_level(text: &str) -> String {
    text.lines()
        .map(|line| {
            let trimmed = line.trim_start();
            if let Some(after) = trimmed.strip_prefix("> ") {
                after
            } else if let Some(after) = trimmed.strip_prefix('>') {
                after
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn chunk_email(text: &str) -> Vec<Chunk> {
    // Group consecutive lines by their quote depth.
    let mut groups: Vec<(usize, Vec<&str>)> = Vec::new(); // (depth, lines)

    for line in text.lines() {
        let d = quote_depth(line);
        if let Some(last) = groups.last_mut() {
            if last.0 == d {
                last.1.push(line);
                continue;
            }
        }
        groups.push((d, vec![line]));
    }

    let mut chunks: Vec<Chunk> = Vec::new();
    for (depth, lines) in groups {
        let raw = lines.join("\n");
        // Strip the quote markers that correspond to this depth level.
        let content = if depth == 0 {
            raw.clone()
        } else {
            // Strip `depth` levels of `> `.
            let mut stripped = raw.clone();
            for _ in 0..depth {
                stripped = strip_one_quote_level(&stripped);
            }
            stripped
        };
        let content = content.trim().to_string();
        if content.is_empty() {
            continue;
        }
        // A quote-depth run is a boundary, not a size bound: an unquoted
        // newsletter or a long reply is one run. Split an oversized run the
        // way every other prose source is split, as the transcript and Slack
        // strategies already do for an oversized turn or message.
        if count_tokens(&content) > TOKEN_TARGET {
            for piece in chunk_generic(&content) {
                let position = chunks.len();
                chunks.push(Chunk::new(piece.text, position));
            }
            continue;
        }
        let position = chunks.len();
        chunks.push(Chunk::new(content, position));
    }
    chunks
}

// ---------------------------------------------------------------------------
// Slack strategy
// ---------------------------------------------------------------------------

/// A pre-split Slack message (as produced by the Slack API).
#[derive(Debug, Clone)]
pub struct SlackMessage {
    pub text: String,
    pub user: String,
    pub parent_message_id: Option<String>,
}

/// Chunk a sequence of Slack messages.
///
/// Rules:
/// - Each message becomes one chunk.
/// - If a message > 512 tokens, apply Generic splitting.
/// - Consecutive messages from the same user, each < 64 tokens, are merged.
pub fn chunk_slack_messages(messages: &[SlackMessage]) -> Vec<Chunk> {
    const SLACK_MERGE_MAX: usize = 64;

    // Step 1: merge short consecutive same-user messages.
    struct Acc {
        user: String,
        texts: Vec<String>,
        parent_message_id: Option<String>,
    }

    let mut merged: Vec<Acc> = Vec::new();
    for msg in messages {
        let msg_tokens = count_tokens(&msg.text);
        let should_merge = msg_tokens < SLACK_MERGE_MAX
            && merged.last().map(|a: &Acc| a.user == msg.user).unwrap_or(false);
        if should_merge {
            if let Some(last) = merged.last_mut() {
                last.texts.push(msg.text.clone());
                if msg.parent_message_id.is_some() {
                    last.parent_message_id = msg.parent_message_id.clone();
                }
                continue;
            }
        }
        merged.push(Acc {
            user: msg.user.clone(),
            texts: vec![msg.text.clone()],
            parent_message_id: msg.parent_message_id.clone(),
        });
    }

    // Step 2: build chunks.
    let mut chunks: Vec<Chunk> = Vec::new();
    for acc in merged {
        let combined = acc.texts.join("\n");
        let parent_id = acc.parent_message_id;

        if count_tokens(&combined) > TOKEN_TARGET {
            // Apply generic splitting and attach parent_message_id.
            let sub_chunks = chunk_generic(&combined);
            for mut sc in sub_chunks {
                sc.position = chunks.len();
                sc.parent_message_id = parent_id.clone();
                chunks.push(sc);
            }
        } else {
            let position = chunks.len();
            let mut c = Chunk::new(combined, position);
            c.parent_message_id = parent_id;
            chunks.push(c);
        }
    }
    chunks
}

/// Chunk plain text using the Slack strategy (no structured message objects).
///
/// Each non-empty line is treated as a separate Slack message from an unknown user.
fn chunk_slack(text: &str, _parent_id: Option<String>) -> Vec<Chunk> {
    let messages: Vec<SlackMessage> = text
        .lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
        .map(|(i, l)| SlackMessage {
            text: l.trim().to_string(),
            user: format!("line_{i}"),
            parent_message_id: None,
        })
        .collect();
    chunk_slack_messages(&messages)
}

// ---------------------------------------------------------------------------
// Code strategy (docs/connectors/github.md §3.9, PR B)
// ---------------------------------------------------------------------------

/// `ChunkStrategy::Code`'s implementation. Two halves:
///
/// * A fixed-line-window fallback ([`chunk_fallback`]) — always compiled,
///   used for [`CodeLanguage::Unknown`] and for any oversized single item's
///   own split, ~80 lines per window with ~15 lines of overlap.
/// * A tree-sitter item-boundary walk ([`ts::try_chunk`]), compiled only
///   under the `code-chunking` feature (see that feature's own doc comment
///   in `Cargo.toml` for why it must stay opt-in) — chunks on top-level and
///   nested item boundaries (functions, impls/classes, modules, types),
///   merging adjacent small items up to [`TOKEN_TARGET`] and splitting any
///   single oversized item back through [`chunk_fallback`]'s line-window
///   logic. Falls through to the line-window fallback whenever the feature
///   is off, the language is `Unknown`, parsing fails outright, or parsing
///   somehow yields zero items (never a silent empty result for non-empty
///   input — see [`chunk_code`]'s own fallthrough).
mod code_boundaries {
    use super::{Chunk, CodeLanguage};

    /// Window size for the fixed line-window fallback, AND for splitting a
    /// single item too large to keep whole (§3.9).
    const WINDOW_LINES: usize = 80;
    const WINDOW_OVERLAP_LINES: usize = 15;

    /// Entry point `ChunkStrategy::Code` dispatches to. Empty (or
    /// whitespace-only) input always produces zero chunks — the backend
    /// treats that as `skipped_empty`, never as a normaliser `EmptyBody`
    /// defect (`connectors::github_backfill::IngestOutcome`'s own doc).
    pub(super) fn chunk_code(text: &str, path: &str, language: CodeLanguage) -> Vec<Chunk> {
        if text.trim().is_empty() {
            return Vec::new();
        }
        #[cfg(feature = "code-chunking")]
        {
            if let Some(chunks) = ts::try_chunk(text, path, language) {
                return chunks;
            }
        }
        chunk_fallback(text, path, language)
    }

    /// One line-comment header, self-describing without a join back to
    /// `atoms.source_id`: `// {path} — {kind} [{name}] [(part i/n)]`.
    /// `name`/`part` are independently optional; `part` is only rendered
    /// when `total > 1`.
    fn header_line(path: &str, language: CodeLanguage, kind: &str, name: Option<&str>, part: Option<(usize, usize)>) -> String {
        let prefix = language.comment_prefix();
        let mut label = kind.to_string();
        if let Some(n) = name.map(str::trim).filter(|n| !n.is_empty()) {
            label.push(' ');
            label.push_str(n);
        }
        if let Some((i, total)) = part {
            if total > 1 {
                label.push_str(&format!(" (part {i}/{total})"));
            }
        }
        format!("{prefix} {path} — {label}\n")
    }

    /// Split `text` into overlapping line windows. Returns `(body,
    /// first_line, last_line)`, both 1-based and inclusive. Empty input (or
    /// input with no lines at all) yields an empty `Vec` — never a single
    /// empty-bodied window.
    fn line_windows(text: &str, window: usize, overlap: usize) -> Vec<(String, usize, usize)> {
        let lines: Vec<&str> = text.lines().collect();
        if lines.is_empty() {
            return Vec::new();
        }
        let step = window.saturating_sub(overlap).max(1);
        let mut out = Vec::new();
        let mut start = 0usize;
        loop {
            let end = (start + window).min(lines.len());
            out.push((lines[start..end].join("\n"), start + 1, end));
            if end >= lines.len() {
                break;
            }
            start += step;
        }
        out
    }

    /// The fixed-line-window fallback (§3.9): every language the tree-sitter
    /// half does not recognise, and every language when the `code-chunking`
    /// feature is off. Each window gets the same one-line header convention
    /// as a syntax-aware chunk, naming the line range instead of an item.
    fn chunk_fallback(text: &str, path: &str, language: CodeLanguage) -> Vec<Chunk> {
        let windows = line_windows(text, WINDOW_LINES, WINDOW_OVERLAP_LINES);
        let mut chunks = Vec::with_capacity(windows.len());
        for (body, first, last) in windows {
            let header = header_line(path, language, "lines", Some(&format!("{first}-{last}")), None);
            let position = chunks.len();
            chunks.push(Chunk::new(format!("{header}{body}"), position));
        }
        chunks
    }

    // ── Tree-sitter item-boundary walk (feature-gated) ─────────────────────

    #[cfg(feature = "code-chunking")]
    mod ts {
        use super::{chunk_fallback, header_line, line_windows, Chunk, CodeLanguage, WINDOW_LINES, WINDOW_OVERLAP_LINES};
        use super::super::{count_tokens, TOKEN_TARGET};
        use tree_sitter::Node;

        /// One extracted item: a byte range into the ORIGINAL source (so the
        /// chunk's text is a verbatim slice — no re-serialisation, no risk
        /// of drifting from what is actually in the file) plus the display
        /// kind/name this module derived for its header.
        struct RawItem {
            kind: String,
            name: Option<String>,
            start_byte: usize,
            end_byte: usize,
            /// A comment or attribute/decorator/annotation node — real
            /// source that stays in the chunk BODY, but never worth a slot
            /// in a merged chunk's `items …` header (see
            /// [`is_decorative_kind`] and [`combined_label`]).
            decorative: bool,
        }

        /// Grammar node kinds that describe or annotate a neighbouring item
        /// rather than being one: every comment kind (`line_comment`,
        /// `block_comment`, `comment`, plus Rust's `doc_comment`-shaped
        /// `line_comment`s) and the attribute-ish kinds across the seven
        /// grammars (`attribute_item`/`inner_attribute_item` for Rust,
        /// `decorator` for TS/JS/Python, `annotation`/`marker_annotation`
        /// for Java, `attribute_list`/`global_attribute` for C#). Checked on
        /// the RAW grammar kind, before [`kind_label`] strips its suffix.
        /// Before this filter a merged header could read `items
        /// line_comment, line_comment, line_comment, +2 more` — three
        /// doc-comment lines named, the function they document hidden
        /// behind "+2 more" — which is a retrieval hint that hints at
        /// nothing.
        /// The `{kind}` used when a chunk's every item is decorative. Kept as
        /// one constant so the header text and the tests that pin it cannot
        /// drift apart.
        const ALL_DECORATIVE_LABEL: &str = "comments";

        fn is_decorative_kind(kind: &str) -> bool {
            kind.contains("comment")
                || matches!(
                    kind,
                    "attribute_item"
                        | "inner_attribute_item"
                        | "attribute"
                        | "attribute_list"
                        | "global_attribute"
                        | "decorator"
                        | "annotation"
                        | "marker_annotation"
                )
        }

        fn ts_language(language: CodeLanguage) -> Option<tree_sitter::Language> {
            Some(match language {
                CodeLanguage::Rust => tree_sitter_rust::LANGUAGE.into(),
                CodeLanguage::TypeScript => tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
                CodeLanguage::Tsx => tree_sitter_typescript::LANGUAGE_TSX.into(),
                CodeLanguage::JavaScript => tree_sitter_javascript::LANGUAGE.into(),
                CodeLanguage::Python => tree_sitter_python::LANGUAGE.into(),
                CodeLanguage::Go => tree_sitter_go::LANGUAGE.into(),
                CodeLanguage::Java => tree_sitter_java::LANGUAGE.into(),
                CodeLanguage::CSharp => tree_sitter_c_sharp::LANGUAGE.into(),
                CodeLanguage::Unknown => return None,
            })
        }

        /// `None` when `language` has no grammar ([`CodeLanguage::Unknown`])
        /// or parsing produced zero items (a file that is, say, entirely
        /// comments, or a grammar mismatch) — either way [`chunk_code`]
        /// falls through to the line-window fallback rather than returning
        /// an empty result for non-empty input.
        pub(super) fn try_chunk(text: &str, path: &str, language: CodeLanguage) -> Option<Vec<Chunk>> {
            let ts_lang = ts_language(language)?;
            let mut parser = tree_sitter::Parser::new();
            parser.set_language(&ts_lang).ok()?;
            let tree = parser.parse(text, None)?;
            let src = text.as_bytes();
            let mut items = Vec::new();
            walk(tree.root_node(), src, language, None, &mut items);
            if items.is_empty() {
                return None;
            }
            Some(items_to_chunks(items, src, path, language))
        }

        /// Container node kinds whose `body` field holds further
        /// DECLARATIONS worth their own item boundary (methods inside an
        /// `impl`/class/interface, nested items inside a Rust `mod`) — as
        /// opposed to every other node with a `body` field (a plain
        /// function/method), whose body is executable STATEMENTS that must
        /// stay one whole item. Go is absent: its grammar has no nested
        /// method bodies at all (`func (r Receiver) Name()` is already a
        /// top-level `method_declaration`), so no recursion is needed there.
        fn is_container(language: CodeLanguage, kind: &str) -> bool {
            matches!(
                (language, kind),
                (CodeLanguage::Rust, "impl_item")
                    | (CodeLanguage::Rust, "trait_item")
                    | (CodeLanguage::Rust, "mod_item")
                    | (CodeLanguage::TypeScript, "class_declaration")
                    | (CodeLanguage::TypeScript, "abstract_class_declaration")
                    | (CodeLanguage::TypeScript, "interface_declaration")
                    | (CodeLanguage::TypeScript, "internal_module")
                    | (CodeLanguage::Tsx, "class_declaration")
                    | (CodeLanguage::Tsx, "abstract_class_declaration")
                    | (CodeLanguage::Tsx, "interface_declaration")
                    | (CodeLanguage::Tsx, "internal_module")
                    | (CodeLanguage::JavaScript, "class_declaration")
                    | (CodeLanguage::Python, "class_definition")
                    | (CodeLanguage::Java, "class_declaration")
                    | (CodeLanguage::Java, "interface_declaration")
                    | (CodeLanguage::Java, "enum_declaration")
                    | (CodeLanguage::CSharp, "class_declaration")
                    | (CodeLanguage::CSharp, "interface_declaration")
                    | (CodeLanguage::CSharp, "struct_declaration")
                    | (CodeLanguage::CSharp, "namespace_declaration")
            )
        }

        /// Unwrap a node that only WRAPS the item worth classifying —
        /// `export function foo() {}` (TS/JS `export_statement`) and
        /// `@decorator\ndef foo():` (Python `decorated_definition`) both
        /// name their real declaration via a field, so classification (kind,
        /// name, whether it's a container) reads THROUGH the wrapper, while
        /// the chunk's own byte range stays the OUTER node's — keeping the
        /// `export`/`@decorator` text attached to the chunk it describes.
        fn effective(n: Node) -> Node {
            match n.kind() {
                "export_statement" => n.child_by_field_name("declaration").unwrap_or(n),
                "decorated_definition" => n.child_by_field_name("definition").unwrap_or(n),
                _ => n,
            }
        }

        /// Every grammar this module supports names an item's own
        /// identifier via a `name` field — EXCEPT Rust's `impl_item`, which
        /// has no `name` field at all (fields are `type`/`trait`); that one
        /// case is special-cased to build `"Trait for Type"` (or bare
        /// `"Type"` for an inherent impl).
        fn item_name(n: Node, src: &[u8]) -> Option<String> {
            if let Some(name_node) = n.child_by_field_name("name") {
                return name_node.utf8_text(src).ok().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string);
            }
            if n.kind() == "impl_item" {
                let ty = n.child_by_field_name("type").and_then(|c| c.utf8_text(src).ok()).map(str::trim).filter(|s| !s.is_empty());
                let tr = n.child_by_field_name("trait").and_then(|c| c.utf8_text(src).ok()).map(str::trim).filter(|s| !s.is_empty());
                return match (tr, ty) {
                    (Some(t), Some(ty)) => Some(format!("{t} for {ty}")),
                    (None, Some(ty)) => Some(ty.to_string()),
                    _ => None,
                };
            }
            None
        }

        /// `function_item` -> `function`, `class_declaration` -> `class`,
        /// … — strips the grammar's own noun suffix so a header reads like
        /// prose rather than a raw grammar symbol. Falls back to the raw
        /// kind string untouched when none of the known suffixes match
        /// (still a valid, if slightly more verbose, header — never a
        /// panic).
        fn kind_label(kind: &str) -> String {
            for suffix in ["_item", "_declaration", "_definition", "_statement"] {
                if let Some(stripped) = kind.strip_suffix(suffix) {
                    return stripped.to_string();
                }
            }
            kind.to_string()
        }

        /// Depth-first walk collecting one [`RawItem`] per top-level or
        /// nested-container child, in source order (nested items are
        /// spliced in at the position their container occupied, which is
        /// exactly source order). `prefix`, when `Some`, is the enclosing
        /// container's own display name — combined with a nested item's own
        /// name via [`CodeLanguage::nesting_separator`] so a method's header
        /// reads `Outer::inner`/`Outer.inner` rather than just `inner`.
        fn walk(node: Node, src: &[u8], language: CodeLanguage, prefix: Option<&str>, out: &mut Vec<RawItem>) {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                let eff = effective(child);
                let own_name = item_name(eff, src);
                // A nested member with NO own name (a bare field/docstring/
                // expression inside a class or impl body — no `name` field
                // on its node) does NOT inherit the bare container name:
                // `Some("Widget")` would read as "a field literally named
                // Widget", not "an unnamed field inside Widget" — showing no
                // name at all is the honest header, not a misleading one.
                let full_name = match (prefix, own_name) {
                    (Some(p), Some(n)) => Some(format!("{p}{sep}{n}", sep = language.nesting_separator())),
                    (Some(_), None) => None,
                    (None, name) => name,
                };

                if is_container(language, eff.kind()) {
                    if let Some(body) = eff.child_by_field_name("body") {
                        let before = out.len();
                        walk(body, src, language, full_name.as_deref(), out);
                        if out.len() > before {
                            continue;
                        }
                        // Empty container (e.g. `impl Foo {}`) — fall
                        // through and keep it as one leaf item below rather
                        // than silently dropping it.
                    }
                }

                out.push(RawItem {
                    kind: kind_label(eff.kind()),
                    name: full_name,
                    start_byte: child.start_byte(),
                    end_byte: child.end_byte(),
                    decorative: is_decorative_kind(eff.kind()),
                });
            }
        }

        /// The `"{kind} {name}"` (or bare `kind` when a nested member has no
        /// name of its own — see [`walk`]'s comment on that case) spelling one
        /// declaration contributes to a chunk's `items …` header AND to
        /// [`Chunk::symbols`]. One place, so the two can never drift to
        /// different text for the same [`RawItem`].
        fn declaration_label(it: &RawItem) -> String {
            match &it.name {
                Some(n) => format!("{} {n}", it.kind),
                None => it.kind.clone(),
            }
        }

        /// `group.len() == 1` keeps that one item's own kind/name verbatim;
        /// a merged group of several small adjacent items gets a synthetic
        /// `"items"` kind naming up to the first three (`"+N more"` beyond
        /// that) — still self-describing, never a bare unlabeled dump.
        /// Comment and attribute nodes ([`RawItem::decorative`]) are left
        /// out of that list — they stay in the body, but the header names
        /// the declarations they sit beside (`items function foo, struct
        /// Bar, +3 more`), not the comments. A group that is NOTHING but
        /// comments/attributes (a file-header comment block flushed on its
        /// own) keeps listing what it has, so the header never goes blank.
        ///
        /// Returns `(kind, display_name, symbols)`. `symbols` is
        /// [`Chunk::symbols`]'s payload for this group — the SAME
        /// declarations the header names, but UNCAPPED (never cut to the
        /// first three the way `display_name`'s summary is):
        ///
        /// * `group.len() == 1`, decorative: no declaration at all — `vec![]`.
        ///   A lone comment/attribute block names nothing to search for.
        /// * `group.len() == 1`, substantive: the ONE declaration, still
        ///   contributed (`vec![declaration_label(group[0])]`) even though a
        ///   single item never hits the header's truncation — the symbol leg
        ///   (`store::keyword_search_pg`) reads this column independent of
        ///   whether the header would have needed to cut anything.
        /// * multi-item, all decorative: no declarations — `vec![]`, same as
        ///   the single-decorative case above.
        /// * multi-item, some substantive: every substantive item's label, in
        ///   source order, with NO cap — this is exactly the list the header
        ///   truncates to `", +N more"`, kept whole.
        fn combined_label(group: &[&RawItem]) -> (String, Option<String>, Vec<String>) {
            if group.len() == 1 {
                // A lone decorative item -- a doc block long enough to flush
                // on its own -- is labelled by WHAT IT IS, never by its
                // grammar kind. `// path — line_comment` names nothing.
                if group[0].decorative {
                    return (ALL_DECORATIVE_LABEL.to_string(), None, Vec::new());
                }
                return (group[0].kind.clone(), group[0].name.clone(), vec![declaration_label(group[0])]);
            }
            let substantive: Vec<&RawItem> = group.iter().copied().filter(|it| !it.decorative).collect();
            // A group that is NOTHING but comments/attributes: label it by
            // what it is rather than listing the grammar kinds of its lines.
            //
            // The previous fallback named them, which on a codebase whose doc
            // comments routinely exceed `TOKEN_TARGET` -- so that a doc block
            // flushes as its own all-decorative group -- produced headers like
            // `items line_comment, line_comment, line_comment, +29 more`.
            // Measured on this repo's own ingest, 2026-09-22: 1,531 of 16,735
            // code chunks (9.1%). The path in the header line still says where
            // this is, and the prose itself is right there in the body.
            if substantive.is_empty() {
                return (ALL_DECORATIVE_LABEL.to_string(), None, Vec::new());
            }
            let named: &[&RawItem] = &substantive;
            let labels: Vec<String> = named.iter().map(|it| declaration_label(it)).collect();
            let mut summary = labels.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
            if labels.len() > 3 {
                summary.push_str(&format!(", +{} more", labels.len() - 3));
            }
            ("items".to_string(), Some(summary), labels)
        }

        fn flush_group(group: &mut Vec<&RawItem>, src: &[u8], path: &str, language: CodeLanguage, chunks: &mut Vec<Chunk>) {
            if group.is_empty() {
                return;
            }
            let (kind, name, symbols) = combined_label(group);
            let header = header_line(path, language, &kind, name.as_deref(), None);
            let mut body = String::new();
            for (i, item) in group.iter().enumerate() {
                if i > 0 {
                    body.push_str("\n\n");
                }
                body.push_str(std::str::from_utf8(&src[item.start_byte..item.end_byte]).unwrap_or(""));
            }
            let position = chunks.len();
            let mut chunk = Chunk::new(format!("{header}{body}"), position);
            chunk.symbols = symbols;
            chunks.push(chunk);
            group.clear();
        }

        /// A single item too large to keep whole splits through the SAME
        /// line-window logic the whole-file fallback uses, each part
        /// carrying the item's own kind/name plus a `(part i/n)` suffix —
        /// still self-describing per split part. Every part carries the SAME
        /// [`Chunk::symbols`] entry (empty for a decorative item — a huge
        /// comment block declares nothing): a split is a size accident, not a
        /// second declaration, and a "where is X declared" question must reach
        /// whichever part the caller's cap happens to keep as readily as it
        /// would an unsplit item.
        fn push_split_item(item: &RawItem, src: &[u8], path: &str, language: CodeLanguage, chunks: &mut Vec<Chunk>) {
            let item_text = std::str::from_utf8(&src[item.start_byte..item.end_byte]).unwrap_or("");
            let windows = line_windows(item_text, WINDOW_LINES, WINDOW_OVERLAP_LINES);
            let total = windows.len();
            let symbol = if item.decorative { None } else { Some(declaration_label(item)) };
            for (i, (body, _first, _last)) in windows.into_iter().enumerate() {
                let header = header_line(path, language, &item.kind, item.name.as_deref(), Some((i + 1, total)));
                let position = chunks.len();
                let mut chunk = Chunk::new(format!("{header}{body}"), position);
                if let Some(s) = &symbol {
                    chunk.symbols = vec![s.clone()];
                }
                chunks.push(chunk);
            }
        }

        /// Merge adjacent small items up to [`TOKEN_TARGET`]; split any
        /// single item that alone exceeds it. `items` arrives in source
        /// order (see [`walk`]), which is preserved in the output.
        fn items_to_chunks(items: Vec<RawItem>, src: &[u8], path: &str, language: CodeLanguage) -> Vec<Chunk> {
            let mut chunks = Vec::new();
            let mut group: Vec<&RawItem> = Vec::new();
            let mut group_tokens = 0usize;

            for item in &items {
                let item_text = std::str::from_utf8(&src[item.start_byte..item.end_byte]).unwrap_or("");
                let item_tokens = count_tokens(item_text);

                if item_tokens > TOKEN_TARGET {
                    flush_group(&mut group, src, path, language, &mut chunks);
                    group_tokens = 0;
                    push_split_item(item, src, path, language, &mut chunks);
                    continue;
                }

                if group_tokens + item_tokens > TOKEN_TARGET && !group.is_empty() {
                    flush_group(&mut group, src, path, language, &mut chunks);
                    group_tokens = 0;
                }
                group.push(item);
                group_tokens += item_tokens;
            }
            flush_group(&mut group, src, path, language, &mut chunks);

            // A pathological input that parsed to items summing to zero
            // chunks (should be unreachable — `items` was checked non-empty
            // by the caller) falls back rather than returning nothing for
            // non-empty text.
            if chunks.is_empty() {
                return chunk_fallback(std::str::from_utf8(src).unwrap_or(""), path, language);
            }
            chunks
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // count_tokens
    // ------------------------------------------------------------------

    #[test]
    fn count_tokens_empty() {
        assert_eq!(count_tokens(""), 0);
    }

    #[test]
    fn count_tokens_reasonable() {
        // 3 words → 4 tokens (3 * 4 / 3 = 4)
        assert_eq!(count_tokens("hello world foo"), 4);
        // 6 words → 8
        assert_eq!(count_tokens("one two three four five six"), 8);
    }

    #[test]
    fn provenance_header_omits_empty_clauses_and_skips_chat() {
        let when = DateTime::parse_from_rfc3339("2026-07-15T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            build_provenance_header(Some("Q3 plan"), Some(when), Some("reef")),
            Some("Title: Q3 plan. Date: 2026-07-15. Project: reef.".to_string())
        );
        assert_eq!(
            build_provenance_header(None, Some(when), Some("  ")),
            Some("Date: 2026-07-15.".to_string())
        );
        assert!(build_provenance_header(None, None, None).is_none());
        assert!(provenance_source("email"));
        assert!(provenance_source("note"));
        assert!(provenance_source("notion"));
        assert!(!provenance_source("slack"));
        assert!(!provenance_source("transcript"));
        assert!(!provenance_source("coding_session"));
    }

    #[test]
    fn count_tokens_larger_text() {
        // ~100 words should give ~133 tokens (> 100, < 200)
        let text = "word ".repeat(100);
        let t = count_tokens(&text);
        assert!(t > 100 && t < 200, "expected 100..200 got {t}");
    }

    // ------------------------------------------------------------------
    // for_source_type
    // ------------------------------------------------------------------

    #[test]
    fn for_source_type_slack() {
        assert!(matches!(
            ChunkStrategy::for_source_type("slack"),
            ChunkStrategy::Slack
        ));
    }

    #[test]
    fn for_source_type_email() {
        assert!(matches!(
            ChunkStrategy::for_source_type("email"),
            ChunkStrategy::Email
        ));
    }

    #[test]
    fn for_source_type_unknown_is_generic() {
        assert!(matches!(
            ChunkStrategy::for_source_type("gdrive"),
            ChunkStrategy::Generic
        ));
    }

    // GitHub connector PR A — all six §3.5 source types are accepted
    // (registered) at this switch site, routing to the prose-shaped Generic
    // strategy today (see the match arm's own comment for what PR B changes).
    #[test]
    fn for_source_type_github_all_six_types_are_accepted() {
        // PR B repointed "github_code" at the real syntax-aware `Code`
        // strategy (see that match arm's own comment) — every OTHER github
        // source type is prose-shaped and still routes to `Generic`.
        for t in ["github_pr", "github_issue", "github_release", "github_commit", "github_code_summary"] {
            assert!(
                matches!(ChunkStrategy::for_source_type(t), ChunkStrategy::Generic),
                "source_type {t} must be accepted (routed to a real strategy, not panic/unreachable)"
            );
        }
        assert!(
            matches!(ChunkStrategy::for_source_type("github_code"), ChunkStrategy::Code { .. }),
            "github_code must route to the syntax-aware Code strategy"
        );
    }

    // gsuite Train G4 PR 2 (design doc §3.6) — google_chat is registered as
    // chat-shaped (Slack strategy), for both archetypes (spaces AND DMs/group
    // chats — the strategy label doesn't branch on archetype, routing does).
    #[test]
    fn for_source_type_google_chat_is_slack_strategy() {
        assert!(matches!(
            ChunkStrategy::for_source_type("google_chat"),
            ChunkStrategy::Slack
        ));
    }

    // ------------------------------------------------------------------
    // Generic strategy
    // ------------------------------------------------------------------

    #[test]
    fn generic_short_text_single_chunk() {
        let text = "Hello world. This is a short document.";
        let chunks = chunk(text, ChunkStrategy::Generic);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].position, 0);
    }

    #[test]
    fn generic_long_text_splits_correctly() {
        // Build a text that is clearly over 512 tokens.
        // Each word ≈ 1.33 tokens, so 600 words ≈ 800 tokens → 2+ chunks.
        let sentence = "The quick brown fox jumps over the lazy dog. ";
        // 9 words × repeating to get ~600 words total
        let repetitions = 70; // 70 × 9 = 630 words ≈ 840 tokens
        let text = sentence.repeat(repetitions);
        let chunks = chunk(&text, ChunkStrategy::Generic);
        assert!(
            chunks.len() >= 2,
            "expected at least 2 chunks, got {}",
            chunks.len()
        );
        // Positions should be sequential.
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.position, i);
        }
    }

    #[test]
    fn generic_overlap_is_included() {
        // Build two paragraphs of 400 words each (≈ 533 tokens, > 512).
        // They cannot be merged into one window, so we get at least 2 chunks.
        // The second chunk should begin with overlap text from the first.
        //
        // We need sentence-ending punctuation so the sentence splitter can
        // produce overlap candidates.  Use 9-word sentences repeated enough
        // times to reach 400 words total per paragraph.
        let sentence = "The quick brown fox jumps over the lazy dog. ";
        // 9 words per sentence × 45 repetitions = 405 words ≈ 540 tokens > 512.
        let repetitions = 45usize;
        let para_a = sentence.repeat(repetitions);
        let para_b = "One two three four five six seven eight nine. ".repeat(repetitions);
        let text = format!("{}\n\n{}", para_a.trim(), para_b.trim());
        let chunks = chunk(&text, ChunkStrategy::Generic);
        // Each paragraph exceeds TOKEN_TARGET so we expect at least 2 chunks.
        assert!(
            chunks.len() >= 2,
            "expected >= 2 chunks, got {}",
            chunks.len()
        );
        // All chunk positions are sequential.
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.position, i);
        }
        // At least one non-first chunk should have non-zero token count.
        let non_first_non_empty = chunks[1..].iter().any(|c| c.token_count > 0);
        assert!(non_first_non_empty);
    }

    // ------------------------------------------------------------------
    // Transcript strategy
    // ------------------------------------------------------------------

    #[test]
    fn transcript_splits_two_speakers() {
        let text = "Alice: hello world\nBob: hi there";
        let chunks = chunk(text, ChunkStrategy::Transcript);
        assert_eq!(chunks.len(), 2, "expected 2 chunks got {:?}", chunks.len());
        assert_eq!(chunks[0].speaker.as_deref(), Some("Alice"));
        assert_eq!(chunks[1].speaker.as_deref(), Some("Bob"));
        assert!(chunks[0].text.contains("hello world"));
        assert!(chunks[1].text.contains("hi there"));
    }

    #[test]
    fn transcript_without_speakers_falls_back_to_prose() {
        // Raw Whisper output / unlabeled paste: no "Name:" turns. Must
        // still produce chunks (was previously dropped to 0 → nothing ingested).
        let text = "Well that got me. Guys we are all pushing thirty now. Come on. \
                    We should ship the desktop recorder this week and circle back on detection.";
        let chunks = chunk(text, ChunkStrategy::Transcript);
        assert!(!chunks.is_empty(), "unlabeled transcript should still chunk");
        assert!(chunks[0].speaker.is_none(), "prose fallback has no speaker");
        assert!(chunks.iter().any(|c| c.text.contains("desktop recorder")));
    }

    #[test]
    fn transcript_bracket_format() {
        let text = "[Alice] hello\n[Bob] hi";
        let chunks = chunk(text, ChunkStrategy::Transcript);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].speaker.as_deref(), Some("Alice"));
        assert_eq!(chunks[1].speaker.as_deref(), Some("Bob"));
    }

    #[test]
    fn transcript_speaker_is_separate_from_text_by_design() {
        // `chunk.speaker` is intentionally split out of `chunk.text` (needed for
        // same-speaker merging in `chunk_transcript`). That means any consumer
        // that persists `chunk.text` verbatim WITHOUT also reading
        // `chunk.speaker` silently loses attribution. Backend insert sites
        // (`chunk_text_with_speaker` in `routes/ingest.rs`,
        // `ingest_huddle_transcript` in `workers/huddle.rs`) re-prepend
        // `"{speaker}: "` before persisting — this test documents the
        // precondition they rely on.
        let text = "Alice: hello world";
        let chunks = chunk(text, ChunkStrategy::Transcript);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].speaker.as_deref(), Some("Alice"));
        assert!(
            !chunks[0].text.contains("Alice"),
            "chunk.text has the label stripped by design; callers must \
             re-attach chunk.speaker themselves before persisting"
        );
    }

    #[test]
    fn transcript_merges_same_speaker() {
        // Two short consecutive turns from the same speaker should be merged.
        let text = "Alice: first part\nAlice: second part\nBob: hello";
        let chunks = chunk(text, ChunkStrategy::Transcript);
        assert_eq!(chunks.len(), 2, "expected 2 after merge, got {}", chunks.len());
        assert_eq!(chunks[0].speaker.as_deref(), Some("Alice"));
        assert!(chunks[0].text.contains("first part"));
        assert!(chunks[0].text.contains("second part"));
    }

    // ------------------------------------------------------------------
    // Email strategy
    // ------------------------------------------------------------------

    #[test]
    fn email_splits_on_quoted_boundary() {
        let text = "Hello,\nThis is the new message.\n> This is a quoted reply.\n> It continues here.";
        let chunks = chunk(text, ChunkStrategy::Email);
        // Should produce at least 2 chunks: the new message and the quoted part.
        assert!(
            chunks.len() >= 2,
            "expected >= 2 chunks, got {}",
            chunks.len()
        );
        // The first chunk should contain the new message.
        assert!(chunks[0].text.contains("Hello"));
        // The quoted chunk should have the `> ` prefix stripped.
        let quoted = &chunks[1];
        assert!(
            !quoted.text.contains("> "),
            "quote prefix should be stripped: {:?}",
            quoted.text
        );
        assert!(quoted.text.contains("quoted reply"));
    }

    #[test]
    fn email_positions_are_sequential() {
        let text =
            "Original message.\n> Quote level 1.\n> > Quote level 2.\nReply continues.";
        let chunks = chunk(text, ChunkStrategy::Email);
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.position, i, "position mismatch at index {i}");
        }
    }

    /// Every word of `text`, in order, with whitespace normalised: what a
    /// split must preserve (overlap aside).
    fn words(text: &str) -> Vec<&str> {
        text.split_whitespace().collect()
    }

    #[test]
    fn email_long_unquoted_body_is_split_and_nothing_is_lost() {
        // The prod shape (2026-10-23): one long unquoted Gmail body was ONE
        // chunk over the embedder's 8192-token input limit.
        let para = "The quarterly bond schedule moved to the second week of March. ".repeat(12);
        let body = (0..60).map(|i| format!("Section {i}. {para}")).collect::<Vec<_>>().join("\n\n");
        assert!(body.len() > 40_000);
        let chunks = chunk(&body, ChunkStrategy::Email);
        assert!(chunks.len() > 10, "a ~45 KB body must split, got {}", chunks.len());
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.position, i);
            assert!(c.text.len() <= MAX_CHUNK_BYTES);
        }
        // Generic splitting overlaps windows, so check coverage: every
        // section marker survives.
        let joined = chunks.iter().map(|c| c.text.as_str()).collect::<Vec<_>>().join(" ");
        for i in 0..60 {
            assert!(joined.contains(&format!("Section {i}.")), "section {i} lost");
        }
    }

    #[test]
    fn email_short_groups_are_unchanged() {
        let text = "Thanks, see you Tuesday.\n> Can we move the walkthrough?\n> It clashes with the board meeting.";
        let chunks = chunk(text, ChunkStrategy::Email);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "Thanks, see you Tuesday.");
        assert_eq!(chunks[1].text, "Can we move the walkthrough?\nIt clashes with the board meeting.");
    }

    #[test]
    fn no_strategy_emits_a_chunk_over_the_byte_ceiling() {
        // No whitespace and no punctuation: the word-count estimate sees one
        // "word", so before the ceiling every strategy kept this whole.
        let blob = "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo".repeat(1000);
        let spaced = format!("{} {}", "x".repeat(9000), "tail words here");
        let speaker = format!("Alice: {}", "y".repeat(20_000));
        for strategy in [
            ChunkStrategy::Generic,
            ChunkStrategy::Email,
            ChunkStrategy::Slack,
            ChunkStrategy::Transcript,
            ChunkStrategy::code_for_path("data/blob.txt"),
        ] {
            for text in [blob.as_str(), spaced.as_str(), speaker.as_str()] {
                let chunks = chunk(text, strategy.clone());
                assert!(!chunks.is_empty());
                for (i, c) in chunks.iter().enumerate() {
                    assert_eq!(c.position, i, "{strategy:?}");
                    assert!(
                        c.text_with_speaker().len() <= MAX_CHUNK_BYTES,
                        "{strategy:?}: chunk {i} is {} bytes",
                        c.text_with_speaker().len()
                    );
                }
            }
        }
    }

    #[test]
    fn byte_split_keeps_every_byte_of_a_single_run() {
        let blob = "é".repeat(7000) + "z";
        let pieces = split_to_bytes(&blob, MAX_CHUNK_BYTES);
        assert!(pieces.len() >= 3);
        assert!(pieces.iter().all(|p| p.len() <= MAX_CHUNK_BYTES && !p.is_empty()));
        assert_eq!(pieces.concat(), blob);
    }

    #[test]
    fn byte_split_prefers_a_paragraph_break() {
        let a = "a".repeat(4000);
        let b = "b ".repeat(2000);
        let text = format!("{a}\n\n{b}");
        let pieces = split_to_bytes(&text, MAX_CHUNK_BYTES);
        assert_eq!(pieces[0], a);
        assert_eq!(words(&pieces[1..].join(" ")), words(&b));
    }

    #[test]
    fn chunks_under_the_ceiling_pass_through_untouched() {
        let chunks = vec![Chunk::new("one".into(), 0), Chunk::new("two".into(), 1)];
        let bounded = bound_chunk_bytes(chunks.clone());
        assert_eq!(bounded.len(), 2);
        assert_eq!(bounded[1].text, "two");
        assert_eq!(bounded[1].position, 1);
    }

    // ------------------------------------------------------------------
    // Slack strategy
    // ------------------------------------------------------------------

    #[test]
    fn slack_short_messages_merge() {
        let messages = vec![
            SlackMessage {
                text: "hey".to_string(),
                user: "alice".to_string(),
                parent_message_id: None,
            },
            SlackMessage {
                text: "how are you".to_string(),
                user: "alice".to_string(),
                parent_message_id: None,
            },
            SlackMessage {
                text: "doing well thanks".to_string(),
                user: "bob".to_string(),
                parent_message_id: None,
            },
        ];
        let chunks = chunk_slack_messages(&messages);
        // alice's two short messages should be merged into one chunk.
        assert_eq!(chunks.len(), 2, "expected 2 chunks got {}", chunks.len());
        assert!(chunks[0].text.contains("hey"));
        assert!(chunks[0].text.contains("how are you"));
    }

    #[test]
    fn slack_different_users_not_merged() {
        let messages = vec![
            SlackMessage {
                text: "hello".to_string(),
                user: "alice".to_string(),
                parent_message_id: None,
            },
            SlackMessage {
                text: "hi".to_string(),
                user: "bob".to_string(),
                parent_message_id: None,
            },
        ];
        let chunks = chunk_slack_messages(&messages);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn slack_parent_message_id_propagated() {
        let messages = vec![SlackMessage {
            text: "thread reply".to_string(),
            user: "alice".to_string(),
            parent_message_id: Some("msg_001".to_string()),
        }];
        let chunks = chunk_slack_messages(&messages);
        assert_eq!(chunks[0].parent_message_id.as_deref(), Some("msg_001"));
    }

    // ------------------------------------------------------------------
    // build_meeting_header (cs-phase0-chunk-headers)
    // ------------------------------------------------------------------

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn header_title_date_and_participants() {
        let header = build_meeting_header(
            Some("Weekly Standup"),
            Some(ts("2026-07-15T14:00:00Z")),
            &["Alice".to_string(), "Bob".to_string()],
        );
        assert_eq!(
            header.as_deref(),
            Some("Meeting: Weekly Standup, 2026-07-15. Participants: Alice, Bob.")
        );
    }

    #[test]
    fn header_without_participants() {
        // No diarized speakers (e.g. transcript not yet attributed) — the
        // Participants clause is omitted entirely, not left empty.
        let header = build_meeting_header(
            Some("Sprint Planning"),
            Some(ts("2026-07-15T14:00:00Z")),
            &[],
        );
        assert_eq!(header.as_deref(), Some("Meeting: Sprint Planning, 2026-07-15."));
    }

    #[test]
    fn header_without_title() {
        // Slack Huddles have no calendar subject — falls back to date only.
        let header = build_meeting_header(
            None,
            Some(ts("2026-07-15T14:00:00Z")),
            &["Alice".to_string()],
        );
        assert_eq!(header.as_deref(), Some("Meeting on 2026-07-15. Participants: Alice."));
    }

    #[test]
    fn header_title_and_date_only_no_participants() {
        let header = build_meeting_header(None, Some(ts("2026-07-15T14:00:00Z")), &[]);
        assert_eq!(header.as_deref(), Some("Meeting on 2026-07-15."));
    }

    #[test]
    fn header_none_when_nothing_known() {
        // Non-meeting atoms (Slack, email, generic docs) simply never call this
        // function — but if all three inputs are empty/absent, it degrades to
        // None rather than emitting a content-free header.
        assert_eq!(build_meeting_header(None, None, &[]), None);
    }

    #[test]
    fn header_blank_title_treated_as_absent() {
        let header = build_meeting_header(Some("   "), Some(ts("2026-07-15T14:00:00Z")), &[]);
        assert_eq!(header.as_deref(), Some("Meeting on 2026-07-15."));
    }

    #[test]
    fn header_blank_participant_names_filtered() {
        let header = build_meeting_header(
            Some("1:1"),
            Some(ts("2026-07-15T14:00:00Z")),
            &["Alice".to_string(), "  ".to_string(), "Bob".to_string()],
        );
        assert_eq!(
            header.as_deref(),
            Some("Meeting: 1:1, 2026-07-15. Participants: Alice, Bob.")
        );
    }

    // ------------------------------------------------------------------
    // build_coding_session_header (coding-session capture Phase 1, A3)
    // ------------------------------------------------------------------

    #[test]
    fn coding_session_header_all_fields() {
        let header = build_coding_session_header(
            Some("Fix retrieval bug"),
            Some("coralswarm/coralswarm"),
            Some("fix/retrieval-bug"),
        );
        assert_eq!(
            header.as_deref(),
            Some("Session: Fix retrieval bug. Repo: coralswarm/coralswarm. Branch: fix/retrieval-bug.")
        );
    }

    #[test]
    fn coding_session_header_repo_and_branch_only() {
        let header = build_coding_session_header(None, Some("coralswarm/coralswarm"), Some("main"));
        assert_eq!(header.as_deref(), Some("Repo: coralswarm/coralswarm. Branch: main."));
    }

    #[test]
    fn coding_session_header_branch_only() {
        let header = build_coding_session_header(None, None, Some("main"));
        assert_eq!(header.as_deref(), Some("Branch: main."));
    }

    #[test]
    fn coding_session_header_none_when_nothing_known() {
        assert_eq!(build_coding_session_header(None, None, None), None);
    }

    #[test]
    fn coding_session_header_blank_fields_treated_as_absent() {
        let header = build_coding_session_header(Some("  "), Some(""), Some("main"));
        assert_eq!(header.as_deref(), Some("Branch: main."));
    }
}


// ---------------------------------------------------------------------------
// Code strategy tests (docs/connectors/github.md §3.9, PR B)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod code_strategy_tests {
    use super::*;

    // ── CodeLanguage::from_path / ChunkStrategy construction ───────────────

    #[test]
    fn code_language_from_path_recognises_every_extension() {
        assert_eq!(CodeLanguage::from_path("src/lib.rs"), CodeLanguage::Rust);
        assert_eq!(CodeLanguage::from_path("src/index.ts"), CodeLanguage::TypeScript);
        assert_eq!(CodeLanguage::from_path("src/index.mts"), CodeLanguage::TypeScript);
        assert_eq!(CodeLanguage::from_path("src/index.cts"), CodeLanguage::TypeScript);
        assert_eq!(CodeLanguage::from_path("src/App.tsx"), CodeLanguage::Tsx);
        assert_eq!(CodeLanguage::from_path("src/index.js"), CodeLanguage::JavaScript);
        assert_eq!(CodeLanguage::from_path("src/index.jsx"), CodeLanguage::JavaScript);
        assert_eq!(CodeLanguage::from_path("src/index.mjs"), CodeLanguage::JavaScript);
        assert_eq!(CodeLanguage::from_path("src/index.cjs"), CodeLanguage::JavaScript);
        assert_eq!(CodeLanguage::from_path("widget.py"), CodeLanguage::Python);
        assert_eq!(CodeLanguage::from_path("widget.pyi"), CodeLanguage::Python);
        assert_eq!(CodeLanguage::from_path("widget.go"), CodeLanguage::Go);
        assert_eq!(CodeLanguage::from_path("Widget.java"), CodeLanguage::Java);
        assert_eq!(CodeLanguage::from_path("Widget.cs"), CodeLanguage::CSharp);
        assert_eq!(CodeLanguage::from_path("README.md"), CodeLanguage::Unknown);
        assert_eq!(CodeLanguage::from_path("Makefile"), CodeLanguage::Unknown);
    }

    #[test]
    fn code_language_from_path_is_case_insensitive() {
        assert_eq!(CodeLanguage::from_path("src/LIB.RS"), CodeLanguage::Rust);
    }

    #[test]
    fn for_source_type_github_code_is_code_strategy_with_unknown_language() {
        match ChunkStrategy::for_source_type("github_code") {
            ChunkStrategy::Code { language, path } => {
                assert_eq!(language, CodeLanguage::Unknown);
                assert_eq!(path, "");
            }
            other => panic!("expected ChunkStrategy::Code, got {other:?}"),
        }
    }

    #[test]
    fn for_source_type_github_code_summary_stays_generic() {
        // A GENERATED prose summary, never raw code — must never route to
        // the code strategy even though it shares the "github_code" prefix.
        assert!(matches!(ChunkStrategy::for_source_type("github_code_summary"), ChunkStrategy::Generic));
    }

    #[test]
    fn code_for_path_resolves_the_real_language() {
        match ChunkStrategy::code_for_path("backend/src/main.rs") {
            ChunkStrategy::Code { language, path } => {
                assert_eq!(language, CodeLanguage::Rust);
                assert_eq!(path, "backend/src/main.rs");
            }
            other => panic!("expected ChunkStrategy::Code, got {other:?}"),
        }
    }

    // ── Empty input -> zero chunks, regardless of language or feature ──────

    #[test]
    fn empty_file_produces_zero_chunks() {
        assert!(chunk("", ChunkStrategy::code_for_path("empty.rs")).is_empty());
    }

    #[test]
    fn whitespace_only_file_produces_zero_chunks() {
        assert!(chunk("   \n\n\t\n  ", ChunkStrategy::code_for_path("blank.py")).is_empty());
    }

    // ── Fallback: fixed line windows with overlap, header present ──────────
    // These run in EVERY build (feature on or off) because `CodeLanguage::
    // Unknown` never touches tree-sitter — proving the fallback used by a
    // no-`code-chunking` build actually works.

    fn many_lines(n: usize) -> String {
        (0..n).map(|i| format!("line number {i} of the fallback fixture file")).collect::<Vec<_>>().join("\n")
    }

    #[test]
    fn fallback_unknown_language_produces_windows_with_headers() {
        let text = many_lines(200);
        let chunks = chunk(&text, ChunkStrategy::code_for_path("data/notes.csv"));
        assert!(chunks.len() >= 2, "200 lines at an 80-line window must split into multiple chunks, got {}", chunks.len());
        for c in &chunks {
            let header = c.text.lines().next().unwrap_or("");
            assert!(header.starts_with("// data/notes.csv — lines "), "unexpected header: {header:?}");
        }
    }

    #[test]
    fn fallback_windows_overlap() {
        let text = many_lines(200);
        let chunks = chunk(&text, ChunkStrategy::code_for_path("data/notes.csv"));
        assert!(chunks.len() >= 2);
        // The second window's body (after its header line) must start with
        // a line that also appears near the END of the first window's body
        // — the ~15-line overlap band.
        let first_body: Vec<&str> = chunks[0].text.lines().skip(1).collect();
        let second_first_line = chunks[1].text.lines().nth(1).unwrap_or("");
        assert!(
            first_body.contains(&second_first_line),
            "expected the second window to overlap the tail of the first; second window's first body line was {second_first_line:?}"
        );
    }

    #[test]
    fn fallback_short_file_is_a_single_chunk() {
        let chunks = chunk("just three\nshort\nlines\n", ChunkStrategy::code_for_path("notes.txt"));
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.starts_with("// notes.txt — lines 1-3"));
    }

    #[test]
    fn fallback_python_extension_uses_hash_comment() {
        let chunks = chunk("x = 1\ny = 2\n", ChunkStrategy::code_for_path("script.py"));
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.starts_with('#'), "Python fallback header must use '#', got {:?}", chunks[0].text.lines().next());
    }

    // ── Non-`code-chunking` builds: even a recognised extension falls back
    // to line windows rather than failing or silently dropping content
    // (this is what makes a build without `code-chunking` safe). Only meaningful to assert when the feature is
    // OFF: with it on, `.rs` gets real tree-sitter boundaries instead, which
    // is covered by the `code_chunking_feature_tests` module below.
    #[cfg(not(feature = "code-chunking"))]
    #[test]
    fn without_the_feature_a_recognised_extension_still_falls_back_to_line_windows() {
        let text = many_lines(10);
        let chunks = chunk(&text, ChunkStrategy::code_for_path("src/main.rs"));
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.starts_with("// src/main.rs — lines 1-10"));
    }
}

// ---------------------------------------------------------------------------
// Tree-sitter boundary tests — only compiled/run with `--features
// code-chunking` (`cargo test -p coralswarm-core --features code-chunking`).
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "code-chunking"))]
mod code_chunking_feature_tests {
    use super::*;

    /// Every produced chunk must start with the `// {path} — ...` (or `#
    /// {path} — ...` for Python) self-describing header line.
    fn assert_all_headers(chunks: &[Chunk], expected_prefix: &str) {
        assert!(!chunks.is_empty(), "expected at least one chunk");
        for c in chunks {
            let header = c.text.lines().next().unwrap_or("");
            assert!(header.starts_with(expected_prefix), "chunk {} missing header prefix {expected_prefix:?}: {header:?}", c.position);
        }
    }

    fn pad_lines(n: usize) -> String {
        (0..n).map(|i| format!("    let v{i} = compute_something_with_a_reasonably_long_name({i});")).collect::<Vec<_>>().join("\n")
    }

    // ── Rust ─────────────────────────────────────────────────────────────

    #[test]
    fn rust_chunks_on_item_boundaries() {
        let src = format!(
            "use std::fmt;\n\nimpl Widget {{\n    pub fn new(name: &str) -> Self {{\n{}\n        Widget {{ name: name.to_string() }}\n    }}\n\n    fn helper(&self) -> usize {{\n{}\n        0\n    }}\n}}\n\npub fn free_function() -> i32 {{\n{}\n    42\n}}\n",
            pad_lines(60), pad_lines(60), pad_lines(60)
        );
        let chunks = chunk(&src, ChunkStrategy::code_for_path("src/widget.rs"));
        assert_all_headers(&chunks, "// src/widget.rs — ");
        let headers: Vec<String> = chunks.iter().map(|c| c.text.lines().next().unwrap_or("").to_string()).collect();
        assert!(headers.iter().any(|h| h.contains("Widget::new")), "missing Widget::new header: {headers:?}");
        assert!(headers.iter().any(|h| h.contains("Widget::helper")), "missing Widget::helper header: {headers:?}");
        assert!(headers.iter().any(|h| h.contains("free_function")), "missing free_function header: {headers:?}");
        // Each large function landed in its OWN chunk, not merged together.
        assert!(chunks.len() >= 3, "expected at least 3 separate chunks, got {}", chunks.len());
    }

    #[test]
    fn rust_small_items_merge_into_one_chunk() {
        let src = "use std::fmt;\n\nconst A: i32 = 1;\nconst B: i32 = 2;\n\nfn tiny() -> i32 { 3 }\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/tiny.rs"));
        assert_eq!(chunks.len(), 1, "small adjacent items must merge into a single chunk, got {}", chunks.len());
        assert!(chunks[0].text.contains("const A"));
        assert!(chunks[0].text.contains("fn tiny"));
    }

    /// Comment and attribute nodes stay in the BODY but are dropped from a
    /// merged chunk's `items …` header: three doc-comment lines and a
    /// `#[derive]` must not crowd out the struct and function they
    /// annotate.
    #[test]
    fn rust_merged_header_names_declarations_not_comments_or_attributes() {
        let src = "// file header\n\n/// Doc line one.\n/// Doc line two.\n/// Doc line three.\n#[derive(Debug)]\nstruct Bar;\n\n/// Documents foo.\nfn foo() -> i32 { 1 }\n\nconst C: u8 = 0;\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/small.rs"));
        assert_eq!(chunks.len(), 1, "small items must merge into one chunk: {chunks:?}");
        let header = chunks[0].text.lines().next().unwrap_or("").to_string();
        assert_eq!(header, "// src/small.rs — items struct Bar, function foo, const C", "header: {header:?}");
        assert!(!header.contains("comment"), "comments must not be listed in the header: {header:?}");
        assert!(!header.contains("attribute"), "attributes must not be listed in the header: {header:?}");
        // …but every one of them is still in the body.
        assert!(chunks[0].text.contains("/// Doc line one."));
        assert!(chunks[0].text.contains("#[derive(Debug)]"));
        assert!(chunks[0].text.contains("// file header"));
    }

    /// `+N more` counts only the substantive items — a header must not
    /// promise "+7 more" when the seven are doc-comment lines.
    #[test]
    fn rust_merged_header_plus_more_counts_only_declarations() {
        let src = "/// a\n/// b\n/// c\n/// d\nfn one() {}\n/// e\nfn two() {}\n/// f\nfn three() {}\n/// g\nfn four() {}\n/// h\nfn five() {}\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/n.rs"));
        assert_eq!(chunks.len(), 1);
        let header = chunks[0].text.lines().next().unwrap_or("").to_string();
        assert_eq!(header, "// src/n.rs — items function one, function two, function three, +2 more", "header: {header:?}");
    }

    /// `Chunk::symbols` is the sibling this header's `+2 more` hides — it
    /// carries EVERY declaration, uncapped, even though the header text
    /// above (unchanged, asserted byte-for-byte in the sibling test) still
    /// truncates to three. This is the fixture the retrieval-side fix
    /// (`backend/src/store/mod.rs::keyword_search_pg`'s symbol leg) depends
    /// on: `function four`/`function five` must reach a searcher even though
    /// neither one is ever visible in `atoms.excerpt`.
    #[test]
    fn rust_merged_chunk_symbols_are_never_truncated() {
        let src = "/// a\n/// b\n/// c\n/// d\nfn one() {}\n/// e\nfn two() {}\n/// f\nfn three() {}\n/// g\nfn four() {}\n/// h\nfn five() {}\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/n.rs"));
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0].symbols,
            vec![
                "function one".to_string(),
                "function two".to_string(),
                "function three".to_string(),
                "function four".to_string(),
                "function five".to_string(),
            ],
            "symbols: {:?}",
            chunks[0].symbols
        );
    }

    /// A single-item, non-decorative group never hits the header's
    /// truncation at all, but still contributes its one declaration to
    /// `Chunk::symbols` — the symbol leg must not depend on a chunk having
    /// been MERGED to be searchable.
    #[test]
    fn rust_single_item_chunk_still_carries_its_symbol() {
        let src = "pub fn intake_window(now: i64) -> i64 { now }\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/w.rs"));
        assert_eq!(chunks.len(), 1, "one lone item, one chunk: {chunks:?}");
        assert_eq!(chunks[0].symbols, vec!["function intake_window".to_string()]);
    }

    /// A lone decorative item (an all-comment group flushed on its own, see
    /// `rust_all_comment_group_keeps_a_non_empty_header`) declares nothing —
    /// `Chunk::symbols` must be empty, not a comment-labelled entry.
    #[test]
    fn rust_all_comment_group_has_no_symbols() {
        let src = "// only\n// comments\n// here\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/c.rs"));
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].symbols.is_empty(), "symbols: {:?}", chunks[0].symbols);
    }

    /// An item too large to keep whole splits into several parts (see
    /// `oversized_item_splits_into_overlapping_parts_with_the_same_name`) —
    /// every part must still carry the SAME symbol, not just the first.
    #[test]
    fn rust_split_item_parts_all_carry_the_same_symbol() {
        let body = pad_lines(400);
        let src = format!("fn giant() {{\n{body}\n}}\n");
        let chunks = chunk(&src, ChunkStrategy::code_for_path("src/big.rs"));
        assert!(chunks.len() >= 2, "expected the item to split into multiple parts: {}", chunks.len());
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.symbols, vec!["function giant".to_string()], "part {i}: {:?}", c.symbols);
        }
    }

    /// A group that is nothing but comments still gets a self-describing
    /// header (the old listing), never a blank `items` label.
    #[test]
    fn rust_all_comment_group_keeps_a_non_empty_header() {
        let src = "// only\n// comments\n// here\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/c.rs"));
        assert_eq!(chunks.len(), 1);
        let header = chunks[0].text.lines().next().unwrap_or("").to_string();
        // This used to read `items line_comment, line_comment, line_comment`.
        // The invariant this test is named for -- the header never goes blank
        // -- is unchanged; what changed (2026-09-22) is that an all-decorative
        // group is labelled by WHAT IT IS instead of by its grammar kinds.
        // Naming them put the literal word "line_comment" at the top of 9.1%
        // of this repo's own code chunks, which is what the embedding read.
        assert_eq!(header, "// src/c.rs — comments", "header: {header:?}");
        assert!(!header.ends_with("— "), "header must still carry a label");
    }

    #[test]
    fn rust_impl_trait_for_type_name() {
        let src = "struct Widget;\n\nimpl std::fmt::Display for Widget {\n    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {\n        Ok(())\n    }\n}\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/w.rs"));
        let joined: String = chunks.iter().map(|c| c.text.clone()).collect::<Vec<_>>().join("\n");
        assert!(joined.contains("Display for Widget::fmt"), "expected a 'Trait for Type::method' header, got: {joined}");
    }

    /// Same rule through the TS grammar (`comment` nodes) and the Python
    /// grammar (`comment` nodes, `#` header prefix).
    #[test]
    fn typescript_and_python_merged_headers_skip_comment_nodes() {
        let ts = "// header\n/** doc */\nexport function alpha() { return 1; }\n// mid\nexport const beta = 2;\n";
        let chunks = chunk(ts, ChunkStrategy::code_for_path("src/a.ts"));
        assert_eq!(chunks.len(), 1);
        let header = chunks[0].text.lines().next().unwrap_or("").to_string();
        assert!(header.starts_with("// src/a.ts — items function alpha, "), "header: {header:?}");
        assert!(!header.contains("comment"), "header: {header:?}");

        let py = "# header\ndef alpha():\n    return 1\n# mid\nBETA = 2\n";
        let chunks = chunk(py, ChunkStrategy::code_for_path("pkg/a.py"));
        assert_eq!(chunks.len(), 1);
        let header = chunks[0].text.lines().next().unwrap_or("").to_string();
        assert!(header.starts_with("# pkg/a.py — items function alpha, "), "header: {header:?}");
        assert!(!header.contains("comment"), "header: {header:?}");
    }

    // ── TypeScript / TSX ─────────────────────────────────────────────────

    #[test]
    fn typescript_chunks_on_class_and_function_boundaries() {
        let src = format!(
            "export class Widget {{\n    constructor(private name: string) {{}}\n\n    greet(): string {{\n{}\n        return this.name;\n    }}\n}}\n\nexport function freeFunction(): number {{\n{}\n    return 42;\n}}\n",
            pad_lines(60), pad_lines(60)
        );
        let chunks = chunk(&src, ChunkStrategy::code_for_path("src/widget.ts"));
        assert_all_headers(&chunks, "// src/widget.ts — ");
        let headers: Vec<String> = chunks.iter().map(|c| c.text.lines().next().unwrap_or("").to_string()).collect();
        assert!(headers.iter().any(|h| h.contains("Widget.greet")), "missing Widget.greet header: {headers:?}");
        assert!(headers.iter().any(|h| h.contains("freeFunction")), "missing freeFunction header: {headers:?}");
    }

    #[test]
    fn tsx_extension_uses_the_tsx_grammar_without_erroring() {
        let src = "export function App() {\n    return <div>hello</div>;\n}\n";
        let chunks = chunk(src, ChunkStrategy::code_for_path("src/App.tsx"));
        assert_all_headers(&chunks, "// src/App.tsx — ");
        assert!(chunks.iter().any(|c| c.text.contains("App")));
    }

    // ── JavaScript ───────────────────────────────────────────────────────

    #[test]
    fn javascript_chunks_on_class_and_function_boundaries() {
        let src = format!(
            "class Widget {{\n    greet() {{\n{}\n        return \"hi\";\n    }}\n}}\n\nfunction freeFunction() {{\n{}\n    return 42;\n}}\n",
            pad_lines(60), pad_lines(60)
        );
        let chunks = chunk(&src, ChunkStrategy::code_for_path("src/widget.js"));
        assert_all_headers(&chunks, "// src/widget.js — ");
        let headers: Vec<String> = chunks.iter().map(|c| c.text.lines().next().unwrap_or("").to_string()).collect();
        assert!(headers.iter().any(|h| h.contains("Widget.greet")));
        assert!(headers.iter().any(|h| h.contains("freeFunction")));
    }

    // ── Python ───────────────────────────────────────────────────────────

    #[test]
    fn python_chunks_on_class_and_function_boundaries() {
        let src = format!(
            "import os\n\nclass Widget:\n    def __init__(self, name):\n{}\n        self.name = name\n\n    def greet(self):\n{}\n        return self.name\n\n\ndef free_function():\n{}\n    return 42\n",
            pad_lines(60).replace("    ", "        "),
            pad_lines(60).replace("    ", "        "),
            pad_lines(60)
        );
        let chunks = chunk(&src, ChunkStrategy::code_for_path("widget.py"));
        assert_all_headers(&chunks, "# widget.py — ");
        let headers: Vec<String> = chunks.iter().map(|c| c.text.lines().next().unwrap_or("").to_string()).collect();
        assert!(headers.iter().any(|h| h.contains("Widget.__init__")), "missing Widget.__init__ header: {headers:?}");
        assert!(headers.iter().any(|h| h.contains("Widget.greet")), "missing Widget.greet header: {headers:?}");
        assert!(headers.iter().any(|h| h.contains("free_function")), "missing free_function header: {headers:?}");
    }

    // ── Go ───────────────────────────────────────────────────────────────

    #[test]
    fn go_chunks_on_function_and_method_boundaries() {
        let src = format!(
            "package main\n\ntype Widget struct {{\n    Name string\n}}\n\nfunc (w *Widget) Greet() string {{\n{}\n    return w.Name\n}}\n\nfunc FreeFunction() int {{\n{}\n    return 42\n}}\n",
            pad_lines(60), pad_lines(60)
        );
        let chunks = chunk(&src, ChunkStrategy::code_for_path("widget.go"));
        assert_all_headers(&chunks, "// widget.go — ");
        let headers: Vec<String> = chunks.iter().map(|c| c.text.lines().next().unwrap_or("").to_string()).collect();
        assert!(headers.iter().any(|h| h.contains("Greet")), "missing Greet header: {headers:?}");
        assert!(headers.iter().any(|h| h.contains("FreeFunction")), "missing FreeFunction header: {headers:?}");
    }

    // ── Java ─────────────────────────────────────────────────────────────

    #[test]
    fn java_chunks_on_class_and_method_boundaries() {
        let src = format!(
            "package com.example;\n\npublic class Widget {{\n    private String name;\n\n    public String greet() {{\n{}\n        return name;\n    }}\n}}\n",
            pad_lines(60)
        );
        let chunks = chunk(&src, ChunkStrategy::code_for_path("Widget.java"));
        assert_all_headers(&chunks, "// Widget.java — ");
        let headers: Vec<String> = chunks.iter().map(|c| c.text.lines().next().unwrap_or("").to_string()).collect();
        assert!(headers.iter().any(|h| h.contains("Widget.greet")), "missing Widget.greet header: {headers:?}");
    }

    // ── C# ───────────────────────────────────────────────────────────────

    #[test]
    fn csharp_chunks_on_namespace_class_and_method_boundaries() {
        let src = format!(
            "namespace Example {{\n    public class Widget {{\n        public string Greet() {{\n{}\n            return \"hi\";\n        }}\n    }}\n}}\n",
            pad_lines(60)
        );
        let chunks = chunk(&src, ChunkStrategy::code_for_path("Widget.cs"));
        assert_all_headers(&chunks, "// Widget.cs — ");
        let headers: Vec<String> = chunks.iter().map(|c| c.text.lines().next().unwrap_or("").to_string()).collect();
        assert!(headers.iter().any(|h| h.contains("Example.Widget.Greet")), "missing namespaced header: {headers:?}");
    }

    // ── Oversized single item splits by line windows ────────────────────

    #[test]
    fn oversized_item_splits_into_overlapping_parts_with_the_same_name() {
        let body = pad_lines(400);
        let src = format!("fn giant() {{\n{body}\n}}\n");
        let chunks = chunk(&src, ChunkStrategy::code_for_path("src/big.rs"));
        assert!(chunks.len() >= 2, "a 400+-line function must split into multiple parts, got {}", chunks.len());
        for (i, c) in chunks.iter().enumerate() {
            let header = c.text.lines().next().unwrap_or("");
            assert!(header.contains("giant"), "part {i} lost the item name: {header:?}");
            assert!(header.contains(&format!("(part {}/{})", i + 1, chunks.len())), "part {i} missing its part marker: {header:?}");
        }
    }

    // ── Every chunk is self-describing (header present, non-empty body) ─

    #[test]
    fn every_produced_chunk_has_a_non_empty_body_after_its_header() {
        let src = format!(
            "impl Widget {{\n    pub fn new() -> Self {{\n{}\n        Widget\n    }}\n}}\n",
            pad_lines(60)
        );
        let chunks = chunk(&src, ChunkStrategy::code_for_path("src/w.rs"));
        for c in &chunks {
            let mut lines = c.text.lines();
            lines.next(); // header
            let body: String = lines.collect::<Vec<_>>().join("\n");
            assert!(!body.trim().is_empty(), "chunk {} has an empty body", c.position);
        }
    }

    /// A doc block long enough to flush as its own all-decorative group must
    /// be labelled by WHAT IT IS, not by the grammar kinds of its lines.
    ///
    /// Measured on CoralSwarm's own repo (2026-09-22): before this, 1,531 of
    /// 16,735 code chunks (9.1%) carried a header reading
    /// `items line_comment, line_comment, line_comment, +29 more`. The
    /// embedding's first line was the word "line_comment" repeated.
    #[test]
    fn an_all_comment_group_is_never_labelled_by_its_comment_kinds() {
        // A long run of doc comments, no declaration in sight.
        let mut src = String::new();
        for i in 0..120 {
            src.push_str(&format!("/// Documentation line {i} describing the module in detail.\n"));
        }
        src.push_str("pub fn after(a: u64) -> u64 { a }\n");

        let chunks = chunk(&src, ChunkStrategy::code_for_path("backend/src/routes/jobs.rs"));
        assert!(!chunks.is_empty(), "fixture produced no chunks");

        let offenders: Vec<&str> = chunks
            .iter()
            .filter_map(|c| c.text.lines().next())
            .filter(|h| h.contains("line_comment") || h.contains("block_comment"))
            .collect();
        assert!(
            offenders.is_empty(),
            "no chunk header may name a comment grammar kind, got: {offenders:?}"
        );

        // And the header must still say something -- never blank, and still
        // carrying the path, which is the part retrieval leans on.
        for c in &chunks {
            let header = c.text.lines().next().unwrap_or("");
            assert!(
                header.contains("backend/src/routes/jobs.rs"),
                "every header keeps its path, got {header:?}"
            );
            assert!(header.len() > "// backend/src/routes/jobs.rs — ".len(), "header has no label: {header:?}");
        }
    }
}

//! The ingest door's request contract: the [`IngestAtomRequest`] every
//! private capture carries (HTTP `/v1/ingest/atom`, MCP `add_context`, the
//! member connectors), the [`IngestAtomResponse`] [`super::submit`] answers
//! with, and the rules [`super::submit`] applies to them: session kinds and
//! their derived source types, provenance-label and coding-session caps, the
//! FTS language allowlist, and the meeting header and speaker merge.

use chrono::{DateTime, Utc};
use std::collections::HashSet;
use uuid::Uuid;

use coralswarm_core::chunker::{distinct_chunk_speakers, Chunk, ChunkStrategy};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Merge speakers surfaced by transcript chunking into the request's
/// `participants`, so identity also lands in the queryable `participants`
/// column and not just the prose text. Preserves the caller-supplied order
/// first, then appends newly-seen chunk speakers, deduped.
pub(crate) fn merge_chunk_speakers(participants: &[String], chunks: &[Chunk]) -> Vec<String> {
    let seen: HashSet<&str> = participants.iter().map(String::as_str).collect();
    let mut merged: Vec<String> = participants.to_vec();
    for speaker in distinct_chunk_speakers(chunks) {
        if !seen.contains(speaker.as_str()) {
            merged.push(speaker);
        }
    }
    merged
}

/// Build the contextual chunk-header (cs-phase0-chunk-headers) for this
/// ingest request, if its source type is meeting-shaped. Scoped to exactly
/// the sources `ChunkStrategy::Transcript` handles ("transcript" / "zoom" /
/// "meet" / "teams_transcript") — chat sources (Slack, Teams channel/chat)
/// already carry their own intentionally user-visible "{sender} in
/// #{channel} · {when}" header (`routes/slack.rs`), and generic/email
/// sources have no well-defined "meeting" to describe, so both get `None`
/// here (no header at all — see `build_meeting_header`'s doc comment).
///
/// Title is derived from `raw_content` (the verbatim transcript, when the
/// caller supplied one) via the SAME first-line heuristic
/// `routes::meetings::meeting_summary` uses to label the post-meeting view,
/// so a meeting's embedded header and its displayed title never disagree.
/// Absent that, the header still carries date + participants — see
/// `build_meeting_header`'s graceful-omission rules.
pub(crate) fn build_contextual_header(
    source_type: &str,
    raw_content: Option<&str>,
    timestamp: DateTime<Utc>,
    participants: &[String],
) -> Option<String> {
    if !matches!(
        ChunkStrategy::for_source_type(source_type),
        ChunkStrategy::Transcript
    ) {
        return None;
    }
    let title = raw_content.and_then(crate::routes::meetings::meeting_title_from_raw);
    coralswarm_core::chunker::build_meeting_header(title.as_deref(), Some(timestamp), participants)
}

// ── Allowlist ─────────────────────────────────────────────────────────────────

/// PostgreSQL `regconfig` names accepted for per-atom FTS language.
///
/// Callers may pass any value from this list in `IngestAtomRequest.fts_language`.
/// `"simple"` is language-agnostic (no stemming) and works for any script.
pub(crate) const ALLOWED_LANGUAGES: &[&str] = &[
    "english", "french", "german", "spanish", "portuguese",
    "italian", "dutch", "russian", "arabic", "simple",
];

// ── Shared provenance-label validation (migration 0072) ──────────────────────

/// Cap (chars) enforced on the free-text MCP-style provenance labels
/// (`project`, `mcp_client_name`) at EVERY entry point that accepts them: the
/// plain REST `/v1/ingest/atom` route (`IngestAtomRequest`, below) and the MCP
/// `add_context` tool (`mcp/tools/write.rs`, which calls
/// [`validate_provenance_label`] directly rather than re-deriving its own
/// cap/NUL check). One shared constant + validator so a future third entry
/// point — or a change to the cap itself — only needs to touch this one
/// place (CodeRabbit review: the REST route originally enforced no cap at all
/// on these two fields, while the MCP boundary enforced a length-only cap
/// with no NUL-byte check).
pub(crate) const PROVENANCE_LABEL_CAP: usize = 256;

/// Why [`validate_provenance_label`] rejected a value. Callers format their
/// own user-facing message (an MCP tool's `invalid_params` vs. this route's
/// `422` JSON body differ in shape) using [`PROVENANCE_LABEL_CAP`] for the
/// length case.
pub(crate) enum ProvenanceLabelError {
    /// Postgres `TEXT` columns reject an embedded NUL byte outright — this
    /// stops that from ever reaching the INSERT/UPDATE as an opaque DB error
    /// instead of a clean, caller-facing validation rejection.
    ContainsNul,
    /// Over [`PROVENANCE_LABEL_CAP`] chars.
    TooLong,
}

/// Validate a single optional free-text provenance label (`project` or
/// `mcp_client_name`). `Ok(())` when the field was omitted (`None`) or the
/// supplied value is acceptable.
pub(crate) fn validate_provenance_label(value: &Option<String>) -> Result<(), ProvenanceLabelError> {
    let Some(v) = value else { return Ok(()) };
    if v.contains('\0') {
        return Err(ProvenanceLabelError::ContainsNul);
    }
    if v.chars().count() > PROVENANCE_LABEL_CAP {
        return Err(ProvenanceLabelError::TooLong);
    }
    Ok(())
}

/// The `422` message for a [`ProvenanceLabelError`] on `IngestAtomRequest`'s
/// `field_name` field (REST route only — the MCP boundary maps the same
/// error to its own `invalid_params` tool-error shape instead, in
/// `mcp/tools/write.rs`).
pub(crate) fn provenance_label_error(field_name: &str, e: ProvenanceLabelError) -> String {
    match e {
        ProvenanceLabelError::ContainsNul => format!("{field_name} must not contain NUL bytes"),
        ProvenanceLabelError::TooLong => {
            format!("{field_name} must be at most {PROVENANCE_LABEL_CAP} chars")
        }
    }
}

// ── Session kinds and their derived source types ────────────────────────────

/// `atoms.source_type` stamped on every coral captured under a CODING session
/// ([`SessionKind::Coding`] — the default when a caller declares no kind).
///
/// DERIVED, never caller-chosen: [`submit`](crate::ingest::submit) overwrites whatever
/// `source_type` the caller sent whenever `session_id` is present, exactly the
/// way the `"cc:{session_id}:{checkpoint_id}"` `source_id` convention takes
/// over in `mcp::tools::write`. Two reasons it works this way rather than as
/// one more value on the `add_context` allowlist:
///
/// * **Unspoofable.** The MCP allowlist exists to stop an LLM-driven write
///   from claiming a provenance-looking source type for content that never
///   came through that provenance path (see `ADD_CONTEXT_SOURCE_TYPES`).
///   `"coding_session"` is precisely such a type — a coral only earns it by
///   actually carrying a `session_id`, which is also what upserts the
///   `coding_sessions` row. The label and the row can therefore never
///   disagree.
/// * **No third place to keep in sync.** Both entry points that accept
///   `session_id` (the REST `/v1/ingest/atom` route and the MCP `add_context`
///   tool) funnel through `ingest::submit`, so the rule lives at the single
///   choke point instead of being re-derived per caller.
///
/// Chunking is deliberately unaffected: `ChunkStrategy::for_source_type` maps
/// this to `Generic`, which is right for prose checkpoint summaries, and the
/// coding-session chunk header keys off `session_id` (not `source_type`), so
/// the embedded/FTS-indexed text of an atom is byte-identical before and after
/// this label existed. That is what makes migration `0098`'s backfill of
/// historical `"note"`-stamped session corals safe without a re-embed.
pub(crate) const CODING_SESSION_SOURCE_TYPE: &str = "coding_session";

/// `atoms.source_type` for the SCREEN channel of a native meeting recording.
///
/// Ticket 11 decision (2026-08-23): an armed meeting is ONE session container
/// with TWO channels, and they carry DIFFERENT labels on purpose —
///
///   * the **transcript** channel keeps the existing `"transcript"` source
///     type and keeps landing in `meeting_transcripts`;
///   * the **screen** channel — what was on screen while the meeting ran —
///     goes through the session choke point and lands under this value.
///
/// The conflict rule that makes one container with two labels coherent:
/// **the transcript is authoritative for what was SAID, the screen is
/// authoritative for what was SHOWN.** That is the same shape as the
/// file/screen rule (the file parse wins for content; the window's OCR text
/// only detects activity and is then discarded), and it exists for the same
/// reason: `atoms` is unique on `(graph_id, source_type, source_id)` and
/// never on content, so without a rule the same meeting arrives twice.
///
/// **Why the transcript channel was NOT relabelled to this value.**
/// `"transcript"` is not the recorder's private label. `workers::huddle`
/// already WRITES it for Slack huddles and reads it back
/// (`WHERE source_type = 'transcript'`), and three display mappings fan
/// `"transcript" | "meeting" | "zoom" | "huddle"` to `"Meeting"`/`"meetings"`
/// (`store/mod.rs`, and `routes/queries.rs` twice). Relabelling would split
/// "a meeting transcript" into two source types by WHERE it was recorded,
/// fragmenting every one of those readers to buy nothing but symmetry. The
/// choke point protects the huddle path for free: `huddle` writes atoms
/// directly and never carries a `session_id`, so no `session_kind` can reach
/// it.
///
/// INSERT-only across sync, so the first row written under this value fixes
/// it forever. Session readers go through [`is_session_source_type`], which
/// recognises this value too.
pub(crate) const MEETING_SESSION_SOURCE_TYPE: &str = "meeting_session";

/// `atoms.source_type` stamped on every coral captured under an AGENT
/// session ([`SessionKind::Agent`]) — an autonomous, non-interactive agent
/// (a cloud worker, a bot, an orchestrator and the subagents it spawns)
/// posting through MCP or REST, as opposed to a human-driven coding session.
///
/// Same derivation and freeze properties as the three siblings above: only a
/// `session_id` plus a declared `session_kind=agent` produces it, and the
/// value is INSERT-only across sync. Every reader that goes through
/// [`is_session_source_type`] / [`SESSION_SOURCE_TYPES_SQL_ARRAY`] picks it
/// up automatically; the display maps (`services::atom_title`,
/// `store::journal_block_title_fallback`, the web taxonomy) gained explicit
/// arms in the same change that introduced this constant.
pub(crate) const AGENT_SESSION_SOURCE_TYPE: &str = "agent_session";

/// **Rider A (capture unit 03) — the B1 read side.** Every source type a
/// session coral can carry ([`CODING_SESSION_SOURCE_TYPE`],
/// [`MEETING_SESSION_SOURCE_TYPE`], [`AGENT_SESSION_SOURCE_TYPE`]) shares the SAME `source_id` convention
/// (`"cc:{session_id}:{checkpoint_id}[:chunk:N]"`) and is joined against
/// `coding_sessions` the same way (that table has carried all three kinds
/// since migration `0185_coding_sessions_session_kind`). The session readers
/// (`workers::journal_sweep::journal_group_key`,
/// `store::PostgresStore::journal_range_aggregate` and its sibling journal
/// reads, `store::journal_block_title_fallback`) used to literal-compare
/// `source_type == "coding_session"` to decide "is this a session coral" —
/// which silently degraded every `meeting_session` block to
/// one-block-per-checkpoint and rendered as `"Meeting_session"`. This is the single predicate every one of those call
/// sites now shares, so the answer can never drift between them again.
///
/// Third-round review of PR #728: this used to `matches!` the three
/// constants by hand, which was itself a fourth hand-copy of the set
/// [`SessionKind::ALL`] already closes over. It now iterates
/// [`SessionKind::ALL`] directly, so it is derived, not duplicated — adding
/// a variant to the enum is the only edit a fourth kind ever needs here.
pub(crate) fn is_session_source_type(source_type: &str) -> bool {
    session_source_types().any(|t| t == source_type)
}

/// Session source types that are still READ but can no longer be written:
/// `"work_session"`, the retired desktop app's screen-capture sessions. No
/// [`SessionKind`] derives it (ingest rejects `kind=work`), but persisted
/// rows still need their session titles and grouping in the journal.
/// Removed with the desktop data purge.
pub(crate) const LEGACY_READ_ONLY_SESSION_SOURCE_TYPES: &[&str] = &["work_session"];

/// Every session source type a reader must recognise: each [`SessionKind`]'s
/// own label, then [`LEGACY_READ_ONLY_SESSION_SOURCE_TYPES`].
fn session_source_types() -> impl Iterator<Item = &'static str> {
    SessionKind::ALL
        .iter()
        .map(|k| k.source_type())
        .chain(LEGACY_READ_ONLY_SESSION_SOURCE_TYPES.iter().copied())
}

/// The CLOSED set of session kinds a caller may declare, and the sole input
/// from which a session capture's `atoms.source_type` is derived.
///
/// This is the fix for capture blocker B1. Before it, `ingest::submit` did
/// `if req.session_id.is_some() { req.source_type = CODING_SESSION_SOURCE_TYPE }`
/// — right in its intent (see [`CODING_SESSION_SOURCE_TYPE`]: derive, never
/// trust), but total: every session was a coding session, so a second capture
/// surface could only ever mislabel its corals, permanently.
///
/// What replaces it is a BETTER-INFORMED derivation, not the removal of
/// derivation. The caller declares a kind; the route validates it against
/// this closed set and rejects anything else (422 / `invalid_params`); the
/// label is then computed by [`SessionKind::source_type`]. A caller still
/// cannot put an arbitrary string in `source_type` and have it stick, and
/// still cannot claim a session label without also creating the session row.
///
/// **The wire vocabulary is deliberately NOT the `source_type` vocabulary.**
/// `"coding"`/`"meeting"`/`"agent"` map to `"coding_session"`/
/// `"meeting_session"`/`"agent_session"` through the explicit `match` in
/// [`SessionKind::source_type`]. If the two vocabularies
/// were identical strings, the mapping would be an identity function and the
/// next refactor could "simplify" it into `source_type = session_kind`, which
/// is precisely the trust-the-caller regression this type exists to prevent.
///
/// **Third kind — DECIDED (ticket 11, 2026-08-23).** The native meeting
/// recorder DOES get its own kind, [`SessionKind::Meeting`]. It does not
/// simply reuse the existing `meeting`/`transcript` source types, because an
/// armed meeting captures a SCREEN channel as well as audio and that channel
/// is a session container's worth of corals. It does not displace them
/// either: the audio channel keeps `"transcript"`. See
/// [`MEETING_SESSION_SOURCE_TYPE`] for the two-channel split, the
/// said-versus-shown authority rule, and why the transcript kept its label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionKind {
    /// An AI coding-agent session (Claude Code and friends), captured as
    /// progressive checkpoints through MCP `add_context`. The default.
    Coding,
    /// The SCREEN channel of a native meeting recording. The audio channel of the same meeting keeps `"transcript"`
    /// — see [`MEETING_SESSION_SOURCE_TYPE`].
    Meeting,
    /// An AUTONOMOUS agent session — a cloud worker, bot or orchestrator
    /// running without a human at the keyboard, plus the subagents it
    /// spawns (which roll up to it via `parent_session_id`). Carries
    /// `agent_name` / `platform` / `task` metadata; `platform` is required
    /// for this kind (declared or detected from the User-Agent). Interactive
    /// Claude Code / Cursor sessions stay [`SessionKind::Coding`].
    Agent,
}

impl SessionKind {
    /// Every kind, in wire order — the single source for the closed set, used
    /// by [`SessionKind::parse`], by the 422/`invalid_params` message, and by
    /// the MCP tool's parameter documentation.
    pub const ALL: &'static [SessionKind] = &[
        SessionKind::Coding,
        SessionKind::Meeting,
        SessionKind::Agent,
    ];

    /// The kind assumed when a caller declares none.
    ///
    /// This is the backward-compatibility contract, and it is load-bearing:
    /// the MCP plugin, the `cs` CLI and the Claude Code harness primer all
    /// post `session_id` today and none of them send `session_kind`. An
    /// absent kind must keep producing `"coding_session"` byte-for-byte, or
    /// every existing capture path silently changes label on deploy.
    pub const DEFAULT: SessionKind = SessionKind::Coding;

    /// Wire value, and the value stored in `coding_sessions.session_kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            SessionKind::Coding => "coding",
            SessionKind::Meeting => "meeting",
            SessionKind::Agent => "agent",
        }
    }

    /// The `atoms.source_type` every coral of a session of this kind carries.
    ///
    /// The ONLY place a session's source type is decided. Exhaustive `match`
    /// on purpose: adding a variant to [`SessionKind`] without deciding its
    /// label is a compile error, not a silently-wrong ingest.
    pub fn source_type(self) -> &'static str {
        match self {
            SessionKind::Coding => CODING_SESSION_SOURCE_TYPE,
            SessionKind::Meeting => MEETING_SESSION_SOURCE_TYPE,
            SessionKind::Agent => AGENT_SESSION_SOURCE_TYPE,
        }
    }

    /// Parse a caller-declared kind. `None` for anything outside the closed
    /// set — callers turn that into a 422 / `invalid_params` rather than
    /// falling back to a default, because a typo'd kind ("Coding", "codingsession")
    /// would otherwise land a permanently mislabelled coral.
    pub fn parse(value: &str) -> Option<SessionKind> {
        SessionKind::ALL.iter().copied().find(|k| k.as_str() == value)
    }

    /// `"coding, meeting, agent"` — the closed set as it appears in
    /// error messages and in the MCP parameter description.
    pub fn allowed_values() -> String {
        SessionKind::ALL
            .iter()
            .map(|k| k.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Was a session's [`SessionKind`] taken verbatim from the caller's declared
/// `session_kind` (or the "no session_id at all" default, where inference
/// never runs), or worked out server-side from other signals (session-kind-
/// inference plan)? Kept as an explicit value threaded alongside the kind —
/// not re-derived later from, say, "is this an Agent whose `req.session_kind`
/// was `None`" — because the platform safety valve just below needs to tell
/// the two apart at the exact moment it decides whether a missing platform is
/// a 422 or a silent downgrade, and a re-derivation is one more place that
/// rule could drift from the one that actually ran.
///
/// A DECLARED kind always wins, in both directions (`session_kind: "coding"`
/// + `agent_name` stays `coding`; `session_kind: "agent"` + nothing else
/// stays `agent`) — inference only ever runs when the caller declared
/// nothing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KindSource {
    /// The caller sent a `session_kind` (validated against the closed set),
    /// or sent none at all with no `session_id` to infer anything about.
    Declared,
    /// The caller sent no `session_kind`; the value came from
    /// [`resolve_undeclared_session_kind`].
    Inferred,
}

/// When a caller declares no `session_kind` at all, decide whether this
/// checkpoint is an AGENT session rather than defaulting blindly to
/// [`SessionKind::DEFAULT`] (session-kind-inference plan). Only called when
/// `session_id_present` — kind is a property of a SESSION (decision 1); with
/// no session there is nothing to infer about, and the caller returns
/// `SessionKind::DEFAULT` itself for that case without going through this
/// function.
///
/// Order, most specific first:
/// 1. **Structural tell** — a non-empty (after trim) `agent_name`, or a
///    `parent_session_id` at all (a subagent's kind is decided by having a
///    parent, full stop). `task` deliberately does NOT count: a coding
///    session can carry a task too.
/// 2. **Per-client default** — the request's clientInfo name (never the
///    `User-Agent` fallback `detect_platform` may otherwise use — see
///    [`crate::services::platform::AGENT_FRAMEWORKS`]) belongs to a framework
///    that is autonomous by construction, e.g. `openclaw`.
/// 3. Otherwise [`SessionKind::DEFAULT`] (coding) — every caller that exists
///    today (none of which send `session_kind`) keeps landing exactly where
///    it always has.
///
/// Pure and side-effect-free (no 422, no `tracing::warn!`) so the whole truth
/// table is unit-testable with no HTTP request and no database — see the
/// `session_kind_inference` tests below. The platform safety valve (an
/// inferred `agent` with no platform anywhere) is decided separately, once
/// `platform_detected` is known.
pub(crate) fn resolve_undeclared_session_kind(
    agent_name: Option<&str>,
    parent_session_id_present: bool,
    client_info_name: Option<&str>,
) -> SessionKind {
    let agent_name_present = agent_name.map(str::trim).is_some_and(|s| !s.is_empty());
    if agent_name_present || parent_session_id_present {
        return SessionKind::Agent;
    }
    if crate::services::platform::is_agent_framework_client_info(client_info_name) {
        return SessionKind::Agent;
    }
    SessionKind::DEFAULT
}

/// The platform safety valve (session-kind-inference plan, decision 4).
/// Applies only to a ROOT session (`parent_session_id_present == false`) of
/// kind [`SessionKind::Agent`] with no platform anywhere — a subagent always
/// inherits its root's platform and never needs one of its own, and every
/// other kind is never gated. Two outcomes for that one shape:
///
/// * `kind_source == Declared` — the caller explicitly asked for
///   `session_kind=agent` and gets the existing contract: `Err(())`, which
///   the call site turns into the `agent_platform_required` 422.
/// * `kind_source == Inferred` — the SERVER decided this looked like an
///   agent session from `agent_name`/`parent_session_id`/an agent
///   framework's clientInfo, and nothing that saves today may start failing
///   just because the server got smarter about filing it: `Ok(SessionKind::
///   DEFAULT)`, degrading silently to `coding` (the call site logs a
///   `tracing::warn!` naming the session and why).
///
/// Any other shape (platform present, not an agent, or a subagent) is a
/// no-op `Ok(kind)`. Pure and side-effect-free for the same reason
/// [`resolve_undeclared_session_kind`] is — testable without a warn! or an
/// HTTP request.
pub(crate) fn apply_agent_platform_safety_valve(
    kind: SessionKind,
    kind_source: KindSource,
    parent_session_id_present: bool,
    declared_platform: Option<&str>,
    platform_detected: Option<&str>,
) -> Result<SessionKind, ()> {
    if kind != SessionKind::Agent
        || parent_session_id_present
        || declared_platform.is_some()
        || platform_detected.is_some()
    {
        return Ok(kind);
    }
    match kind_source {
        KindSource::Declared => Err(()),
        KindSource::Inferred => Ok(SessionKind::DEFAULT),
    }
}

/// What MCP `add_context` should echo after a successful ingest: the derived
/// `atoms.source_type` (`filed_as`) and the session kind (`None` when the
/// call created no session).
///
/// Mirrors `ingest::submit`'s kind-resolution + platform safety valve +
/// source_type derivation so the tool result names what actually landed.
/// `Err(())` is the declared-agent-without-platform 422 — the MCP success
/// path never hits it.
pub(crate) fn add_context_filing(
    session_id_present: bool,
    declared_kind: Option<&str>,
    agent_name: Option<&str>,
    parent_session_id: Option<&str>,
    client_info_name: Option<&str>,
    declared_platform: Option<&str>,
    trusted_user_agent: Option<&str>,
    caller_source_type: &str,
) -> Result<(String, Option<&'static str>), ()> {
    if !session_id_present {
        return Ok((caller_source_type.to_string(), None));
    }
    // Same empty-string collapse `ingest::submit` applies before inference
    // (`Some("")` becomes absent). A bool `is_some()` would treat that as a
    // parent and echo `agent_session` while ingest persisted `coding_session`.
    let parent_session_id_present = parent_session_id.is_some_and(|id| !id.is_empty());
    let (kind, kind_source) = match declared_kind {
        Some(raw) => match SessionKind::parse(raw) {
            Some(k) => (k, KindSource::Declared),
            None => return Ok((caller_source_type.to_string(), None)),
        },
        None => (
            resolve_undeclared_session_kind(
                agent_name,
                parent_session_id_present,
                client_info_name,
            ),
            KindSource::Inferred,
        ),
    };
    let declared_platform = declared_platform
        .and_then(crate::services::platform::canonicalize_platform);
    let platform_detected =
        crate::services::platform::detect_platform(client_info_name, trusted_user_agent);
    let kind = apply_agent_platform_safety_valve(
        kind,
        kind_source,
        parent_session_id_present,
        declared_platform.as_deref(),
        platform_detected.as_deref(),
    )?;
    Ok((kind.source_type().to_string(), Some(kind.as_str())))
}

/// The agent-kind platform policy (module doc / [`crate::services::
/// platform`]): for [`SessionKind::Agent`] a declared platform wins and an
/// undeclared one falls back to the detected value; every other kind keeps
/// the declared value only, never back-filled from detection. Extracted as
/// its own pure function so an INFERRED agent can be proven to get the exact
/// same policy as a DECLARED one (decision 6) without a database — see
/// `an_inferred_agent_gets_the_agent_kind_platform_policy` below — rather
/// than inlining the ternary a second time at the one call site that needs
/// it (the root `SessionIdentity` builder).
pub(crate) fn effective_root_platform(
    kind: SessionKind,
    declared_platform: Option<&str>,
    platform_detected: Option<&str>,
) -> Option<String> {
    if kind == SessionKind::Agent {
        declared_platform.or(platform_detected).map(str::to_string)
    } else {
        declared_platform.map(str::to_string)
    }
}

/// `ARRAY['coding_session', 'meeting_session', 'agent_session']` — built from
/// [`SessionKind::ALL`] rather than hand-written, so a fourth [`SessionKind`]
/// variant flows into it (and every SQL site below that interpolates it)
/// automatically instead of needing a fifth hand-edit to stay in lockstep.
///
/// Third-round review of PR #728 found this exact literal duplicated five
/// times: the four raw-SQL sites in `store::mod` (`store::PostgresStore::
/// journal_range_aggregate` — twice — `journal_hour_items`,
/// `journal_hour_inputs`, and `journal_hour_inputs_scoped`), plus
/// [`is_session_source_type`] itself, kept in sync only by "keep this
/// three-value list in lockstep" comments. This constant is the fix: those
/// four SQL sites now interpolate it instead of hand-copying the literal,
/// and [`is_session_source_type`] iterates [`SessionKind::ALL`] directly, so
/// there is exactly ONE hand-maintained list left anywhere — `SessionKind::
/// ALL` itself.
///
/// This is interpolated into SQL TEXT, not bound as a query parameter (a
/// `= ANY($n)` bind would be the safer shape in isolation, but every call
/// site already builds its `sql` string via `format!`/`str::replace` at the
/// call boundary, and changing the bind arity of four existing queries is
/// out of scope for a pure de-duplication). Safety therefore rests on
/// [`SessionKind::source_type`] only ever returning a hardcoded, lowercase,
/// underscore-delimited identifier with no embedded quote — see the
/// `session_source_types_sql_array_is_built_from_safe_identifiers` test
/// below, which pins that shape so a future variant cannot smuggle one in.
pub(crate) static SESSION_SOURCE_TYPES_SQL_ARRAY: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| {
        let quoted: Vec<String> = session_source_types().map(|t| format!("'{t}'")).collect();
        format!("ARRAY[{}]", quoted.join(", "))
    });

// ── Coding-session field truncation (Phase 1, audit amendment A10) ──────────

/// Cap (chars) applied to coding-session MACHINE-GENERATED metadata labels
/// (`session_title`, `repo_path`, `hostname`, `harness_version`, `model`,
/// `branch`) — same size as [`PROVENANCE_LABEL_CAP`], reused for consistency.
/// Unlike [`validate_provenance_label`], over-cap values here are TRUNCATED,
/// never rejected with a `422`: these are automated, best-effort provenance
/// fields a coding-agent harness stamps on every checkpoint save — failing an
/// entire ingest over a too-long `repo_path` would silently drop real user
/// content over a cosmetic metadata overflow (audit A10).
pub(crate) const CODING_SESSION_LABEL_CAP: usize = 256;

/// Cap (chars) applied to the RAW `remote` URL before it ever reaches
/// [`crate::project_key::normalize_remote`]. Larger than
/// [`CODING_SESSION_LABEL_CAP`] because real git remote URLs (self-hosted
/// GitLab/Azure DevOps nested group paths) run longer than a typical short
/// label — still bounded so a pathological input can't do unbounded work in
/// the normalizer or the eventual `remote_sanitized` column.
pub(crate) const CODING_SESSION_REMOTE_RAW_CAP: usize = 2048;

/// Cap (chars) applied to `session_id`. Unlike the machine labels above,
/// `session_id` is an IDENTIFIER — it's used verbatim in the `cc:{session_id}:
/// {checkpoint}` `source_id` convention and as an exact-match key on
/// `coding_sessions` — so an over-cap value is REJECTED (422), never
/// truncated: silently truncating an identifier risks merging two distinct
/// sessions into one row or splitting a caller's own session across two.
pub(crate) const CODING_SESSION_ID_CAP: usize = 512;

/// The effective `harness_version` for a checkpoint (clientinfo-platform-detection,
/// decision 3): a DECLARED value is always kept verbatim — this never rewrites
/// what the caller sent, same rule as `platform`/`platform_detected`. An
/// UNDECLARED value is filled from the MCP request's SEP-2575
/// `clientInfo.version` ([`IngestAtomRequest::client_info`]), when present.
/// `None` when neither is available, exactly as before this field existed.
/// Split out as its own pure function so it's testable without a DB or an
/// HTTP request (see the unit tests below).
pub(crate) fn effective_harness_version(
    declared: Option<String>,
    client_info: Option<&McpClientInfo>,
) -> Option<String> {
    declared.or_else(|| client_info.map(|ci| ci.version.clone()))
}

/// Truncate (never reject) an optional machine-generated coding-session
/// label to `cap` chars, also stripping embedded NUL bytes (Postgres `TEXT`
/// columns reject those outright — same reasoning as
/// [`validate_provenance_label`], just truncate-don't-reject here). An
/// all-NUL or empty-after-trim input collapses to `None` rather than storing
/// an empty string.
pub(crate) fn truncate_machine_label(value: Option<String>, cap: usize) -> Option<String> {
    let v = value?;
    let cleaned = v.replace('\0', "");
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.chars().count() > cap {
        Some(trimmed.chars().take(cap).collect())
    } else {
        Some(trimmed.to_string())
    }
}

/// Derive the fallback `project_key` input from a working-directory path when
/// no git remote is available: the LAST path component (folder basename),
/// handling both `/`- and `\`-separated paths (a Windows caller's
/// `repo_path` may use either). Sanitized via
/// [`crate::project_key::normalize_fallback`] by the caller — this function
/// only extracts the basename.
pub(crate) fn basename_for_fallback_key(path: &str) -> Option<String> {
    let trimmed = path.trim().trim_end_matches(['/', '\\']);
    if trimmed.is_empty() {
        return None;
    }
    trimmed
        .rsplit(['/', '\\'])
        .next()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

// ── Request / response types ──────────────────────────────────────────────────

/// Request body for atom ingestion.
#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct IngestAtomRequest {
    /// ID of the graph to associate the atom with.
    pub graph_id: Uuid,
    /// Provider source type: `"slack"`, `"email"`, `"transcript"`, `"notion"`, etc.
    pub source_type: String,
    /// Provider-native ID — unique within `(graph_id, source_type)`.
    pub source_id: String,
    /// Raw text to chunk and index.
    pub content: String,
    /// Names / emails of participants.
    pub participants: Vec<String>,
    /// Original timestamp of the source item (RFC 3339 / ISO 8601).
    #[schema(value_type = String, format = DateTime)]
    pub timestamp: DateTime<Utc>,
    /// Visibility scope: `"private"` (default) or `"account"`.
    pub scope: Option<String>,
    /// PostgreSQL `regconfig` name for full-text search tokenisation.
    /// Defaults to `"english"` when not provided.
    /// Must be one of: english, french, german, spanish, portuguese,
    /// italian, dutch, russian, arabic, simple.
    pub fts_language: Option<String>,
    /// Optional structured provenance (migration 0034): the container/channel and
    /// author the atom came from, typed so consumers don't parse the prose. The
    /// generic path leaves these `None`; the Microsoft/Teams funnel
    /// (`graph_webhook::ingest_message`) sets them so a Teams source surfaces with
    /// a human channel + author rather than raw GUIDs. Mirrors what the Slack
    /// auto-capture path writes directly.
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub channel_name: Option<String>,
    #[serde(default)]
    pub author_id: Option<String>,
    #[serde(default)]
    pub author_name: Option<String>,
    /// Optional MCP `add_context` provenance (migration 0072): the caller-supplied
    /// project/repo label and the identifying value of the MCP harness/client that
    /// made the call. `None` on every ingest path except the remote MCP
    /// `add_context` write tool (`mcp/tools/write.rs`).
    ///
    /// Sourced from `McpAuthContext::mcp_client_name`, i.e. the request's
    /// `User-Agent` — deliberately NOT from [`Self::client_info`] below, even
    /// though a real SEP-2575 `clientInfo` is now available on every MCP
    /// request (see that field's doc): reclassifying this stamp onto
    /// clientInfo would mean rows written before and after this change carry
    /// differently-sourced values for the same column with no way to tell
    /// them apart, whereas every row today already carries a UA string.
    /// `client_info` is used only for `platform_detected` (`trusted_user_agent`
    /// below) and the `harness_version` fallback, both newer, more targeted
    /// uses that don't inherit this column's history.
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub mcp_client_name: Option<String>,
    /// Optional RAW verbatim transcript for this source (cs-raw-provenance). When
    /// present it is stored WHOLE and verbatim in `meeting_transcripts` — encrypted
    /// to R2 in prod (mirroring the atom blob path), inline in dev — separate from
    /// and without affecting how `content` is chunked into atoms. Idempotent per
    /// `(graph_id, source_id)`.
    #[serde(default)]
    pub raw_content: Option<String>,
    /// Optional meeting duration in milliseconds, stored alongside the transcript.
    #[serde(default)]
    pub duration_ms: Option<i64>,
    /// Optional timestamped transcript segments (`[{speaker,start_ms,end_ms,text}]`)
    /// for this meeting (cs-uxp3a-meeting-summary). Stored verbatim as JSONB on the
    /// `meeting_transcripts` row so the async summary job can anchor each decision /
    /// action-item to a transcript timestamp. Stored as an opaque array; the
    /// summary worker validates the shape. Best-effort — absent for sources with no
    /// per-segment timing.
    #[serde(default)]
    pub segments: Option<serde_json::Value>,
    /// Optional coding-session capture fields (Phase 1, audit A1/A2/A3).
    /// `None` on every ingest path except the remote MCP `add_context` write
    /// tool when the caller supplies `session_id`. When `session_id` is
    /// `Some`, `ingest::submit` upserts a `coding_sessions` row (never
    /// touching the `atoms` schema) and prepends a
    /// `build_coding_session_header` context header (mirroring the meeting
    /// header mechanism, migration 0066) to the persisted/embedded chunk
    /// text. All of these fields are best-effort provenance — no logic may
    /// depend on their completeness (audit P1-5).
    #[serde(default)]
    pub session_id: Option<String>,
    /// Which KIND of session this is — `"coding"` (default),
    /// `"meeting"` or `"agent"`; see [`SessionKind`] for the closed set and why it exists. Validated
    /// server-side and rejected with a 422 when it is not one of those
    /// values; `atoms.source_type` is then DERIVED from it
    /// ([`SessionKind::source_type`]), never taken from `source_type` above.
    ///
    /// Only meaningful alongside `session_id`, and ignored without one — a
    /// declared kind with no session is not an error (a caller that always
    /// stamps its kind should not have to conditionally omit it), it simply
    /// has nothing to label.
    ///
    /// Absent (with a `session_id`) → INFERRED, never blindly
    /// [`SessionKind::DEFAULT`] (session-kind-inference plan): `agent_name`
    /// (non-empty) or `parent_session_id` present ⇒ `agent`; otherwise a
    /// known agent framework's clientInfo name (e.g. `openclaw`) ⇒ `agent`;
    /// otherwise [`SessionKind::DEFAULT`] → `"coding_session"`, exactly what
    /// every caller got before this field existed. See
    /// [`resolve_undeclared_session_kind`]. An inferred `agent` with no
    /// platform declared or detected anywhere degrades to `coding` rather
    /// than 422ing — see [`apply_agent_platform_safety_valve`] — a DECLARED
    /// `"agent"` still 422s in that shape.
    #[serde(default)]
    pub session_kind: Option<String>,
    /// Human-readable session title (session-constant metadata, refinable —
    /// see `Store::upsert_coding_session`'s COALESCE semantics).
    #[serde(default)]
    pub session_title: Option<String>,
    /// Working-directory path on the caller's machine at capture time.
    #[serde(default)]
    pub repo_path: Option<String>,
    /// RAW git remote URL as reported by the caller. The server ALWAYS
    /// re-normalizes this (`project_key::normalize_remote`) before it is
    /// ever persisted or used to derive `project_key` — a credential-bearing
    /// remote must never reach storage verbatim (audit P0-1). This field
    /// itself is never written to any column; only its normalized/sanitized
    /// output (`coding_sessions.remote_sanitized`, `project_key`) is.
    #[serde(default)]
    pub remote: Option<String>,
    /// Caller machine hostname.
    #[serde(default)]
    pub hostname: Option<String>,
    /// Coding-agent harness version string (e.g. Claude Code's own version).
    /// When omitted, filled from the MCP request's `clientInfo.version`
    /// ([`Self::client_info`]) if present — see [`effective_harness_version`]
    /// — never the other way around: a value sent here is always kept.
    #[serde(default)]
    pub harness_version: Option<String>,
    /// Model identifier the harness was using at capture time.
    #[serde(default)]
    pub model: Option<String>,
    /// Current git branch at capture time. Per-checkpoint (may change across
    /// calls within the same session). The last reported value is stored on
    /// the `coding_sessions` row (`COALESCE` — overwrite when sent, never
    /// blank when omitted; migration 0220) so the list can filter on it.
    /// Each checkpoint still also rides the contextual chunk-header
    /// mechanism (embedded/tsvectored text, stripped on display via
    /// `header_len`) so per-checkpoint history stays searchable.
    #[serde(default)]
    pub branch: Option<String>,
    /// What the agent calls itself (`grokbot`, `pr-reviewer`, `cs-worker-3`,
    /// ...). Free text, trimmed and capped like `session_title`, refinable
    /// across checkpoints (COALESCE). On a session that declares
    /// `parent_session_id` this is stored as the child's `subagent_name`
    /// instead — the effective `agent_name` is inherited from the root.
    ///
    /// Naming your agent is what makes this an agent session: with no
    /// `session_kind` declared, a non-empty `agent_name` alone is enough for
    /// the server to infer `session_kind=agent` (see [`Self::session_kind`]).
    #[serde(default)]
    pub agent_name: Option<String>,
    /// Where the agent runs (`claude-code`, `cursor`, `codex`, `grok`, ...).
    /// Canonicalized server-side by `services::platform::canonicalize_platform`
    /// — aliases collapse to one slug, unknown values pass through as a
    /// normalized slug. REQUIRED for `session_kind=agent` unless the
    /// request's User-Agent identifies the harness (422 otherwise); optional
    /// and never enforced for the other kinds.
    #[serde(default)]
    pub platform: Option<String>,
    /// What the agent is working on — free text or a story id (`CS-NNN`).
    /// Refinable across checkpoints; a subagent may declare its own slice
    /// and otherwise inherits the root's task.
    #[serde(default)]
    pub task: Option<String>,
    /// The `session_id` of the agent session that spawned this one. Fixed at
    /// the child's first checkpoint: the server resolves `root_session_id`
    /// through the parent, copies the root's `agent_name` / `platform` /
    /// `platform_detected` onto the child, and 422s when the parent does not
    /// exist in the same ocean or is not an `agent` session.
    #[serde(default)]
    pub parent_session_id: Option<String>,
    /// The HTTP `User-Agent` of the request that carried this body, set by
    /// the TRANSPORT (the REST handler reads the header; the MCP tool copies
    /// `McpAuthContext::mcp_client_name`, itself sourced from the header) —
    /// never by the caller: `#[serde(skip)]` means a JSON body cannot supply
    /// it and it is not part of the wire schema. This and [`Self::client_info`]
    /// are the only inputs to `platform_detected` / `platform_verified`
    /// (`services::platform::detect_platform`) — `client_info` wins when
    /// present, this is the fallback. `mcp_client_name` above is deliberately
    /// not used for either: on `/v1/ingest/atom` it is a caller-controlled
    /// provenance field, and a caller that could make its own `platform`
    /// claim "verified" by also sending a matching `mcp_client_name` would
    /// defeat the point of detection.
    #[serde(skip)]
    #[schema(ignore)]
    pub trusted_user_agent: Option<String>,
    /// The MCP request's SEP-2575 `clientInfo` (`{name, version}`), set by
    /// the TRANSPORT — the remote MCP `add_context` tool copies
    /// `RequestContext::meta.client_info()` (rmcp 3.1.2's raw per-request
    /// `_meta` reader — deliberately NOT the `RequestContext::client_info()`
    /// convenience method, whose stateless-mode legacy fallback is a
    /// non-`None` synthetic default; see `McpAuthContext::mcp_client_name`'s
    /// doc for the verified trap). `None` when this request's own body
    /// carried no `_meta.io.modelcontextprotocol/clientInfo`. Every other
    /// ingest path, including the REST `/v1/ingest/atom` handler (there is
    /// no MCP `clientInfo` on a plain HTTP POST), leaves this `None`. Never
    /// by the caller: `#[serde(skip)]` means a JSON body cannot forge its
    /// own detection input the same way `trusted_user_agent` cannot — a
    /// `_meta.io.modelcontextprotocol/clientInfo` value lives in the
    /// JSON-RPC ENVELOPE the transport parses before this struct ever
    /// exists, not in this struct's own body.
    ///
    /// Two uses, both best-effort provenance, neither ever overwriting a
    /// declared value (audit P1-5's rule, same as every other coding-session
    /// field): `.name` feeds `platform_detected` ahead of `trusted_user_agent`
    /// (`services::platform::detect_platform`); `.version` fills
    /// `harness_version` only when the caller declared none.
    #[serde(skip)]
    #[schema(ignore)]
    pub client_info: Option<McpClientInfo>,
    /// The repo-relative file path this atom's content came from — the ONE
    /// piece of information `coralswarm_core::chunker::ChunkStrategy::
    /// for_source_type` structurally cannot know (it only ever sees the bare
    /// `source_type` string). Only meaningful alongside `source_type =
    /// "github_code"` (docs/connectors/github.md §3.9, PR B): when both are
    /// present, chunking uses `ChunkStrategy::code_for_path(path)` (real
    /// syntax-aware boundaries, extension-selected) instead of `for_source_type`'s
    /// `Unknown`-language fallback. `None` on every other ingest path,
    /// including every other GitHub atom type — deliberately additive so
    /// this is the ONLY change to this request shape PR B needed.
    #[serde(default)]
    pub code_path: Option<String>,
}

/// See [`IngestAtomRequest::client_info`]. Not itself part of the wire
/// schema (the field it lives on is `#[serde(skip)]`), so this derives
/// nothing serde/utoipa needs to walk into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpClientInfo {
    /// The client's self-reported name (`Implementation::name`), e.g.
    /// `"claude-code"`, `"OpenCode"`, `"Hermes Agent"` — canonicalized the
    /// same way a declared `platform` is (`services::platform::
    /// canonicalize_platform`) before it ever reaches `platform_detected`.
    pub name: String,
    /// The client's self-reported version (`Implementation::version`), used
    /// to fill `harness_version` when the caller declared none.
    pub version: String,
}

/// Response body returned after successful ingestion.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct IngestAtomResponse {
    /// `atom_id`s whose content this request wrote: newly created rows plus
    /// existing positions whose text changed and was replaced in place. An
    /// unchanged position is adopted and not listed.
    pub atom_ids: Vec<Uuid>,
    /// Number of background embedding jobs enqueued (`0` or `1`).
    pub jobs_enqueued: usize,
    /// Total chunks produced from the content (includes both new and skipped).
    pub chunks_created: usize,
    /// How many of `chunks_created` hit an existing position this request
    /// did not rewrite: its stored text already matched (adopted and linked
    /// to the new source version), or the position belongs to another
    /// source and was left alone. Before content-verified ingest this also
    /// counted edits that were silently discarded; an edited position is now
    /// counted in `replaced_chunks` instead.
    pub skipped_duplicate_chunks: usize,
    /// Existing positions whose text changed and was replaced in place
    /// (content revision bumped, re-embedded).
    pub replaced_chunks: usize,
    /// Positions past the new chunk count deleted because the source shrank.
    pub retired_chunks: usize,
    /// Semantic processing state, distinct from successful retention/embedding.
    pub processing_status: String,
    pub processing_reason: Option<&'static str>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use coralswarm_core::chunker::chunk;
    // Only the hash-derivation test below spells the digest out by hand
    // (to pin what `services::source_reingest::content_hash_hex` must
    // produce); production code goes through that one helper.
    use sha2::{Digest, Sha256};

    // ── Agent platform detection trusts the transport, not the body ──────────

    /// The body field is NOT deserializable into the trusted slot: a caller
    /// naming `trusted_user_agent` in JSON gets it ignored.
    #[test]
    fn trusted_user_agent_is_not_settable_from_the_wire() {
        let req: IngestAtomRequest = serde_json::from_value(serde_json::json!({
            "graph_id": Uuid::new_v4(),
            "source_type": "note",
            "source_id": "x",
            "content": "y",
            "participants": [],
            "timestamp": "2026-09-13T00:00:00Z",
            "trusted_user_agent": "claude-code/1.0",
        }))
        .expect("unknown/skipped fields are ignored");
        assert_eq!(req.trusted_user_agent, None);
    }

    /// Same guarantee as above for `client_info`: a JSON body cannot forge
    /// its own SEP-2575 clientInfo — the field simply isn't part of the wire
    /// schema, so naming it deserializes to the type's default (`None`)
    /// regardless of what shape the caller sends.
    #[test]
    fn client_info_is_not_settable_from_the_wire() {
        let req: IngestAtomRequest = serde_json::from_value(serde_json::json!({
            "graph_id": Uuid::new_v4(),
            "source_type": "note",
            "source_id": "x",
            "content": "y",
            "participants": [],
            "timestamp": "2026-09-13T00:00:00Z",
            "client_info": {"name": "opencode", "version": "1.4.2"},
        }))
        .expect("unknown/skipped fields are ignored");
        assert!(req.client_info.is_none());
    }

    #[test]
    fn effective_harness_version_never_overwrites_a_declared_value() {
        let ci = McpClientInfo {
            name: "opencode".to_string(),
            version: "9.9.9".to_string(),
        };
        assert_eq!(
            effective_harness_version(Some("1.2.3".to_string()), Some(&ci)).as_deref(),
            Some("1.2.3"),
            "a declared harness_version is kept even though clientInfo disagrees"
        );
    }

    #[test]
    fn effective_harness_version_fills_from_client_info_when_undeclared() {
        let ci = McpClientInfo {
            name: "opencode".to_string(),
            version: "1.4.2".to_string(),
        };
        assert_eq!(
            effective_harness_version(None, Some(&ci)).as_deref(),
            Some("1.4.2")
        );
    }

    #[test]
    fn effective_harness_version_is_none_when_neither_is_present() {
        assert_eq!(effective_harness_version(None, None), None);
    }

    // ── Authorization logic unit test (pure — no DB required) ────────────────
    //
    // Verify that the content-hash and chunk-source-id derivation logic is correct.

    #[test]
    fn content_hash_is_hex_sha256() {
        let content = "hello world";
        let hash = hex::encode(Sha256::digest(content.as_bytes()));
        // SHA-256 always produces 32 bytes = 64 hex characters.
        assert_eq!(hash.len(), 64, "hash should be 64 hex chars");
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "hash should be all hex digits"
        );
        // Verify determinism — same input always yields the same hash.
        let hash2 = hex::encode(Sha256::digest(content.as_bytes()));
        assert_eq!(hash, hash2, "hashing must be deterministic");
        // Different input must yield a different hash.
        let hash_other = hex::encode(Sha256::digest(b"different input"));
        assert_ne!(hash, hash_other, "different content must yield different hash");
    }

    #[test]
    fn chunk_source_id_format() {
        let source_id = "msg_001";
        let position = 3usize;
        let chunk_source_id = format!("{source_id}:chunk:{position}");
        assert_eq!(chunk_source_id, "msg_001:chunk:3");
    }

    #[test]
    fn blob_key_format() {
        let graph_id = Uuid::nil();
        let atom_id = Uuid::nil();
        // Personal/consumer scheme — 2-segment, byte-identical to pre-tenancy
        // (org-tenancy PR 2.4: org tenants get `{org_id}/{graph_id}/{atom_id}`
        // via `blob_key_for`; see packages/core/src/r2.rs tests).
        let key = coralswarm_core::r2::personal_blob_key(graph_id, atom_id);
        assert_eq!(
            key,
            "00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            coralswarm_core::r2::blob_key_for(None, graph_id, atom_id).unwrap(),
            key
        );
    }

    // ── Speaker attribution survives chunk → insert (bug: chunker strips the
    // "Alice:" label into `chunk.speaker`, which no insert site read) ─────────

    #[test]
    fn merge_chunk_speakers_dedupes_and_preserves_caller_order() {
        let chunks = chunk(
            "Alice: hi\nBob: hi\nAlice: bye",
            ChunkStrategy::for_source_type("transcript"),
        );
        // Caller already supplied "Alice" (e.g. from calendar invite metadata);
        // chunking should add only the newly-seen "Bob", not duplicate Alice.
        let participants = vec!["Alice".to_string()];
        let merged = merge_chunk_speakers(&participants, &chunks);
        assert_eq!(merged, vec!["Alice".to_string(), "Bob".to_string()]);
    }

    #[test]
    fn merge_chunk_speakers_empty_participants_yields_only_chunk_speakers() {
        let chunks = chunk("[Alice] hi\n[Bob] hi", ChunkStrategy::Transcript);
        let merged = merge_chunk_speakers(&[], &chunks);
        assert_eq!(merged, vec!["Alice".to_string(), "Bob".to_string()]);
    }

    // ── build_contextual_header (cs-phase0-chunk-headers) ─────────────────────

    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn contextual_header_meeting_source_with_title_and_participants() {
        let header = build_contextual_header(
            "transcript",
            Some("Weekly Standup\n\nAlice: good morning"),
            dt("2026-07-15T14:00:00Z"),
            &["Alice".to_string(), "Bob".to_string()],
        );
        assert_eq!(
            header.as_deref(),
            Some("Meeting: Weekly Standup, 2026-07-15. Participants: Alice, Bob.")
        );
    }

    #[test]
    fn contextual_header_meeting_source_without_raw_content_falls_back_to_date() {
        // No raw_content supplied → no title heuristic to run, but the header
        // still carries date + participants.
        let header = build_contextual_header(
            "zoom",
            None,
            dt("2026-07-15T14:00:00Z"),
            &["Alice".to_string()],
        );
        assert_eq!(header.as_deref(), Some("Meeting on 2026-07-15. Participants: Alice."));
    }

    #[test]
    fn contextual_header_covers_all_transcript_strategy_source_types() {
        for source_type in ["transcript", "zoom", "meet", "teams_transcript"] {
            let header = build_contextual_header(source_type, None, dt("2026-07-15T14:00:00Z"), &[]);
            assert!(header.is_some(), "{source_type} should get a header");
        }
    }

    #[test]
    fn contextual_header_none_for_non_meeting_source_types() {
        // Slack already carries its own "{sender} in #{channel} · {when}"
        // header (routes/slack.rs); email/generic/notion have no well-defined
        // "meeting" to describe. None of these should get a header here.
        for source_type in ["slack", "email", "notion", "teams_channel", "teams_chat", "gdrive"] {
            let header = build_contextual_header(
                source_type,
                Some("Some Title\n\nbody"),
                dt("2026-07-15T14:00:00Z"),
                &["Alice".to_string()],
            );
            assert!(header.is_none(), "{source_type} should NOT get a meeting header");
        }
    }

    #[test]
    fn contextual_header_len_accounts_for_separator() {
        let header = build_contextual_header(
            "transcript",
            None,
            dt("2026-07-15T14:00:00Z"),
            &[],
        )
        .unwrap();
        let full = format!("{header}\n\nbody text");
        let header_len = (header.len() + 2) as i32;
        assert_eq!(&full[header_len as usize..], "body text");
    }

    #[test]
    fn scope_defaults_to_private() {
        let scope_input: Option<String> = None;
        let scope = scope_input.as_deref().unwrap_or("private");
        assert_eq!(scope, "private");
    }

    #[test]
    fn scope_account_is_preserved() {
        let scope_input: Option<String> = Some("account".to_string());
        let scope = scope_input.as_deref().unwrap_or("private");
        assert_eq!(scope, "account");
    }

    // ── fts_language validation unit tests ───────────────────────────────────

    #[test]
    fn fts_language_defaults_to_english() {
        // When None is provided the handler falls back to "english".
        let lang_input: Option<String> = None;
        let lang = lang_input.as_deref().unwrap_or("english");
        assert_eq!(lang, "english");
        // And "english" must be in the allowlist.
        assert!(ALLOWED_LANGUAGES.contains(&lang));
    }

    #[test]
    fn invalid_fts_language_not_in_allowlist() {
        // Languages not in the allowlist must be rejected.
        let invalid_langs = ["klingon", "latin", "pirate", "english'; DROP TABLE atoms;--", ""];
        for lang in &invalid_langs {
            assert!(
                !ALLOWED_LANGUAGES.contains(lang),
                "expected {lang:?} to be rejected by allowlist"
            );
        }
    }

    #[test]
    fn all_allowed_languages_pass_validation() {
        for lang in ALLOWED_LANGUAGES {
            assert!(
                ALLOWED_LANGUAGES.contains(lang),
                "language {lang:?} should be in allowlist"
            );
        }
    }

    // ── session_kind: the closed set and its derived source types ────────────

    #[test]
    fn absent_session_kind_still_derives_coding_session() {
        // THE backward-compatibility contract, and the single most important
        // assertion in this change. The MCP plugin, the `cs` CLI and the
        // Claude Code harness primer all post `session_id` today and NONE of
        // them sends `session_kind`. If the default ever stops being
        // `"coding_session"`, every capture path that exists silently
        // relabels itself on deploy — permanently, because `atoms.source_type`
        // is INSERT-only across sync.
        //
        // Pinned as a literal, not as `CODING_SESSION_SOURCE_TYPE`: writing
        // the constant on both sides of the assert would make it tautological
        // and would keep passing if the constant itself were edited.
        assert_eq!(SessionKind::DEFAULT, SessionKind::Coding);
        assert_eq!(SessionKind::DEFAULT.source_type(), "coding_session");

        // ...and it is the value an ABSENT wire field deserializes to, which
        // is the part the route actually relies on.
        let req: IngestAtomRequest = serde_json::from_value(serde_json::json!({
            "graph_id": Uuid::new_v4(),
            "source_type": "note",
            "source_id": "cc:sess-1:ckpt-1",
            "content": "hello",
            "participants": [],
            "timestamp": "2024-01-01T00:00:00Z",
            "session_id": "sess-1",
        }))
        .expect("a request with no session_kind must still deserialize");
        assert_eq!(req.session_kind, None);
        let kind = match req.session_kind.as_deref() {
            None => SessionKind::DEFAULT,
            Some(raw) => SessionKind::parse(raw).expect("declared kind"),
        };
        assert_eq!(kind.source_type(), "coding_session");
    }

    #[test]
    fn declared_session_kinds_map_to_their_own_source_types() {
        assert_eq!(SessionKind::parse("coding"), Some(SessionKind::Coding));
        assert_eq!(SessionKind::parse("meeting"), Some(SessionKind::Meeting));
        assert_eq!(SessionKind::Coding.source_type(), "coding_session");
        // Asserted against the string literal, not the constant, so the test
        // cannot go tautological if the constant is ever edited.
        assert_eq!(SessionKind::Meeting.source_type(), "meeting_session");
    }

    // ── session_kind inference (session-kind-inference plan) ─────────────────
    //
    // `resolve_undeclared_session_kind`, `apply_agent_platform_safety_valve`
    // and `effective_root_platform` are pure and side-effect-free by design
    // (see their doc comments) precisely so this truth table needs neither an
    // HTTP request nor a database. The full-stack behavior (real
    // `agent_name`/clientInfo/User-Agent through `add_context` against real
    // Postgres) is covered separately in `mcp_pg_e2e.rs`.

    /// Rows (a)-(f) of the required truth table: no structural tell and no
    /// agent-framework clientInfo, or a DECLARED kind, decide the result —
    /// `resolve_undeclared_session_kind` is only ever consulted for the
    /// "declared nothing" arm, which the match at the call site enforces, so
    /// the DECLARED rows here call `SessionKind::parse` directly instead.
    #[test]
    fn session_kind_inference_truth_table() {
        // (a) no kind, agent_name -> agent.
        assert_eq!(
            resolve_undeclared_session_kind(Some("grokbot"), false, None),
            SessionKind::Agent
        );
        // (b) no kind, parent -> agent.
        assert_eq!(
            resolve_undeclared_session_kind(None, true, None),
            SessionKind::Agent
        );
        // (c) no kind, task only -> coding. `task` never reaches this
        // function's parameters at all — it structurally cannot count.
        assert_eq!(
            resolve_undeclared_session_kind(None, false, None),
            SessionKind::DEFAULT
        );
        // (d) no kind, clientInfo openclaw -> agent.
        assert_eq!(
            resolve_undeclared_session_kind(None, false, Some("openclaw")),
            SessionKind::Agent
        );
        assert_eq!(
            resolve_undeclared_session_kind(None, false, Some("OpenClaw")),
            SessionKind::Agent,
            "clientInfo is canonicalized case-insensitively, same as a declared platform"
        );
        // (e) no kind, clientInfo opencode -> coding. `opencode` is a KNOWN
        // platform but not an AGENT FRAMEWORK — the two lists are deliberately
        // different.
        assert_eq!(
            resolve_undeclared_session_kind(None, false, Some("opencode")),
            SessionKind::DEFAULT
        );
        // (f) kind coding, agent_name -> coding. A DECLARED value always
        // wins, in both directions — this row never calls the inference
        // function at all, mirroring the real call site's `match`.
        assert_eq!(SessionKind::parse("coding"), Some(SessionKind::Coding));
        // (g) kind agent, nothing else -> agent. Same: declared wins.
        assert_eq!(SessionKind::parse("agent"), Some(SessionKind::Agent));
    }

    /// Row (h): an inferred agent (structural tell present) with no platform
    /// declared or detected ANYWHERE degrades to coding rather than 422ing —
    /// the platform safety valve only refuses a DECLARED agent kind in that
    /// shape.
    #[test]
    fn inferred_agent_with_no_platform_anywhere_degrades_to_coding() {
        let kind = resolve_undeclared_session_kind(Some("grokbot"), false, None);
        assert_eq!(kind, SessionKind::Agent, "structural tell still infers agent");
        let resolved = apply_agent_platform_safety_valve(kind, KindSource::Inferred, false, None, None)
            .expect("an inferred agent with no platform must degrade, not 422");
        assert_eq!(resolved, SessionKind::DEFAULT, "degrades to coding, not left as agent");
    }

    /// Contrast case for row (h): the SAME missing-platform shape on a
    /// DECLARED agent kind is still the existing 422 — the safety valve must
    /// not have quietly widened to cover every agent session, only inferred
    /// ones.
    #[test]
    fn declared_agent_with_no_platform_anywhere_still_refuses() {
        let resolved =
            apply_agent_platform_safety_valve(SessionKind::Agent, KindSource::Declared, false, None, None);
        assert_eq!(resolved, Err(()), "a DECLARED agent kind must still 422, never silently degrade");
    }

    #[test]
    fn add_context_filing_echoes_a_note_when_there_is_no_session() {
        let (filed_as, kind) = add_context_filing(
            false, None, None, None, None, None, None, "note",
        )
        .expect("a note has no platform gate");
        assert_eq!(filed_as, "note");
        assert_eq!(kind, None);
    }

    #[test]
    fn add_context_filing_echoes_coding_for_an_undeclared_session() {
        let (filed_as, kind) = add_context_filing(
            true, None, None, None, None, None, None, "note",
        )
        .expect("coding sessions are never gated on platform");
        assert_eq!(filed_as, "coding_session");
        assert_eq!(kind, Some("coding"));
    }

    #[test]
    fn add_context_filing_echoes_agent_when_kind_is_declared_with_a_platform() {
        let (filed_as, kind) = add_context_filing(
            true,
            Some("agent"),
            Some("grokbot"),
            None,
            None,
            Some("grok"),
            None,
            "note",
        )
        .expect("declared agent + platform must file");
        assert_eq!(filed_as, "agent_session");
        assert_eq!(kind, Some("agent"));
    }

    #[test]
    fn add_context_filing_degrades_an_inferred_agent_with_no_platform() {
        let (filed_as, kind) = add_context_filing(
            true, None, Some("grokbot"), None, None, None, None, "note",
        )
        .expect("inferred agent with no platform degrades rather than 422s");
        assert_eq!(filed_as, "coding_session");
        assert_eq!(kind, Some("coding"));
    }

    #[test]
    fn add_context_filing_treats_an_empty_parent_session_id_as_absent() {
        // `ingest::submit` collapses Some("") to None before inference. An
        // empty parent must not make the echo claim agent_session.
        let (filed_as, kind) = add_context_filing(
            true, None, None, Some(""), None, None, None, "note",
        )
        .expect("empty parent is not a platform-gated agent");
        assert_eq!(filed_as, "coding_session");
        assert_eq!(kind, Some("coding"));
    }

    /// The safety valve is a no-op whenever a platform IS available (declared
    /// or detected) or the session is a subagent (which inherits its root's
    /// platform) — none of those shapes are "missing a platform" at all,
    /// declared or inferred.
    #[test]
    fn platform_safety_valve_is_a_no_op_when_a_platform_exists_or_the_session_is_a_subagent() {
        assert_eq!(
            apply_agent_platform_safety_valve(SessionKind::Agent, KindSource::Inferred, false, Some("cursor"), None),
            Ok(SessionKind::Agent),
            "a declared platform satisfies the rule"
        );
        assert_eq!(
            apply_agent_platform_safety_valve(SessionKind::Agent, KindSource::Inferred, false, None, Some("cursor")),
            Ok(SessionKind::Agent),
            "a detected platform satisfies the rule"
        );
        assert_eq!(
            apply_agent_platform_safety_valve(SessionKind::Agent, KindSource::Inferred, true, None, None),
            Ok(SessionKind::Agent),
            "a subagent inherits its root's platform and is never gated here"
        );
        assert_eq!(
            apply_agent_platform_safety_valve(SessionKind::Coding, KindSource::Inferred, false, None, None),
            Ok(SessionKind::Coding),
            "a non-agent kind is never gated by this valve at all"
        );
    }

    /// Decision 6, proven directly: once inference settles on `Agent`, the
    /// SessionIdentity platform policy treats it exactly like a declared
    /// `agent` kind — declared-or-detected, never declared-only. Contrasted
    /// against the SAME detected platform being dropped for a coding session,
    /// so this isn't just "detection always wins".
    #[test]
    fn an_inferred_agent_gets_the_agent_kind_platform_policy() {
        let kind = resolve_undeclared_session_kind(Some("grokbot"), false, Some("some-other-harness"));
        assert_eq!(kind, SessionKind::Agent);
        assert_eq!(
            effective_root_platform(kind, None, Some("cursor")),
            Some("cursor".to_string()),
            "an inferred agent falls back to the detected platform, same as a declared one would"
        );
        assert_eq!(
            effective_root_platform(SessionKind::Coding, None, Some("cursor")),
            None,
            "the SAME detected platform is dropped for a coding session — declared-only policy"
        );
    }

    #[test]
    fn the_session_kind_set_is_closed() {
        // Anything outside the set is `None` — the route turns that into a
        // 422 rather than silently defaulting, because a typo'd kind coerced
        // to the default lands a coral that can never be relabelled.
        for bad in [
            "",
            "work", // the retired desktop screen-capture kind
            "Coding",
            "CODING",
            "coding ",
            "codingsession",
            "coding_session",  // the SOURCE TYPE, not the wire kind
            "meeting_session", // ditto — `"meeting"` IS the wire kind now
            "Meeting",
            "transcript", // the audio channel's source type, never a kind
        ] {
            assert!(
                SessionKind::parse(bad).is_none(),
                "{bad:?} must not parse as a session kind"
            );
        }
        assert_eq!(SessionKind::ALL.len(), 3);
        assert_eq!(SessionKind::allowed_values(), "coding, meeting, agent");
    }

    #[test]
    fn the_wire_vocabulary_is_not_the_source_type_vocabulary() {
        // Guards the "simplify it into `source_type = session_kind`" refactor
        // that would turn the derivation back into trusting the caller: no
        // kind's wire value may equal any kind's source type.
        for k in SessionKind::ALL {
            for other in SessionKind::ALL {
                assert_ne!(
                    k.as_str(),
                    other.source_type(),
                    "wire value {:?} must never equal a source_type",
                    k.as_str()
                );
            }
        }
    }

    #[test]
    fn is_session_source_type_recognises_every_session_kinds_own_label() {
        // Rider A: this predicate is the single source of truth every
        // session-aware reader (journal_group_key, journal_range_aggregate
        // and its sibling reads, journal_block_title_fallback) now shares —
        // it must agree with `SessionKind::ALL` exactly, or one of those
        // readers can drift from the others again.
        for kind in SessionKind::ALL {
            assert!(
                is_session_source_type(kind.source_type()),
                "{:?}'s source_type must be recognised as a session source type",
                kind
            );
        }
        // The retired desktop kind is read-only: still a session source
        // type for persisted rows, but no kind derives it.
        assert!(is_session_source_type("work_session"));
        assert!(SessionKind::ALL.iter().all(|k| k.source_type() != "work_session"));
        assert!(!is_session_source_type("transcript"));
        assert!(!is_session_source_type("slack"));
        assert!(!is_session_source_type(""));
    }

    #[test]
    fn every_session_kind_has_a_distinct_source_type() {
        // Two kinds sharing a label would make the discriminator pointless
        // and, worse, unrecoverable after the fact.
        let mut labels: Vec<&str> = SessionKind::ALL.iter().map(|k| k.source_type()).collect();
        let before = labels.len();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), before, "session source types must be distinct");
    }

    #[test]
    fn no_session_kind_claims_the_transcript_label() {
        // Ticket 11 decision (2026-08-23): an armed meeting is ONE session
        // container with TWO channels, and only the SCREEN channel passes
        // through this choke point. The audio channel keeps `"transcript"`,
        // the label `workers::huddle` already writes for Slack huddles and
        // that three display mappings fan to "Meeting"/"meetings".
        //
        // A refactor deciding `Meeting => "transcript"` would route the
        // SCREEN channel into the transcript readers' path, making OCR'd
        // screen content indistinguishable from what was actually said.
        for k in SessionKind::ALL {
            assert_ne!(
                k.source_type(),
                "transcript",
                "{:?} must not derive the transcript label",
                k.as_str()
            );
        }
    }

    #[test]
    fn session_source_types_are_safe_sql_identifiers() {
        // `SESSION_SOURCE_TYPES_SQL_ARRAY` is interpolated into SQL TEXT
        // (`store::mod`'s four journal queries), not bound as a parameter,
        // so every value `SessionKind::source_type()` can ever return must
        // be a plain lowercase/underscore identifier — no quote, backslash,
        // whitespace, or anything else that could break out of the
        // `ARRAY['...']` literal it is quoted into. This pins that shape so
        // a future `SessionKind` variant cannot smuggle a SQL injection into
        // every reader of the fragment at once.
        let shape = regex_lite_is_safe_identifier;
        for source_type in session_source_types() {
            assert!(
                shape(source_type),
                "{source_type:?} is not a safe SQL identifier — \
                 SESSION_SOURCE_TYPES_SQL_ARRAY interpolates it unescaped",
            );
        }
    }

    /// Hand-rolled `^[a-z][a-z0-9_]*$` check — deliberately not a `regex`
    /// dependency pull-in for one test; the shape is simple enough to assert
    /// byte-by-byte.
    fn regex_lite_is_safe_identifier(value: &str) -> bool {
        let mut chars = value.chars();
        match chars.next() {
            Some(c) if c.is_ascii_lowercase() => {}
            _ => return false,
        }
        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    }

    #[test]
    fn session_source_types_sql_array_matches_session_kind_all() {
        // One quoted entry per kind's own `source_type()`, in `SessionKind::
        // ALL` order, then the legacy read-only types.
        assert_eq!(
            *SESSION_SOURCE_TYPES_SQL_ARRAY,
            "ARRAY['coding_session', 'meeting_session', 'agent_session', 'work_session']"
        );
        assert_eq!(
            SESSION_SOURCE_TYPES_SQL_ARRAY.matches('\'').count(),
            (SessionKind::ALL.len() + LEGACY_READ_ONLY_SESSION_SOURCE_TYPES.len()) * 2,
            "expected exactly one quoted entry per session source type"
        );
        for kind in SessionKind::ALL {
            assert!(
                SESSION_SOURCE_TYPES_SQL_ARRAY.contains(&format!("'{}'", kind.source_type())),
                "{:?}'s source_type() is missing from SESSION_SOURCE_TYPES_SQL_ARRAY",
                kind
            );
        }
    }
}

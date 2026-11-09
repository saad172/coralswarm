//! Static half of the capture-funnel coverage guard (PR 4 of the
//! source-agnostic pipeline plan). No database: it reads the crate's own
//! production source.
//!
//! Every connector reaches the knowledge graph through ONE door,
//! `ingest::submit` (ADR-0016), and the door through ONE transactional core,
//! `ingest::capture::store_capture_tx`, which writes the `source_versions` row
//! (and with it the standing-admission row and extraction enqueue), the chunk
//! atoms with their fallback titles and provenance, the verified chunk links,
//! and the embed job. The core is private to `ingest`, so the compiler already
//! refuses a call from anywhere else; this test makes skipping a stage a
//! compile-time-adjacent failure too:
//!
//! 1. No production code outside the capture core and the store layer inserts
//!    `atoms` rows. A connector that hand-rolls an insert would skip versions,
//!    links, titles or embeds the moment someone forgets one of them.
//! 2. Source versions are captured and chunks reconciled only by the capture
//!    core (plus the named non-connector re-capture paths, each with a reason).
//! 3. Only the door calls the core. Every function in the core that inserts
//!    atoms is listed in [`CORE_CALLS`] and private to `ingest`, so a new one
//!    cannot be reached from outside unnoticed. The door's one maintenance
//!    entry ([`MAINTENANCE_ENTRY`], a re-chunk of an existing version, not
//!    new ingest) has only its named callers.
//! 4. Every file that calls `ingest::submit` is registered in
//!    [`ENTRY_POINTS`] with the source types it captures, and every registered
//!    file still calls it. A new connector therefore has to be listed here,
//!    and the dynamic half (`ingest::capture::pg_tests`) proves the stages for
//!    every route into the core.
//!
//! The scan skips test-only code: files under `pg_tests`, `tests.rs`, `bin/`,
//! and everything from the first column-0 `#[cfg(test)]` module onwards.

use std::path::{Path, PathBuf};

/// Where an insert into `atoms` may legitimately live. Everything else is a
/// connector bypassing the funnel.
const ATOM_INSERT_OWNERS: &[(&str, &str)] = &[
    ("ingest/capture.rs", "the capture core itself"),
    ("store/mod.rs", "store layer: promotion copies and legacy store methods"),
];

/// Where `source_evidence::capture_source_version*` may be called.
const VERSION_CAPTURE_OWNERS: &[(&str, &str)] = &[
    ("ingest/capture.rs", "the capture core itself"),
    ("source_evidence.rs", "definition"),
    ("managed_history.rs", "legacy-history promotion of atoms that already exist (not a connector)"),
    ("meeting_evidence.rs", "legacy meeting re-capture of atoms that already exist (not a connector)"),
];

/// Where `chunk_reconcile::reconcile_tx` may be called.
const RECONCILE_OWNERS: &[&str] = &["ingest/capture.rs", "chunk_reconcile.rs"];

/// The ingest module: the door (`ingest.rs`) and everything under it,
/// including the capture core. The only code that may call the core.
const DOOR: &str = "ingest";

/// Every production file that calls `ingest::submit`, with the
/// `source_type`s it produces. The door itself is not a caller.
const ENTRY_POINTS: &[(&str, &[&str])] = &[
    // HTTP `/v1/ingest/atom`, MCP `add_context`, sessions, meetings, notes.
    ("routes/ingest.rs", &["note", "document", "coding_session", "agent_session", "meeting_session", "meeting", "transcript"]),
    ("mcp/tools/write.rs", &["note", "coding_session", "agent_session", "meeting_session"]),
    ("routes/connectors.rs", &["document"]),
    // Composio: every `{toolkit}_{kind}` from the checked-in recipes.
    ("routes/composio.rs", &["linear_issue", "linear_comment", "linear_project", "jira_issue", "jira_comment", "zendesk_ticket", "zendesk_comment", "confluence_content", "confluence_comment", "dart_task", "hubspot_company", "hubspot_contact", "hubspot_deal", "hubspot_note", "hubspot_call", "hubspot_meeting"]),
    // Google.
    ("connectors/google_meet.rs", &["meet"]),
    ("routes/google_calendar_push.rs", &["google_calendar"]),
    ("routes/google_push.rs", &["gmail"]),
    ("workers/gmail_everything_mode.rs", &["gmail"]),
    // Microsoft.
    ("routes/graph_webhook.rs", &["teams_channel", "teams_chat", "teams_transcript", "outlook"]),
    ("workers/graph_ingest.rs", &["teams_channel", "teams_chat", "outlook"]),
    ("workers/microsoft_calendar_sync.rs", &["microsoft_calendar"]),
    ("workers/outlook_everything_mode.rs", &["outlook"]),
    // Slack member capture and history backfill (personal oceans).
    ("connectors/slack_member_capture.rs", &["slack"]),
    ("connectors/history_backfill.rs", &["slack"]),
    // Shared-ocean connectors, under an `OrgShared` routing grant.
    ("routes/slack_events.rs", &["slack"]),
    ("connectors/github_backfill.rs", &["github_code", "github_pr", "github_issue", "github_commit", "github_release", "github_code_summary"]),
    ("workers/huddle.rs", &["transcript"]),
    ("connectors/google_chat.rs", &["google_chat"]),
];

/// A `submit` caller must name `IngestRequest`, however it imports `submit`.
const ENTRY_CALL: &str = "IngestRequest";

/// The capture core's functions that insert atoms. Only the door calls them,
/// and each is `pub(super)` in `ingest::capture`.
const CORE_CALLS: &[&str] = &["store_capture_tx", "rechunk_version_tx"];

/// The capture core's one atom INSERT, private to `ingest::capture`. Every
/// core function that calls it must be in [`CORE_CALLS`].
const CORE_INSERT: &str = "insert_chunks_tx";

/// The capture core file.
const CORE_FILE: &str = "ingest/capture.rs";

/// The door's maintenance entry: re-chunks a version that is already stored
/// (no new version, no extraction). Not ingest, so not `submit`, but it
/// writes atoms through the core, so its callers are named here.
const MAINTENANCE_ENTRY: &str = "rechunk_existing_version_tx";

/// Who may call [`MAINTENANCE_ENTRY`], and why.
const MAINTENANCE_CALLERS: &[(&str, &str)] = &[(
    "chunk_reconcile/backfill.rs",
    "`rechunk_oversized`: re-chunks versions holding chunks over the embedder's input limit",
)];

/// `ingest.rs` or a file under `ingest/`.
fn in_the_door(rel: &str) -> bool {
    rel == format!("{DOOR}.rs") || rel.starts_with(&format!("{DOOR}/"))
}

fn src_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// `src`-relative path with `/` separators.
fn rel(path: &Path) -> String {
    path.strip_prefix(src_root()).unwrap().to_string_lossy().replace('\\', "/")
}

fn test_only_file(rel: &str, declared: &[String]) -> bool {
    rel.starts_with("bin/")
        || rel.contains("pg_tests")
        || rel.ends_with("/tests.rs")
        || rel.ends_with("_tests.rs")
        || rel.contains("/tests/")
        || declared.iter().any(|d| d == rel)
}

/// Byte offset just past the `}` matching the `{` at `open`, skipping
/// comments, string, raw-string and char literals. `None` if unbalanced.
fn matching_brace(text: &str, open: usize) -> Option<usize> {
    let b = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut nest = 1;
                i += 2;
                while i < b.len() && nest > 0 {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        nest += 1;
                        i += 1;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        nest -= 1;
                        i += 1;
                    }
                    i += 1;
                }
                continue;
            }
            b'r' if (b.get(i + 1) == Some(&b'#') || b.get(i + 1) == Some(&b'"'))
                && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_')) =>
            {
                let mut j = i + 1;
                let mut hashes = 0;
                while b.get(j) == Some(&b'#') {
                    hashes += 1;
                    j += 1;
                }
                if b.get(j) == Some(&b'"') {
                    let close: Vec<u8> = std::iter::once(b'"').chain(std::iter::repeat_n(b'#', hashes)).collect();
                    j += 1;
                    while j < b.len() && !b[j..].starts_with(&close) {
                        j += 1;
                    }
                    i = j + close.len();
                    continue;
                }
            }
            b'"' => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b'\'' => {
                // A char literal (`'{'`, `'\n'`, `'\u{7b}'`) or a lifetime.
                if b.get(i + 1) == Some(&b'\\') {
                    i += 2;
                    while i < b.len() && b[i] != b'\'' {
                        i += 1;
                    }
                } else if let Some(c) = text[i + 1..].chars().next() {
                    let after = i + 1 + c.len_utf8();
                    if b.get(after) == Some(&b'\'') {
                        i = after;
                    }
                }
            }
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The item that follows a `#[cfg(test)]` at `at`: skips attributes and
/// comments, and returns the offset where the item starts.
fn item_after(text: &str, mut at: usize) -> usize {
    loop {
        let rest = &text[at..];
        let trimmed = rest.trim_start();
        at += rest.len() - trimmed.len();
        if trimmed.starts_with("//") {
            at += trimmed.find('\n').map_or(trimmed.len(), |n| n + 1);
        } else if trimmed.starts_with("#[") {
            let open = at + 1;
            match text[open..].find('[').and_then(|_| matching_bracket(text, open)) {
                Some(end) => at = end,
                None => return at,
            }
        } else {
            return at;
        }
    }
}

fn matching_bracket(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (k, c) in text[open..].char_indices() {
        match c {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + k + 1);
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether the `#[cfg(test)]` at `at` is a real attribute: only whitespace
/// precedes it on its line (not a mention inside a comment or string).
fn starts_line(text: &str, at: usize) -> bool {
    let line_start = text[..at].rfind('\n').map_or(0, |n| n + 1);
    text[line_start..at].trim().is_empty()
}

/// `(module name, #[path] override)` for every `#[cfg(test)] mod x;`.
fn declared_test_modules(text: &str) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(k) = text[from..].find("#[cfg(test)]") {
        let at = from + k;
        from = at + 1;
        if !starts_line(text, at) {
            continue;
        }
        // `#[path]` may sit just before or just after `#[cfg(test)]`.
        let mut window = at.saturating_sub(200);
        while !text.is_char_boundary(window) {
            window += 1;
        }
        let attrs = &text[window..item_after(text, at + "#[cfg(test)]".len())];
        let item = &text[item_after(text, at + "#[cfg(test)]".len())..];
        let head = item.split(['{', ';']).next().unwrap_or("");
        let words: Vec<&str> = head.split_whitespace().collect();
        let Some(pos) = words.iter().position(|w| *w == "mod") else { continue };
        if !item[head.len()..].starts_with(';') {
            continue;
        }
        let name = words.get(pos + 1).map(|w| w.to_string()).unwrap_or_default();
        let path = attrs.find("#[path = \"").map(|p| {
            let rest = &attrs[p + 10..];
            rest[..rest.find('"').unwrap_or(0)].to_string()
        });
        out.push((name, path));
    }
    out
}

/// Drop every `#[cfg(test)]` item with a body (test modules and test-only
/// helpers), then comment lines, so a doc comment naming a pattern is not a
/// call. Unbalanced input keeps everything (fails toward reporting).
fn production_text(text: &str) -> String {
    let mut kept = String::with_capacity(text.len());
    let mut cursor = 0;
    let mut from = 0;
    while let Some(k) = text[from..].find("#[cfg(test)]") {
        let at = from + k;
        if !starts_line(text, at) {
            from = at + 1;
            continue;
        }
        let item = item_after(text, at + "#[cfg(test)]".len());
        let head_end = text[item..].find(['{', ';']).map(|n| item + n);
        from = at + 1;
        let Some(head_end) = head_end else { break };
        let end = if text.as_bytes()[head_end] == b';' {
            Some(head_end + 1)
        } else {
            matching_brace(text, head_end)
        };
        let Some(end) = end else { continue };
        if at < cursor {
            continue;
        }
        kept.push_str(&text[cursor..at]);
        cursor = end;
        from = end;
    }
    kept.push_str(&text[cursor..]);
    kept.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn production_sources() -> Vec<(String, String)> {
    let mut files = Vec::new();
    rust_files(&src_root(), &mut files);
    files.sort();
    let mut declared = Vec::new();
    for p in &files {
        let rel_path = rel(p);
        let text = std::fs::read_to_string(p).unwrap();
        let dir = match rel_path.rsplit_once('/') {
            Some((d, f)) if f == "mod.rs" || f == "lib.rs" => format!("{d}/"),
            Some((d, f)) => format!("{d}/{}/", f.trim_end_matches(".rs")),
            None if rel_path == "lib.rs" || rel_path == "main.rs" => String::new(),
            None => format!("{}/", rel_path.trim_end_matches(".rs")),
        };
        let base = match rel_path.rsplit_once('/') {
            Some((d, _)) => format!("{d}/"),
            None => String::new(),
        };
        for (name, path) in declared_test_modules(&text) {
            match path {
                Some(path) => declared.push(format!("{base}{path}")),
                None => {
                    declared.push(format!("{dir}{name}.rs"));
                    declared.push(format!("{dir}{name}/mod.rs"));
                }
            }
        }
    }
    files
        .into_iter()
        .filter_map(|p| {
            let rel = rel(&p);
            if test_only_file(&rel, &declared) {
                return None;
            }
            Some((rel, production_text(&std::fs::read_to_string(&p).unwrap())))
        })
        .collect()
}

fn inserts_atoms(text: &str) -> bool {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ").to_ascii_lowercase();
    flat.contains("insert into atoms(") || flat.contains("insert into atoms (") || flat.contains("insert into atoms ")
}

#[test]
fn only_the_funnel_core_and_store_insert_atoms() {
    let offenders: Vec<String> = production_sources()
        .into_iter()
        .filter(|(rel, text)| inserts_atoms(text) && !ATOM_INSERT_OWNERS.iter().any(|(o, _)| rel == o))
        .map(|(rel, _)| rel)
        .collect();
    assert!(
        offenders.is_empty(),
        "these production files insert atoms themselves instead of going through \
         ingest::submit, so they can skip a version, chunk link, title or embed: \
         {offenders:?}"
    );
}

#[test]
fn only_the_funnel_core_inserts_through_the_store_bulk_insert() {
    let offenders: Vec<String> = production_sources()
        .into_iter()
        .filter(|(rel, text)| text.contains(".insert_atoms_scoped(") && !rel.starts_with("store/"))
        .map(|(rel, _)| rel)
        .collect();
    assert!(offenders.is_empty(), "atom writes bypass the funnel core: {offenders:?}");
}

/// `fact_supersession::reconcile_tx` shares the name but re-derives fact
/// decisions, not chunk links, so a bare `reconcile_tx(` is only the chunk
/// reconcile when the file also names `chunk_reconcile`.
fn calls_chunk_reconcile(text: &str) -> bool {
    text.contains("reconcile_tx(") && text.contains("chunk_reconcile")
}

#[test]
fn a_same_named_fact_reconcile_is_not_a_chunk_reconcile() {
    let facts = "pub async fn reconcile_tx(tx: &mut Tx) -> R { reconcile_in_pass(tx).await }";
    assert!(!calls_chunk_reconcile(facts));
    assert!(calls_chunk_reconcile("crate::chunk_reconcile::reconcile_tx(&mut tx).await"));
    assert!(calls_chunk_reconcile("use crate::chunk_reconcile::{reconcile_tx};\nreconcile_tx(&mut tx)"));
}

#[test]
fn versions_and_chunk_links_are_written_only_by_the_funnel_core() {
    let mut offenders = Vec::new();
    for (rel, text) in production_sources() {
        if text.contains("capture_source_version") && !VERSION_CAPTURE_OWNERS.iter().any(|(o, _)| rel == *o) {
            offenders.push(format!("{rel}: captures a source version outside the funnel"));
        }
        if calls_chunk_reconcile(&text) && !RECONCILE_OWNERS.contains(&rel.as_str()) {
            offenders.push(format!("{rel}: reconciles chunks outside the funnel"));
        }
    }
    assert!(offenders.is_empty(), "{offenders:#?}");
}

/// Production files outside the door that call an atom-inserting core
/// function. Each is `pub(super)` in `ingest::capture`, so such a call does
/// not compile; this names the file if a visibility is ever widened.
fn core_callers_outside_the_door(sources: &[(String, String)]) -> Vec<&str> {
    sources
        .iter()
        .filter(|(rel, text)| !in_the_door(rel) && CORE_CALLS.iter().any(|f| text.contains(&format!("{f}("))))
        .map(|(rel, _)| rel.as_str())
        .collect()
}

#[test]
fn only_the_door_calls_the_capture_core() {
    let sources = production_sources();
    let outside = core_callers_outside_the_door(&sources);
    assert!(outside.is_empty(), "{CORE_CALLS:?} are the door's alone; these call them directly: {outside:?}");
    let core = sources.iter().find(|(rel, _)| rel == CORE_FILE).map(|(_, text)| text.as_str()).unwrap();
    for f in CORE_CALLS {
        assert!(
            core.contains(&format!("pub(super) async fn {f}(")),
            "{f} inserts atoms and must stay private to the ingest module"
        );
    }
}

/// One `fn` in a source text: its name, whether it is private to its parent
/// module or narrower (no `pub`, or `pub(super)`), and its body.
struct Function<'a> {
    name: &'a str,
    door_private: bool,
    body: &'a str,
}

/// Every `fn` with a body in `text`.
fn functions(text: &str) -> Vec<Function<'_>> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(k) = text[from..].find("fn ") {
        let at = from + k;
        from = at + 3;
        if at > 0 && !text.as_bytes()[at - 1].is_ascii_whitespace() {
            continue;
        }
        let rest = &text[at + 3..];
        let name_len = rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).unwrap_or(rest.len());
        let name = &rest[..name_len];
        if name.is_empty() {
            continue;
        }
        let line_start = text[..at].rfind('\n').map_or(0, |n| n + 1);
        let prefix = &text[line_start..at];
        let door_private = !prefix.contains("pub") || prefix.contains("pub(super)") || prefix.contains("pub(self)");
        let (Some(open), Some(semi)) = (text[at..].find('{'), text[at..].find(';').or(Some(usize::MAX))) else { continue };
        if semi < open {
            continue;
        }
        let Some(end) = matching_brace(text, at + open) else { continue };
        out.push(Function { name, door_private, body: &text[at + open..end] });
    }
    out
}

/// Functions in the core file that insert atoms (call [`CORE_INSERT`] or
/// write an INSERT themselves) but are not listed in [`CORE_CALLS`], or are
/// reachable from outside `ingest`.
fn unsealed_atom_writers(core: &str) -> Vec<String> {
    functions(core)
        .into_iter()
        .filter(|f| f.body.contains(&format!("{CORE_INSERT}(")) || inserts_atoms(f.body))
        .filter_map(|f| {
            if f.name == CORE_INSERT {
                return (!f.door_private).then(|| format!("{}: the atom INSERT is reachable outside ingest", f.name));
            }
            if !CORE_CALLS.contains(&f.name) {
                return Some(format!("{}: inserts atoms but is not in CORE_CALLS", f.name));
            }
            (!f.door_private).then(|| format!("{}: inserts atoms and is reachable outside ingest", f.name))
        })
        .collect()
}

#[test]
fn every_atom_inserting_core_function_is_sealed() {
    let sources = production_sources();
    let core = sources.iter().find(|(rel, _)| rel == CORE_FILE).map(|(_, text)| text.as_str()).unwrap();
    let unsealed = unsealed_atom_writers(core);
    assert!(unsealed.is_empty(), "{unsealed:#?}");
    let writers: Vec<&str> = functions(core)
        .into_iter()
        .filter(|f| f.body.contains(&format!("{CORE_INSERT}(")))
        .map(|f| f.name)
        .collect();
    for f in CORE_CALLS {
        assert!(writers.contains(f), "{f} is listed in CORE_CALLS but no longer inserts atoms");
    }
}

/// Production files that call the door's maintenance entry, outside the door.
fn maintenance_callers(sources: &[(String, String)]) -> Vec<&str> {
    sources
        .iter()
        .filter(|(rel, text)| !in_the_door(rel) && text.contains(&format!("{MAINTENANCE_ENTRY}(")))
        .map(|(rel, _)| rel.as_str())
        .collect()
}

#[test]
fn the_maintenance_entry_has_only_its_named_callers() {
    let sources = production_sources();
    let callers = maintenance_callers(&sources);
    let named: Vec<&str> = MAINTENANCE_CALLERS.iter().map(|(f, _)| *f).collect();
    let unnamed: Vec<&&str> = callers.iter().filter(|c| !named.contains(c)).collect();
    assert!(
        unnamed.is_empty(),
        "{MAINTENANCE_ENTRY} re-chunks an existing version; new ingest goes through ingest::submit. \
         Unnamed callers: {unnamed:?}"
    );
    let stale: Vec<&&str> = named.iter().filter(|n| !callers.contains(n)).collect();
    assert!(stale.is_empty(), "named maintenance callers that no longer call it: {stale:?}");
}

#[test]
fn a_core_caller_outside_the_door_is_named() {
    let sources = [
        ("ingest.rs", "let stored = capture::store_capture_tx(&mut tx, dest, &req, &mut uploads).await;"),
        ("ingest.rs", "capture::rechunk_version_tx(tx, dest, item, version, id, prepared, atoms, uploads).await"),
        ("ingest/capture.rs", "pub(super) async fn store_capture_tx(tx: &mut Tx) -> R { store_capture_tx(tx) }"),
        ("connectors/rogue.rs", "crate::ingest::capture::store_capture_tx(&mut tx, dest, &req, &mut uploads).await"),
        ("ingest_receive.rs", "store_capture_tx(&mut tx, dest, &req, &mut uploads)"),
        ("chunk_reconcile/backfill.rs", "crate::ingest::capture::rechunk_version_tx(&mut tx, dest, a, b, c, d, e, f)"),
        ("chunk_reconcile/sweep.rs", "crate::ingest::rechunk_existing_version_tx(&mut tx, version, p, a, u)"),
        ("connectors/polite.rs", "crate::ingest::submit(crate::ingest::IngestRequest::private(member)).await"),
    ]
    .map(|(rel, text)| (rel.to_string(), text.to_string()));
    assert_eq!(
        core_callers_outside_the_door(&sources),
        ["connectors/rogue.rs", "ingest_receive.rs", "chunk_reconcile/backfill.rs"]
    );
    assert_eq!(maintenance_callers(&sources), ["chunk_reconcile/sweep.rs"], "a maintenance caller is not a core caller");
}

#[test]
fn an_unsealed_atom_writer_in_the_core_is_named() {
    let core = "
fn insert_chunks_tx(tx: &mut Tx) -> R { q(\"INSERT INTO atoms (a) VALUES ($1)\") }
pub(super) async fn store_capture_tx(tx: &mut Tx) -> R { insert_chunks_tx(tx).await }
pub(super) async fn rechunk_version_tx(tx: &mut Tx) -> R { insert_chunks_tx(tx).await }
pub(crate) async fn repair_chunks_tx(tx: &mut Tx) -> R { let ids = insert_chunks_tx(tx).await?; Ok(ids) }
pub async fn raw_tx(tx: &mut Tx) -> R { q(\"INSERT INTO atoms (b) VALUES ($1)\") }
pub fn chunk_strategy(source_type: &str) -> S { S::for_source_type(source_type) }
";
    assert_eq!(
        unsealed_atom_writers(core),
        [
            "repair_chunks_tx: inserts atoms but is not in CORE_CALLS",
            "raw_tx: inserts atoms but is not in CORE_CALLS",
        ]
    );
    let widened = "pub(crate) async fn store_capture_tx(tx: &mut Tx) -> R { insert_chunks_tx(tx).await }";
    assert_eq!(unsealed_atom_writers(widened), ["store_capture_tx: inserts atoms and is reachable outside ingest"]);
    let open_insert = "pub fn insert_chunks_tx(tx: &mut Tx) -> R { q(\"INSERT INTO atoms (a) VALUES ($1)\") }";
    assert_eq!(unsealed_atom_writers(open_insert), ["insert_chunks_tx: the atom INSERT is reachable outside ingest"]);
}

/// Production files outside the door that call `ingest::submit`.
fn submit_callers(sources: &[(String, String)]) -> Vec<&str> {
    sources
        .iter()
        .filter(|(rel, text)| !in_the_door(rel) && text.contains(ENTRY_CALL))
        .map(|(rel, _)| rel.as_str())
        .collect()
}

#[test]
fn every_submit_caller_is_registered_with_its_source_types() {
    let sources = production_sources();
    let callers = submit_callers(&sources);
    let registered: Vec<&str> = ENTRY_POINTS.iter().map(|(f, _)| *f).collect();
    let unregistered: Vec<&&str> = callers.iter().filter(|c| !registered.contains(c)).collect();
    assert!(
        unregistered.is_empty(),
        "new ingest::submit callers must be registered in ENTRY_POINTS with their source types \
         (and covered by ingest::capture::pg_tests): {unregistered:?}"
    );
    let stale: Vec<&&str> = registered.iter().filter(|r| !callers.contains(r)).collect();
    assert!(stale.is_empty(), "registered entry points that no longer call ingest::submit: {stale:?}");
    for (file, types) in ENTRY_POINTS {
        assert!(!in_the_door(file), "{file}: the door is not its own caller");
        assert!(!types.is_empty(), "{file}: list the source types it captures");
    }
}

/// The four shared-ocean connectors enter through the door with a routing
/// grant, so their writes get the grant check.
#[test]
fn shared_ocean_connectors_write_through_the_door() {
    let sources = production_sources();
    for file in ["routes/slack_events.rs", "connectors/github_backfill.rs", "workers/huddle.rs", "connectors/google_chat.rs"] {
        let (_, text) = sources.iter().find(|(rel, _)| rel == file).unwrap();
        assert!(text.contains("Destination::OrgShared("), "{file} must write through ingest::submit with an OrgShared grant");
    }
}

#[test]
fn the_scan_sees_production_code_and_skips_test_modules() {
    let text = "fn a() {}\n// INSERT INTO atoms (doc)\n#[cfg(test)]\nmod tests {\n  fn t() { let _ = '{'; \"INSERT INTO atoms (x) }\"; }\n}\nfn after() { q(\"INSERT INTO atoms (y)\"); }\n";
    let prod = production_text(text);
    assert!(prod.contains("fn after()"), "production code after a test module is still scanned");
    assert!(inserts_atoms(&prod), "and its insert is seen");
    assert!(!prod.contains("fn t()"), "the test module body is skipped");
    assert!(!inserts_atoms(&production_text("#[cfg(test)]\nmod tests {\n fn t() { \"INSERT INTO atoms (x)\"; }\n}\n")));
    assert!(inserts_atoms("sqlx::query(\"INSERT INTO atoms(atom_id) VALUES($1)\")"));
    assert!(inserts_atoms("\"INSERT INTO\n atoms (atom_id)\""));
    let sources = production_sources();
    assert!(sources.iter().any(|(rel, _)| rel == "ingest.rs"), "the scan must find the door");
    assert!(sources.iter().any(|(rel, _)| rel == "ingest/capture.rs"), "and the capture core");
    assert!(
        !sources.iter().any(|(rel, _)| rel.ends_with("pg_tests.rs")),
        "and skip the core's test-only suites"
    );
    assert!(sources.len() > 100, "the scan must cover the crate, saw {}", sources.len());
}

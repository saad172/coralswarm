//! Shared email/HTML body normalisation (gsuite Train G2 PR 2 —
//! `.scratch/gsuite-enterprise/g2-design.md` §1.8, PR G2.2; upgraded by m365
//! PR 2.0, the residual after gsuite G2.2 — CodeRabbit finding F1 on #687).
//! Pure code only: no I/O, no routes, no migrations.
//!
//! # Why this exists
//! Before G2.2 there was no shared normaliser: `microsoft.rs` carried a
//! private, Teams-only `html_to_text` and Outlook's `message_to_item` did no
//! quoted-reply trimming at all (design doc §1.8). G2.2 moved `html_to_text`
//! here verbatim (byte-identical, deliberately keeping its two known gaps —
//! no `<script>`/`<style>` awareness, a five-entity table) so the move
//! itself couldn't be blamed for a behavior change; #687 then pinned those
//! two gaps in fixture tests so a fix would be caught, not silently
//! reintroduced. **This PR (m365 2.0) is that fix.**
//!
//! - [`html_to_text`] — tag-stripping + entity-decoding core. As of this PR:
//!   `<script>`/`<style>`/`<head>`/`<title>` elements (tags AND their text
//!   content) and HTML comments are skipped entirely, and entity decoding
//!   covers a curated named-entity table plus full numeric (decimal/hex)
//!   entity support — see [`decode_entities`]'s own doc for why a full
//!   HTML5 named-entity table/crate wasn't pulled in. **Still** does not
//!   insert newlines at block-element boundaries — that's a deliberate,
//!   separate difference from [`gmail_html_to_text`]; see that function's
//!   doc for why the split survives this PR unify-or-keep decision.
//! - [`gmail_html_to_text`] — sibling that also inserts a newline at `<br>`
//!   and common block-close boundaries. Shares this PR's script/style/
//!   comment/entity core via [`strip_tags`]/[`decode_entities`] (unified)
//!   but keeps its own boundary-insertion pass separate from `html_to_text`
//!   (not unified) — see its doc. Originally Gmail-only; as of this PR also
//!   backs [`from_graph_body_boundaried`] (Outlook mail).
//! - [`trim_quoted_and_signature`] — conservative quoted-reply-chain and
//!   signature trimming, Gmail-tuned (Gmail's own reply/forward header
//!   shapes: "On ... wrote:", a From:/Sent:/To:/Subject: block, ">"
//!   quoting, RFC 3676 "-- " signatures).
//! - [`trim_outlook_reply_chain`] — **new in this PR.** The same
//!   conservative line-scanning machinery (shared via `trim_with_markers`),
//!   extended with two Outlook-specific recognizers: a
//!   "-----Original Message-----"/"-----Original Appointment-----"
//!   delimiter line, and a small set of exact-line Outlook-mobile signature
//!   markers ("Get Outlook for iOS", "Sent from my iPhone", etc). Wired into
//!   `microsoft::message_to_item` for the Outlook **mail** path only —
//!   Teams messages are never trimmed (no reply-chain concept there), and
//!   Calendar is untouched. This closes the "founder call for a separate
//!   PR" deferral G2.2's doc used to note here (that note is now stale and
//!   removed) and the plan's original "quoted-reply-chain trimming for
//!   mail" spec item. Runs against [`from_graph_body_boundaried`]'s
//!   boundary-preserving output (not `from_graph_body`'s) — CodeRabbit
//!   finding F2 on #691: run against the boundary-less conversion, Outlook's
//!   own reply markers get flattened into the middle of a collapsed single
//!   line and this scan can never match them. See
//!   `from_graph_body_boundaried`'s doc.
//! - [`from_graph_body`] — Microsoft Graph `itemBody` adapter
//!   (`contentType` text vs html). `microsoft::body_to_text` is now a
//!   one-line wrapper around this. Used by Teams messages; as of m365 PR
//!   2.0, Outlook **mail** uses [`from_graph_body_boundaried`] instead (see
//!   below) — not this function — and so does the Calendar event body
//!   (`microsoft::shape_graph_event`, cs-mscal-body-window).
//! - [`from_graph_body_boundaried`] — **new in this PR.** Outlook-mail-only
//!   sibling of `from_graph_body` that converts HTML via
//!   [`gmail_html_to_text`] (block-boundary-preserving) instead of
//!   `html_to_text`, so `trim_outlook_reply_chain`'s line-scan can see
//!   Outlook's `<div>`-per-line reply markers — CodeRabbit finding F2 on
//!   #691. `microsoft::body_to_text_boundaried` wraps this; wired into
//!   `microsoft::message_to_item` only. Deliberately changes Outlook mail's
//!   atom text shape (adds newlines) — an accepted change for this PR, see
//!   its own doc.
//! - [`from_gmail_payload`] — Gmail `users.messages.get(format=full)`
//!   `payload` adapter: base64url MIME part walk, `text/plain` preferred,
//!   `text/html` (via [`gmail_html_to_text`]) as fallback, `multipart/*`
//!   nesting handled to any depth, attachments excluded from both.
//! - [`gmail_message_to_item`] — Gmail message → `NormalizedItem` shaping.
//!   `connectors::google_gmail` (Train G2 PR 3, now merged as #686) builds
//!   ingest/watch machinery around this.

use chrono::DateTime;
use serde::Deserialize;

use super::NormalizedItem;

// ---------------------------------------------------------------------------
// HTML → text core
// ---------------------------------------------------------------------------

/// Element names whose start tag through matching end tag — content
/// included — must be dropped entirely rather than merely having their own
/// tags stripped. `<script>`/`<style>` are this PR's whole reason for
/// existing (CodeRabbit finding F1 on #687: their text content was leaking
/// into ingested atom bodies). `<head>`/`<title>` are included too: a
/// full HTML document pasted into a mail body can carry both, and neither
/// belongs in chat/email content.
const SKIPPED_ELEMENTS: &[&str] = &["script", "style", "head", "title"];

/// Find the index just past the `>` that closes the tag starting at
/// `lower[start]` (which must be `<`), treating `>` inside a single- or
/// double-quoted attribute value as ordinary text rather than the tag's end
/// (so `<a title="a > b">` doesn't truncate early). `None` if the tag is
/// never closed (caller drops the remainder of the input — tolerant of
/// truncated/malformed markup rather than panicking or looping forever).
/// Operates on an ASCII-lowercased copy of the input purely so callers can
/// do case-insensitive tag-name matching elsewhere; quote/`>` scanning
/// itself doesn't care about case. `to_ascii_lowercase` only rewrites bytes
/// `< 0x80`, so byte offsets stay valid into the original (possibly
/// multi-byte UTF-8) string — same trick `microsoft.rs`'s pre-existing
/// `remove_tagged_block` (huddle transcript path) relies on.
fn find_tag_close(lower: &str, start: usize) -> Option<usize> {
    let bytes = lower.as_bytes();
    let mut i = start + 1;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'"' | b'\'' => quote = Some(b),
                b'>' => return Some(i + 1),
                _ => {}
            },
        }
        i += 1;
    }
    None
}

/// Find the end (index just past the closing `>`) of the next well-formed
/// `</{tag}` closing tag at or after `from` in `lower` (an ASCII-lowercased
/// copy — same convention as [`find_tag_close`]). Requires a tag boundary
/// (`>`, ASCII whitespace, or end-of-string) immediately after the matched
/// `</{tag}` prefix, and keeps scanning forward past a candidate that fails
/// that check rather than accepting it — CodeRabbit round 1 finding on
/// #691: a bare prefix search for `</script` would accept `</scripture>` as
/// `<script>`'s closing tag (the substring `</script` is a prefix of
/// `</scripture`), leaking everything between the real end of the script
/// and the next actual `</script>` — e.g. `leaked()` in
/// `<script>ignored</scripture>leaked()</script>` — into the output as
/// ordinary page text. `None` if no boundary-respecting closing tag is
/// found before the input ends (caller drops the remainder — same
/// tolerant-of-malformed-markup posture as the rest of this module).
fn find_closing_tag(lower: &str, from: usize, tag: &str) -> Option<usize> {
    let bytes = lower.as_bytes();
    let close_pat = format!("</{tag}");
    let mut search_from = from;
    loop {
        let rel = lower[search_from..].find(&close_pat)?;
        let cand = search_from + rel;
        let after = bytes.get(cand + close_pat.len());
        let boundary = matches!(
            after,
            None | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r') | Some(b'>')
        );
        if boundary {
            return find_tag_close(lower, cand);
        }
        // Not a real closing tag for `tag` (e.g. `</scripture>` while
        // looking for `</script`) — keep scanning past this candidate
        // rather than giving up or accepting it.
        search_from = cand + close_pat.len();
    }
}

/// Remove HTML comments (`<!-- ... -->`) and every [`SKIPPED_ELEMENTS`]
/// element (start tag through its matching end tag, content included),
/// case-insensitively, tolerant of attributes on the start tag and of an
/// unclosed comment/element (drops the remainder of the input rather than
/// looping or panicking — same conservative-on-malformed-input posture as
/// the rest of this module). Leaves ordinary tags and text untouched for a
/// later pass ([`strip_tags`]) to handle.
///
/// A matched skip-element name must be followed by whitespace, `>`, or `/`
/// — not by another tag-name character — so `<style>` doesn't accidentally
/// match a hypothetical `<styles>` element.
fn remove_comments_and_skipped_elements(html: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut out = String::with_capacity(html.len());
    let mut pos = 0usize;
    let len = html.len();
    while pos < len {
        if lower[pos..].starts_with("<!--") {
            match lower[pos..].find("-->") {
                Some(rel) => pos += rel + 3,
                None => break, // Unclosed comment: drop the remainder.
            }
            continue;
        }
        let mut matched_skip: Option<&str> = None;
        if bytes.get(pos) == Some(&b'<') {
            for tag in SKIPPED_ELEMENTS {
                let open = format!("<{tag}");
                if lower[pos..].starts_with(&open) {
                    let after = bytes.get(pos + open.len());
                    let boundary = matches!(
                        after,
                        None | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r') | Some(b'>') | Some(b'/')
                    );
                    if boundary {
                        matched_skip = Some(tag);
                        break;
                    }
                }
            }
        }
        if let Some(tag) = matched_skip {
            let start_tag_end = match find_tag_close(&lower, pos) {
                Some(e) => e,
                None => break, // Unclosed start tag: drop the remainder.
            };
            match find_closing_tag(&lower, start_tag_end, tag) {
                Some(e) => pos = e,
                None => break, // Unclosed element: drop the remainder.
            }
            continue;
        }
        let ch_len = html[pos..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        out.push_str(&html[pos..pos + ch_len]);
        pos += ch_len;
    }
    out
}

/// Block-level close tags [`strip_tags`] inserts a newline after, when
/// asked to — same set [`gmail_html_to_text`] has always used.
const BLOCK_CLOSE_TAGS: &[&str] =
    &["div", "p", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote"];

/// Strip `<...>` tags from HTML that has already passed through
/// [`remove_comments_and_skipped_elements`]. When `insert_block_boundaries`
/// is true, also inserts a newline for `<br>` and for the close of a
/// [`BLOCK_CLOSE_TAGS`] element — [`gmail_html_to_text`]'s pre-existing
/// boundary-preservation behavior, unchanged by this PR. Entity decoding is
/// a separate pass ([`decode_entities`]) so this function only ever sees/
/// emits raw entity text.
fn strip_tags(html: &str, insert_block_boundaries: bool) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut tag_buf = String::new();
    for ch in html.chars() {
        match ch {
            '<' => {
                in_tag = true;
                tag_buf.clear();
            }
            '>' => {
                in_tag = false;
                if insert_block_boundaries {
                    let tag_lower = tag_buf.to_ascii_lowercase();
                    let is_close = tag_lower.starts_with('/');
                    let name = tag_lower
                        .trim_start_matches('/')
                        .trim_end_matches('/')
                        .split(|c: char| c.is_whitespace())
                        .next()
                        .unwrap_or("");
                    let is_br = name == "br";
                    let is_block_close = is_close && BLOCK_CLOSE_TAGS.contains(&name);
                    if is_br || is_block_close {
                        out.push('\n');
                    }
                }
            }
            _ if in_tag => tag_buf.push(ch),
            _ => out.push(ch),
        }
    }
    out
}

/// Curated named-entity table beyond the six this module has always
/// decoded (`nbsp` as a plain space — not U+00A0, preserving pre-existing
/// behavior/tests — `amp`, `lt`, `gt`, `quot`, `apos`). Covers the
/// typographic punctuation, symbols, and accented Latin letters that
/// actually show up in real Teams/Outlook/Gmail HTML (Word/Outlook
/// "smart quotes" and dashes chief among them — the exact gap CodeRabbit
/// finding F1 on #687 called out via `&mdash;`).
///
/// **Deliberately NOT the full HTML5 named-character-reference table**
/// (2000+ entries, many obscure/mathematical) and deliberately hand-rolled
/// rather than a new crate dependency: `backend/Cargo.toml` has no
/// HTML-entity or HTML-parsing crate today, and pulling in a full DOM
/// parser (the natural "just do it properly" option) would be a much
/// bigger dependency than this module's job — stripping tags from
/// chat/email bodies — justifies. A curated table plus full numeric
/// support ([`decode_entities`]) covers the realistic gap without either
/// extreme. Matched case-insensitively against the entity name (real HTML
/// overwhelmingly uses the canonical lowercase form; being lenient about
/// stray capitalization from Word-generated markup costs nothing here).
const NAMED_ENTITIES: &[(&str, char)] = &[
    ("nbsp", ' '),
    ("amp", '&'),
    ("lt", '<'),
    ("gt", '>'),
    ("quot", '"'),
    ("apos", '\''),
    ("mdash", '\u{2014}'),
    ("ndash", '\u{2013}'),
    ("hellip", '\u{2026}'),
    ("rsquo", '\u{2019}'),
    ("lsquo", '\u{2018}'),
    ("rdquo", '\u{201D}'),
    ("ldquo", '\u{201C}'),
    ("bull", '\u{2022}'),
    ("middot", '\u{00B7}'),
    ("copy", '\u{00A9}'),
    ("reg", '\u{00AE}'),
    ("trade", '\u{2122}'),
    ("sect", '\u{00A7}'),
    ("para", '\u{00B6}'),
    ("plusmn", '\u{00B1}'),
    ("times", '\u{00D7}'),
    ("divide", '\u{00F7}'),
    ("laquo", '\u{00AB}'),
    ("raquo", '\u{00BB}'),
    ("deg", '\u{00B0}'),
    ("eacute", '\u{00E9}'),
    ("egrave", '\u{00E8}'),
    ("ecirc", '\u{00EA}'),
    ("agrave", '\u{00E0}'),
    ("acirc", '\u{00E2}'),
    ("uuml", '\u{00FC}'),
    ("ouml", '\u{00F6}'),
    ("auml", '\u{00E4}'),
    ("ccedil", '\u{00E7}'),
    ("ntilde", '\u{00F1}'),
    ("aacute", '\u{00E1}'),
    ("iacute", '\u{00ED}'),
    ("oacute", '\u{00F3}'),
    ("uacute", '\u{00FA}'),
];

/// Decode one entity reference starting at `s[0] == '&'`. Returns the
/// decoded `char` and the total byte length consumed (including the
/// leading `&` and trailing `;`), or `None` if `s` doesn't start with a
/// recognizable, well-formed entity — in which case the caller leaves the
/// literal `&` (and whatever follows) untouched. Handles:
/// - Named entities from [`NAMED_ENTITIES`].
/// - Decimal numeric entities (`&#8212;`).
/// - Hexadecimal numeric entities (`&#x2014;`/`&#X2014;`).
///
/// An out-of-range or surrogate codepoint (invalid per [`char::from_u32`])
/// is treated the same as an unrecognized name: `None`, left undecoded —
/// "invalid → skip gracefully" rather than panicking or substituting a
/// replacement character, since a byte-for-byte-preserved literal is more
/// useful for downstream debugging than a silent substitution would be.
fn decode_one_entity(s: &str) -> Option<(char, usize)> {
    debug_assert!(s.starts_with('&'));
    let rest = &s[1..];
    // Bounded lookahead: every named entity in our table and any realistic
    // numeric entity is well under this; an unterminated '&' followed by
    // ordinary prose (no ';' within reach) must not scan the rest of a long
    // body looking for a semicolon that isn't there.
    let semi = rest.get(..32.min(rest.len())).unwrap_or(rest).find(';')?;
    if semi == 0 {
        return None; // "&;" — not an entity.
    }
    let body = &rest[..semi];
    let total_len = 1 + semi + 1; // '&' + body + ';'

    if let Some(numeric) = body.strip_prefix('#') {
        let (radix, digits) = match numeric.strip_prefix('x').or_else(|| numeric.strip_prefix('X')) {
            Some(hex) => (16, hex),
            None => (10, numeric),
        };
        if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
            return None;
        }
        let code = u32::from_str_radix(digits, radix).ok()?;
        let ch = char::from_u32(code)?;
        return Some((ch, total_len));
    }

    for (name, ch) in NAMED_ENTITIES {
        if name.eq_ignore_ascii_case(body) {
            return Some((*ch, total_len));
        }
    }
    None
}

/// Decode every recognizable HTML entity in `text` (see
/// [`decode_one_entity`]); anything unrecognized is left as literal text.
fn decode_entities(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'&' {
            if let Some((decoded, consumed)) = decode_one_entity(&text[i..]) {
                out.push(decoded);
                i += consumed;
                continue;
            }
        }
        let ch_len = text[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        out.push_str(&text[i..i + ch_len]);
        i += ch_len;
    }
    out
}

/// Strip HTML tags and decode entities in Teams message bodies and Outlook
/// mail bodies. Adequate for chat/email-length content.
///
/// As of this PR (m365 2.0 — the residual after gsuite G2.2, CodeRabbit
/// finding F1 on #687): `<script>`/`<style>`/`<head>`/`<title>` element
/// content and HTML comments are skipped entirely (see
/// [`remove_comments_and_skipped_elements`]), and entity decoding covers
/// [`NAMED_ENTITIES`] plus full numeric decimal/hex support (see
/// [`decode_entities`]). **Still does not** insert newlines at block-element
/// boundaries — `"<div>Approve</div><div>today</div>"` still collapses to
/// `"Approvetoday"` — see [`gmail_html_to_text`]'s doc for why that
/// particular gap is a deliberate, surviving split rather than something
/// this PR also unifies.
pub(crate) fn html_to_text(html: &str) -> String {
    let cleaned = remove_comments_and_skipped_elements(html);
    let stripped = strip_tags(&cleaned, false);
    decode_entities(&stripped)
}

/// Block/line-boundary-preserving HTML→text conversion: same
/// tag-skipping/stripping + entity-decoding core as [`html_to_text`]
/// (shared via [`remove_comments_and_skipped_elements`]/[`strip_tags`]/
/// [`decode_entities`]), but ALSO inserts a newline for `<br>` and for the
/// close of common block-level elements (`</div>`, `</p>`, `</li>`,
/// `</tr>`, `</h1>`–`</h6>`, `</blockquote>`). Without this,
/// `"<div>Approve</div><div>today</div>"` collapses to `"Approvetoday"` —
/// CodeRabbit round 2 finding #2 on PR #681.
///
/// Originally Gmail-only (hence the name); as of m365 PR 2.0 it also backs
/// [`from_graph_body_boundaried`] — Outlook **mail**'s HTML conversion, via
/// `microsoft::body_to_text_boundaried` — so `trim_outlook_reply_chain`'s
/// line-scan can see Outlook's `<div>`-per-line reply markers on their own
/// line (CodeRabbit finding F2 on #691; see `from_graph_body_boundaried`'s
/// doc for the full reasoning), and Calendar event descriptions
/// (`microsoft::shape_graph_event`, whose agendas are `<div>`/`<p>`-per-
/// line too). Teams messages still go through [`html_to_text`] (no
/// boundaries) — see that function's doc.
///
/// **Deliberately a separate function from [`html_to_text`] for this ONE
/// difference (block-boundary insertion)**, even though both this PR and
/// m365 PR 2.0's Outlook-mail change unified/reused everything else this
/// module does. Teams still has no use for boundary preservation (no
/// reply-chain concept to unblock, per the module doc), so `html_to_text`
/// stays the boundary-less function Teams calls, unchanged.
fn gmail_html_to_text(html: &str) -> String {
    let cleaned = remove_comments_and_skipped_elements(html);
    let stripped = strip_tags(&cleaned, true);
    decode_entities(&stripped)
}

// ---------------------------------------------------------------------------
// Quoted-reply / signature trimming — Gmail and Outlook variants
// ---------------------------------------------------------------------------

/// Shared line-scanning core behind [`trim_quoted_and_signature`] (Gmail)
/// and [`trim_outlook_reply_chain`] (Outlook mail): find the first line
/// matching either the common recognizers (`"On ... wrote:"`, a
/// From:/Sent:/To:/Subject: header block, a `">"`-quoted line, an RFC 3676
/// `"-- "` signature delimiter) or `extra_marker` (the caller's
/// provider-specific additions), and drop everything from there onward —
/// unless that match is the body's very first line, in which case the
/// original text is returned untouched (the newest/only content is never
/// reduced to nothing). See [`trim_quoted_and_signature`]'s doc for the
/// full "conservative by construction" reasoning; both callers share it
/// verbatim.
fn trim_with_markers(text: &str, extra_marker: impl Fn(&[&str], usize) -> bool) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut cut_at: Option<usize> = None;
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        let trimmed_end = trimmed.trim_end();
        if is_wrote_header(trimmed)
            || is_forwarded_header_block_start(&lines, i)
            || trimmed_end == "--"
            || trimmed.starts_with('>')
            || extra_marker(&lines, i)
        {
            cut_at = Some(i);
            break;
        }
    }
    match cut_at {
        None | Some(0) => text.to_string(),
        Some(i) => {
            let kept = lines[..i].join("\n");
            let kept = kept.trim_end();
            if kept.is_empty() {
                text.to_string()
            } else {
                kept.to_string()
            }
        }
    }
}

/// Trim a quoted-reply chain and/or trailing signature block from an
/// already-plain-text email body (run [`html_to_text`] first if the source
/// was HTML). Deliberately a simple, line-oriented heuristic rather than a
/// full quote-parsing grammar: false negatives (an unrecognized quote style
/// slips through untrimmed) are acceptable, false positives (trimming live
/// content) are not.
///
/// Recognizes, anchored at the start of a line (after trimming leading
/// whitespace):
/// - `"On ... wrote:"` — the standard reply-quote header most mail clients
///   (Gmail included) prepend to a quoted chain.
/// - A forwarded/replied plain-text header **block**: a `"From:"` line
///   followed, within the next few lines, by a `"Sent:"`, `"To:"`, or
///   `"Subject:"` line. A bare `"From:"` line alone does NOT trigger this —
///   CodeRabbit round 1 finding #1 on PR #681: a lone `"From:"` check
///   matched legitimate current-message content that happened to start a
///   line with "From:" (e.g. "From: our records, this looks correct."),
///   violating the "never trim live content" rule. Requiring the rest of
///   the block confirms it's actually a quoted header, not body text.
/// - A `">"`-prefixed line — classic inline quoting; the first such line
///   ends the kept body (everything from there down is assumed quoted).
/// - `"-- "` (or `"--"`) alone on a line — the RFC 3676 signature
///   delimiter.
///
/// **Conservative by construction**: everything from the first matching
/// line onward is dropped, but if that match is the body's very first line
/// — i.e. the whole message looks like quoted/signature content with
/// nothing ahead of it — the original text is returned untouched instead.
/// The newest message is never reduced to nothing.
pub(crate) fn trim_quoted_and_signature(text: &str) -> String {
    trim_with_markers(text, |_lines, _i| false)
}

/// Outlook-mail-tuned sibling of [`trim_quoted_and_signature`], added in
/// m365 PR 2.0 to close the plan's original "quoted-reply-chain trimming
/// for mail" spec item (G2.2 shipped the Gmail half only). Recognizes
/// everything [`trim_quoted_and_signature`] does — Outlook reply chains DO
/// use "On ... wrote:" (e.g. when the thread crossed from a non-Outlook
/// client) and the From:/Sent:/To:/Subject: block is itself an Outlook-style
/// shape already — plus two Outlook-specific markers:
/// - [`is_original_message_delimiter`] — the classic Outlook desktop
///   `"-----Original Message-----"`/`"-----Original Appointment-----"`
///   divider line.
/// - [`is_outlook_mobile_signature_marker`] — a small set of exact-line
///   Outlook/generic mobile-client signatures ("Get Outlook for iOS",
///   "Sent from my iPhone", etc).
///
/// Same conservative posture throughout: an exact, whole-trimmed-line match
/// is required for both new markers (never a substring match), and the
/// "never trim the body's very first line to nothing" guarantee is
/// inherited unchanged from [`trim_with_markers`]. Wired into
/// `microsoft::message_to_item` for the Outlook **mail** path only — never
/// Teams messages (no reply-chain concept there), never Calendar.
pub(crate) fn trim_outlook_reply_chain(text: &str) -> String {
    trim_with_markers(text, |lines, i| {
        is_original_message_delimiter(lines[i]) || is_outlook_mobile_signature_marker(lines[i])
    })
}

/// `"On <anything> wrote:"` — the reply-quote header. Loose on purpose (the
/// middle varies by locale/client); anchored on both ends so it doesn't
/// false-positive on an unrelated sentence that merely contains "wrote:".
fn is_wrote_header(line: &str) -> bool {
    line.starts_with("On ") && line.trim_end().ends_with("wrote:")
}

/// Whether `lines[i]` starts a forwarded/replied plain-text header block:
/// a `"From:"` line followed, within the next few lines, by a `"Sent:"`,
/// `"To:"`, or `"Subject:"` line. Requiring a companion line (not just the
/// bare `"From:"` line alone) is what keeps this from false-positiving on
/// current-message content that happens to start a line with "From:" — see
/// [`trim_quoted_and_signature`]'s doc, CodeRabbit round 1 finding #1 on PR
/// #681. The lookahead window is small (4 lines) since a genuine header
/// block's fields are consecutive, not scattered through the message.
fn is_forwarded_header_block_start(lines: &[&str], i: usize) -> bool {
    if !lines[i].trim_start().starts_with("From:") {
        return false;
    }
    lines
        .iter()
        .skip(i + 1)
        .take(4)
        .any(|l| {
            let t = l.trim_start();
            t.starts_with("Sent:") || t.starts_with("To:") || t.starts_with("Subject:")
        })
}

/// Outlook desktop's classic reply/forward divider: a line that, once
/// leading/trailing whitespace and a run of 2+ dashes on each side are
/// trimmed away, reads exactly `"Original Message"` or `"Original
/// Appointment"` (case-insensitive) — `"-----Original Message-----"`,
/// `"----- Original Message -----"`, `"-----Original Appointment-----"`.
/// Requires the dash runs on both ends (not just the phrase appearing
/// somewhere) so an ordinary sentence mentioning "the original message"
/// can never match.
fn is_original_message_delimiter(line: &str) -> bool {
    let t = line.trim();
    if !(t.starts_with("--") && t.ends_with("--")) {
        return false;
    }
    let inner = t.trim_matches(|c: char| c == '-' || c.is_whitespace());
    inner.eq_ignore_ascii_case("Original Message") || inner.eq_ignore_ascii_case("Original Appointment")
}

/// Outlook/generic mobile-client signature lines. Deliberately an
/// exact-whole-trimmed-line match (case-insensitive) against a small fixed
/// set, never a substring match — a message that happens to mention "sent
/// from my iPhone" mid-sentence (e.g. "I sent from my iPhone last time and
/// it worked fine") must not be trimmed, the same false-positive concern
/// [`is_forwarded_header_block_start`]'s doc raises for a bare "From:"
/// line. A real mobile signature is always its own standalone line.
fn is_outlook_mobile_signature_marker(line: &str) -> bool {
    const MARKERS: &[&str] = &[
        "Get Outlook for iOS",
        "Get Outlook for Android",
        "Sent from Outlook for iOS",
        "Sent from Outlook for Android",
        "Sent from my iPhone",
        "Sent from my iPad",
        "Sent from my Android device",
        "Sent from Mail for Windows",
    ];
    let t = line.trim();
    MARKERS.iter().any(|m| t.eq_ignore_ascii_case(m))
}

/// Strip the Microsoft Teams join block Outlook injects into the body of
/// every Teams meeting — the Calendar sibling of
/// [`trim_outlook_reply_chain`], run by `microsoft::shape_graph_event` on
/// the boundaried text of the event body (and of `bodyPreview`, when that
/// is all there is). Human-written agenda text above the block, and
/// anything below it, is kept.
///
/// Microsoft's invite format (`aka.ms/JoinTeamsMeeting`) wraps the block in
/// two long underscore rules, with the heading on the line after the first:
///
/// ```text
/// ________________________________________________________________________________
/// Microsoft Teams  Need help?
/// Join the meeting now
/// Meeting ID: 283 462 719 05
/// Passcode: Hq7pKx3
/// ________________________________
/// Dial in by phone
/// +1 323-555-0100,,28346271905# United States, Los Angeles
/// Find a local number
/// Phone conference ID: 283 462 719#
/// For organizers: Meeting options | Reset dial-in PIN
/// ________________________________________________________________________________
/// ```
///
/// (the 2021–2023 shape reads "Microsoft Teams meeting / Join on your
/// computer, mobile app or room device / Click here to join the meeting /
/// … / Learn More | Meeting options" inside the same rules). The block is
/// recognised by a heading line ([`is_teams_block_heading`]) directly under
/// a [`is_teams_block_rule`] line — blank lines between them allowed,
/// anything else not — and runs through the FIRST rule after the heading
/// (the closing one; the inner "Dial in" rule is 32 underscores and does
/// not count as a rule, see [`is_teams_block_rule`]), or to the end when
/// there is none, which is what a `bodyPreview` truncated mid-block looks
/// like. Ending at the first closing rule rather than the last rule in the
/// text keeps organizer text that sits between the block and a later rule
/// — Outlook's plain-text reply separator is itself a run of underscores,
/// and the first revision of this trimmer dropped everything above it. A
/// heading-like line with no rule above it is skipped, not taken as the
/// block: an agenda line that starts "Microsoft Teams migration…" above the
/// real block is live content, and the block below it is still found. A
/// body with no such pair is left alone. Unlike the reply-chain trimmer
/// this one CAN reduce a body to nothing — a meeting with no agenda has
/// nothing but the block, and the caller then renders no description at
/// all.
pub(crate) fn trim_teams_meeting_block(text: &str) -> String {
    let mut remaining = text.to_string();
    loop {
        let lines: Vec<&str> = remaining.lines().collect();
        let Some((start, heading)) = (0..lines.len())
            .filter(|&i| is_teams_block_heading(lines[i]))
            .find_map(|heading| teams_block_opening_rule(&lines, heading).map(|start| (start, heading)))
        else {
            return remaining;
        };
        let end = (heading + 1..lines.len())
            .find(|&i| is_teams_block_rule(lines[i]))
            .map(|i| i + 1)
            .unwrap_or(lines.len());
        let head = lines[..start].join("\n");
        let tail = lines[end..].join("\n");
        let (head, tail) = (head.trim_end(), tail.trim_start());
        remaining = match (head.is_empty(), tail.is_empty()) {
            (true, true) => String::new(),
            (false, true) => head.to_string(),
            (true, false) => tail.to_string(),
            (false, false) => format!("{head}\n{tail}"),
        };
    }
}

/// The index of the rule directly above `heading` (blank lines between
/// them allowed, anything else not), or `None` when the heading has no rule
/// over it and so is not the top of a Teams block.
fn teams_block_opening_rule(lines: &[&str], heading: usize) -> Option<usize> {
    (0..heading)
        .rev()
        .find(|&i| !lines[i].trim().is_empty())
        .filter(|&i| is_teams_block_rule(lines[i]))
}

/// A line that is nothing but a run of 40+ underscores — the rule Outlook
/// draws above and below the Teams block (80 underscores in both the 2024
/// and the 2021–2023 shape). Long enough that the block's inner dial-in
/// rule (32) and Outlook's plain-text reply separator (also 32) never
/// count, so the first rule after the heading is always the closing one;
/// an agenda's `___` blank-to-fill never does either.
fn is_teams_block_rule(line: &str) -> bool {
    let t = line.trim();
    t.len() >= 40 && t.bytes().all(|b| b == b'_')
}

/// The first line of the block in either of Microsoft's invite shapes —
/// anchored at the start of the trimmed line, never a substring match.
fn is_teams_block_heading(line: &str) -> bool {
    let t = line.trim();
    t.starts_with("Microsoft Teams")
        || t == "Join the meeting now"
        || t == "Click here to join the meeting"
        || t.starts_with("Meeting ID:")
}

// ---------------------------------------------------------------------------
// Microsoft Graph itemBody adapter
// ---------------------------------------------------------------------------

/// Microsoft Graph `itemBody` adapter: `{contentType, content}` → plain
/// text. `contentType` is compared case-insensitively against `"html"`
/// (Graph's other documented value is `"text"`, treated as the pass-through
/// default — same rule the original Teams-only `body_to_text` used).
///
/// Takes the two scalar fields rather than `microsoft::GraphItemBody`
/// directly so this module has no dependency on that connector's private
/// serde type; `microsoft::body_to_text` is a one-line wrapper that passes
/// its fields through.
pub(crate) fn from_graph_body(content_type: Option<&str>, content: &str) -> String {
    let is_html = content_type
        .map(|c| c.eq_ignore_ascii_case("html"))
        .unwrap_or(false);
    if is_html {
        html_to_text(content)
    } else {
        content.to_string()
    }
}

/// Outlook-**mail**-only sibling of [`from_graph_body`] that converts HTML
/// via [`gmail_html_to_text`] (block-boundary-preserving) instead of
/// [`html_to_text`]. Added in m365 PR 2.0 to close CodeRabbit finding F2 on
/// #691: `microsoft::message_to_item` runs [`trim_outlook_reply_chain`] on
/// the converted body to drop quoted-reply chains, but that trim is a
/// line-by-line scan, and Outlook's own reply markers
/// (`"-----Original Message-----"`, a mobile-signature line like `"Sent
/// from my iPhone"`) are always their own `<div>` in Outlook's HTML. Run
/// through the boundary-LESS `html_to_text`, `<div>Approved</div><div>
/// -----Original Message-----</div>` collapses to one line —
/// `"Approved-----Original Message-----"` — so the line scan can never see
/// the delimiter on a line by itself, and the whole quoted tail survives
/// untrimmed. Using the same boundary-preserving conversion Gmail already
/// uses (chosen over a boundary-aware-then-map-back trim: it's the same
/// machinery already proven correct for Gmail, not new machinery) fixes
/// that.
///
/// **This deliberately changes Outlook mail's normalized atom text shape**
/// (adds newlines at paragraph/div/br boundaries where `html_to_text`
/// collapsed them). That is an accepted, deliberate change for this PR —
/// see `microsoft::message_to_item`'s doc for why: the "byte-identical"
/// constraint on this module's move from `microsoft.rs` (G2.2) applies to
/// that move, not to this PR's own new behavior, and better paragraph
/// structure is a retrieval improvement in its own right, independent of
/// the trimming fix it happens to unblock here.
///
/// Callers: `microsoft::message_to_item` (the Outlook mail path) and, since
/// cs-mscal-body-window, `microsoft::shape_graph_event` (the Calendar event
/// body — no reply-chain trim there, just the paragraph structure). Teams
/// keeps calling [`from_graph_body`] (`html_to_text`, no boundaries) via
/// its own `microsoft::body_to_text` — no reply-chain concept applies
/// there, so there is nothing for boundary preservation to unblock, and
/// changing Teams atom shape with no matching benefit isn't worth doing.
pub(crate) fn from_graph_body_boundaried(content_type: Option<&str>, content: &str) -> String {
    let is_html = content_type
        .map(|c| c.eq_ignore_ascii_case("html"))
        .unwrap_or(false);
    if is_html {
        gmail_html_to_text(content)
    } else {
        content.to_string()
    }
}

// ---------------------------------------------------------------------------
// Gmail MIME payload adapter
// ---------------------------------------------------------------------------

/// One node of a Gmail `users.messages.get(format=full)` MIME tree (the
/// top-level `payload`, or any of its nested `parts`). Deserialized
/// directly from Gmail's documented `MessagePart` shape
/// (https://developers.google.com/gmail/api/reference/rest/v1/users.messages#messagepart)
/// — only the fields this module needs.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GmailMessagePart {
    #[serde(rename = "mimeType", default)]
    pub mime_type: String,
    /// Non-empty when this part is an attachment (Gmail sets it from the
    /// MIME `Content-Disposition`/`Content-Type` `filename`/`name`
    /// parameter). Empty for body-text parts. Used by [`is_attachment`] —
    /// see its doc for why this matters (CodeRabbit round 2 finding #1 on
    /// PR #681).
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub headers: Vec<GmailHeader>,
    #[serde(default)]
    pub body: GmailPartBody,
    #[serde(default)]
    pub parts: Vec<GmailMessagePart>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct GmailPartBody {
    /// Base64url-encoded content, present on leaf (non-multipart) parts
    /// whose size is within Gmail's inline threshold. `None` for parts whose
    /// body is only reachable via `attachmentId` (large attachments) —
    /// deliberately out of scope here (email body text only, mirroring
    /// Outlook's `bodyPreview`-vs-attachment split).
    #[serde(default)]
    pub data: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GmailHeader {
    pub name: String,
    pub value: String,
}

/// Look up a header by name, case-insensitively (`Subject`/`From`/`To`/`Cc`
/// — Gmail preserves the sender's original header casing).
pub(crate) fn header_value<'a>(headers: &'a [GmailHeader], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

/// Decode a Gmail MIME part's base64url `body.data`. Gmail's docs specify
/// base64url (RFC 4648 §5, unpadded in observed responses); both unpadded
/// and padded are accepted since the API doesn't guarantee which. `None` on
/// decode failure or invalid UTF-8 — a malformed part is treated as absent,
/// same posture as a missing field.
fn decode_part_data(data: &str) -> Option<String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(data)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(data))
        .ok()?;
    String::from_utf8(bytes).ok()
}

/// Whether this MIME part is an attachment rather than message body text.
/// Gmail signals this two ways: a non-empty `filename` on the part, or a
/// `Content-Disposition: attachment` header — checked in that order, either
/// is sufficient.
///
/// CodeRabbit round 2 finding #1 on PR #681: a `text/plain` (or
/// `text/html`) ATTACHMENT small enough to inline carries `body.data` just
/// like a real body leaf, so mime-type-and-decodability alone isn't a safe
/// body selector — in `multipart/mixed`, an attachment appearing before the
/// real `multipart/alternative` body would otherwise win the walk.
fn is_attachment(part: &GmailMessagePart) -> bool {
    if !part.filename.trim().is_empty() {
        return true;
    }
    header_value(&part.headers, "Content-Disposition")
        .map(|cd| cd.trim_start().to_ascii_lowercase().starts_with("attachment"))
        .unwrap_or(false)
}

/// Depth-first search for the first part whose `mimeType` matches `wanted`
/// (case-insensitive, exact — e.g. `"text/plain"`), is NOT an attachment
/// (see [`is_attachment`]), AND whose `body.data` decodes successfully
/// (valid base64url/UTF-8). Recurses into every part's `parts` (covers
/// `multipart/mixed` wrapping `multipart/alternative`, and deeper nesting)
/// regardless of the container's own `mimeType`, since Gmail's tree shape
/// for a given message is not predictable beyond "somewhere in here".
///
/// Deliberately keeps searching past a structurally-matching part whose
/// data fails to decode, instead of stopping there — CodeRabbit round 1
/// finding #2 on PR #681: an earlier version picked the first
/// mime-type-and-has-data match and decoded only that one, so one malformed
/// leaf hid an otherwise-good later leaf of the same mime type (rare in
/// practice, but the point of walking the whole tree is to not assume the
/// shape).
///
/// **An attachment part is skipped ENTIRELY — itself and every part
/// nested under it — via an early return before either is inspected.**
/// CodeRabbit round 3 finding on PR #681, a follow-on to the round-2
/// attachment fix: that fix only excluded an attachment's OWN `body.data`
/// from matching, but still recursed into `part.parts` regardless. A
/// forwarded email attached as `message/rfc822` (or any attachment marked
/// `multipart/*`) can itself contain `text/plain`/`text/html` children —
/// those must not be reachable as the OUTER message's body just because
/// the walk can structurally reach them. Checking `is_attachment` first
/// and returning `None` for the whole subtree closes that.
fn find_decodable_text(part: &GmailMessagePart, wanted: &str) -> Option<String> {
    if is_attachment(part) {
        return None;
    }
    if part.mime_type.eq_ignore_ascii_case(wanted) {
        if let Some(data) = part.body.data.as_deref() {
            if let Some(text) = decode_part_data(data) {
                return Some(text);
            }
        }
    }
    for child in &part.parts {
        if let Some(found) = find_decodable_text(child, wanted) {
            return Some(found);
        }
    }
    None
}

/// Reduce a Gmail message's `payload` MIME tree to plain text: `text/plain`
/// preferred, `text/html` (→ [`gmail_html_to_text`]) as fallback, walking
/// `multipart/*` nesting to any depth, attachments excluded (see
/// [`is_attachment`]). `None` when neither is present or decodable —
/// callers treat that as a missing body, mirroring [`from_graph_body`]'s
/// "no content" case (Gmail's tree shape just means "missing" can only be
/// discovered after a walk, not a single field check).
pub(crate) fn from_gmail_payload(payload: &GmailMessagePart) -> Option<String> {
    if let Some(text) = find_decodable_text(payload, "text/plain") {
        return Some(text);
    }
    if let Some(html) = find_decodable_text(payload, "text/html") {
        return Some(gmail_html_to_text(&html));
    }
    None
}

// ---------------------------------------------------------------------------
// Gmail message → NormalizedItem shaping
// ---------------------------------------------------------------------------

/// A Gmail `users.messages.get(format=full)` response — only the fields
/// this module's shaping needs plus `labelIds` (`snippet`, `sizeEstimate`,
/// etc. are out of scope for G2.2; `connectors::google_gmail`, if it needs
/// them, deserializes its own superset).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GmailMessage {
    #[serde(default)]
    pub id: String,
    #[serde(rename = "threadId", default)]
    pub thread_id: String,
    /// Epoch milliseconds as a decimal STRING — Gmail's documented shape for
    /// this field (not a JSON number).
    #[serde(rename = "internalDate", default)]
    pub internal_date: Option<String>,
    /// The message's CURRENT system/user label ids. Read by
    /// `connectors::google_gmail` to keep `TRASH`/`SPAM` messages out of the
    /// ocean (a `threads.get` expansion returns trashed siblings too, and a
    /// `history.list` `messagesAdded` can name a message that landed straight
    /// in Spam); this module's shaping itself never consults it.
    #[serde(rename = "labelIds", default)]
    pub label_ids: Vec<String>,
    #[serde(default)]
    pub payload: Option<GmailMessagePart>,
}

/// Split an RFC 5322 address-list header value (`From`/`To`/`Cc`) on
/// top-level commas — i.e. commas NOT inside a double-quoted display name,
/// so `"Doe, Jane" <jane@x.com>, john@x.com` splits into two entries, not
/// three.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut in_quotes = false;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                parts.push(&s[start..i]);
                start = i + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Parse an RFC 5322 `From`/`To`/`Cc` header value into one participant
/// label per address, preferring the display name over the bare address
/// (mirrors `microsoft::recipient_label`'s convention). A small,
/// deliberately non-exhaustive parser — real-world header quirks (encoded
/// words, nested comments) are left as the raw text rather than rejected.
fn parse_participants(header: &str) -> Vec<String> {
    split_top_level_commas(header)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            if let (Some(open), Some(close)) = (entry.find('<'), entry.rfind('>')) {
                if open < close {
                    let name = entry[..open].trim().trim_matches('"').trim();
                    let addr = entry[open + 1..close].trim();
                    let label = if !name.is_empty() { name } else { addr };
                    return (!label.is_empty()).then(|| label.to_string());
                }
            }
            let bare = entry.trim_matches('"').trim();
            (!bare.is_empty()).then(|| bare.to_string())
        })
        .collect()
}

/// Shape one Gmail message into a [`NormalizedItem`]. `source_type =
/// "gmail"` — `packages/core/src/chunker.rs`'s
/// `ChunkStrategy::for_source_type` already maps `"gmail"` to the email
/// chunker, so no core change is needed. `source_id` is the Gmail message
/// id; `thread_id` is `threadId`; `timestamp` comes from `internalDate`.
///
/// Returns `None` for the two poison-pill shapes `ingest_atom` would 422 on
/// anyway — empty/missing id, and empty body after normalisation +
/// quoted-chain trimming — filtered here, at the one funnel every ingest
/// path (backfill and live push) shares, mirroring
/// `google_calendar::process_events_page`'s empty-id lesson (design doc
/// §1.2/§1.8: filter at the funnel, not just at the `ingest_atom` boundary).
/// The entry point for the whole Gmail chain above — called by
/// `connectors::google_gmail` (Train G2 PR 3, merged as #686).
pub(crate) fn gmail_message_to_item(m: &GmailMessage) -> Option<NormalizedItem> {
    let id = m.id.trim();
    if id.is_empty() {
        tracing::warn!("gmail: message with missing/empty id skipped (malformed upstream data)");
        return None;
    }
    let payload = m.payload.as_ref()?;
    let raw_body = from_gmail_payload(payload).unwrap_or_default();
    let body = trim_quoted_and_signature(&raw_body);
    let body = body.trim();
    if body.is_empty() {
        tracing::warn!(source_id = %id, "gmail: message with empty body skipped");
        return None;
    }

    // Missing/unparseable `internalDate` is treated as malformed upstream
    // data and skipped — same funnel-filter posture as the empty-id/empty-
    // body checks above, and consistent with `google_calendar.rs::
    // shape_calendar_item`'s own `parse_g_time(..)?` (an event with no
    // parseable start time is dropped, not stamped with `Utc::now()`).
    // CodeRabbit round 1 finding #3 on PR #681: the previous version fell
    // back to `Utc::now()`, which is non-deterministic across replays and
    // would silently misorder historical mail relative to its real send
    // time — worse than skipping the one malformed message.
    let internal_date_millis = m.internal_date.as_deref().and_then(|s| s.parse::<i64>().ok());
    let timestamp = match internal_date_millis.and_then(DateTime::from_timestamp_millis) {
        Some(ts) => ts,
        None => {
            tracing::warn!(
                source_id = %id,
                internal_date = ?m.internal_date,
                "gmail: message with missing/invalid internalDate skipped (malformed upstream data)"
            );
            return None;
        }
    };

    let subject = header_value(&payload.headers, "Subject").unwrap_or("").trim();
    let content = if subject.is_empty() {
        body.to_string()
    } else {
        format!("{subject}\n\n{body}")
    };

    let from_header = header_value(&payload.headers, "From");
    let mut participants = Vec::new();
    if let Some(from) = from_header {
        participants.extend(parse_participants(from));
    }
    if let Some(to) = header_value(&payload.headers, "To") {
        participants.extend(parse_participants(to));
    }
    if let Some(cc) = header_value(&payload.headers, "Cc") {
        participants.extend(parse_participants(cc));
    }
    let author_name = from_header.and_then(|f| parse_participants(f).into_iter().next());

    let thread_id = (!m.thread_id.trim().is_empty()).then(|| m.thread_id.clone());

    Some(NormalizedItem {
        source_type: "gmail".into(),
        source_id: id.to_string(),
        content,
        participants,
        timestamp,
        scope: None,
        thread_id,
        channel_id: None,
        author_id: None,
        author_name,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- html_to_text ---------------------------------------------------

    // m365 PR 2.0: &mdash; is now decoded — this test used to pin the
    // opposite (see git history / #687's original assertion). CodeRabbit
    // finding F1 on #687.
    #[test]
    fn html_to_text_strips_tags_and_decodes_entities() {
        let out = html_to_text("<p>Ship it &amp; <b>celebrate</b> &mdash;ish</p>");
        assert_eq!(out, "Ship it & celebrate \u{2014}ish");
    }

    #[test]
    fn html_to_text_decodes_nbsp_lt_gt_quot_apos() {
        let out = html_to_text("a&nbsp;b &lt;tag&gt; &quot;q&quot; &#39;a&#39;");
        assert_eq!(out, "a b <tag> \"q\" 'a'");
    }

    #[test]
    fn html_to_text_plain_input_passes_through() {
        assert_eq!(html_to_text("no tags here"), "no tags here");
    }

    // -- html_to_text: script/style/head/title/comment skipping (m365 2.0,
    // CodeRabbit finding F1 on #687) -------------------------------------

    #[test]
    fn html_to_text_drops_script_element_and_its_text_content() {
        let out = html_to_text("<p>before</p><script>alert('x');</script><p>after</p>");
        assert_eq!(out, "beforeafter");
    }

    #[test]
    fn html_to_text_drops_style_element_and_its_text_content() {
        let out = html_to_text("<style>.warn{color:red}</style><p>content</p>");
        assert_eq!(out, "content");
    }

    #[test]
    fn html_to_text_drops_head_and_title() {
        let out = html_to_text("<head><title>My Doc</title><meta charset=\"utf-8\"></head><body>Hi</body>");
        assert_eq!(out, "Hi");
    }

    #[test]
    fn html_to_text_drops_html_comments() {
        let out = html_to_text("Before<!-- this is a comment with <p>fake tags</p> --> after");
        assert_eq!(out, "Before after");
    }

    #[test]
    fn html_to_text_drops_unclosed_comment_to_end_of_input() {
        let out = html_to_text("Kept text<!-- never closed, rest is dropped");
        assert_eq!(out, "Kept text");
    }

    #[test]
    fn html_to_text_script_tag_is_case_insensitive_and_tolerates_attributes() {
        let out = html_to_text("<SCRIPT type=\"text/javascript\">var x = 1;</SCRIPT>kept");
        assert_eq!(out, "kept");
    }

    #[test]
    fn html_to_text_style_tag_mixed_case_variant() {
        let out = html_to_text("<StYlE>body{color:blue}</StYlE>kept");
        assert_eq!(out, "kept");
    }

    #[test]
    fn html_to_text_drops_unclosed_script_to_end_of_input() {
        let out = html_to_text("Kept<script>var x = '<not a real close tag>';");
        assert_eq!(out, "Kept");
    }

    #[test]
    fn html_to_text_nested_style_like_content_inside_script_is_all_dropped() {
        // A <style>-looking string INSIDE a <script> block must not confuse
        // the scan into closing early — the whole script element, verbatim
        // text included, is dropped up to its own </script>.
        let out = html_to_text("<script>var s = \"<style>fake</style>\";</script>kept");
        assert_eq!(out, "kept");
    }

    #[test]
    fn html_to_text_does_not_match_element_name_prefix() {
        // A hypothetical <styles> element (not a real HTML tag, but this
        // guards the boundary check) must not be treated as <style> — its
        // tags are stripped like any ordinary tag, but its text content
        // survives (it is NOT a skipped element).
        let out = html_to_text("<styles>not skipped</styles>");
        assert_eq!(out, "not skipped");
    }

    // CodeRabbit round 1 finding on #691: the closing-tag scan used to
    // accept any occurrence of the `</script` PREFIX, so `</scripture>`
    // (which starts with that same prefix) was mistaken for `<script>`'s
    // real closing tag — leaking everything between the false match and the
    // real `</script>` into the output.
    #[test]
    fn html_to_text_closing_tag_match_requires_tag_boundary_not_just_prefix() {
        let out = html_to_text("<script>ignored</scripture>leaked()</script>kept");
        assert_eq!(out, "kept");
    }

    #[test]
    fn html_to_text_closing_tag_tolerates_space_before_angle_bracket() {
        let out = html_to_text("<script>ignored</script >kept");
        assert_eq!(out, "kept");
    }

    #[test]
    fn html_to_text_tag_close_ignores_gt_inside_quoted_attribute() {
        // A `>` inside a quoted attribute value must not end the <script>
        // start tag early (which would otherwise leave its real content
        // unprotected).
        let out = html_to_text("<script data-x=\"a>b\">alert(1);</script>kept");
        assert_eq!(out, "kept");
    }

    // -- html_to_text: entity decoding (m365 2.0) --------------------------

    #[test]
    fn html_to_text_decodes_curated_named_entities() {
        let out = html_to_text(
            "&mdash;&ndash;&hellip;&rsquo;&lsquo;&rdquo;&ldquo;&bull;&middot;&copy;&trade;&sect;&para;&plusmn;&times;&divide;&laquo;&raquo;&eacute;",
        );
        assert_eq!(
            out,
            "\u{2014}\u{2013}\u{2026}\u{2019}\u{2018}\u{201D}\u{201C}\u{2022}\u{00B7}\u{00A9}\u{2122}\u{00A7}\u{00B6}\u{00B1}\u{00D7}\u{00F7}\u{00AB}\u{00BB}\u{00E9}"
        );
    }

    #[test]
    fn html_to_text_decodes_decimal_numeric_entities() {
        assert_eq!(html_to_text("&#8212;&#39;&#65;"), "\u{2014}'A");
    }

    #[test]
    fn html_to_text_decodes_hex_numeric_entities_both_case_variants() {
        assert_eq!(html_to_text("&#x2014;&#X2014;&#x41;"), "\u{2014}\u{2014}A");
    }

    #[test]
    fn html_to_text_named_entity_matching_is_case_insensitive() {
        assert_eq!(html_to_text("&MDASH;&Amp;"), "\u{2014}&");
    }

    #[test]
    fn html_to_text_invalid_numeric_entity_left_undecoded() {
        // Not valid digits for the declared radix — left as literal text.
        assert_eq!(html_to_text("&#zzzz;"), "&#zzzz;");
    }

    #[test]
    fn html_to_text_out_of_range_numeric_entity_left_undecoded() {
        // A surrogate codepoint is not a valid scalar value — char::from_u32
        // returns None, and this must not panic; the entity is left as-is.
        assert_eq!(html_to_text("&#xD800;"), "&#xD800;");
        // Wildly out of Unicode range.
        assert_eq!(html_to_text("&#99999999;"), "&#99999999;");
    }

    #[test]
    fn html_to_text_unknown_named_entity_left_undecoded() {
        assert_eq!(html_to_text("&notarealentity;"), "&notarealentity;");
    }

    #[test]
    fn html_to_text_unterminated_ampersand_left_undecoded() {
        assert_eq!(html_to_text("Q&A time"), "Q&A time");
        assert_eq!(html_to_text("Tom & Jerry"), "Tom & Jerry");
    }

    // Regression for the exact fixture shape (teams_message_html_script_
    // style_entities.json / outlook_message_html_script_style_entities.json)
    // — see `connectors::microsoft`'s flipped fixture tests for the
    // end-to-end version through `message_to_item`/`teams_message_to_item`.
    #[test]
    fn html_to_text_fixture_shape_script_style_and_mdash() {
        let out = html_to_text(
            "<style>.warn{color:red}</style><script>alert('x');</script><p>Deploy at 5&nbsp;pm &amp; watch dashboards &mdash; don&#39;t forget the &lt;prod&gt; flag.</p>",
        );
        assert_eq!(out, "Deploy at 5 pm & watch dashboards \u{2014} don't forget the <prod> flag.");
    }

    // -- trim_quoted_and_signature ---------------------------------------

    #[test]
    fn trims_on_wrote_header_and_everything_after() {
        let text = "Sounds good, let's do Tuesday.\n\nOn Mon, Jan 1, 2024 at 10:00 AM Jane Doe <jane@example.com> wrote:\n> When works for you?\n> - Jane";
        let out = trim_quoted_and_signature(text);
        assert_eq!(out, "Sounds good, let's do Tuesday.");
    }

    #[test]
    fn trims_from_header_block() {
        let text = "Approved, thanks.\n\nFrom: John Smith <john@example.com>\nSent: Monday, January 1, 2024 9:00 AM\nTo: Jane Doe <jane@example.com>\nSubject: Budget\n\nCan you approve this?";
        let out = trim_quoted_and_signature(text);
        assert_eq!(out, "Approved, thanks.");
    }

    // CodeRabbit round 1 finding #1 on PR #681: a lone "From:" line (no
    // companion Sent:/To:/Subject: line following it) is live message
    // content, not a quoted header block, and must survive untouched.
    #[test]
    fn does_not_trim_lone_from_line_without_header_block() {
        let text = "From: our records, this looks correct. Let's proceed.\n\nThanks.";
        assert_eq!(trim_quoted_and_signature(text), text);
    }

    #[test]
    fn does_not_trim_from_line_followed_by_unrelated_lines() {
        let text = "From: accounting's perspective this is fine.\nWe can move forward.\nLet me know if you have questions.\nOne more line for good measure.";
        assert_eq!(trim_quoted_and_signature(text), text);
    }

    #[test]
    fn trims_forwarded_header_block_when_subject_is_a_couple_lines_after_from() {
        // The companion line doesn't have to be immediately next — only
        // within the lookahead window — since real Outlook-style blocks
        // sometimes carry a blank line or an extra header in between.
        let text = "Approved.\n\nFrom: John Smith <john@example.com>\n\nSubject: Budget\nBody text of the forwarded message.";
        let out = trim_quoted_and_signature(text);
        assert_eq!(out, "Approved.");
    }

    #[test]
    fn trims_angle_bracket_quote_run() {
        let text = "New content here.\n> old quoted line one\n> old quoted line two";
        let out = trim_quoted_and_signature(text);
        assert_eq!(out, "New content here.");
    }

    #[test]
    fn trims_signature_delimiter() {
        let text = "See you at the meeting.\n\n-- \nJane Doe\nSenior Engineer";
        let out = trim_quoted_and_signature(text);
        assert_eq!(out, "See you at the meeting.");
    }

    #[test]
    fn trims_signature_delimiter_without_trailing_space() {
        let text = "See you at the meeting.\n\n--\nJane Doe";
        let out = trim_quoted_and_signature(text);
        assert_eq!(out, "See you at the meeting.");
    }

    #[test]
    fn never_trims_to_empty_when_whole_body_looks_quoted() {
        let text = "> entirely quoted\n> nothing new here";
        let out = trim_quoted_and_signature(text);
        assert_eq!(out, text, "must keep the newest (only) content intact");
    }

    #[test]
    fn leaves_body_with_no_markers_untouched() {
        let text = "Just a plain reply with no quoting or signature.";
        assert_eq!(trim_quoted_and_signature(text), text);
    }

    #[test]
    fn does_not_false_positive_on_unrelated_wrote_sentence() {
        // "wrote:" appears but the line doesn't start with "On " — must not trim.
        let text = "She wrote: this is fine.\n\nNo further context needed.";
        assert_eq!(trim_quoted_and_signature(text), text);
    }

    // -- trim_outlook_reply_chain (m365 PR 2.0) ---------------------------

    #[test]
    fn outlook_trims_original_message_delimiter() {
        let text = "Approved, go ahead.\n\n-----Original Message-----\nFrom: John Smith <john@example.com>\nSent: Monday, January 1, 2024 9:00 AM\nSubject: Deploy window\n\nCan we deploy at 5pm?";
        let out = trim_outlook_reply_chain(text);
        assert_eq!(out, "Approved, go ahead.");
    }

    #[test]
    fn outlook_trims_original_message_delimiter_with_spaces_and_short_dash_run() {
        let text = "Sounds good.\n\n----- Original Message -----\nOld content here.";
        let out = trim_outlook_reply_chain(text);
        assert_eq!(out, "Sounds good.");
    }

    #[test]
    fn outlook_trims_original_appointment_delimiter() {
        let text = "I'll be there.\n\n-----Original Appointment-----\nFrom: Calendar <calendar@example.com>\nWhen: Monday";
        let out = trim_outlook_reply_chain(text);
        assert_eq!(out, "I'll be there.");
    }

    #[test]
    fn outlook_does_not_trim_a_sentence_merely_mentioning_original_message() {
        // No dash-run delimiter — just prose that happens to contain the
        // phrase. Must survive untouched (same false-positive discipline as
        // the bare "From:" line check).
        let text = "Please refer to the original message for context.\n\nThanks.";
        assert_eq!(trim_outlook_reply_chain(text), text);
    }

    #[test]
    fn outlook_trims_mobile_signature_get_outlook_for_ios() {
        let text = "On my way, see you soon.\n\nGet Outlook for iOS";
        let out = trim_outlook_reply_chain(text);
        assert_eq!(out, "On my way, see you soon.");
    }

    #[test]
    fn outlook_trims_mobile_signature_sent_from_my_iphone() {
        let text = "Sounds good to me.\n\nSent from my iPhone";
        let out = trim_outlook_reply_chain(text);
        assert_eq!(out, "Sounds good to me.");
    }

    #[test]
    fn outlook_mobile_signature_marker_is_case_insensitive() {
        let text = "Approved.\n\nSENT FROM MY IPHONE";
        let out = trim_outlook_reply_chain(text);
        assert_eq!(out, "Approved.");
    }

    #[test]
    fn outlook_does_not_trim_sentence_merely_mentioning_sent_from_my_iphone() {
        // Exact-line match only — a sentence that happens to include the
        // phrase mid-line must not trigger the marker.
        let text = "I sent from my iPhone last time and it worked fine.\nLet's do that again.";
        assert_eq!(trim_outlook_reply_chain(text), text);
    }

    #[test]
    fn outlook_still_trims_wrote_header_and_from_block_like_gmail() {
        // Outlook mail crossing from/to a non-Outlook client can carry
        // Gmail-style markers too — the Outlook variant must still catch
        // them (it extends, not replaces, the shared recognizers).
        let wrote = "Sounds good.\n\nOn Mon, Jan 1, 2024 at 9:00 AM John <john@example.com> wrote:\n> original message";
        assert_eq!(trim_outlook_reply_chain(wrote), "Sounds good.");

        let block = "Approved, thanks.\n\nFrom: John Smith <john@example.com>\nSent: Monday, January 1, 2024 9:00 AM\nTo: Jane Doe <jane@example.com>\nSubject: Budget\n\nCan you approve this?";
        assert_eq!(trim_outlook_reply_chain(block), "Approved, thanks.");
    }

    #[test]
    fn outlook_does_not_trim_lone_from_line_without_header_block() {
        let text = "From: our records, this looks correct. Let's proceed.\n\nThanks.";
        assert_eq!(trim_outlook_reply_chain(text), text);
    }

    #[test]
    fn outlook_never_trims_to_empty_when_whole_body_is_the_delimiter() {
        let text = "-----Original Message-----\nFrom: a@example.com\nSent: today";
        let out = trim_outlook_reply_chain(text);
        assert_eq!(out, text, "must keep the newest (only) content intact");
    }

    // cs-mscal-body-window: the Teams join block in Calendar event bodies.
    const RULE: &str = "________________________________________________________________________________";

    fn teams_block_2024() -> String {
        format!(
            "{RULE}\nMicrosoft Teams Need help?\nJoin the meeting now\nMeeting ID: 283 462 719 05\nPasscode: Hq7pKx3\n\
             ________________________________\nDial in by phone\n+1 323-555-0100,,28346271905# United States, Los Angeles\n\
             Find a local number\nPhone conference ID: 283 462 719#\n\nFor organizers: Meeting options | Reset dial-in PIN\n\n\n\n{RULE}"
        )
    }

    #[test]
    fn teams_block_is_stripped_and_the_agenda_above_it_kept() {
        let text = format!("Agenda\nQ3 pipeline\n\n{}\n", teams_block_2024());
        assert_eq!(trim_teams_meeting_block(&text), "Agenda\nQ3 pipeline");
        // No agenda: nothing is left, by design (the caller renders no
        // description line).
        assert_eq!(trim_teams_meeting_block(&teams_block_2024()), "");
        // Text after the closing rule survives too.
        let text = format!("Agenda\n{}\nBring the deck.", teams_block_2024());
        assert_eq!(trim_teams_meeting_block(&text), "Agenda\nBring the deck.");
    }

    #[test]
    fn teams_block_older_shape_is_stripped() {
        let text = format!(
            "Standup notes in the channel.\n\n{RULE}\nMicrosoft Teams meeting\nJoin on your computer, mobile app or room device\n\
             Click here to join the meeting\nMeeting ID: 258 357 102 84\nPasscode: QsPjgs\nDownload Teams | Join on the web\n\
             Or call in (audio only)\n+1 323-555-0199,,258357102#   United States, Los Angeles\nPhone Conference ID: 258 357 102#\n\
             Find a local number | Reset PIN\nLearn More | Meeting options\n{RULE}\n"
        );
        assert_eq!(trim_teams_meeting_block(&text), "Standup notes in the channel.");
    }

    #[test]
    fn teams_block_truncated_preview_is_stripped_to_the_end() {
        // `bodyPreview` is ~255 chars: the block opens and never closes.
        let text = format!("Agenda\r\n\r\n{RULE}\r\nMicrosoft Teams Need help?\r\nJoin the meeting now\r\nMeeting ID: 283 4");
        assert_eq!(trim_teams_meeting_block(&text), "Agenda");
    }

    /// The block ends at its own closing rule, not at the last run of
    /// underscores in the body: organizer text between the block and a
    /// later rule (Outlook's plain-text reply separator is 32 underscores)
    /// is kept, and so is everything under that later rule.
    #[test]
    fn teams_block_ends_at_its_closing_rule_not_a_later_reply_separator() {
        let text = format!(
            "Agenda\n{}\nNotes from last time\n________________________________\nFrom: Bob\nOlder thread below",
            teams_block_2024()
        );
        assert_eq!(
            trim_teams_meeting_block(&text),
            "Agenda\nNotes from last time\n________________________________\nFrom: Bob\nOlder thread below"
        );
    }

    /// An agenda line that starts like the block's heading ("Microsoft
    /// Teams …") but has no rule above it is live content; the real block
    /// further down is still found and stripped.
    #[test]
    fn teams_block_is_found_below_an_agenda_line_that_looks_like_its_heading() {
        let text = format!("Microsoft Teams migration kickoff\nAgenda: budget\n\n{}\n", teams_block_2024());
        assert_eq!(trim_teams_meeting_block(&text), "Microsoft Teams migration kickoff\nAgenda: budget");
    }

    #[test]
    fn every_teams_block_in_a_body_is_stripped() {
        let older = format!(
            "{RULE}\nMicrosoft Teams meeting\nJoin on your computer, mobile app or room device\n\
             Click here to join the meeting\nMeeting ID: 258 357 102 84\nPasscode: QsPjgs\n{RULE}"
        );
        let text = format!(
            "Agenda\n{}\nBreakout room below\n\n{older}\n\nBring the deck.\n{}\n",
            teams_block_2024(),
            teams_block_2024()
        );
        assert_eq!(trim_teams_meeting_block(&text), "Agenda\nBreakout room below\nBring the deck.");
        // Two blocks and nothing else leave nothing.
        assert_eq!(trim_teams_meeting_block(&format!("{}\n{older}", teams_block_2024())), "");
        // Back to back, sharing no blank line, and the second one truncated
        // (a `bodyPreview` cut mid-block).
        let text = format!("Agenda\n{}\n{RULE}\nMicrosoft Teams Need help?\nJoin the meeting now\nMeeting ID: 28", teams_block_2024());
        assert_eq!(trim_teams_meeting_block(&text), "Agenda");
        // A heading-like agenda line between two blocks is live content.
        let text = format!("{}\nMicrosoft Teams rollout: phase 2\n{older}", teams_block_2024());
        assert_eq!(trim_teams_meeting_block(&text), "Microsoft Teams rollout: phase 2");
    }

    #[test]
    fn teams_block_is_not_recognised_without_its_rule() {
        let agenda = "Meeting ID: see the ticket.\nWe will discuss the Microsoft Teams rollout.\nJoin the meeting now if you can.";
        assert_eq!(trim_teams_meeting_block(agenda), agenda);
        // An agenda's own rule with no heading under it is not the block;
        // a rule more than a blank line away from the heading is not either.
        let ruled = format!("Part one\n{RULE}\nPart two\nMeeting ID: in the invite");
        assert_eq!(trim_teams_meeting_block(&ruled), ruled);
        let short = "Name: ____________\nMicrosoft Teams rollout notes";
        assert_eq!(trim_teams_meeting_block(short), short);
    }

    #[test]
    fn outlook_leaves_body_with_no_markers_untouched() {
        let text = "Just a plain Outlook reply with no quoting or signature.";
        assert_eq!(trim_outlook_reply_chain(text), text);
    }

    // -- from_graph_body (Outlook/Teams byte-identical guarantee) --------

    #[test]
    fn from_graph_body_html_strips_tags() {
        let out = from_graph_body(Some("html"), "<p>Ship it &amp; <b>celebrate</b></p>");
        assert_eq!(out, "Ship it & celebrate");
    }

    #[test]
    fn from_graph_body_html_case_insensitive() {
        let out = from_graph_body(Some("HTML"), "<b>hi</b>");
        assert_eq!(out, "hi");
    }

    #[test]
    fn from_graph_body_text_passes_through_unchanged() {
        let out = from_graph_body(Some("text"), "plain body, <not a tag>");
        assert_eq!(out, "plain body, <not a tag>");
    }

    #[test]
    fn from_graph_body_missing_content_type_defaults_to_passthrough() {
        let out = from_graph_body(None, "raw content");
        assert_eq!(out, "raw content");
    }

    // -- Gmail MIME walk ---------------------------------------------------

    fn b64(s: &str) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes())
    }

    fn leaf(mime_type: &str, data: &str) -> GmailMessagePart {
        GmailMessagePart {
            mime_type: mime_type.into(),
            filename: String::new(),
            headers: Vec::new(),
            body: GmailPartBody { data: Some(b64(data)) },
            parts: Vec::new(),
        }
    }

    /// A `leaf` that carries a `filename` — i.e. an attachment, not body
    /// text — so `is_attachment` excludes it. Used by the CodeRabbit round
    /// 2 finding #1 regression below.
    fn attachment_leaf(mime_type: &str, filename: &str, data: &str) -> GmailMessagePart {
        GmailMessagePart {
            mime_type: mime_type.into(),
            filename: filename.into(),
            headers: Vec::new(),
            body: GmailPartBody { data: Some(b64(data)) },
            parts: Vec::new(),
        }
    }

    fn container(mime_type: &str, headers: Vec<GmailHeader>, parts: Vec<GmailMessagePart>) -> GmailMessagePart {
        GmailMessagePart {
            mime_type: mime_type.into(),
            filename: String::new(),
            headers,
            body: GmailPartBody::default(),
            parts,
        }
    }

    #[test]
    fn gmail_payload_prefers_text_plain_over_html() {
        let payload = container(
            "multipart/alternative",
            Vec::new(),
            vec![
                leaf("text/plain", "plain body"),
                leaf("text/html", "<p>html body</p>"),
            ],
        );
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("plain body"));
    }

    #[test]
    fn gmail_payload_falls_back_to_html_when_no_plain_part() {
        let payload = container(
            "multipart/alternative",
            Vec::new(),
            vec![leaf("text/html", "<p>only html &amp; body</p>")],
        );
        // gmail_html_to_text inserts a newline at </p> — see the
        // boundary-preservation tests below (CodeRabbit round 2 finding #2).
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("only html & body\n"));
    }

    #[test]
    fn gmail_payload_walks_nested_multipart() {
        // multipart/mixed(attachment, multipart/alternative(plain, html))
        let inner = container(
            "multipart/alternative",
            Vec::new(),
            vec![leaf("text/plain", "nested plain body"), leaf("text/html", "<p>nested html</p>")],
        );
        let attachment = GmailMessagePart {
            mime_type: "application/pdf".into(),
            filename: "notes.pdf".into(),
            headers: Vec::new(),
            body: GmailPartBody { data: None },
            parts: Vec::new(),
        };
        let payload = container("multipart/mixed", Vec::new(), vec![attachment, inner]);
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("nested plain body"));
    }

    #[test]
    fn gmail_payload_single_part_message_not_multipart() {
        let payload = leaf("text/plain", "single part body");
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("single part body"));
    }

    #[test]
    fn gmail_payload_missing_body_returns_none() {
        let payload = container("multipart/mixed", Vec::new(), Vec::new());
        assert_eq!(from_gmail_payload(&payload), None);
    }

    #[test]
    fn gmail_payload_accepts_padded_base64() {
        use base64::Engine as _;
        let data = base64::engine::general_purpose::URL_SAFE.encode(b"padded body");
        let payload = GmailMessagePart {
            mime_type: "text/plain".into(),
            filename: String::new(),
            headers: Vec::new(),
            body: GmailPartBody { data: Some(data) },
            parts: Vec::new(),
        };
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("padded body"));
    }

    // CodeRabbit round 1 finding #2 on PR #681: the walk must not give up
    // after the FIRST structurally-matching (mime-type-and-has-data) part —
    // if that leaf's data is undecodable, a later, valid leaf of the same
    // mime type must still be found rather than falling through to HTML.
    #[test]
    fn gmail_payload_skips_undecodable_leaf_and_finds_later_valid_one() {
        let bad_leaf = GmailMessagePart {
            mime_type: "text/plain".into(),
            filename: String::new(),
            headers: Vec::new(),
            // Not valid base64url (nor valid padded base64) — must fail to decode.
            body: GmailPartBody { data: Some("!!!not-base64!!!".into()) },
            parts: Vec::new(),
        };
        let good_leaf = leaf("text/plain", "the real body");
        let payload = container("multipart/mixed", Vec::new(), vec![bad_leaf, good_leaf]);
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("the real body"));
    }

    #[test]
    fn gmail_payload_falls_back_to_html_when_only_plain_leaf_is_undecodable() {
        let bad_leaf = GmailMessagePart {
            mime_type: "text/plain".into(),
            filename: String::new(),
            headers: Vec::new(),
            body: GmailPartBody { data: Some("!!!not-base64!!!".into()) },
            parts: Vec::new(),
        };
        let html_leaf = leaf("text/html", "<p>fallback html</p>");
        let payload = container("multipart/alternative", Vec::new(), vec![bad_leaf, html_leaf]);
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("fallback html\n"));
    }

    // -- Attachment exclusion (CodeRabbit round 2 finding #1 on PR #681) ---

    #[test]
    fn is_attachment_true_for_nonempty_filename() {
        let part = attachment_leaf("text/plain", "notes.txt", "attachment content");
        assert!(is_attachment(&part));
    }

    #[test]
    fn is_attachment_true_for_content_disposition_header() {
        let part = GmailMessagePart {
            mime_type: "text/plain".into(),
            filename: String::new(),
            headers: headers(&[("Content-Disposition", "attachment; filename=\"notes.txt\"")]),
            body: GmailPartBody { data: Some(b64("attachment content")) },
            parts: Vec::new(),
        };
        assert!(is_attachment(&part));
    }

    #[test]
    fn is_attachment_false_for_a_real_body_leaf() {
        assert!(!is_attachment(&leaf("text/plain", "body text")));
    }

    /// A `text/plain` ATTACHMENT small enough to inline carries `body.data`
    /// exactly like a real body leaf. Appearing BEFORE the real
    /// `multipart/alternative` body in `multipart/mixed` (a realistic
    /// shape: Gmail lists attachments in send order, which need not be
    /// last), the old mime-type-and-decodability-only walk would select the
    /// attachment's content as the message body. It must not.
    #[test]
    fn gmail_payload_excludes_text_attachment_before_the_real_body() {
        let attachment = attachment_leaf("text/plain", "notes.txt", "this is attachment content, not the email");
        let inner = container(
            "multipart/alternative",
            Vec::new(),
            vec![leaf("text/plain", "the real email body")],
        );
        let payload = container("multipart/mixed", Vec::new(), vec![attachment, inner]);
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("the real email body"));
    }

    #[test]
    fn gmail_payload_excludes_html_attachment_via_content_disposition_header() {
        let attachment = GmailMessagePart {
            mime_type: "text/html".into(),
            filename: String::new(),
            headers: headers(&[("Content-Disposition", "attachment; filename=\"page.html\"")]),
            body: GmailPartBody { data: Some(b64("<p>attachment html, not the body</p>")) },
            parts: Vec::new(),
        };
        let real_html = leaf("text/html", "<p>the real html body</p>");
        let payload = container("multipart/mixed", Vec::new(), vec![attachment, real_html]);
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("the real html body\n"));
    }

    /// CodeRabbit round 3 finding on PR #681, a follow-on to the round-2
    /// attachment fix: excluding an attachment PART's own `body.data` isn't
    /// enough — a forwarded email attached as `message/rfc822` (or any
    /// attachment marked `multipart/*`) can carry nested `text/plain`/
    /// `text/html` children of its own. Those must not be reachable as the
    /// OUTER message's body just because the walk can structurally descend
    /// into them. The attachment-marked subtree, nested child included,
    /// must be skipped entirely; the real outer body must still win even
    /// though the attachment subtree appears first.
    #[test]
    fn gmail_payload_excludes_nested_children_of_an_attachment_subtree() {
        let nested_plain_inside_attachment =
            leaf("text/plain", "this is inside the attached forwarded email, not the outer body");
        let attachment_subtree = GmailMessagePart {
            mime_type: "message/rfc822".into(),
            filename: "forwarded-message.eml".into(),
            headers: Vec::new(),
            body: GmailPartBody::default(),
            parts: vec![nested_plain_inside_attachment],
        };
        let inner = container(
            "multipart/alternative",
            Vec::new(),
            vec![leaf("text/plain", "the real outer email body")],
        );
        let payload = container("multipart/mixed", Vec::new(), vec![attachment_subtree, inner]);
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("the real outer email body"));
    }

    #[test]
    fn gmail_payload_falls_back_to_html_when_only_body_is_inside_an_attachment_subtree() {
        // The nested text/plain is unreachable (inside an attachment), and
        // there's no other text/plain in the tree — the walk must return
        // None for text/plain rather than reaching past the attachment
        // boundary, so from_gmail_payload correctly falls through to HTML.
        let nested_plain_inside_attachment = leaf("text/plain", "buried inside the attachment");
        let attachment_subtree = GmailMessagePart {
            mime_type: "message/rfc822".into(),
            filename: "forwarded-message.eml".into(),
            headers: Vec::new(),
            body: GmailPartBody::default(),
            parts: vec![nested_plain_inside_attachment],
        };
        let real_html = leaf("text/html", "<p>the real html body</p>");
        let payload = container("multipart/mixed", Vec::new(), vec![attachment_subtree, real_html]);
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("the real html body\n"));
    }

    // -- gmail_html_to_text boundary preservation (CodeRabbit round 2
    // finding #2 on PR #681) — Gmail path ONLY, html_to_text (Teams/Outlook
    // Graph path) is untouched, asserted separately below. -----------------

    #[test]
    fn gmail_html_to_text_inserts_newline_for_br() {
        let out = gmail_html_to_text("Line one<br>Line two");
        assert_eq!(out, "Line one\nLine two");
    }

    #[test]
    fn gmail_html_to_text_inserts_newline_for_self_closing_br() {
        let out = gmail_html_to_text("Line one<br/>Line two<br />Line three");
        assert_eq!(out, "Line one\nLine two\nLine three");
    }

    #[test]
    fn gmail_html_to_text_separates_adjacent_divs() {
        // The exact CodeRabbit-cited case: without a boundary,
        // "<div>Approve</div><div>today</div>" collapses to "Approvetoday".
        let out = gmail_html_to_text("<div>Approve</div><div>today</div>");
        assert_eq!(out, "Approve\ntoday\n");
    }

    #[test]
    fn gmail_html_to_text_separates_paragraphs_and_list_items() {
        let out = gmail_html_to_text("<p>First</p><p>Second</p><ul><li>One</li><li>Two</li></ul>");
        assert_eq!(out, "First\nSecond\nOne\nTwo\n");
    }

    #[test]
    fn gmail_html_to_text_still_decodes_entities_and_strips_tags() {
        let out = gmail_html_to_text("<div>Ship it &amp; <b>celebrate</b></div>");
        assert_eq!(out, "Ship it & celebrate\n");
    }

    #[test]
    fn gmail_html_to_text_plain_input_passes_through() {
        assert_eq!(gmail_html_to_text("no tags here"), "no tags here");
    }

    // m365 PR 2.0: gmail_html_to_text shares the same script/style/entity
    // core as html_to_text now (unified) — the boundary-insertion behavior
    // above stays its own, separate thing.

    #[test]
    fn gmail_html_to_text_drops_script_and_style_and_decodes_mdash() {
        let out = gmail_html_to_text(
            "<style>.warn{color:red}</style><script>alert('x');</script><div>Approve &mdash; today</div>",
        );
        assert_eq!(out, "Approve \u{2014} today\n");
    }

    #[test]
    fn gmail_html_to_text_decodes_numeric_entities() {
        assert_eq!(gmail_html_to_text("&#8212;&#x2014;"), "\u{2014}\u{2014}");
    }

    /// html_to_text (the Teams/Outlook Graph path, via from_graph_body)
    /// still does not insert block-boundary separators — that gap survives
    /// m365 PR 2.0's script/style/entity fix by deliberate choice (module
    /// doc, [`gmail_html_to_text`]'s doc). This is the guarantee
    /// `connectors::microsoft`'s fixture tests also assert from the other
    /// side.
    #[test]
    fn html_to_text_still_has_no_block_boundary_separators() {
        let out = html_to_text("<div>Approve</div><div>today</div>");
        assert_eq!(out, "Approvetoday", "html_to_text (Graph path) has no block-boundary insertion");
    }

    #[test]
    fn gmail_payload_html_fallback_preserves_adjacent_div_boundary() {
        let payload = container(
            "multipart/alternative",
            Vec::new(),
            vec![leaf("text/html", "<div>Approve</div><div>today</div>")],
        );
        assert_eq!(from_gmail_payload(&payload).as_deref(), Some("Approve\ntoday\n"));
    }

    // -- gmail_message_to_item shaping -------------------------------------

    fn headers(pairs: &[(&str, &str)]) -> Vec<GmailHeader> {
        pairs
            .iter()
            .map(|(n, v)| GmailHeader { name: (*n).to_string(), value: (*v).to_string() })
            .collect()
    }

    #[test]
    fn shapes_participants_thread_id_and_timestamp() {
        let payload = container(
            "multipart/alternative",
            headers(&[
                ("Subject", "Q3 roadmap"),
                ("From", "\"Jane Doe\" <jane@example.com>"),
                ("To", "john@example.com, \"Doe, Priya\" <priya@example.com>"),
                ("Cc", "team@example.com"),
            ]),
            vec![leaf("text/plain", "Here's the plan for Q3.")],
        );
        let m = GmailMessage {
            id: "18c9f2a1b3d4e5f6".into(),
            thread_id: "18c9f2a1b3d4e5f0".into(),
            internal_date: Some("1735689600000".into()),
            label_ids: Vec::new(),

            payload: Some(payload),
        };
        let item = gmail_message_to_item(&m).expect("shaped item");
        assert_eq!(item.source_type, "gmail");
        assert_eq!(item.source_id, "18c9f2a1b3d4e5f6");
        assert_eq!(item.thread_id.as_deref(), Some("18c9f2a1b3d4e5f0"));
        assert_eq!(item.content, "Q3 roadmap\n\nHere's the plan for Q3.");
        assert_eq!(
            item.participants,
            vec!["Jane Doe", "john@example.com", "Doe, Priya", "team@example.com"]
        );
        assert_eq!(item.author_name.as_deref(), Some("Jane Doe"));
        assert_eq!(item.timestamp.timestamp_millis(), 1735689600000);
    }

    #[test]
    fn shaping_trims_quoted_chain_from_body() {
        let payload = container(
            "multipart/alternative",
            headers(&[("Subject", "Re: hi"), ("From", "jane@example.com")]),
            vec![leaf(
                "text/plain",
                "Sounds good.\n\nOn Mon, Jan 1, 2024 at 9:00 AM John <john@example.com> wrote:\n> original message",
            )],
        );
        let m = GmailMessage {
            id: "id1".into(),
            thread_id: "t1".into(),
            internal_date: Some("1735689600000".into()),
            label_ids: Vec::new(),

            payload: Some(payload),
        };
        let item = gmail_message_to_item(&m).expect("shaped item");
        assert_eq!(item.content, "Re: hi\n\nSounds good.");
    }

    #[test]
    fn shaping_skips_empty_id() {
        let payload = container("text/plain", Vec::new(), vec![leaf("text/plain", "body")]);
        let m = GmailMessage {
            id: "   ".into(),
            thread_id: "t1".into(),
            internal_date: None,
            label_ids: Vec::new(),

            payload: Some(payload),
        };
        assert!(gmail_message_to_item(&m).is_none());
    }

    #[test]
    fn shaping_skips_empty_body() {
        let payload = container("multipart/mixed", Vec::new(), Vec::new());
        let m = GmailMessage {
            id: "id1".into(),
            thread_id: "t1".into(),
            internal_date: None,
            label_ids: Vec::new(),

            payload: Some(payload),
        };
        assert!(gmail_message_to_item(&m).is_none());
    }

    #[test]
    fn shaping_skips_missing_payload() {
        let m = GmailMessage {
            id: "id1".into(),
            thread_id: "t1".into(),
            internal_date: None,
            label_ids: Vec::new(),

            payload: None,
        };
        assert!(gmail_message_to_item(&m).is_none());
    }

    // -- Fixture-driven (backend/tests/fixtures/gmail/ — see its README for
    // provenance: hand-authored against Google's documented schema, not a
    // live capture) --------------------------------------------------------

    const MESSAGES_GET_FULL: &str =
        include_str!("../../tests/fixtures/gmail/messages_get_full.json");
    const THREADS_GET: &str = include_str!("../../tests/fixtures/gmail/threads_get.json");

    #[test]
    fn fixture_reply_message_shapes_participants_thread_id_and_trims_quote() {
        let m: GmailMessage = serde_json::from_str(MESSAGES_GET_FULL).unwrap();
        let item = gmail_message_to_item(&m).expect("shaped item");
        assert_eq!(item.source_type, "gmail");
        assert_eq!(item.source_id, "18c9f2a1b3d4e5f6");
        assert_eq!(item.thread_id.as_deref(), Some("18c9f1a0b2c3d4e5"));
        // internalDate = 1787576400000 ms (2026-08-24T13:00:00Z)
        assert_eq!(item.timestamp.timestamp_millis(), 1787576400000);
        assert_eq!(
            item.participants,
            vec!["Priya Nair", "sam.rivera@acme-corp.example", "eng-team@acme-corp.example"]
        );
        assert_eq!(item.author_name.as_deref(), Some("Priya Nair"));
        // text/plain preferred over the text/html part.
        assert!(item.content.starts_with("Re: Q3 roadmap review\n\nThanks, this looks great"));
        // Quoted chain trimmed — the "On ... wrote:" header and its "> " lines
        // must not survive into the atom content.
        assert!(!item.content.contains("wrote:"));
        assert!(!item.content.contains("Draft attached"));
    }

    #[test]
    fn fixture_thread_shapes_both_messages_original_is_single_part_plaintext() {
        #[derive(Deserialize)]
        struct ThreadGetResponse {
            id: String,
            messages: Vec<GmailMessage>,
        }
        let thread: ThreadGetResponse = serde_json::from_str(THREADS_GET).unwrap();
        assert_eq!(thread.id, "18c9f1a0b2c3d4e5");
        assert_eq!(thread.messages.len(), 2);

        let original = gmail_message_to_item(&thread.messages[0]).expect("original shaped");
        assert_eq!(original.source_id, "18c9f1a0b2c3d4e5");
        assert_eq!(original.thread_id.as_deref(), Some("18c9f1a0b2c3d4e5"));
        assert_eq!(original.content, "Q3 roadmap review\n\nHi Sam,\n\nDraft attached, let me know what you think.\n\nSam");
        assert_eq!(original.participants, vec!["Sam Rivera", "priya.nair@acme-corp.example"]);

        let reply = gmail_message_to_item(&thread.messages[1]).expect("reply shaped");
        assert_eq!(reply.source_id, "18c9f2a1b3d4e5f6");
        assert_eq!(reply.thread_id, original.thread_id, "both messages share the thread id");
    }

    #[test]
    fn shaping_empty_thread_id_maps_to_none_with_a_valid_timestamp() {
        let payload = container("text/plain", Vec::new(), vec![leaf("text/plain", "body text")]);
        let m = GmailMessage {
            id: "id1".into(),
            thread_id: String::new(),
            internal_date: Some("1735689600000".into()),
            label_ids: Vec::new(),

            payload: Some(payload),
        };
        let item = gmail_message_to_item(&m).expect("shaped item");
        assert!(item.thread_id.is_none(), "empty threadId maps to None");
        assert_eq!(item.timestamp.timestamp_millis(), 1735689600000);
    }

    // CodeRabbit round 1 finding #3 on PR #681: a missing/unparseable
    // `internalDate` must be treated as malformed upstream data (skip, like
    // the empty-id/empty-body funnel filters above) rather than silently
    // stamped with `Utc::now()` — matches `google_calendar.rs::
    // shape_calendar_item`'s own `parse_g_time(..)?` philosophy for a
    // missing event start time.
    #[test]
    fn shaping_skips_missing_internal_date() {
        let payload = container("text/plain", Vec::new(), vec![leaf("text/plain", "body text")]);
        let m = GmailMessage {
            id: "id1".into(),
            thread_id: "t1".into(),
            internal_date: None,
            label_ids: Vec::new(),

            payload: Some(payload),
        };
        assert!(gmail_message_to_item(&m).is_none());
    }

    #[test]
    fn shaping_skips_invalid_internal_date() {
        let payload = container("text/plain", Vec::new(), vec![leaf("text/plain", "body text")]);
        let m = GmailMessage {
            id: "id1".into(),
            thread_id: "t1".into(),
            internal_date: Some("not-a-number".into()),
            label_ids: Vec::new(),

            payload: Some(payload),
        };
        assert!(gmail_message_to_item(&m).is_none());
    }
}

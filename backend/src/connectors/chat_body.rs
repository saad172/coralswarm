//! Google Chat message normalisation and the `spaceType` archetype
//! discriminator (gsuite Train G4 PR 2 —
//! `.scratch/gsuite-enterprise/g4-chat-design.md` §3.6/§3.7/§5). Pure code
//! only: no I/O, no routes, no migrations — G4.3/G4.4/G4.5 wire this to the
//! real Chat API, watched-space rows, and `connector_identities` lookups.
//!
//! # Why this is its own module, not `email_body.rs`
//!
//! `email_body.rs` is the shared **email** normaliser (MIME walk, HTML→text,
//! quoted-reply-chain trimming). A Google Chat `Message` shares none of that
//! shape: `text`/`formattedText` are already plain (Google's own markup
//! dialect, not HTML), structure lives in `cardsV2` + `annotations[]`, and
//! there is no quoted-reply chain to trim — `quotedMessageMetadata` is a
//! resource *reference*, not inlined text. Design doc §1.6 is the full
//! reasoning; the only overlap with `email_body.rs` is [`html_to_text`],
//! reachable via a one-line `use` if a card's text widgets ever need it —
//! not exercised yet, so not imported here.
//!
//! # What lives here
//!
//! - [`Archetype`] — the `spaceType` discriminator (§2.3/§5). **Reads only
//!   `Space.space_type`** — never `display_name`, `membership_count`, or any
//!   naming heuristic (#672's explicit instruction, pinned by
//!   [`tests::archetype_negative_sweep_reads_only_space_type`]).
//! - [`substitute_mentions`] — `<users/{id}>` → `@{canonical_name}` (§3.7
//!   rule 2), resolved through a caller-supplied lookup (this module does no
//!   I/O; G4.4 wires the real `connector_identities` read). Unresolved ids
//!   are both rendered as `@unknown` and returned for the name-backfill
//!   sweep (Slack's `slack_name_backfill` precedent).
//! - [`chat_message_to_item`] — `Message` → [`super::NormalizedItem`],
//!   §3.7's eight rules end to end. The funnel every ingest path (backfill,
//!   catch-up, live push) shares — `None` is the poison-pill signal, same
//!   posture as `gmail_message_to_item`/`shape_calendar_item`.
//!
//! `#[allow(dead_code)]` throughout: nothing outside this module's own tests
//! calls these yet — G4.4 (spaces ingest) and G4.5 (DM/group opt-in) are the
//! consumers.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::NormalizedItem;

// ---------------------------------------------------------------------------
// Wire shapes — Google Chat API v1, deserialized field-for-field against
// Google's documented `Space`/`Message`/`User` resources (design doc §2.3,
// §2.5). Only the fields this module needs.
// ---------------------------------------------------------------------------

/// A Chat `Space` resource (`spaces.list`/`spaces.get`) — just enough to
/// drive [`Archetype::of`] and the destination-prompt disclosure (§5.4);
/// `google_chat_watched_spaces` (G4.3's migration) is where the durable copy
/// lives.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct ChatSpace {
    /// `"spaces/{space}"` — globally unique, stable.
    pub name: String,
    /// `SPACE_TYPE_UNSPECIFIED | SPACE | GROUP_CHAT | DIRECT_MESSAGE`. The
    /// **only** field [`Archetype::of`] reads.
    #[serde(default)]
    pub space_type: String,
    #[serde(default)]
    pub display_name: String,
    /// `THREADED_MESSAGES | GROUPED_MESSAGES` (§3.6: drives whether
    /// `thread_id` is populated downstream).
    #[serde(default)]
    pub space_threading_state: String,
    /// `HISTORY_STATE_UNSPECIFIED | HISTORY_OFF | HISTORY_ON` (§2.2, §3.4b).
    #[serde(default)]
    pub history_state: String,
    /// A DM with a Chat app — our own bot conversation. Never ingestible
    /// (§2.3, §5.2) but deliberately NOT consulted by [`Archetype::of`]:
    /// the archetype discriminator is `spaceType` alone, and
    /// `singleUserBotDm` filtering is a separate, later concern (the App
    /// Home surface, §5.5) so it can't be mistaken for a second archetype
    /// signal.
    #[serde(default)]
    pub single_user_bot_dm: bool,
    /// Present only to prove [`Archetype::of`] never reads it — see the
    /// module doc and the negative-sweep test. Untyped: this module has no
    /// use for its shape, only for the fact that it exists on the wire.
    #[serde(default)]
    pub membership_count: Option<serde_json::Value>,
}

/// The Slack-model-vs-mailbox-model split (design doc §2.3, §5.1, §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum Archetype {
    /// `spaceType == SPACE` — shared ocean routed at pick time, roster
    /// auto-seating (I4). The Slack model.
    Container,
    /// `spaceType ∈ {GROUP_CHAT, DIRECT_MESSAGE}` (and any unrecognized/
    /// unspecified value — the fail-safe direction, §5.2: a DM is not a
    /// shared consent boundary, so an unknown shape must never be treated
    /// as one). Owner's private ocean, opt-in only, no fan-out ever.
    Mailbox,
}

impl Archetype {
    /// The discriminator. **Reads `Space.space_type` and nothing else** —
    /// #672's explicit instruction (design doc §3.6, §4.1: "Train G4 must
    /// branch and test on `spaceType`, not on inference from message shape
    /// or naming"). See [`tests::archetype_negative_sweep_reads_only_space_type`]
    /// for the sweep that pins this.
    #[allow(dead_code)]
    pub(crate) fn of(space: &ChatSpace) -> Self {
        match space.space_type.as_str() {
            "SPACE" => Archetype::Container,
            _ => Archetype::Mailbox,
        }
    }
}

/// A Chat `User` (`Message.sender`, `Membership.member`). Design doc §2.4:
/// under user auth a roster read may populate only `name`/`type`, but a
/// message's `sender` carries `displayName` too — that asymmetry is what
/// makes the mention-substitution directory (`connector_identities`, §3.3)
/// work without a second API scope.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct ChatUser {
    /// `"users/{id}"` — the only stable identifier; Chat never returns an
    /// email address (§2.4).
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub display_name: String,
    /// `HUMAN | BOT | TYPE_UNSPECIFIED`.
    #[serde(rename = "type", default)]
    pub user_type: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct ChatThread {
    /// `"spaces/{space}/threads/{thread}"`.
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct ChatAttachment {
    /// The filename Chat shows in its UI. §3.7 rule 5: name only, no Drive
    /// fetch (Drive is out of scope by plan decision 1).
    #[serde(default)]
    pub content_name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct ChatQuotedMessageMetadata {
    /// The quoted message's resource name — a reference, never dereferenced
    /// (§3.7 rule 6: it may be a message we have no consent path to).
    #[serde(default)]
    pub name: String,
}

/// A Chat `Message` resource (`spaces.messages.list`/`.get`), field-for-field
/// against design doc §2.5.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct ChatMessage {
    /// `"spaces/{space}/messages/{message}"` — globally unique, stable, and
    /// exactly the string the deletion CloudEvent and
    /// `spaces.spaceEvents.list` hand back (§3.6: this is `source_id`
    /// verbatim, no id reconstruction).
    #[serde(default)]
    pub name: String,
    /// Absent on a system-generated message (space-lifecycle notices Chat
    /// renders inline but does not attribute to a sender) — the signal this
    /// module uses for §3.7 rule 7's "system message" poison pill.
    /// `spaces.messages.list`'s own docs say it "excludes system messages"
    /// from that particular read path, but the push/catch-up paths are not
    /// documented with the same guarantee, so the funnel filters
    /// defensively rather than assuming upstream already did it.
    #[serde(default)]
    pub sender: Option<ChatUser>,
    /// RFC 3339. A missing/unparseable value skips with a warn (§3.7 rule
    /// 8) — never `Utc::now()`.
    #[serde(default)]
    pub create_time: Option<String>,
    /// Non-empty ⇒ the message was edited after ingest (§5.3's "edits
    /// reported, not honoured" declaration; not consumed by this pure
    /// normaliser, only carried through for G4.4's C9 conformance test).
    #[serde(default)]
    pub last_update_time: Option<String>,
    /// Plain-text body, contains raw `@mentions`. Fallback when
    /// `formatted_text` is absent (§3.7 rule 1).
    #[serde(default)]
    pub text: Option<String>,
    /// Output-only, Google's markup dialect: bold/italic/strikethrough/
    /// monospace, lists, quotes, user mentions as `<users/{id}>`, custom
    /// emoji as `:emojiName:`. Preferred over `text` — the fidelity-
    /// preserving field (§3.7 rule 1).
    #[serde(default)]
    pub formatted_text: Option<String>,
    /// Chat-app-mention-stripped text. **Never used** — we are not the
    /// mentioned app (§2.5, §3.7 rule 1). Present only so a future reader of
    /// this struct doesn't reach for it by mistake; deserialized and
    /// otherwise ignored.
    #[serde(default)]
    pub argument_text: Option<String>,
    /// App-authored structured card content, usually bot output. §3.7 rule
    /// 4: render a deterministic placeholder naming the count, never the
    /// JSON. Untyped — this module only needs `len()`.
    #[serde(default)]
    pub cards_v2: Vec<serde_json::Value>,
    #[serde(default)]
    pub thread: Option<ChatThread>,
    #[serde(default)]
    pub attachment: Vec<ChatAttachment>,
    #[serde(default)]
    pub quoted_message_metadata: Option<ChatQuotedMessageMetadata>,
}

// ---------------------------------------------------------------------------
// Mention substitution (§3.7 rule 2)
// ---------------------------------------------------------------------------

/// Substitute every well-formed `<users/{id}>` mention marker in
/// `formatted_text`-shaped input with `@{canonical_name}`, resolved through
/// `resolve` (the real caller, G4.4, backs this with a `connector_identities`
/// read — this module does no I/O of its own). A miss renders `@unknown`
/// (never a leaked raw id) and is collected into the returned `Vec` for the
/// name-backfill sweep (the `slack_name_backfill` precedent, design doc §3.7
/// rule 2).
///
/// Hand-rolled scanning rather than a `regex` dependency for this one bounded
/// pattern — same "don't reach for a bigger tool than the job needs" posture
/// `email_body.rs`'s entity decoder documents for its own curated table.
///
/// Malformed markers (`<users/>`, an unclosed `<users/123`, or anything not
/// matching `<users/{non-empty-id}>` exactly) are left as literal text
/// untouched, rather than guessed at — the same "invalid → skip gracefully"
/// posture `email_body.rs::decode_one_entity` documents.
pub(crate) fn substitute_mentions(
    text: &str,
    resolve: &dyn Fn(&str) -> Option<String>,
) -> (String, Vec<String>) {
    const PREFIX: &str = "<users/";
    let mut out = String::with_capacity(text.len());
    let mut unresolved = Vec::new();
    let mut rest = text;
    loop {
        match rest.find(PREFIX) {
            None => {
                out.push_str(rest);
                break;
            }
            Some(start) => {
                out.push_str(&rest[..start]);
                let after_prefix = &rest[start + PREFIX.len()..];
                match after_prefix.find('>') {
                    Some(end) if end > 0 => {
                        let id = &after_prefix[..end];
                        // The id itself must look like an opaque token (no
                        // nested '<' — a malformed/partial marker like
                        // "<users/<users/1>" must not be mistaken for one
                        // clean match).
                        if id.contains('<') {
                            out.push_str(&rest[start..start + PREFIX.len()]);
                            rest = &rest[start + PREFIX.len()..];
                            continue;
                        }
                        match resolve(id) {
                            Some(name) => out.push_str(&format!("@{name}")),
                            None => {
                                out.push_str("@unknown");
                                unresolved.push(id.to_string());
                            }
                        }
                        rest = &after_prefix[end + 1..];
                    }
                    _ => {
                        // No closing '>' reachable, or an empty id
                        // ("<users/>") — leave the literal prefix untouched
                        // and keep scanning past it.
                        out.push_str(&rest[start..start + PREFIX.len()]);
                        rest = &rest[start + PREFIX.len()..];
                    }
                }
            }
        }
    }
    (out, unresolved)
}

// ---------------------------------------------------------------------------
// Message → NormalizedItem shaping (§3.7's eight rules, end to end)
// ---------------------------------------------------------------------------

/// Shape one Chat message into a [`NormalizedItem`]. `source_type =
/// "google_chat"` for both archetypes — the archetype lives in the routing,
/// not the label (§3.6). `source_id` is the message resource name;
/// `channel_id` is the space resource name (parsed from `m.name`);
/// `thread_id` is `thread.name` when present; `author_id`/`author_name` come
/// from `sender`.
///
/// `resolve_mention` backs [`substitute_mentions`] (§3.7 rule 2); its misses
/// are silently folded into `@unknown` here — callers that need the
/// name-backfill list should call [`substitute_mentions`] directly on the
/// body they intend to use, or read it back off the returned item's content.
/// `own_bot_user_id` is CoralSwarm's own Chat app's user id; a message sent
/// by it is our own bot's output, not content to ingest (§3.7 rule 7).
/// Normalized the same way `sender.name` is (trimmed, `"users/"` prefix
/// stripped) before comparison, so a caller may pass either the bare id or
/// the full `"users/{id}"` resource name — CodeRabbit finding on #707: an
/// un-normalized `own_bot_user_id` (e.g. a caller passing `"users/abc"`)
/// never matched the normalized `sender_id` (`"abc"`), so our own bot's
/// output was never actually filtered. An empty normalized id (a caller
/// passing `""` or bare `"users/"`) never matches anything — never treated
/// as "empty equals empty" against a malformed sender id.
///
/// Returns `None` — a poison pill, counted by the caller — for (§3.7 rule
/// 7):
/// - an empty/missing message `name`;
/// - no `sender` at all (this module's system-message signal, see
///   [`ChatMessage::sender`]'s doc);
/// - `sender.type == "BOT"` **and** the sender is our own app
///   (`own_bot_user_id`) — a third party's bot message is still ingestible
///   content, only OUR bot's own chatter is not;
/// - an empty rendered body (after mention substitution/card-placeholder/
///   attachment-naming/quote-referencing, whichever rule produced it);
///
/// and (§3.7 rule 8) for a missing/unparseable `create_time` — never
/// `Utc::now()`, same posture `gmail_message_to_item`/`shape_calendar_item`
/// document for their own timestamp fields.
pub(crate) fn chat_message_to_item(
    m: &ChatMessage,
    own_bot_user_id: &str,
    resolve_mention: &dyn Fn(&str) -> Option<String>,
) -> Option<NormalizedItem> {
    let name = m.name.trim();
    if name.is_empty() {
        tracing::warn!("google_chat: message with missing/empty name skipped (malformed upstream data)");
        return None;
    }

    let sender = match m.sender.as_ref() {
        Some(s) => s,
        None => {
            tracing::warn!(source_id = %name, "google_chat: message with no sender skipped (system message)");
            return None;
        }
    };
    let sender_id = sender.name.trim().trim_start_matches("users/");
    let own_bot_id = own_bot_user_id.trim().trim_start_matches("users/");
    if sender.user_type.eq_ignore_ascii_case("BOT") && !own_bot_id.is_empty() && sender_id == own_bot_id {
        tracing::warn!(source_id = %name, "google_chat: message from our own bot skipped");
        return None;
    }

    // Rule 1: formattedText preferred (fidelity-preserving); text as
    // fallback. Never argumentText (§2.5).
    let raw_body = m
        .formatted_text
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| m.text.as_deref())
        .unwrap_or("");
    let (mut body, _unresolved) = substitute_mentions(raw_body, resolve_mention);

    // Rule 4: cardsV2 — a deterministic placeholder, never the JSON.
    if !m.cards_v2.is_empty() {
        if !body.trim().is_empty() {
            body.push('\n');
        }
        let n = m.cards_v2.len();
        let noun = if n == 1 { "card" } else { "cards" };
        body.push_str(&format!("[{n} {noun}]"));
    }

    // Rule 5: attachments — name only, no Drive fetch.
    for att in &m.attachment {
        let content_name = att.content_name.trim();
        if !content_name.is_empty() {
            if !body.trim().is_empty() {
                body.push('\n');
            }
            body.push_str(&format!("[attachment: {content_name}]"));
        }
    }

    // Rule 6: quoted messages — a reference, never dereferenced.
    if let Some(q) = m.quoted_message_metadata.as_ref() {
        let quoted_name = q.name.trim();
        if !quoted_name.is_empty() {
            if !body.trim().is_empty() {
                body.push('\n');
            }
            body.push_str(&format!("[quoting {quoted_name}]"));
        }
    }

    let body = body.trim();
    if body.is_empty() {
        tracing::warn!(source_id = %name, "google_chat: message with empty body skipped");
        return None;
    }

    // Rule 8: createTime — missing/unparseable skips with a warn.
    let timestamp = match m
        .create_time
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
    {
        Some(ts) => ts,
        None => {
            tracing::warn!(
                source_id = %name,
                create_time = ?m.create_time,
                "google_chat: message with missing/invalid createTime skipped (malformed upstream data)"
            );
            return None;
        }
    };

    // §3.6: channel_id is the space resource name — the part of `name`
    // before "/messages/". `spaces/{space}/messages/{message}` is the
    // documented shape; anything not matching it (shouldn't happen against
    // a real API) degrades to `None` rather than panicking on a slice.
    // CodeRabbit finding on #707: `split("/messages/").next()` always
    // yields `Some` (str::split never returns an empty iterator), so a
    // malformed name like `"garbage"` produced `Some("garbage")` instead of
    // the documented `None` degrade — `split_once` only matches when the
    // separator is actually present, and the empty-space filter covers the
    // `"/messages/M1"` (empty space) edge too.
    let channel_id = name
        .split_once("/messages/")
        .map(|(space, _)| space.to_string())
        .filter(|space| !space.is_empty());

    let thread_id = m
        .thread
        .as_ref()
        .map(|t| t.name.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let author_name = (!sender.display_name.trim().is_empty())
        .then(|| sender.display_name.trim().to_string());
    let author_id = (!sender_id.is_empty()).then(|| sender_id.to_string());

    let participants = author_name.clone().into_iter().collect();

    Some(NormalizedItem {
        source_type: "google_chat".into(),
        source_id: name.to_string(),
        content: body.to_string(),
        participants,
        timestamp,
        scope: None,
        thread_id,
        channel_id,
        author_id,
        author_name,
    })
}

/// Convenience wrapper around [`chat_message_to_item`] for a caller that
/// already has a resolved id→name map in hand (e.g. a pre-fetched
/// `connector_identities` batch) rather than a per-call resolver closure.
#[allow(dead_code)]
pub(crate) fn chat_message_to_item_with_directory(
    m: &ChatMessage,
    own_bot_user_id: &str,
    directory: &HashMap<String, String>,
) -> Option<NormalizedItem> {
    chat_message_to_item(m, own_bot_user_id, &|id: &str| directory.get(id).cloned())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn space(json: serde_json::Value) -> ChatSpace {
        serde_json::from_value(json).expect("valid ChatSpace fixture")
    }

    fn message(json: serde_json::Value) -> ChatMessage {
        serde_json::from_value(json).expect("valid ChatMessage fixture")
    }

    fn no_resolve(_id: &str) -> Option<String> {
        None
    }

    // -- Archetype --------------------------------------------------------

    #[test]
    fn archetype_space_is_container() {
        let s = space(serde_json::json!({ "name": "spaces/1", "spaceType": "SPACE" }));
        assert_eq!(Archetype::of(&s), Archetype::Container);
    }

    #[test]
    fn archetype_group_chat_is_mailbox() {
        let s = space(serde_json::json!({ "name": "spaces/1", "spaceType": "GROUP_CHAT" }));
        assert_eq!(Archetype::of(&s), Archetype::Mailbox);
    }

    #[test]
    fn archetype_direct_message_is_mailbox() {
        let s = space(serde_json::json!({ "name": "spaces/1", "spaceType": "DIRECT_MESSAGE" }));
        assert_eq!(Archetype::of(&s), Archetype::Mailbox);
    }

    #[test]
    fn archetype_unspecified_or_unknown_fails_safe_to_mailbox() {
        let unspecified = space(serde_json::json!({ "name": "spaces/1", "spaceType": "SPACE_TYPE_UNSPECIFIED" }));
        assert_eq!(Archetype::of(&unspecified), Archetype::Mailbox);
        let missing = space(serde_json::json!({ "name": "spaces/1" }));
        assert_eq!(Archetype::of(&missing), Archetype::Mailbox);
        let garbage = space(serde_json::json!({ "name": "spaces/1", "spaceType": "NOT_A_REAL_TYPE" }));
        assert_eq!(Archetype::of(&garbage), Archetype::Mailbox);
    }

    /// #672's explicit instruction (design doc §3.6/§4.1): the discriminator
    /// is `spaceType`, and NOTHING ELSE — not `displayName` (a space named
    /// "General DM" or "team-space" must not sway it), not
    /// `membershipCount`, not any inference from message shape. This test
    /// pins that by constructing spaces where every OTHER field points the
    /// "wrong" way and asserting the archetype tracks `spaceType` alone.
    #[test]
    fn archetype_negative_sweep_reads_only_space_type() {
        // A "SPACE" whose displayName/membershipCount look DM-shaped must
        // still resolve as Container.
        let deceptive_space = space(serde_json::json!({
            "name": "spaces/1",
            "spaceType": "SPACE",
            "displayName": "Direct Message with Bob",
            "membershipCount": { "joinedDirectHumanUserCount": 2, "joinedGroupCount": 0 },
            "singleUserBotDm": true
        }));
        assert_eq!(Archetype::of(&deceptive_space), Archetype::Container);

        // A "DIRECT_MESSAGE" whose displayName/membershipCount look
        // SPACE-shaped must still resolve as Mailbox.
        let deceptive_dm = space(serde_json::json!({
            "name": "spaces/2",
            "spaceType": "DIRECT_MESSAGE",
            "displayName": "#engineering-space",
            "membershipCount": { "joinedDirectHumanUserCount": 50, "joinedGroupCount": 3 }
        }));
        assert_eq!(Archetype::of(&deceptive_dm), Archetype::Mailbox);
    }

    #[test]
    fn archetype_over_all_five_fixture_spaces() {
        let list: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/google_chat/spaces_list.json"
        ))
        .expect("valid spaces_list.json fixture");
        let spaces = list["spaces"].as_array().expect("spaces array");
        assert_eq!(spaces.len(), 5, "fixture must carry all five space shapes");

        let mut by_name: HashMap<String, Archetype> = HashMap::new();
        for raw in spaces {
            let s: ChatSpace = serde_json::from_value(raw.clone()).expect("valid space");
            by_name.insert(s.name.clone(), Archetype::of(&s));
        }

        let container_count = by_name.values().filter(|a| **a == Archetype::Container).count();
        let mailbox_count = by_name.values().filter(|a| **a == Archetype::Mailbox).count();
        // One HISTORY_ON SPACE, one HISTORY_OFF SPACE ⇒ two Container.
        assert_eq!(container_count, 2, "expected two SPACE-typed fixtures to be Container");
        // One GROUP_CHAT, one DIRECT_MESSAGE, one singleUserBotDm (also
        // DIRECT_MESSAGE) ⇒ three Mailbox.
        assert_eq!(mailbox_count, 3, "expected three non-SPACE fixtures to be Mailbox");
    }

    // -- substitute_mentions -----------------------------------------------

    #[test]
    fn substitute_mentions_resolves_known_id() {
        let resolve = |id: &str| (id == "123").then(|| "Alice".to_string());
        let (out, unresolved) = substitute_mentions("hi <users/123> ok", &resolve);
        assert_eq!(out, "hi @Alice ok");
        assert!(unresolved.is_empty());
    }

    #[test]
    fn substitute_mentions_unresolved_id_becomes_at_unknown_and_is_recorded() {
        let (out, unresolved) = substitute_mentions("hi <users/999>", &no_resolve);
        assert_eq!(out, "hi @unknown");
        assert_eq!(unresolved, vec!["999".to_string()]);
    }

    #[test]
    fn substitute_mentions_multiple_mixed() {
        let resolve = |id: &str| (id == "1").then(|| "Alice".to_string());
        let (out, unresolved) = substitute_mentions("<users/1> and <users/2>, hi", &resolve);
        assert_eq!(out, "@Alice and @unknown, hi");
        assert_eq!(unresolved, vec!["2".to_string()]);
    }

    #[test]
    fn substitute_mentions_no_markers_passthrough() {
        let (out, unresolved) = substitute_mentions("plain text, no mentions here", &no_resolve);
        assert_eq!(out, "plain text, no mentions here");
        assert!(unresolved.is_empty());
    }

    #[test]
    fn substitute_mentions_malformed_marker_left_literal() {
        let (out, unresolved) = substitute_mentions("broken <users/ marker", &no_resolve);
        assert_eq!(out, "broken <users/ marker");
        assert!(unresolved.is_empty());
    }

    #[test]
    fn substitute_mentions_empty_id_left_literal() {
        let (out, unresolved) = substitute_mentions("empty <users/>", &no_resolve);
        assert_eq!(out, "empty <users/>");
        assert!(unresolved.is_empty());
    }

    #[test]
    fn substitute_mentions_custom_emoji_passes_through_verbatim() {
        // §3.7 rule 3: :emojiName: is not a mention marker and must survive
        // untouched.
        let (out, unresolved) = substitute_mentions("nice :tada: work", &no_resolve);
        assert_eq!(out, "nice :tada: work");
        assert!(unresolved.is_empty());
    }

    // -- chat_message_to_item: happy paths ----------------------------------

    #[test]
    fn formatted_text_preferred_over_text() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "text": "plain fallback",
            "formattedText": "rich version"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "rich version");
    }

    #[test]
    fn falls_back_to_text_when_formatted_text_absent() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "text": "plain fallback"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "plain fallback");
    }

    #[test]
    fn falls_back_to_text_when_formatted_text_is_blank() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "text": "plain fallback",
            "formattedText": "   "
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "plain fallback");
    }

    #[test]
    fn argument_text_never_used() {
        // Even if formattedText/text are both absent but argumentText is
        // present, the body must come up empty and skip — argumentText is
        // never a body source (§2.5, §3.7 rule 1).
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "argumentText": "stripped-of-app-mention text"
        }));
        assert!(chat_message_to_item(&m, "our-bot", &no_resolve).is_none());
    }

    #[test]
    fn mentions_substituted_in_content() {
        let resolve = |id: &str| (id == "42").then(|| "Bob".to_string());
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hey <users/42>, ping"
        }));
        let item = chat_message_to_item(&m, "our-bot", &resolve).expect("must shape");
        assert_eq!(item.content, "hey @Bob, ping");
    }

    #[test]
    fn mentions_unresolved_id_renders_at_unknown() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hey <users/999>"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "hey @unknown");
    }

    #[test]
    fn cards_v2_render_as_deterministic_placeholder_not_json() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Bot Sender", "type": "BOT" },
            "createTime": "2026-08-01T12:00:00Z",
            "cardsV2": [
                { "cardId": "c1", "card": { "sections": [{ "widgets": [{ "textParagraph": { "text": "secret internal card body" } }] }] } },
                { "cardId": "c2", "card": {} }
            ]
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "[2 cards]");
        assert!(!item.content.contains("secret"));
        assert!(!item.content.contains("cardId"));
    }

    #[test]
    fn single_card_uses_singular_noun() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Bot Sender", "type": "BOT" },
            "createTime": "2026-08-01T12:00:00Z",
            "cardsV2": [{ "cardId": "c1" }]
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "[1 card]");
    }

    #[test]
    fn text_and_card_combine() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Bot Sender", "type": "BOT" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "Build finished",
            "cardsV2": [{ "cardId": "c1" }]
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "Build finished\n[1 card]");
    }

    #[test]
    fn attachment_named_only_no_content_fetch() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "see attached",
            "attachment": [{ "contentName": "roadmap.pdf" }]
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "see attached\n[attachment: roadmap.pdf]");
    }

    #[test]
    fn multiple_attachments_each_named() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "files",
            "attachment": [{ "contentName": "a.pdf" }, { "contentName": "b.png" }]
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "files\n[attachment: a.pdf]\n[attachment: b.png]");
    }

    #[test]
    fn quoted_message_referenced_never_dereferenced() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M2",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "agreed",
            "quotedMessageMetadata": { "name": "spaces/S1/messages/M1" }
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "agreed\n[quoting spaces/S1/messages/M1]");
        // Never the quoted text itself — this module has no access to it and
        // must not invent any.
        assert!(!item.content.contains("secret quoted content"));
    }

    #[test]
    fn channel_id_and_thread_id_populated() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hi",
            "thread": { "name": "spaces/S1/threads/T1" }
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.channel_id.as_deref(), Some("spaces/S1"));
        assert_eq!(item.thread_id.as_deref(), Some("spaces/S1/threads/T1"));
    }

    // CodeRabbit finding on #707: `str::split` never returns an empty
    // iterator, so `.next()` used to yield `Some(name)` even when
    // `"/messages/"` was absent — the doc comment's "degrades to None"
    // claim was false. `split_once` fixes it; these two pin the degrade for
    // real.
    #[test]
    fn channel_id_none_when_name_has_no_messages_separator() {
        let m = message(serde_json::json!({
            "name": "garbage",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hi"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.channel_id, None);
    }

    #[test]
    fn channel_id_none_when_space_part_is_empty() {
        let m = message(serde_json::json!({
            "name": "/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hi"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.channel_id, None);
    }

    #[test]
    fn thread_id_none_for_grouped_message() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hi"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.thread_id, None);
    }

    #[test]
    fn author_id_and_name_from_sender() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/777", "displayName": "Carol", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hi"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.author_id.as_deref(), Some("777"));
        assert_eq!(item.author_name.as_deref(), Some("Carol"));
        assert_eq!(item.participants, vec!["Carol".to_string()]);
    }

    #[test]
    fn source_type_is_google_chat_for_both_archetypes() {
        // The normaliser doesn't take an archetype at all — §3.6: the label
        // is identical for both; the archetype lives in routing, not here.
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hi"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.source_type, "google_chat");
        assert_eq!(item.source_id, "spaces/S1/messages/M1");
    }

    // -- chat_message_to_item: poison pills (§3.7 rule 7) -------------------

    #[test]
    fn skips_empty_name() {
        let m = message(serde_json::json!({
            "name": "",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hi"
        }));
        assert!(chat_message_to_item(&m, "our-bot", &no_resolve).is_none());
    }

    #[test]
    fn skips_missing_name() {
        let m = message(serde_json::json!({
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "hi"
        }));
        assert!(chat_message_to_item(&m, "our-bot", &no_resolve).is_none());
    }

    #[test]
    fn skips_empty_body() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "   ",
            "text": ""
        }));
        assert!(chat_message_to_item(&m, "our-bot", &no_resolve).is_none());
    }

    #[test]
    fn skips_our_own_bot_sender() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/our-bot", "displayName": "CoralSwarm", "type": "BOT" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "ingest confirmed"
        }));
        assert!(chat_message_to_item(&m, "our-bot", &no_resolve).is_none());
    }

    // CodeRabbit finding on #707: `own_bot_user_id` was compared against the
    // normalized `sender_id` without being normalized itself, so a caller
    // passing the full `"users/{id}"` resource name (rather than the bare
    // id) never matched — our own bot's output was never actually filtered.
    #[test]
    fn skips_our_own_bot_sender_when_own_bot_user_id_is_prefixed() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/our-bot", "displayName": "CoralSwarm", "type": "BOT" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "ingest confirmed"
        }));
        // Caller passes the full "users/{id}" resource name, not the bare id.
        assert!(chat_message_to_item(&m, "users/our-bot", &no_resolve).is_none());
    }

    #[test]
    fn empty_own_bot_user_id_never_matches() {
        // A caller passing "" (or a bare "users/" that normalizes to "")
        // must never treat that as matching an equally-empty/malformed
        // sender id — the self-bot check is a no-op, not an accidental
        // empty-equals-empty skip.
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/", "displayName": "", "type": "BOT" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "some bot content"
        }));
        assert!(chat_message_to_item(&m, "", &no_resolve).is_some());
        assert!(chat_message_to_item(&m, "users/", &no_resolve).is_some());
    }

    #[test]
    fn does_not_skip_a_third_party_bot_sender() {
        // A third-party integration's bot message IS ingestible content —
        // only OUR bot's own chatter is filtered (§3.7 rule 7).
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/some-other-bot", "displayName": "CI Bot", "type": "BOT" },
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "build passed"
        }));
        let item = chat_message_to_item(&m, "our-bot", &no_resolve).expect("must shape");
        assert_eq!(item.content, "build passed");
    }

    #[test]
    fn skips_system_message_with_no_sender() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "createTime": "2026-08-01T12:00:00Z",
            "formattedText": "Alice added Bob to the space"
        }));
        assert!(chat_message_to_item(&m, "our-bot", &no_resolve).is_none());
    }

    // -- chat_message_to_item: createTime (§3.7 rule 8) ---------------------

    #[test]
    fn skips_missing_create_time() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "formattedText": "hi"
        }));
        assert!(chat_message_to_item(&m, "our-bot", &no_resolve).is_none());
    }

    #[test]
    fn skips_invalid_create_time_never_falls_back_to_now() {
        let m = message(serde_json::json!({
            "name": "spaces/S1/messages/M1",
            "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "createTime": "not-a-real-timestamp",
            "formattedText": "hi"
        }));
        assert!(chat_message_to_item(&m, "our-bot", &no_resolve).is_none());
    }

    // -- fixtures: full-file smoke, and byte-identical email_body proof -----

    #[test]
    fn messages_list_page1_fixture_shapes_without_panicking() {
        let raw = include_str!("../../tests/fixtures/google_chat/messages_list_page1.json");
        let parsed: serde_json::Value = serde_json::from_str(raw).expect("valid JSON");
        let messages = parsed["messages"].as_array().expect("messages array");
        assert!(!messages.is_empty());
        for m in messages {
            let msg: ChatMessage = serde_json::from_value(m.clone()).expect("valid ChatMessage");
            // Every fixture message either shapes to Some or is a documented
            // poison-pill case — either way this must not panic.
            let _ = chat_message_to_item(&msg, "our-own-bot-id", &|_| None);
        }
    }

    /// CodeRabbit finding on #707: `msg-0002`'s `USER_MENTION` annotation
    /// used to carry `length: 15`, which (per the API's documented
    /// `startIndex`/`length` contract — a 0-based offset/length pair over
    /// the plain-text `text` body) covered `"@Alice Ng ack, "` instead of
    /// just `"@Alice Ng"` (9 chars). The current normaliser doesn't read
    /// `annotations` at all (mentions are resolved from `formattedText`'s
    /// `<users/{id}>` markers instead — §3.7 rule 2), so the stale range
    /// never caused a wrong substitution; this test pins the range itself
    /// so a future `annotations`-based consumer isn't handed bad data.
    #[test]
    fn messages_list_page1_fixture_user_mention_annotation_range_matches_text() {
        let raw = include_str!("../../tests/fixtures/google_chat/messages_list_page1.json");
        let parsed: serde_json::Value = serde_json::from_str(raw).expect("valid JSON");
        let messages = parsed["messages"].as_array().expect("messages array");
        let mentioning = messages
            .iter()
            .find(|m| m["annotations"].is_array())
            .expect("fixture must carry a message with annotations");
        let text = mentioning["text"].as_str().expect("text field");
        let ann = &mentioning["annotations"][0];
        assert_eq!(ann["type"].as_str(), Some("USER_MENTION"));
        let start = ann["startIndex"].as_u64().expect("startIndex") as usize;
        let length = ann["length"].as_u64().expect("length") as usize;
        let fragment = &text[start..start + length];
        assert_eq!(fragment, "@Alice Ng");
    }

    #[test]
    fn message_get_fixture_carries_last_update_time_the_edited_case() {
        let raw = include_str!("../../tests/fixtures/google_chat/message_get.json");
        let m: ChatMessage = serde_json::from_str(raw).expect("valid ChatMessage");
        assert!(
            m.last_update_time.as_deref().is_some_and(|s| !s.is_empty()),
            "message_get.json must carry a non-empty lastUpdateTime (test 8' — the edited-message case)"
        );
    }

    #[test]
    fn members_list_fixture_carries_the_group_member_case() {
        let raw = include_str!("../../tests/fixtures/google_chat/members_list.json");
        let parsed: serde_json::Value = serde_json::from_str(raw).expect("valid JSON");
        let memberships = parsed["memberships"].as_array().expect("memberships array");
        assert!(
            memberships.iter().any(|m| m.get("groupMember").is_some()),
            "members_list.json must carry at least one groupMember (§3.4a)"
        );
        assert!(
            memberships
                .iter()
                .any(|m| m.get("state").and_then(|s| s.as_str()) == Some("INVITED")),
            "members_list.json must carry at least one INVITED member"
        );
    }

    /// The whole point of §1.6's decision: adding this module must not
    /// change one byte of `email_body`'s output. Spot-checks a few of its
    /// public entry points against fixed input/output pairs already pinned
    /// by `email_body`'s own suite — a cheap tripwire in this module's test
    /// run, not a substitute for `email_body`'s (much larger) suite, which
    /// runs unchanged and is asserted separately by the caller of this PR's
    /// `cargo test` invocations.
    #[test]
    fn email_body_output_unaffected_by_this_module_existing() {
        use super::super::email_body::html_to_text;
        assert_eq!(
            html_to_text("<p>Ship it &amp; <b>celebrate</b> &mdash;ish</p>"),
            "Ship it & celebrate \u{2014}ish"
        );
    }
}

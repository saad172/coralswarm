//! Reef-write boundary validation (Workstream E §6.2 of
//! `docs/plans/graph-temporal-memory.md`).
//!
//! Prompt adherence is not a control: the retired per-atom entity prompt
//! forbade diarisation placeholders at length (naming "Speaker 1", "Unknown
//! Speaker", "Others", "You", "Attendee 2" explicitly) and the model emitted
//! them anyway (D9 — "Speaker 1" became a live entity reef carrying `knows`
//! edges in both directions). Prompt-side and per-extractor filters only
//! cover the writer they sit in.
//!
//! This module is the deterministic backstop the founder's plan calls for: one
//! rule, applied at the reef-write boundary (`Store::upsert_node_and_link_atom`)
//! rather than re-implemented per extraction path, so "the next bad-reef class"
//! doesn't need a third patch in a third place.
//!
//! By the time a title reaches this layer, `node_type` has already collapsed
//! to `"entity"`/`"decision"`/... (`workers::source_atoms::map_node_type` — every
//! reef the extraction pipeline creates is stored as `type = 'entity'`
//! regardless of whether the LLM classified it person/org/topic/project, see
//! `workers::infer_expertise`'s module doc). That means, unlike
//! `is_placeholder_entity_name`, this check has no `entity_type` to gate on and
//! is therefore intentionally UNCONDITIONAL for entity-typed reefs: a real
//! product or topic named bare "Guest" (no trailing digits) IS rejected here.
//! That is the exact false-positive risk the plan names, not an oversight —
//! see `guest_bare_word_is_rejected_by_design` below, and the caller MUST log
//! every rejection (this module does not log; see `Store::
//! upsert_node_and_link_atom`) so an over-broad drop is visible rather than
//! silent.

use std::sync::LazyLock;

use regex::Regex;

/// Numbered/bare role placeholders: "Speaker", "Speaker 1", "Speaker  12",
/// "Attendee 2", "Participant", "Guest", "Guest1", ... Case-insensitive,
/// anchored to the WHOLE string (`^...$`, no multiline flag) so "Speaker of
/// the House" does not match — `\s*` only consumes whitespace, and the
/// literal " of the house" tail is neither whitespace nor a digit.
static NUMBERED_ROLE_PLACEHOLDER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^(speaker|attendee|participant|guest)\s*\d*$").expect("valid regex")
});

/// Exact-phrase placeholders with no numbered form.
static EXACT_PLACEHOLDER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^(unknown speaker|others|you|me)$").expect("valid regex"));

/// `true` when `title` is a diarisation/transcript placeholder rather than a
/// real entity name, per docs/plans/graph-temporal-memory.md §6.2. Callers
/// gate this on `node_type == "entity"` — see `Store::upsert_node_and_link_atom`.
///
/// Trims surrounding whitespace before matching (defensive; extraction titles
/// are expected to already be trimmed by the time they reach the store, but
/// this is the last line of defense before the row exists). An empty title is
/// a different failure mode (handled elsewhere) and is never flagged here.
pub fn is_diarization_placeholder(title: &str) -> bool {
    let t = title.trim();
    if t.is_empty() {
        return false;
    }
    NUMBERED_ROLE_PLACEHOLDER.is_match(t) || EXACT_PLACEHOLDER.is_match(t)
}

/// `true` when `title` is exactly the literal author-fallback placeholder
/// `"Unknown"` (case-insensitive, whitespace-trimmed) — the identity-
/// resolution-at-the-door reject list's second entry
/// (`docs/plans/identity-resolution-at-the-door.md` §10.2/§4.1): "the
/// literal author fallback `Unknown` (it is already a scored candidate in
/// prod: 'Unknown 2')". Deliberately a SEPARATE, narrower check from
/// [`is_diarization_placeholder`] rather than folded into that function's
/// `EXACT_PLACEHOLDER` set: this one applies to every node type the door's
/// resolver ladder handles (not gated to `entity`), and it exists to catch a
/// different failure mode — a caller's own "author unknown" fallback string
/// leaking into a mint, not a transcript diarisation artifact.
pub fn is_author_fallback_label(title: &str) -> bool {
    title.trim().eq_ignore_ascii_case("unknown")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbered_speaker_labels_rejected() {
        for t in [
            "Speaker 1",
            "Speaker 2",
            "speaker 12",
            "SPEAKER 3",
            "Speaker",
            "Speaker1",
        ] {
            assert!(is_diarization_placeholder(t), "{t:?} must be rejected");
        }
    }

    #[test]
    fn speaker_of_the_house_survives() {
        // Anchoring regression: a real title that merely STARTS WITH "speaker"
        // must not be caught by the numbered-role pattern.
        assert!(!is_diarization_placeholder("Speaker of the House"));
        assert!(!is_diarization_placeholder("Speakers Corner"));
    }

    #[test]
    fn attendee_participant_numbered_and_bare_rejected() {
        for t in [
            "Attendee",
            "Attendee 2",
            "Participant",
            "Participant 3",
            "attendee   7",
        ] {
            assert!(is_diarization_placeholder(t), "{t:?} must be rejected");
        }
    }

    #[test]
    fn exact_phrase_placeholders_rejected_case_insensitively() {
        for t in [
            "Unknown Speaker",
            "UNKNOWN SPEAKER",
            "Others",
            "You",
            "Me",
            "me",
            "you",
        ] {
            assert!(is_diarization_placeholder(t), "{t:?} must be rejected");
        }
    }

    #[test]
    fn exact_phrase_placeholders_do_not_match_with_trailing_digits() {
        // The exact-phrase set has no numbered form ("Unknown Speaker 3" is
        // NOT in this list — it also is not currently rejected by the
        // numbered-role pattern, since "unknown" isn't one of its four
        // prefixes). Documented here rather than silently left untested: this
        // is a real, known gap versus `workers::extract::
        // is_placeholder_entity_name` (which DOES catch "Unknown Speaker 3"
        // via its own, separately-maintained pattern for the entity-extraction
        // choke point specifically).
        assert!(!is_diarization_placeholder("Unknown Speaker 3"));
    }

    #[test]
    fn real_names_survive() {
        for t in [
            "Chris Marchetti",
            "CoralSwarm",
            "Alex Whitfield",
            "Taylor",
            "Anthropic",
        ] {
            assert!(!is_diarization_placeholder(t), "{t:?} must NOT be rejected");
        }
    }

    #[test]
    fn guest_bare_word_is_rejected_by_design() {
        // The false-positive risk the plan names explicitly: a product
        // genuinely titled "Guest" (no trailing digits) IS rejected, because
        // this check has no `entity_type` to gate on (node_type has already
        // collapsed to "entity" by the time it reaches the store — see the
        // module doc). This test documents the tradeoff rather than hiding
        // it: the mitigation is that every rejection is logged by the caller
        // (`Store::upsert_node_and_link_atom`), not that this pattern is
        // perfectly precise.
        assert!(is_diarization_placeholder("Guest"));
        assert!(is_diarization_placeholder("guest"));
        assert!(is_diarization_placeholder("Guest 1"));
    }

    #[test]
    fn whitespace_and_case_are_normalized() {
        assert!(is_diarization_placeholder("  Speaker 1  "));
        assert!(is_diarization_placeholder("SpEaKeR 1"));
    }

    #[test]
    fn empty_title_is_not_this_filters_job() {
        assert!(!is_diarization_placeholder(""));
        assert!(!is_diarization_placeholder("   "));
    }

    #[test]
    fn author_fallback_label_matches_only_bare_unknown() {
        for t in ["Unknown", "unknown", "UNKNOWN", "  Unknown  "] {
            assert!(is_author_fallback_label(t), "{t:?} must be rejected");
        }
    }

    #[test]
    fn author_fallback_label_does_not_match_a_real_name_containing_it() {
        for t in ["Unknown Speaker", "Unknown 2", "Person Unknown", ""] {
            assert!(!is_author_fallback_label(t), "{t:?} must NOT be rejected");
        }
    }
}

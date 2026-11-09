//! Supersession-kind classification (§3.5), shared by the ingest correction
//! path (`routes::ingest`) and the fact-supersession backfill
//! (`fact_supersession`).

use crate::store::SupersessionKind;

/// Case-insensitive substrings whose presence in the newer (superseding)
/// coral's text escalates classification from the default [`SupersessionKind::
/// Change`] to [`SupersessionKind::Correction`] — verbatim the phrase list
/// §3.5 specifies. `"actually it's"` is checked in BOTH word orders
/// (`"actually it's"` and `"it's actually"`): §3.5's own worked example
/// ("it's actually Elmbrook, not Elmbrok") uses the reversed order, so a
/// single-order match would misclassify the spec's own example as `Change`.
/// Deliberately NOT "actually" alone — that word alone is common in ordinary,
/// non-correcting speech ("actually, let's postpone to Friday"), and a
/// looser match would trade the asymmetry §3.5 is built around for false
/// positives that erase real history.
const CORRECTION_PHRASES: &[&str] = &[
    "correction:",
    "i was wrong",
    "actually it's",
    "it's actually",
    "typo",
    "meant to say",
];

/// Classify a supersession edge as [`SupersessionKind::Change`] or
/// [`SupersessionKind::Correction`] from the text of the NEWER (superseding)
/// coral — §3.5.
///
/// **Asymmetric by construction, not merely "conservative":** the function
/// can only ever escalate FROM the default `Change` TO `Correction`, never
/// the reverse, and escalation requires an exact (case-insensitive) match
/// against [`CORRECTION_PHRASES`] — no fuzzy matching, no partial-word
/// matching, no LLM judgment call. Anything that doesn't contain one of those
/// phrases — including text that clearly describes a real change ("we no
/// longer use Fly.io") — falls back to `Change`. This mirrors §3.5's own
/// stated rationale verbatim: "wrongly keeping history is cheap; wrongly
/// erasing it is not", so the classifier is deliberately biased toward the
/// cheap failure mode on anything it isn't certain about.
pub(crate) fn classify_supersession_kind(newer_coral_text: &str) -> SupersessionKind {
    let lower = newer_coral_text.to_ascii_lowercase();
    if CORRECTION_PHRASES.iter().any(|p| lower.contains(p)) {
        SupersessionKind::Correction
    } else {
        SupersessionKind::Change
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_defaults_to_change_for_a_real_change() {
        // §3.5's own worked example of a `change`: the old claim really was
        // true, it just stopped being true.
        assert_eq!(
            classify_supersession_kind("we no longer use Fly.io"),
            SupersessionKind::Change
        );
    }

    #[test]
    fn classify_ambiguous_language_falls_back_to_change_never_correction() {
        // The asymmetry itself: text with NO explicit correction language must
        // never escalate, even when a human reader might guess "correction"
        // from context. "Actually" alone (no adjacent "it's") is the
        // deliberately-chosen near-miss — common in ordinary speech, not a
        // correction signal on its own.
        for ambiguous in [
            "actually, let's postpone the launch to Friday",
            "the deadline moved to next week",
            "we're switching from Postgres to MySQL for the events service",
            "nevermind, going with plan B instead",
        ] {
            assert_eq!(
                classify_supersession_kind(ambiguous),
                SupersessionKind::Change,
                "ambiguous input {ambiguous:?} must classify as Change, not Correction \
                 (§3.5 asymmetry: wrongly erasing history is the expensive mistake)"
            );
        }
    }

    #[test]
    fn classify_explicit_correction_language_escalates() {
        // §3.5's own worked example of a `correction`, plus each phrase in
        // `CORRECTION_PHRASES` exercised at least once.
        for correcting in [
            "it's actually Elmbrook, not Elmbrok",
            "correction: the meeting is at 3pm, not 2pm",
            "I was wrong about the deadline, it's next Friday",
            "sorry, typo in my last message",
            "I meant to say Tuesday, not Thursday",
        ] {
            assert_eq!(
                classify_supersession_kind(correcting),
                SupersessionKind::Correction,
                "{correcting:?} contains explicit correction language and must classify \
                 as Correction"
            );
        }
    }

    #[test]
    fn classify_is_case_insensitive_and_checks_both_word_orders() {
        assert_eq!(
            classify_supersession_kind("ACTUALLY IT'S Elmbrook"),
            SupersessionKind::Correction
        );
        assert_eq!(
            classify_supersession_kind("Correction: it's Elmbrook"),
            SupersessionKind::Correction
        );
    }
}

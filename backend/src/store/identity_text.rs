//! Pure title/identifier text helpers for the identity-resolution-at-the-door
//! seam (`docs/plans/identity-resolution-at-the-door.md` §10.2, builder B1).
//!
//! `normalize_title`/`title_affinity`/`titles_similar` here are DELIBERATE
//! copies of the same-named functions in `workers::entity_resolution`
//! (`normalize_title` :2128, `titles_similar` :2165, `title_affinity` :2203
//! as of `main` `8fe4b2d0`) — the design doc's file-ownership table forbids
//! B1 from editing `workers/*`, and ER (`workers::entity_resolution`, owned
//! by builder B3) keeps using its own copies unchanged. The integrator is
//! expected to dedupe the two into one shared module once all three
//! builders' branches land; until then this is intentional, documented
//! duplication rather than a cross-owner edit.

/// Lowercase + trim a title for blocking comparisons. Copy of
/// `workers::entity_resolution::normalize_title`.
pub fn normalize_title(title: &str) -> String {
    title.trim().to_lowercase()
}

/// Tokenize a normalized title into alphanumeric word tokens. Copy of
/// `workers::entity_resolution::tokens` (private there; needed here as the
/// shared basis for [`title_affinity`]).
fn tokens(norm: &str) -> Vec<String> {
    norm.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_string())
        .collect()
}

/// Levenshtein edit distance (small strings only). Copy of
/// `workers::entity_resolution::edit_distance`.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

/// True if two normalized titles are similar enough to be worth a resolver
/// call: exact match, a shared word token, or a small edit distance on short
/// names. Copy of `workers::entity_resolution::titles_similar`. Unused by
/// the door's ladder today (the ladder uses [`title_affinity`]'s exact-1.0
/// permutation gate and the near-match containment/trgm gate instead), kept
/// as a faithful copy for the integrator's eventual dedupe and available to
/// `identity_door_pg.rs`'s unit tests.
pub fn titles_similar(an: &str, bn: &str) -> bool {
    if an == bn {
        return true;
    }
    let at = tokens(an);
    let bt = tokens(bn);
    let bset: std::collections::HashSet<&String> = bt.iter().collect();
    if at.iter().any(|t| t.len() >= 3 && bset.contains(t)) {
        return true;
    }
    if at.len() == 1 && bt.len() == 1 {
        let (sa, sb) = (&at[0], &bt[0]);
        let maxlen = sa.chars().count().max(sb.chars().count());
        if maxlen > 0 && maxlen <= 8 && edit_distance(sa, sb) <= 2 {
            return true;
        }
    }
    false
}

/// Order-insensitive token-set affinity (Jaccard, containment-floored at
/// 0.75). Copy of `workers::entity_resolution::title_affinity` — see that
/// function's doc comment (and this module's header) for the full
/// rationale. `1.0` is the door's step-4 permutation-equality gate;
/// `>= DOOR_NEAR_CONTAINMENT` is the door's step-5 near-match gate.
pub fn title_affinity(an: &str, bn: &str) -> f64 {
    if an == bn {
        return 1.0;
    }
    let at: std::collections::HashSet<String> = tokens(an).into_iter().collect();
    let bt: std::collections::HashSet<String> = tokens(bn).into_iter().collect();
    if at.is_empty() || bt.is_empty() {
        return 0.0;
    }
    let inter = at.intersection(&bt).count() as f64;
    let union = at.union(&bt).count() as f64;
    let jaccard = if union > 0.0 { inter / union } else { 0.0 };
    if inter > 0.0 && (inter as usize == at.len() || inter as usize == bt.len()) {
        return jaccard.max(0.75);
    }
    jaccard
}

/// `true` when `mention` looks like a hard identifier the door's ladder step
/// 2 should try against `reef_identifiers` before falling through to
/// title/alias/permutation matching: an email address, a bare Slack user id
/// (`U`/`W` + 8+ uppercase-alnum, Slack's own format), or an `@handle`.
/// Returns `(kind, value_norm)` — `kind` matches `reef_identifiers.kind`'s
/// closed vocabulary (`email`, `slack_user`); `value_norm` is
/// lowercased for `email` (matching `record_reef_identifier`'s documented
/// caller-normalization contract) and left byte-exact for a raw Slack id
/// (Slack ids are case-sensitive), lowercased for an `@handle` (handles are
/// conventionally lowercase; this is a best-effort shape match, not a
/// guaranteed hit — the door falls through to steps 3-5 regardless when
/// nothing in `reef_identifiers` carries this value).
///
/// Deliberately conservative: `None` for anything that doesn't look like one
/// of these three shapes, so an ordinary person/org/topic name never takes
/// this branch.
pub fn identifier_shape(mention: &str) -> Option<(&'static str, String)> {
    let m = mention.trim();
    if m.is_empty() {
        return None;
    }
    // Email: exactly one '@', non-empty local part, a '.'-bearing domain,
    // and not itself an `@handle` (which starts with '@').
    if !m.starts_with('@') {
        if let Some((local, domain)) = m.split_once('@') {
            if !local.is_empty()
                && !local.contains(char::is_whitespace)
                && domain.contains('.')
                && !domain.contains('@')
                && !domain.contains(char::is_whitespace)
            {
                return Some(("email", m.to_lowercase()));
            }
        }
    }
    // Slack's own opaque user id shape: `U`/`W` followed by 8+ uppercase
    // alphanumerics (e.g. `U0123ABCD`). Case-sensitive — never lowercased.
    if is_slack_raw_id(m) {
        return Some(("slack_user", m.to_string()));
    }
    // `@handle` — a display-style mention, not Slack's own id, but still an
    // identifier SHAPE worth a lookup (a connector may have indexed handles
    // under `slack_user` too).
    if let Some(rest) = m.strip_prefix('@') {
        if !rest.is_empty()
            && rest
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
        {
            return Some(("slack_user", rest.to_lowercase()));
        }
    }
    None
}

fn is_slack_raw_id(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 9 {
        return false;
    }
    if bytes[0] != b'U' && bytes[0] != b'W' {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_title_trims_and_lowercases() {
        assert_eq!(normalize_title("  Alex Example  "), "Alex Example");
    }

    #[test]
    fn title_affinity_is_order_insensitive_permutation() {
        let a = normalize_title("Alex Alex");
        let b = normalize_title("Alex Example");
        assert_eq!(title_affinity(&a, &b), 1.0);
    }

    #[test]
    fn title_affinity_floors_containment_at_075() {
        let a = normalize_title("Alex");
        let b = normalize_title("Alex Alex");
        assert!((title_affinity(&a, &b) - 0.75).abs() < 1e-9);
    }

    #[test]
    fn title_affinity_unrelated_names_score_low() {
        let a = normalize_title("CoralSwarm");
        let b = normalize_title("Alex Example");
        assert_eq!(title_affinity(&a, &b), 0.0);
    }

    #[test]
    fn identifier_shape_detects_email() {
        assert_eq!(
            identifier_shape("person@example.com"),
            Some(("email", "person@example.com".to_string()))
        );
    }

    #[test]
    fn identifier_shape_detects_slack_raw_id() {
        assert_eq!(
            identifier_shape("U012ABCDEF"),
            Some(("slack_user", "U012ABCDEF".to_string()))
        );
    }

    #[test]
    fn identifier_shape_detects_at_handle() {
        assert_eq!(
            identifier_shape("@Alex_M"),
            Some(("slack_user", "Alex_m".to_string()))
        );
    }

    #[test]
    fn identifier_shape_rejects_ordinary_names() {
        for name in ["Alex Example", "CoralSwarm", "p2ptext repository", "Alex"] {
            assert_eq!(identifier_shape(name), None, "{name:?} must not look like an identifier");
        }
    }

    #[test]
    fn identifier_shape_rejects_short_uppercase_words() {
        // "USA" is 3 chars — too short to be mistaken for a Slack id (min 9).
        assert_eq!(identifier_shape("USA"), None);
    }
}

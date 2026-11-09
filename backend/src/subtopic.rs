//! Shared unique-parent rule for G2 (forward bind-or-drop) and G4
//! (fold existing sub-topic reefs).
//!
//! Whole-word containment with `-` as a word character so
//! `coralswarm-connect` does not contain `CoralSwarm`. Exact title
//! (non-self) wins first; otherwise unique R1 containment. Zero or many
//! matches → no parent. The self reef is never a parent.

use std::collections::HashSet;

use uuid::Uuid;

/// Entity reef already linked to the evidence coral. G2 matches unresolved
/// relationship mentions against this set; G4 uses the same shape for
/// same-coral parent candidates.
#[derive(Debug, Clone)]
pub struct SameCoralReef {
    pub node_id: Uuid,
    pub title: String,
    pub is_self: bool,
}

/// Whole-word containment: `needle` appears in `hay` bounded by non-word
/// characters, with `-` counting as a word character so
/// `coralswarm-connect` does not contain `CoralSwarm`. Parent titles shorter
/// than 4 characters and equal titles do not match (exact title is handled
/// separately by [`unique_bind_parent`]).
fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '-'
}

pub fn whole_word_contains(hay: &str, needle: &str) -> bool {
    let hay: Vec<char> = hay.to_lowercase().chars().collect();
    let needle: Vec<char> = needle.to_lowercase().chars().collect();
    if needle.len() < MIN_NEEDLE_CHARS || needle == hay || needle.len() > hay.len() {
        return false;
    }
    for i in 0..=hay.len() - needle.len() {
        if hay[i..i + needle.len()] == needle[..] {
            let before_ok = i == 0 || !is_word_char(hay[i - 1]);
            let after = i + needle.len();
            let after_ok = after == hay.len() || !is_word_char(hay[after]);
            if before_ok && after_ok {
                return true;
            }
        }
    }
    false
}

const MIN_NEEDLE_CHARS: usize = 4;

/// [`whole_word_contains`] against many needles at once, with the same rules.
///
/// A needle matches only where its first char sits at a left boundary and its
/// last at a right boundary of the haystack, so instead of scanning every
/// needle, enumerate the haystack's boundary-delimited substrings and look
/// each up. Titles are lowercased once, here, rather than once per pair.
pub struct WholeWordIndex {
    needles: HashSet<String>,
    max_chars: usize,
}

impl WholeWordIndex {
    pub fn new<'a>(needles: impl IntoIterator<Item = &'a str>) -> Self {
        let mut set = HashSet::new();
        let mut max_chars = 0;
        for needle in needles {
            let lower = needle.to_lowercase();
            let chars = lower.chars().count();
            if chars >= MIN_NEEDLE_CHARS {
                max_chars = max_chars.max(chars);
                set.insert(lower);
            }
        }
        WholeWordIndex { needles: set, max_chars }
    }

    /// True iff `whole_word_contains(hay, needle)` for some indexed needle.
    pub fn contained_in(&self, hay: &str) -> bool {
        let hay = hay.to_lowercase();
        let chars: Vec<(usize, char)> = hay.char_indices().collect();
        let n = chars.len();
        let byte = |i: usize| chars.get(i).map_or(hay.len(), |(b, _)| *b);
        let right_ok = |j: usize| j == n || !is_word_char(chars[j].1);
        for i in 0..n {
            if i > 0 && is_word_char(chars[i - 1].1) {
                continue;
            }
            for j in i + MIN_NEEDLE_CHARS..=n.min(i + self.max_chars) {
                // The whole haystack is only ever an equal title, which never matches.
                if (i, j) != (0, n)
                    && right_ok(j)
                    && self.needles.contains(&hay[byte(i)..byte(j)])
                {
                    return true;
                }
            }
        }
        false
    }
}

/// Unique same-coral parent for an unresolved mention (G2) or a child
/// reef title (G4): exact title (non-self) first, else unique R1
/// containment. Zero or many matches → `None`.
pub fn unique_bind_parent(mention: &str, reefs: &[SameCoralReef]) -> Option<Uuid> {
    let mention_l = mention.trim().to_lowercase();
    if mention_l.is_empty() {
        return None;
    }
    let exact: Vec<Uuid> = reefs
        .iter()
        .filter(|r| !r.is_self && r.title.trim().to_lowercase() == mention_l)
        .map(|r| r.node_id)
        .collect();
    match exact.as_slice() {
        [id] => return Some(*id),
        [_, _, ..] => return None,
        [] => {}
    }
    let hits: Vec<Uuid> = reefs
        .iter()
        .filter(|r| !r.is_self && whole_word_contains(mention, &r.title))
        .map(|r| r.node_id)
        .collect();
    match hits.as_slice() {
        [id] => Some(*id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_word_contains_hyphen_does_not_split_coralswarm_connect() {
        assert!(!whole_word_contains(
            "coralswarm-connect plugin MCP endpoint configurability",
            "CoralSwarm",
        ));
    }

    #[test]
    fn whole_word_contains_p2ptext_kb_revamp() {
        assert!(whole_word_contains("p2ptext KB revamp", "p2ptext"));
    }

    #[test]
    fn whole_word_contains_react_19() {
        assert!(whole_word_contains("React 19", "React"));
    }

    #[test]
    fn whole_word_contains_rejects_equal_titles() {
        assert!(!whole_word_contains("React", "React"));
    }

    #[test]
    fn unique_bind_parent_picks_the_only_containment_match() {
        let parent = Uuid::new_v4();
        let reefs = vec![
            SameCoralReef {
                node_id: parent,
                title: "p2ptext".into(),
                is_self: false,
            },
            SameCoralReef {
                node_id: Uuid::new_v4(),
                title: "Postgres".into(),
                is_self: false,
            },
        ];
        assert_eq!(
            unique_bind_parent("p2ptext KB revamp", &reefs),
            Some(parent)
        );
    }

    #[test]
    fn unique_bind_parent_exact_title_binds() {
        let parent = Uuid::new_v4();
        let reefs = vec![SameCoralReef {
            node_id: parent,
            title: "React".into(),
            is_self: false,
        }];
        assert_eq!(unique_bind_parent("React", &reefs), Some(parent));
    }

    #[test]
    fn unique_bind_parent_two_parents_drops() {
        let reefs = vec![
            SameCoralReef {
                node_id: Uuid::new_v4(),
                title: "Claude Code".into(),
                is_self: false,
            },
            SameCoralReef {
                node_id: Uuid::new_v4(),
                title: "Cursor".into(),
                is_self: false,
            },
        ];
        assert_eq!(unique_bind_parent("Claude Code vs Cursor", &reefs), None);
    }

    #[test]
    fn unique_bind_parent_only_self_drops() {
        let reefs = vec![SameCoralReef {
            node_id: Uuid::new_v4(),
            title: "Alex Example".into(),
            is_self: true,
        }];
        assert_eq!(
            unique_bind_parent("Alex Example coding session", &reefs),
            None
        );
    }

    fn index_matches(hay: &str, needles: &[&str]) -> bool {
        WholeWordIndex::new(needles.iter().copied()).contained_in(hay)
    }

    fn oracle(hay: &str, needles: &[&str]) -> bool {
        needles.iter().any(|n| whole_word_contains(hay, n))
    }

    #[test]
    fn index_agrees_with_pairwise_on_edge_cases() {
        let cases: &[(&str, &[&str])] = &[
            ("", &[""]),
            ("", &["abcd"]),
            ("abcd", &[""]),
            ("React 19", &["React"]),
            ("React", &["React", "react"]),
            ("REACT", &["react"]),
            ("React Native", &["React", "React Native"]),
            ("coralswarm-connect plugin", &["CoralSwarm"]),
            ("coralswarm-connect plugin", &["coralswarm-connect"]),
            ("p2ptext KB revamp", &["p2ptext", "KB r"]),
            ("abc", &["abc"]),
            ("abcd efg", &["abc", "cd e", "d ef"]),
            ("(react) hooks", &["react", "(react)", "react)"]),
            ("x .react. y", &[".react.", " .react"]),
            ("  spaced  ", &["spaced", " spaced", "spaced  ", "  spaced  "]),
            ("a_b_c_d test", &["a_b_c_d", "b_c_d"]),
            ("ΟΔΥΣΣΕΥΣ voyage", &["ΟΔΥΣΣΕΥΣ", "οδυσσευς", "οδυσσευσ"]),
            ("Straße Berlin", &["STRASSE", "straße"]),
            ("İstanbul trip", &["istanbul", "i̇stanbul"]),
            ("café menu", &["cafe", "café"]),
            ("cafe\u{301} menu", &["cafe", "cafe\u{301}"]),
            ("日本語 テスト", &["日本語", "テスト"]),
            ("rust🦀crab day", &["rust", "crab day", "🦀crab"]),
            ("tab\tsep word", &["word", "tab\tsep"]),
            ("end-", &["end-", "end"]),
            ("-lead dash", &["lead", "-lead"]),
        ];
        let mut positives = 0;
        for (hay, needles) in cases {
            let expected = oracle(hay, needles);
            positives += expected as usize;
            assert_eq!(index_matches(hay, needles), expected, "hay {hay:?} needles {needles:?}");
            for needle in *needles {
                assert_eq!(
                    index_matches(hay, &[needle]),
                    whole_word_contains(hay, needle),
                    "hay {hay:?} needle {needle:?}"
                );
            }
        }
        assert!(positives >= 10, "edge cases must exercise matches, got {positives}");
    }

    #[test]
    fn index_agrees_with_pairwise_on_random_titles() {
        use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
        const PIECES: &[&str] = &[
            "ab", "abc", "abcd", "Abcd", "ABCD", "xyz", "react", "React", "19", "-", " ", " ",
            "  ", ".", "(", ")", "_", "/", ":", "Σ", "σ", "ς", "İ", "i", "ß", "ss", "é",
            "e\u{301}", "\u{301}", "日本", "🦀", "\t", "kb", "KB", "x",
        ];
        let mut rng = StdRng::seed_from_u64(0xC0_96);
        let title = |rng: &mut StdRng| -> String {
            (0..rng.gen_range(0..7))
                .map(|_| *PIECES.choose(rng).unwrap())
                .collect()
        };
        let (mut positives, mut trials) = (0, 0);
        for _ in 0..20_000 {
            let needles: Vec<String> = (0..rng.gen_range(0..6)).map(|_| title(&mut rng)).collect();
            let hay = match needles.choose(&mut rng) {
                Some(n) if rng.gen_bool(0.5) => {
                    format!("{}{}{}", title(&mut rng), n, title(&mut rng))
                }
                _ => title(&mut rng),
            };
            let refs: Vec<&str> = needles.iter().map(String::as_str).collect();
            let expected = oracle(&hay, &refs);
            positives += expected as usize;
            trials += 1;
            assert_eq!(index_matches(&hay, &refs), expected, "hay {hay:?} needles {refs:?}");
        }
        assert!(
            positives > trials / 20 && positives < trials,
            "random inputs must mix matches and misses: {positives}/{trials}"
        );
    }
}

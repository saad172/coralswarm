//! Text Postgres can store.
//!
//! Postgres `text` and `jsonb` cannot hold U+0000 (NUL): a bind carrying one
//! fails with `invalid byte sequence for encoding "UTF8": 0x00`, and a jsonb
//! string escaping one (`\u0000`) with `unsupported Unicode escape
//! sequence`. A Rust `String` can hold NUL, so source text that reaches the
//! database only later (a chunk body sealed in the object store, then bound
//! into `content_tsv` by the embed worker) fails long after capture, on every
//! retry, once per billed embedding call (prod, 2026-10-25: a GitHub file
//! with a literal NUL in a template string).
//!
//! NUL is the only character a valid UTF-8 `String` can carry that Postgres
//! refuses, so replacing it is the whole rule. It becomes U+FFFD, the
//! replacement character: the hole stays visible, and the words on either
//! side stay separate tokens.
//!
//! [`PgText`] is text that has been through [`sanitize`]: its field is
//! private and every constructor sanitizes, so a value of the type cannot
//! carry NUL. The capture funnel's chunk text is a `PgText`.

use std::borrow::Cow;

use serde_json::Value;

/// What a NUL becomes.
pub const NUL_REPLACEMENT: char = '\u{FFFD}';

/// `text` with every NUL replaced by [`NUL_REPLACEMENT`]. Borrowed, at no
/// cost, when there is none.
pub fn sanitize(text: &str) -> Cow<'_, str> {
    if text.contains('\0') {
        Cow::Owned(text.replace('\0', "\u{FFFD}"))
    } else {
        Cow::Borrowed(text)
    }
}

/// An owned [`sanitize`].
pub fn sanitized(text: impl Into<String>) -> String {
    let text = text.into();
    match sanitize(&text) {
        Cow::Borrowed(_) => text,
        Cow::Owned(clean) => clean,
    }
}

/// [`sanitize`] for an optional value.
pub fn sanitize_opt(text: Option<&str>) -> Option<Cow<'_, str>> {
    text.map(sanitize)
}

/// `value` with NUL replaced in every string and object key, so it can be
/// bound as `jsonb`. Borrowed when there is none.
pub fn sanitize_json(value: &Value) -> Cow<'_, Value> {
    if json_has_nul(value) {
        Cow::Owned(clean_json(value))
    } else {
        Cow::Borrowed(value)
    }
}

fn json_has_nul(value: &Value) -> bool {
    match value {
        Value::String(s) => s.contains('\0'),
        Value::Array(items) => items.iter().any(json_has_nul),
        Value::Object(map) => map.iter().any(|(k, v)| k.contains('\0') || json_has_nul(v)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn clean_json(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(sanitized(s.as_str())),
        Value::Array(items) => Value::Array(items.iter().map(clean_json).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (sanitized(k.as_str()), clean_json(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Text with no NUL, so it can be bound as `text` or inside `jsonb`.
///
/// The field is private and every constructor goes through [`sanitize`], so
/// no `PgText` can carry a NUL.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct PgText(String);

impl PgText {
    pub fn new(text: impl Into<String>) -> Self {
        PgText(sanitized(text))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::ops::Deref for PgText {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for PgText {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for PgText {
    fn from(text: &str) -> Self {
        PgText::new(text)
    }
}

impl From<String> for PgText {
    fn from(text: String) -> Self {
        PgText::new(text)
    }
}

impl From<PgText> for String {
    fn from(text: PgText) -> Self {
        text.0
    }
}

impl std::fmt::Display for PgText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nul_becomes_the_replacement_character_and_nothing_else_changes() {
        assert_eq!(sanitize("a\0b\0"), "a\u{FFFD}b\u{FFFD}");
        assert!(matches!(sanitize("plain é ✓"), Cow::Borrowed("plain é ✓")));
        assert_eq!(sanitized(String::from("x\0")), "x\u{FFFD}");
        assert_eq!(sanitize_opt(Some("\0")).as_deref(), Some("\u{FFFD}"));
        assert_eq!(sanitize_opt(None), None);
    }

    #[test]
    fn a_pg_text_never_holds_nul_however_it_is_made() {
        let made = [
            PgText::new("q\0"),
            PgText::from("q\0"),
            PgText::from(String::from("q\0")),
        ];
        for text in made {
            assert!(!text.contains('\0'));
            assert_eq!(text.as_str(), "q\u{FFFD}");
        }
    }

    #[test]
    fn json_strings_and_keys_are_cleaned_and_clean_json_is_borrowed() {
        let dirty = serde_json::json!({"t\0": ["a\0", {"n": 1, "s": "\0"}], "ok": true});
        let clean = sanitize_json(&dirty);
        assert!(matches!(clean, Cow::Owned(_)));
        assert!(!clean.to_string().contains("\\u0000"));
        assert_eq!(clean["t\u{FFFD}"][0], "a\u{FFFD}");
        assert_eq!(clean["t\u{FFFD}"][1]["s"], "\u{FFFD}");
        assert_eq!(clean["ok"], true);
        let fine = serde_json::json!({"a": ["b", 1, null]});
        assert!(matches!(sanitize_json(&fine), Cow::Borrowed(_)));
    }
}

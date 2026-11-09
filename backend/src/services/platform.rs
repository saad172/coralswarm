//! Agent-platform identity (`SessionKind::Agent`).
//!
//! An agent session carries a `platform` — WHERE the agent runs (Claude Code,
//! Cursor, Codex, ...). Three layers make that value trustworthy without
//! making it a closed set:
//!
//! 1. [`canonicalize_platform`] collapses the many spellings a caller might
//!    send (`"Claude Code"`, `claude_code`, `anthropic`) onto one canonical
//!    slug so filters match by exact equality. Values outside
//!    [`KNOWN_PLATFORMS`] pass through as a normalized slug — a new platform
//!    needs no deploy.
//! 2. [`detect_platform`] identifies the harness from whichever the request
//!    carries: an MCP `clientInfo.name` (SEP-2575, present on every request
//!    once the client sends it — read off the raw per-request `_meta` via
//!    `RequestContext::meta.client_info()`, rmcp 3.1.2, NOT the
//!    `RequestContext::client_info()` convenience method, whose
//!    stateless-mode legacy fallback is a non-`None` synthetic default; see
//!    `crate::mcp::auth::McpAuthContext::mcp_client_name`'s doc for the
//!    verified trap) wins when present, and the `User-Agent` header is the
//!    fallback for callers with none (older harnesses, the REST
//!    `/v1/ingest/atom` path). `clientInfo.name` is run through the SAME
//!    canonicalize/pass-through logic as a declared
//!    `platform` ([`canonicalize_platform`]) rather than the substring
//!    `USER_AGENT_HINTS` table, because it is a clean, self-reported product
//!    name (`"OpenCode"`, `"Hermes Agent"`) rather than a free-form UA string
//!    (`"cursor/2.0 (macOS)"`) — this is exactly what makes clientInfo work
//!    for harnesses whose runtime UA is a generic SDK/language string
//!    (`Bun/1.x`, `node`, `python-httpx/…`) that could never be matched to a
//!    harness by substring. The result is recorded separately
//!    (`coding_sessions.platform_detected`) and never overwrites what the
//!    caller declared; the read side derives `platform_verified = declared
//!    IS NULL OR detected IS NULL OR they agree` from the pair (see
//!    [`platform_verified`]). Reads that need a single platform value
//!    (filtering, faceting, display) fall back to the detected value when
//!    nothing was declared — a Cursor session that never sent `platform`
//!    still lists, filters and facets as `cursor` because its clientInfo or
//!    User-Agent said so (`SessionIdentity::effective_platform`).
//! 3. The ingest policy (in `routes::ingest`): for `agent` sessions a declared
//!    platform wins, an undeclared one falls back to the detected value, and
//!    neither is a 422 — an agent session never lands without a platform.

/// The canonical platform slugs the registry knows aliases for. This is the
/// single list surfaced in MCP tool docs and in the agent-kind 422 message.
/// It is NOT an allowlist: [`canonicalize_platform`] accepts anything.
pub const KNOWN_PLATFORMS: &[&str] = &[
    "claude-code",
    "cursor",
    "codex",
    "grok",
    "openai",
    "gemini-cli",
    "copilot",
    "windsurf",
    // Open-source MCP-SDK harnesses (clientinfo-platform-detection): these
    // run on generic runtime User-Agents (Bun, node, python-httpx, ...) that
    // can never be matched to a harness by `USER_AGENT_HINTS`, so they are
    // only ever named through SEP-2575 `clientInfo.name`. Listed here (rather
    // than left to pass-through) so they show up in `known_platforms_list()`
    // and the MCP tool docs/422 message.
    "opencode",
    "hermes",
    "pi",
    "openclaw",
];

/// Frameworks that are autonomous by construction — a human never types into
/// them — so their clientInfo NAME alone is enough for `routes::ingest`'s
/// session-kind inference (session-kind-inference plan, decision 3) to
/// default an undeclared session to [`crate::ingest::atom::SessionKind::Agent`]
/// with no other signal (`agent_name`, `parent_session_id`). A CODING harness
/// running headless — no human present, or driven by another agent — is
/// deliberately NOT inferred this way: this list is closed and reviewed by
/// hand, never derived from "no human at the keyboard" or any other runtime
/// signal, because that heuristic would misclassify every unattended CI/cron
/// invocation of an ordinary coding harness. A member of [`KNOWN_PLATFORMS`]
/// on purpose (every framework here already has a canonical platform slug —
/// membership here only ADDS the kind-inference behavior, it doesn't grant a
/// platform identity that didn't already exist).
///
/// Checked against the canonicalized clientInfo name ONLY — see
/// [`is_agent_framework_client_info`] — never against the `User-Agent`
/// fallback [`detect_platform`] may otherwise use, because a generic runtime
/// UA (`node`, `Bun/1.x`, `python-httpx/…`) shared with countless other
/// clients must never itself imply an agent session.
pub const AGENT_FRAMEWORKS: &[&str] = &["openclaw"];

/// `(alias, canonical)` pairs, matched against the NORMALIZED slug of the
/// caller's value (see [`normalize_slug`]). Canonical names map to
/// themselves implicitly and need no entry here.
const ALIASES: &[(&str, &str)] = &[
    ("claude", "claude-code"),
    ("claudecode", "claude-code"),
    ("claude-code-cli", "claude-code"),
    ("anthropic", "claude-code"),
    // NOT an alias: Claude Desktop is a different product from Claude Code
    // (both Anthropic-built, but distinct clients) — a caller/clientInfo
    // spelling of "Claude Desktop" passes through as its own slug rather
    // than being folded into `claude-code`'s numbers.
    ("cursor-agent", "cursor"),
    ("cursor-cloud", "cursor"),
    ("cursor-cloud-agent", "cursor"),
    ("cursor-ide", "cursor"),
    ("cursor-vscode", "cursor"),
    ("openai-codex", "codex"),
    ("codex-cli", "codex"),
    ("xai", "grok"),
    ("grok-code", "grok"),
    ("chatgpt", "openai"),
    ("gemini", "gemini-cli"),
    ("google-gemini", "gemini-cli"),
    ("github-copilot", "copilot"),
    ("copilot-cli", "copilot"),
    ("codeium", "windsurf"),
];

/// `(User-Agent substring, canonical)` pairs for the `User-Agent` fallback
/// path in [`detect_platform`]. Matched case-insensitively, first hit wins,
/// so more specific tokens go first. Deliberately has NO entries for the
/// open-source MCP-SDK harnesses added to [`KNOWN_PLATFORMS`]
/// (`opencode`, `hermes`, `pi`, `openclaw`): their runtime User-Agents are
/// generic SDK/language strings (`Bun/1.x`, `node`, `python-httpx/…`) shared
/// with countless other clients, so a substring hint here would misattribute
/// every other Bun/Node/httpx caller to one of them. Those harnesses are
/// named only through `clientInfo.name` (`canonicalize_platform`'s
/// alias/pass-through logic) — the entire reason `detect_platform` grew a
/// `clientInfo` input.
const USER_AGENT_HINTS: &[(&str, &str)] = &[
    ("claude-code", "claude-code"),
    ("claude_code", "claude-code"),
    ("claudecode", "claude-code"),
    ("claude-cli", "claude-code"),
    ("cursor", "cursor"),
    ("codex", "codex"),
    ("windsurf", "windsurf"),
    ("codeium", "windsurf"),
    ("gemini", "gemini-cli"),
    ("copilot", "copilot"),
    ("grok", "grok"),
    ("openai", "openai"),
];

/// Longest a canonicalized platform slug may be; longer input is truncated
/// (by chars) after normalization so a pathological value cannot bloat the
/// column or an index.
pub const PLATFORM_SLUG_CAP: usize = 64;

/// Lowercase, trim, and collapse every run of whitespace / `_` / `.` / `/`
/// / `:` into a single `-`, dropping any character that is not ASCII
/// alphanumeric or `-`. `"Claude Code"` → `claude-code`, `"cursor_cloud"` →
/// `cursor-cloud`, `"  Codex  "` → `codex`. Returns an empty string when
/// nothing survives.
pub fn normalize_slug(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut pending_dash = false;
    for ch in raw.trim().chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            if pending_dash && !out.is_empty() {
                out.push('-');
            }
            pending_dash = false;
            out.push(lower);
        } else if lower == '-' || lower.is_whitespace() || matches!(lower, '_' | '.' | '/' | ':' | '+') {
            pending_dash = true;
        }
        // Anything else (punctuation, non-ASCII) is dropped without
        // producing a separator so `"Claude (Code)"` still yields
        // `claude-code`.
    }
    out.chars().take(PLATFORM_SLUG_CAP).collect()
}

/// Canonicalize a caller-declared platform. `None` when the input is empty
/// after normalization (a caller sending `""` or `"   "` declared nothing).
pub fn canonicalize_platform(raw: &str) -> Option<String> {
    let slug = normalize_slug(raw);
    if slug.is_empty() {
        return None;
    }
    if KNOWN_PLATFORMS.contains(&slug.as_str()) {
        return Some(slug);
    }
    if let Some((_, canonical)) = ALIASES.iter().find(|(alias, _)| *alias == slug) {
        return Some((*canonical).to_string());
    }
    Some(slug)
}

/// Best-effort platform detection for a request, preferring the MCP
/// `clientInfo.name` SEP-2575 carries (this request's raw `_meta`, via
/// `RequestContext::meta.client_info()` — see the module doc for why NOT
/// the `RequestContext::client_info()` convenience method) over the HTTP
/// `User-Agent` the auth layer captured (`mcp_client_name` /
/// `trusted_user_agent`): `client_info_name` wins whenever it canonicalizes
/// to a non-empty slug, `user_agent` is only consulted when it is absent —
/// the closed set of harnesses that predate clientInfo threading (or any
/// caller with no MCP context at all, e.g. `/v1/ingest/atom`) still get
/// detected exactly as before. `None` only when NEITHER input names a
/// harness the registry knows (an empty/whitespace `client_info_name` counts
/// as absent, same as `None`) — callers must then rely on a declared value.
///
/// Returns an OWNED `String`, unlike the old `&'static str`: `client_info_name`
/// is canonicalized via [`canonicalize_platform`], which passes an unknown
/// name through as a normalized slug rather than mapping onto one of a fixed
/// set of static strings.
pub fn detect_platform(client_info_name: Option<&str>, user_agent: Option<&str>) -> Option<String> {
    if let Some(slug) = client_info_name.and_then(canonicalize_platform) {
        return Some(slug);
    }
    detect_platform_from_user_agent(user_agent?)
}

/// The `User-Agent`-only half of [`detect_platform`], split out as its own
/// `pub` function for any caller that has no `clientInfo` to read at all
/// (today: `detect_platform`'s own fallback branch; kept public rather than
/// private so a future non-MCP transport can call it directly without
/// threading a spurious `None` through `detect_platform`). Matched
/// case-insensitively against [`USER_AGENT_HINTS`], first hit wins. `None`
/// when the string names no harness the registry knows.
pub fn detect_platform_from_user_agent(user_agent: &str) -> Option<String> {
    let lowered = user_agent.trim().to_ascii_lowercase();
    if lowered.is_empty() {
        return None;
    }
    USER_AGENT_HINTS
        .iter()
        .find(|(needle, _)| lowered.contains(needle))
        .map(|(_, canonical)| canonical.to_string())
}

/// `"claude-code, cursor, codex, ..."` — [`KNOWN_PLATFORMS`] as it appears in
/// error messages and MCP parameter docs.
pub fn known_platforms_list() -> String {
    KNOWN_PLATFORMS.join(", ")
}

/// Does THIS REQUEST'S clientInfo name — canonicalized the same way a
/// declared `platform` is, via [`canonicalize_platform`] — belong to
/// [`AGENT_FRAMEWORKS`]? Used only by `routes::ingest`'s session-kind
/// inference's per-client default (decision 3): deliberately takes the
/// clientInfo name alone, never the `User-Agent` fallback [`detect_platform`]
/// would otherwise consult, so a generic runtime UA shared with unrelated
/// clients can never trigger it. `false` for `None`/blank input, same as
/// every other "absent counts as no signal" rule in this module.
pub fn is_agent_framework_client_info(client_info_name: Option<&str>) -> bool {
    client_info_name
        .and_then(canonicalize_platform)
        .is_some_and(|slug| AGENT_FRAMEWORKS.contains(&slug.as_str()))
}

/// Does the recorded detection agree with the declared value? A missing
/// declaration is not a mismatch (there is nothing to disagree WITH), and
/// neither is a missing detection (there is nothing to disagree ABOUT) — only
/// an explicit `Some(p) != Some(d)` pair flags a session as unverified. In
/// particular `(None, Some(_))` — a harness that declared no platform but
/// whose User-Agent was detected as one, e.g. every pre-fix Cursor coding
/// session — is verified: the detected value becomes the effective platform
/// (see [`crate::store::SessionIdentity::effective_platform`]) and there is
/// no declared value for it to contradict.
pub fn platform_verified(platform: Option<&str>, platform_detected: Option<&str>) -> bool {
    match (platform, platform_detected) {
        (None, _) | (_, None) => true,
        (Some(p), Some(d)) => p == d,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_platforms_are_canonical_slugs() {
        for p in KNOWN_PLATFORMS {
            assert_eq!(normalize_slug(p), *p, "{p} is not in canonical slug form");
            assert_eq!(canonicalize_platform(p).as_deref(), Some(*p));
        }
        for (alias, canonical) in ALIASES {
            assert!(
                KNOWN_PLATFORMS.contains(canonical),
                "alias {alias} points at unknown canonical {canonical}"
            );
            assert_eq!(normalize_slug(alias), *alias, "alias {alias} must be pre-normalized");
        }
        for (_, canonical) in USER_AGENT_HINTS {
            assert!(KNOWN_PLATFORMS.contains(canonical));
        }
    }

    #[test]
    fn canonicalize_collapses_aliases_and_spellings() {
        assert_eq!(canonicalize_platform("Claude Code").as_deref(), Some("claude-code"));
        assert_eq!(canonicalize_platform("claude_code").as_deref(), Some("claude-code"));
        assert_eq!(canonicalize_platform("claudecode").as_deref(), Some("claude-code"));
        assert_eq!(canonicalize_platform("anthropic").as_deref(), Some("claude-code"));
        assert_eq!(canonicalize_platform("Cursor").as_deref(), Some("cursor"));
        assert_eq!(canonicalize_platform("cursor-cloud").as_deref(), Some("cursor"));
        assert_eq!(canonicalize_platform("cursor_agent").as_deref(), Some("cursor"));
        assert_eq!(canonicalize_platform("OpenAI Codex").as_deref(), Some("codex"));
        assert_eq!(canonicalize_platform("codex-cli").as_deref(), Some("codex"));
        assert_eq!(canonicalize_platform("  grok ").as_deref(), Some("grok"));
    }

    #[test]
    fn canonicalize_passes_unknown_values_through_as_slugs() {
        assert_eq!(canonicalize_platform("My Custom Bot v2").as_deref(), Some("my-custom-bot-v2"));
        assert_eq!(canonicalize_platform("acme/orchestrator").as_deref(), Some("acme-orchestrator"));
        assert_eq!(canonicalize_platform("").as_deref(), None);
        assert_eq!(canonicalize_platform("   ").as_deref(), None);
        assert_eq!(canonicalize_platform("!!!").as_deref(), None);
    }

    #[test]
    fn canonicalize_caps_length() {
        let long = "x".repeat(500);
        let slug = canonicalize_platform(&long).unwrap();
        assert_eq!(slug.chars().count(), PLATFORM_SLUG_CAP);
    }

    #[test]
    fn detect_platform_reads_user_agent_shapes_when_no_client_info() {
        assert_eq!(detect_platform(None, Some("claude-code/2.1.263 (cli)")).as_deref(), Some("claude-code"));
        assert_eq!(detect_platform(None, Some("Claude-Code/2.1 (macOS)")).as_deref(), Some("claude-code"));
        assert_eq!(detect_platform(None, Some("cursor/2.0")).as_deref(), Some("cursor"));
        assert_eq!(detect_platform(None, Some("Cursor-Cloud-Agent/1.4")).as_deref(), Some("cursor"));
        assert_eq!(detect_platform(None, Some("codex-cli 0.9")).as_deref(), Some("codex"));
        assert_eq!(detect_platform(None, Some("Windsurf/1.0")).as_deref(), Some("windsurf"));
        assert_eq!(detect_platform(None, Some("node-fetch/1.0")), None);
        assert_eq!(detect_platform(None, Some("my-harness")), None);
        assert_eq!(detect_platform(None, Some("")), None);
        assert_eq!(detect_platform(None, None), None);
    }

    /// clientInfo wins whenever it names a harness (canonicalizes to a
    /// non-empty slug), even when the User-Agent independently identifies a
    /// DIFFERENT harness — this is the OpenCode/Hermes/Pi case: their
    /// runtime UA (`Bun/1.x`, `python-httpx/…`) is worthless, but their
    /// clientInfo name is authoritative.
    #[test]
    fn detect_platform_prefers_client_info_over_user_agent() {
        assert_eq!(
            detect_platform(Some("OpenCode"), Some("Bun/1.1.30")).as_deref(),
            Some("opencode"),
            "clientInfo names a harness the UA never could"
        );
        assert_eq!(
            detect_platform(Some("opencode"), Some("cursor/2.0")).as_deref(),
            Some("opencode"),
            "clientInfo wins even when the UA suggests a DIFFERENT known platform"
        );
    }

    /// An empty/whitespace-only `client_info_name` counts as absent, not as
    /// a value that wins with an empty slug — the User-Agent fallback still
    /// runs.
    #[test]
    fn detect_platform_falls_back_to_user_agent_when_client_info_is_blank() {
        assert_eq!(detect_platform(Some(""), Some("cursor/2.0")).as_deref(), Some("cursor"));
        assert_eq!(detect_platform(Some("   "), Some("cursor/2.0")).as_deref(), Some("cursor"));
        assert_eq!(detect_platform(Some(""), None), None);
    }

    /// clientInfo names are canonicalized through the SAME alias table a
    /// declared `platform` uses, not the substring `USER_AGENT_HINTS` table.
    #[test]
    fn detect_platform_canonicalizes_known_client_info_spellings() {
        assert_eq!(detect_platform(Some("Cursor VSCode"), None).as_deref(), Some("cursor"));
        assert_eq!(detect_platform(Some("cursor-agent"), None).as_deref(), Some("cursor"));
        assert_eq!(detect_platform(Some("OpenAI Codex"), None).as_deref(), Some("codex"));
        assert_eq!(detect_platform(Some("claude-code"), None).as_deref(), Some("claude-code"));
    }

    /// An unknown clientInfo name passes through as its own normalized slug
    /// (no denylist) — the entire point of this design: a brand-new harness
    /// needs no deploy to show up as its own platform.
    #[test]
    fn detect_platform_passes_through_unknown_client_info_names() {
        assert_eq!(detect_platform(Some("OpenCode"), None).as_deref(), Some("opencode"));
        assert_eq!(detect_platform(Some("Hermes Agent"), None).as_deref(), Some("hermes-agent"));
        assert_eq!(detect_platform(Some("Claude Desktop"), None).as_deref(), Some("claude-desktop"));
    }

    #[test]
    fn detect_platform_caps_a_pass_through_client_info_name() {
        let long = "x".repeat(500);
        let slug = detect_platform(Some(&long), None).expect("some slug");
        assert_eq!(slug.chars().count(), PLATFORM_SLUG_CAP);
    }

    #[test]
    fn detect_platform_from_user_agent_matches_the_public_helper_directly() {
        assert_eq!(detect_platform_from_user_agent("cursor/2.0").as_deref(), Some("cursor"));
        assert_eq!(detect_platform_from_user_agent("my-harness"), None);
    }

    /// Full four-arm truth table: verified is false ONLY when both a
    /// declared and a detected platform are present and disagree. A missing
    /// declaration (the Cursor case: `(None, Some("cursor"))`) is verified,
    /// not unverified — that arm flipped when `effective_platform` shipped so
    /// a session that declared nothing no longer renders as "unverified"
    /// just because the server managed to detect it.
    #[test]
    fn platform_verified_only_flags_explicit_mismatch() {
        // (Some, Some) agree -> verified.
        assert!(platform_verified(Some("claude-code"), Some("claude-code")));
        // (Some, Some) disagree -> the only false arm.
        assert!(!platform_verified(Some("grok"), Some("claude-code")));
        // (Some, None) -> nothing detected to disagree with -> verified.
        assert!(platform_verified(Some("claude-code"), None));
        // (None, Some) -> nothing declared to disagree WITH -> verified.
        assert!(platform_verified(None, Some("claude-code")));
        // (None, None) -> verified.
        assert!(platform_verified(None, None));
    }

    #[test]
    fn known_platforms_list_is_comma_joined() {
        let list = known_platforms_list();
        assert!(list.starts_with("claude-code, cursor"));
        assert_eq!(list.split(", ").count(), KNOWN_PLATFORMS.len());
    }

    /// [`AGENT_FRAMEWORKS`] is a closed, hand-reviewed subset of
    /// [`KNOWN_PLATFORMS`] — every framework named here must already be a
    /// canonical platform slug, or `is_agent_framework_client_info` could
    /// match a value `canonicalize_platform` would never actually produce.
    #[test]
    fn agent_frameworks_are_a_subset_of_known_platforms() {
        for framework in AGENT_FRAMEWORKS {
            assert!(
                KNOWN_PLATFORMS.contains(framework),
                "{framework} must also be a KNOWN_PLATFORMS entry"
            );
        }
    }

    #[test]
    fn is_agent_framework_client_info_matches_only_the_closed_list() {
        assert!(is_agent_framework_client_info(Some("openclaw")));
        assert!(is_agent_framework_client_info(Some("OpenClaw")), "canonicalized case-insensitively");
        assert!(is_agent_framework_client_info(Some("  openclaw  ")), "canonicalized like any other spelling");
        // A coding harness is never an agent framework just because it's
        // also in KNOWN_PLATFORMS.
        assert!(!is_agent_framework_client_info(Some("opencode")));
        assert!(!is_agent_framework_client_info(Some("claude-code")));
        assert!(!is_agent_framework_client_info(Some("cursor")));
        // Absent/blank is "no signal", not a match.
        assert!(!is_agent_framework_client_info(None));
        assert!(!is_agent_framework_client_info(Some("")));
        assert!(!is_agent_framework_client_info(Some("   ")));
    }
}

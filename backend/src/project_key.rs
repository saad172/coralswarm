//! Pinned, versioned project-key normalizer (coding-session capture, Phase 1,
//! audit amendment A5).
//!
//! `coding_sessions.project_key` (migration 0076) is a canonical identity for
//! "which repo/project is this coding session about", derived from a git
//! remote URL (or, when no remote is available, a sanitized fallback such as
//! the working-directory basename). It is **atom-stamped and frozen forever**
//! (audit P1-4: COALESCE-only stamping, excluded from atom sync, no bulk
//! rekey path) — so the mapping from raw input to key must never silently
//! change shape once a version has shipped. That's why this module pins a
//! version constant ([`PROJECT_KEY_NORMALIZER_VERSION`]) and every future
//! change to the algorithm bumps it rather than mutating v1's behavior
//! in-place; the `projects` alias table (same migration) is the intended
//! absorption layer for reconciling v1 vs v2 keys, never this module.
//!
//! # The P0: credential stripping
//!
//! `ingest.rs`'s `validate_provenance_label` checks only NUL bytes + a length
//! cap — a raw `https://x-access-token:ghp_xxxx@github.com/org/repo.git`
//! remote passes that validator **verbatim** and would otherwise be stored,
//! readable by every account member. [`normalize_remote`] unconditionally
//! strips userinfo (everything before an `@` in the authority component)
//! before anything else touches the string, so a credential-bearing URL is
//! structurally unable to reach storage through this path. This is run in
//! two places (defense in depth, audit A5): client-side in the future
//! capture hook (Phase 2, not built here) AND unconditionally server-side in
//! `add_context` — a caller can never opt out of server-side normalization by
//! claiming its input is "already normalized".
//!
//! # What normalization does (v1)
//!
//! 1. Strip userinfo (`user:pass@` / `user@`) — CRITICAL, see above.
//! 2. Fold SCP-style syntax (`git@github.com:org/repo.git`) into the same
//!    shape as an HTTPS remote (`github.com/org/repo`) before the rest of the
//!    pipeline runs, so both forms of the same remote converge on one key.
//! 3. Strip the scheme (`https://`, `ssh://`, `git://`, `git+ssh://`, ...).
//! 4. Strip an explicit port (`:22`, `:443`, ...).
//! 5. Lowercase the host (git hosting is host-case-insensitive; path case is
//!    preserved — GitHub/GitLab paths ARE case-sensitive in practice).
//! 6. Strip a trailing `.git` suffix and any trailing `/`.
//!
//! Output shape: `"{host}/{path}"`, e.g. `"github.com/coralswarm/coralswarm"`.
//!
//! # Fallback (no remote available)
//!
//! [`normalize_fallback`] sanitizes a plain folder basename (or any other
//! non-URL label) for use as a project key when the caller has no git remote
//! at all (a non-git working directory, or git present with no `origin`).
//! It applies the same lowercasing + trim + `.git` stripping, but never
//! attempts host/path URL parsing.

/// Version of the normalization algorithm below. Bump this — and add a new
/// `normalize_remote_v{N}` / branch — any time the v1 rules below change in a
/// way that would produce a different key for the same input. Never mutate
/// v1's behavior in place once shipped (see module doc).
///
/// Not read anywhere yet in Phase 1 (`coding_sessions`/`projects` have no
/// normalizer-version column — the pin's job right now is purely to freeze
/// v1's *behavior*, not to be persisted). Reserved for the capture hook
/// (Phase 2) and any future re-key/alias tooling that needs to know which
/// algorithm version produced an existing `project_key`.
///
/// v2 (CodeRabbit reviews on PRs #246/#247): reject Windows drive-letter paths
/// (`C:/...` and `C:\...`) outright, and extend the final output invariant to
/// forbid `?`/`#`/`=` (query-string / fragment credential forms) in addition
/// to `@`/`:`. Both changes turn previously-emitted (unsafe) keys into `None`,
/// so this bumps the version per the freeze-forever contract above. Must stay
/// in lockstep with the JS mirror `PROJECT_KEY_NORMALIZER_VERSION` in
/// `hooks/project-key.mjs` in github.com/CoralSwarm/coralswarm-connect.
///
/// v3 (CodeRabbit re-review on PR #247): the v2 drive-letter guard only saw the
/// RAW input, so a scheme-prefixed drive path — `file://C:/home/dev/repo` —
/// slipped past it and still minted fake host `c`. v3 re-applied the guard to
/// the post-scheme-strip string.
///
/// v4 (CodeRabbit re-review on PR #247, round 3): a drive letter hidden behind
/// USERINFO — `ssh://user@C:/x`, `user@C:\x` — still slipped past v3, because
/// the guard ran before userinfo stripping revealed the drive letter. v4 is a
/// STRUCTURAL fix, not another patch: userinfo is now stripped BEFORE
/// authority/path splitting, and the drive-letter guard runs once on the
/// fully-revealed residue (after both scheme AND userinfo removal), immediately
/// before the port-strip that would erase the drive colon. `fold_scp_syntax`
/// also learns to reject a userinfo-cloaked single-letter drive host. Bumped in
/// lockstep with the JS mirror.
///
/// v5 (CodeRabbit Data-Integrity review on PR #246): a bracketed IPv6 authority
/// — `ssh://[2001:db8::1]/team/repo`, `[::1]/x` — minted the fake host `[2001`
/// (the `[` was not in the forbidden-char set, and the inner `:` was eaten by
/// the port-split). v5 rejects any authority starting with `[` (IPv6 is out of
/// scope for v1) and adds `[`/`]` to the forbidden-char output invariant as
/// belt-and-braces. Bumped in lockstep with the JS mirror.
#[allow(dead_code)]
pub const PROJECT_KEY_NORMALIZER_VERSION: u32 = 5;

/// Normalize a raw git remote URL (HTTPS, SSH, git://, or SCP-style) into a
/// canonical project key of the shape `"{host}/{path}"`.
///
/// Returns `None` when the input is empty/whitespace-only, when nothing
/// resembling a host survives normalization (e.g. a bare local filesystem
/// path with no `/` separators recognizable as `host/path`) — callers should
/// fall back to [`normalize_fallback`] in that case — OR when the final
/// invariant below trips.
///
/// # Security
///
/// Userinfo (credentials embedded in the URL authority) is ALWAYS stripped,
/// unconditionally, before any other transform runs. This is not optional
/// and has no bypass.
///
/// As a SECOND, independent line of defense (audit P0, CodeRabbit review on
/// PR #246 — a slash embedded inside a credential, e.g.
/// `"user:pa/ss@host/org/repo"`, can defeat the step-by-step stripping above
/// no matter where in the pipeline the naive "split at the first `/`" lands:
/// both the SCP-fold and the scheme-based authority/path split below make
/// that same simplifying assumption, and a `/` inside the password breaks
/// it either way), the FINAL computed key is checked before ever being
/// returned: if it still contains `@`, `:`, `?`, `#`, or `=` ANYWHERE, this
/// returns `None` instead — a project key is structurally forbidden from
/// carrying any of those characters, full stop, regardless of which step let
/// it through (`?`/`#`/`=` cover the query-string/fragment credential forms
/// such as `repo?access_token=secret`, CodeRabbit review on PR #247). This
/// can produce a false-negative on a legitimate remote whose path segment
/// happens to contain one of them (vanishingly rare) — that trade-off is
/// intentional: dropping a usable key to `None` is always safer than risking
/// a credential leak, and the basename fallback ([`normalize_fallback`])
/// still covers keying in that case.
pub fn normalize_remote(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    // ── Step 0: reject a Windows drive-letter path ("C:\..." OR "C:/...")
    // OUTRIGHT, before any splitting (P0, CodeRabbit review on PR #246). The
    // backslash form ("C:\Users\...") is already refused by fold_scp_syntax's
    // drive-letter guard and then produces no usable key, but the FORWARD-slash
    // form ("C:/home/dev/repo") slips past that guard and flows into the
    // scheme-based path: split_once('/') yields authority "C:", the port-strip
    // discards ":port" leaving host "c", and the drive letter is emitted as a
    // fake host ("c/home/dev/repo"). A local filesystem path is never a git
    // remote — reject both slash directions here so neither can mint a key.
    if is_windows_drive_path(raw) {
        return None;
    }

    // ── Step 1: fold SCP-style syntax ("git@host:path" or "host:path", no
    // scheme, no "//") into "host/path" BEFORE scheme/userinfo stripping, so
    // the rest of the pipeline only ever has to deal with one shape. Must NOT
    // fire on an already-schemed URL ("https://host/path") or on a Windows
    // drive letter path ("C:\..." / "C:/...") — both contain a ':' too.
    let folded: String = if let Some(scp) = fold_scp_syntax(raw) {
        scp
    } else {
        raw.to_string()
    };

    // ── Step 2: strip scheme, if any ("https://", "ssh://", "git://",
    // "git+ssh://", "ssh+git://", ...) — anything matching "^[a-zA-Z][a-zA-Z0-9+.-]*://".
    let without_scheme = strip_scheme(&folded);

    // ── Step 3: strip userinfo (CRITICAL, P0) BEFORE authority/path splitting.
    // Userinfo is everything up to and including the LAST '@' in the authority
    // component (the segment before the first '/'), so even a pathological
    // "user@name:pass@host" keeps only the real host. Doing this here — rather
    // than after the split — is what lets the structural guard below run on the
    // FULLY-REVEALED residue.
    let residue = strip_userinfo(&without_scheme);

    // ── Step 3b: STRUCTURAL drive-letter guard (P0, CodeRabbit reviews on PRs
    // #246/#247, three rounds). Scheme stripping AND userinfo stripping are the
    // only two transforms that can UNCOVER a drive-letter path that was hidden
    // behind a prefix — a scheme ("file://C:/x"), userinfo ("ssh://user@C:/x"),
    // or both. This single guard runs on the residue AFTER both of those
    // reveals and immediately BEFORE the port-strip (step 5) that would
    // otherwise destroy the evidence by eating the drive colon. It therefore
    // catches every position the drive letter can hide in — bare, schemed,
    // and userinfo-cloaked, in either slash direction — instead of playing
    // whack-a-mole with one patch per uncovered case. (Step 0 above is a cheap
    // early-out for the common bare form; this is the load-bearing check.)
    if is_windows_drive_path(&residue) {
        return None;
    }

    // ── Step 4: split authority (host[:port]) from path. The residue no longer
    // carries userinfo (stripped in step 3), so authority is just "host[:port]".
    let (authority, path) = match residue.split_once('/') {
        Some((a, p)) => (a, p),
        None => (residue.as_str(), ""),
    };
    if authority.is_empty() {
        return None;
    }

    // ── Step 4b: reject a bracketed IPv6 authority ("[2001:db8::1]",
    // "[::1]") OUTRIGHT (P0, CodeRabbit Data-Integrity review on PR #246). An
    // IPv6 literal is never a git-hosting identity, and its inner ':' would be
    // swallowed by the naive port-split below — "[2001:db8::1]/team/repo"
    // otherwise mints the fake host "[2001" (the '[' survives because it is not
    // in the forbidden-char set). IPv6 remotes are explicitly OUT OF SCOPE for
    // v1: bail rather than emit a mangled, non-canonical key that could never
    // round-trip. The '[' start is the unambiguous signal.
    if authority.starts_with('[') {
        return None;
    }

    // ── Step 5: strip an explicit port ("host:22" -> "host"). IPv6 literals
    // ("[::1]:22") are rejected above (step 4b) and out of scope for v1 — the
    // colon-split below is intentionally simple.
    let host = match authority.split_once(':') {
        Some((h, _port)) => h,
        None => authority,
    };
    if host.is_empty() {
        return None;
    }

    // ── Step 6: lowercase the host only — git hosting is host-case-
    // insensitive; the path (owner/repo) is NOT lowercased, since GitHub/
    // GitLab paths are case-sensitive in practice (two different repos can
    // differ only by case).
    let host = host.to_lowercase();

    // ── Step 7: clean the path — trim slashes, strip a trailing ".git". ─────
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    let path = path.trim_matches('/');

    if path.is_empty() {
        // A bare host with no path isn't a usable project key.
        return None;
    }

    let key = format!("{host}/{path}");

    // ── Final output invariant (P0 defense in depth, see doc comment above):
    // a project key must be STRUCTURALLY incapable of carrying any character a
    // credential can ride in on — '@' and ':' (userinfo), plus '?' and '#'
    // (query string / fragment) and '=' (key=value pair, e.g.
    // "repo?access_token=secret" or "repo#token=secret"). A git remote's repo
    // identity never legitimately contains '?', '#', or '='; those only appear
    // when a credential/query rides along, so any surviving one means the key
    // is unsafe. Every step above is believed to already guarantee this, but
    // this check does not trust that belief — it re-verifies the actual
    // output, independent of which step (if any) had a gap.
    if key_has_forbidden_char(&key) {
        return None;
    }

    Some(key)
}

/// True if `s` contains any character a project key is structurally forbidden
/// from carrying: userinfo delimiters (`@`, `:`), query/fragment/pair delimiters
/// (`?`, `#`, `=`), or IPv6-authority brackets (`[`, `]`). Shared by
/// [`normalize_remote`]'s final output invariant and [`normalize_fallback`] so
/// both reject the exact same set. The brackets are belt-and-braces for the
/// step-4b IPv6 rejection (CodeRabbit reviews on PRs #246/#247): a git-hosting
/// identity never legitimately contains `[`/`]`, and forbidding them in the
/// output catches any bracket that slips through the path segment too, not just
/// the authority.
fn key_has_forbidden_char(s: &str) -> bool {
    s.contains(['@', ':', '?', '#', '=', '[', ']'])
}

/// True if `raw` is a Windows drive-letter path — `^[A-Za-z]:[/\\]`, i.e. a
/// single ASCII letter, a colon, then a slash in either direction (`C:/...`
/// or `C:\...`). Such a local filesystem path is never a git remote and must
/// not be allowed to mint a project key (CodeRabbit reviews on PRs #246/#247).
fn is_windows_drive_path(raw: &str) -> bool {
    let mut chars = raw.chars();
    match (chars.next(), chars.next(), chars.next()) {
        (Some(letter), Some(':'), Some(sep)) => {
            letter.is_ascii_alphabetic() && (sep == '/' || sep == '\\')
        }
        _ => false,
    }
}

/// Strip userinfo (`user@` / `user:pass@`) from the authority segment (the part
/// before the first `/`) of a scheme-stripped remote, returning the residue
/// with the credential removed and the path preserved verbatim. Everything up
/// to and including the LAST `@` in that segment is dropped, so
/// `user@name:pass@host/path` -> `host/path`. Splitting userinfo off HERE,
/// before authority/path splitting, is what lets [`normalize_remote`]'s
/// structural drive-letter guard run on the fully-revealed residue. Mirror of
/// the JS `stripUserinfo`.
fn strip_userinfo(s: &str) -> String {
    match s.split_once('/') {
        Some((authority, path)) => {
            let host = match authority.rfind('@') {
                Some(idx) => &authority[idx + 1..],
                None => authority,
            };
            format!("{host}/{path}")
        }
        None => match s.rfind('@') {
            Some(idx) => s[idx + 1..].to_string(),
            None => s.to_string(),
        },
    }
}

/// Sanitize a non-URL fallback input (e.g. a working-directory basename) into
/// a project key when no git remote is available at all. Lowercases,
/// trims whitespace/slashes, and strips a trailing `.git` — never attempts
/// URL/host parsing (an arbitrary folder name is not expected to look like
/// one).
///
/// Returns `None` for empty/whitespace-only input, OR when the cleaned
/// input still contains `@`/`:` (P0 defense in depth — same final-output
/// invariant [`normalize_remote`] enforces, see its doc comment). A folder
/// basename legitimately needing either character is vanishingly rare; the
/// value of guaranteeing this fallback path can NEVER be the channel a
/// credential-bearing string (e.g. a caller mistakenly passing a raw remote
/// URL into `repo_path` instead of `remote`) reaches a stored `project_key`
/// through outweighs preserving that edge case. Callers get `project_key =
/// None` instead — session capture still works without one.
pub fn normalize_fallback(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let lowered = trimmed.to_lowercase();
    let cleaned = lowered.strip_suffix(".git").unwrap_or(&lowered);
    let cleaned = cleaned.trim_matches('/');
    if cleaned.is_empty() {
        return None;
    }
    // Same forbidden-char invariant as normalize_remote's final output check:
    // reject '@'/':'/'?'/'#'/'=' outright (P0 defense in depth) so a caller
    // mistakenly feeding a credential- or query-bearing string down the
    // fallback path can never mint a key carrying it.
    if key_has_forbidden_char(cleaned) {
        return None;
    }
    Some(cleaned.to_string())
}

/// Detect and fold SCP-style git syntax (`[user@]host:path`, e.g.
/// `git@github.com:coralswarm/coralswarm.git`) into `host/path`, matching the
/// shape [`normalize_remote`]'s scheme/userinfo/path steps expect.
///
/// Must NOT fire on:
/// - An already-schemed URL (`"https://host/path"`, `"ssh://host/path"`) —
///   those contain `"://"`, which this checks for first.
/// - A Windows drive-letter local path (`"C:\Users\..."` / `"C:/home/dev"`)
///   — the single letter before `:` is rejected as "too short to be a host".
/// - A bare local filesystem path with no `:` at all — `find(':')` returns
///   `None` and this is a no-op.
/// - **Credential-bearing schemeless input** (`"user:pass@host/path"`,
///   `"token:secret@host:2222/repo"`, or a credential containing its OWN
///   `/` such as `"user:pa/ss@host/org/repo"`) — CRITICAL, see below.
///
/// # The bypass this guards against (P0, CodeRabbit review on PR #246, two rounds)
///
/// Genuine SCP syntax is `[user@]host:path` — when userinfo is present, the
/// `@` sits in the AUTHORITY, strictly BEFORE the colon (`git@github.com:
/// org/repo`). A schemeless `user:pass@host/path` URL-shaped input instead
/// has its `@` strictly AFTER the first colon, in what would become the
/// "path" half of an SCP split. Folding that anyway produces
/// `"user/pass@host/path"` — the credential text survives intact in the
/// output, defeating [`normalize_remote`]'s whole userinfo-stripping
/// contract, because folding runs BEFORE the authority `@`-split that would
/// otherwise have stripped it. This function therefore refuses to fold
/// whenever an `@` appears ANYWHERE after the colon (not merely in the
/// first `/`-delimited segment — an earlier version of this fix only
/// checked the first segment, which a credential containing its own `/`
/// — e.g. `"user:pa/ss@host/org/repo"` — slipped past, since the `@`
/// then lands in the SECOND segment). That shape is never genuine SCP
/// syntax. The caller's normal (unfolded) path is intended to handle it
/// correctly instead via the authority `@`-split in [`normalize_remote`] —
/// though see that function's own doc comment: a slash-containing
/// credential can ALSO defeat the unfolded path's naive first-`/` authority
/// split, which is why [`normalize_remote`] additionally re-verifies its
/// own final output rather than relying on this function alone.
fn fold_scp_syntax(raw: &str) -> Option<String> {
    if raw.contains("://") {
        return None;
    }
    let colon = raw.find(':')?;
    // Reject drive letters ("C:", "D:") — a single alphabetic char before the
    // colon is never a plausible SCP host.
    if colon < 2 {
        return None;
    }
    let (authority, rest) = raw.split_at(colon);
    // Reject a drive letter that is HIDDEN BEHIND USERINFO ("user@C:\x",
    // "user:pass@D:/repo") — CodeRabbit re-review on PR #247, round 3. The
    // `colon < 2` check above only catches a BARE leading drive letter; once a
    // "user@" prefix is present the colon is no longer near the start, so strip
    // the SCP authority's own userinfo and re-test: if what remains is a single
    // ASCII letter, it's a drive letter, not a host — never SCP syntax, refuse
    // to fold. (Genuine SCP authorities like "git@github.com" are multi-char.)
    let scp_host = match authority.rfind('@') {
        Some(idx) => &authority[idx + 1..],
        None => authority,
    };
    if scp_host.len() == 1 && scp_host.chars().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    let path = &rest[1..]; // drop the ':'
    // A real SCP remote's path never starts with '/' (that's the absolute-
    // path SSH form "ssh://host/path" or "host:/abs/path", which already
    // parses fine without folding) and must be non-empty.
    if path.is_empty() || path.starts_with('/') {
        return None;
    }
    // CRITICAL (P0 bypass fix, round 2): if an `@` appears ANYWHERE in the
    // post-colon remainder — not just its first `/`-delimited segment — this
    // is credential-bearing "user:pass@host[:port]/path" input (a
    // slash-containing credential included), not SCP syntax — refuse to
    // fold it. When in doubt, don't fold.
    if path.contains('@') {
        return None;
    }
    // authority may itself contain "user@host" — leave that for the caller's
    // userinfo-stripping step; just reassemble as "authority/path".
    Some(format!("{authority}/{path}"))
}

/// Strip a leading URL scheme (`"https://"`, `"ssh://"`, `"git://"`,
/// `"git+ssh://"`, ...) if present. Recognizes any `[a-zA-Z][a-zA-Z0-9+.-]*://`
/// prefix rather than an exact allowlist, so unusual-but-valid schemes
/// (`"ssh+git://"`, `"http://"`) still fold correctly.
fn strip_scheme(s: &str) -> String {
    if let Some(idx) = s.find("://") {
        let scheme = &s[..idx];
        if !scheme.is_empty()
            && scheme.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '.' || c == '-')
        {
            return s[idx + 3..].to_string();
        }
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── P0: credential stripping (the load-bearing test in this module) ────

    #[test]
    fn strips_username_and_token_from_https_remote() {
        let key = normalize_remote("https://x-access-token:ghp_SUPERSECRETTOKEN1234@github.com/coralswarm/coralswarm.git")
            .expect("should normalize");
        assert_eq!(key, "github.com/coralswarm/coralswarm");
        assert!(!key.contains("ghp_"), "token must never survive normalization");
        assert!(!key.contains("x-access-token"));
    }

    #[test]
    fn strips_plain_username_password_from_https_remote() {
        let key = normalize_remote("https://alice:hunter2@gitlab.com/team/proj.git").unwrap();
        assert_eq!(key, "gitlab.com/team/proj");
        assert!(!key.contains("hunter2"));
        assert!(!key.contains("alice"));
    }

    #[test]
    fn strips_bare_username_with_no_password() {
        let key = normalize_remote("https://deploy-key@bitbucket.org/org/repo").unwrap();
        assert_eq!(key, "bitbucket.org/org/repo");
        assert!(!key.contains("deploy-key"));
    }

    #[test]
    fn strips_userinfo_containing_an_at_sign_in_the_credential() {
        // A pathological credential that itself contains '@' — rfind('@')
        // must anchor on the LAST '@' so only the real host remains.
        let key = normalize_remote("https://user@name:p@ssw0rd@github.com/o/r.git").unwrap();
        assert_eq!(key, "github.com/o/r");
    }

    // ── P0 regression: schemeless credential-bearing input must never be
    // mistaken for SCP syntax (CodeRabbit review on PR #246 — the SCP-fold
    // ran BEFORE userinfo stripping, so "user:pass@host/path" folded into
    // "user/pass@host/path" with the credential text still intact) ────────

    #[test]
    fn schemeless_userinfo_before_plain_path_never_leaks_credentials() {
        let key = normalize_remote("user:pass@github.com/org/repo").unwrap();
        assert_eq!(key, "github.com/org/repo");
        assert!(!key.contains("pass"), "credential must never survive: {key}");
        assert!(!key.contains("user"), "credential must never survive: {key}");
    }

    #[test]
    fn schemeless_userinfo_before_host_with_port_never_leaks_credentials() {
        let key = normalize_remote("user:pass@host:port/repo").unwrap();
        assert_eq!(key, "host/repo");
        assert!(!key.contains("pass"), "credential must never survive: {key}");
        assert!(!key.contains("user"), "credential must never survive: {key}");
    }

    #[test]
    fn minimal_schemeless_credential_shape_never_leaks_credentials() {
        // "a:b@c/d.git" — already caught by the drive-letter guard
        // (single-char authority), but kept as an explicit regression case.
        let key = normalize_remote("a:b@c/d.git").unwrap();
        assert_eq!(key, "c/d");
        assert!(!key.contains("a:b"), "credential must never survive: {key}");
    }

    #[test]
    fn schemeless_realistic_token_credential_never_leaks() {
        let key = normalize_remote("x-access-token:ghp_SUPERSECRET123@github.com/coralswarm/coralswarm.git")
            .unwrap();
        assert_eq!(key, "github.com/coralswarm/coralswarm");
        assert!(!key.contains("ghp_"), "token must never survive: {key}");
    }

    #[test]
    fn schemeless_credential_with_no_further_path_segment_never_leaks() {
        // No trailing '/' after the host at all — `first_segment` covers the
        // whole remainder in this case.
        let key = normalize_remote("user:pass@github.com");
        // No usable path survives either way; the important invariant is
        // just that no credential text appears in whatever comes out.
        if let Some(k) = key {
            assert!(!k.contains("pass"), "credential must never survive: {k}");
        }
    }

    #[test]
    fn genuine_scp_syntax_with_userinfo_still_folds_correctly() {
        // Regression guard the other direction: the P0 fix must not break
        // real SCP remotes, whose '@' sits in the AUTHORITY (before the
        // colon), not the path (after it).
        let key = normalize_remote("git@github.com:coralswarm/coralswarm.git").unwrap();
        assert_eq!(key, "github.com/coralswarm/coralswarm");
    }

    // ── P0 regression, round 2 (CodeRabbit review on PR #246, comment
    // 3625091224): a credential containing its OWN '/' defeats a check that
    // only looks at the FIRST post-colon segment — the '@' lands in a LATER
    // segment instead. Also proven to defeat the unfolded scheme-based path
    // via the same "split at the first '/'" assumption in step 3 of
    // `normalize_remote`. The final-output invariant is what actually closes
    // this class of bug, independent of where in the pipeline it occurs. ──

    #[test]
    fn slash_in_password_never_leaks_via_scp_fold() {
        // The exact shape CodeRabbit reported: '@' lands in the SECOND
        // post-colon segment ("ss@github.com"), past the first-segment-only
        // check a prior version of this fix used.
        assert_eq!(normalize_remote("user:pa/ss@github.com/org/repo"), None);
    }

    #[test]
    fn slash_in_password_never_leaks_via_scheme_based_path() {
        // Same bug class, but through the SCHEME-based authority/path split
        // (step 3 of normalize_remote) rather than the SCP fold — proves the
        // final-output invariant is doing real, independent work here, not
        // just backstopping fold_scp_syntax.
        assert_eq!(normalize_remote("https://user:pa/ss@github.com/org/repo"), None);
    }

    #[test]
    fn multiple_slashes_in_password_never_leak() {
        assert_eq!(normalize_remote("user:pa/ss/word@github.com/org/repo"), None);
        assert_eq!(
            normalize_remote("https://user:pa/ss/word@github.com/org/repo"),
            None
        );
    }

    #[test]
    fn at_sign_in_a_deep_path_segment_is_rejected_not_leaked() {
        // A credential-shaped '@' appearing several segments deep — the
        // invariant doesn't try to distinguish "real" credentials from a
        // coincidental '@' in a path; ANY surviving '@' trips it. A false
        // negative here (a legitimate repo path containing '@') is the
        // accepted trade-off per the audit: drop to None, never risk a leak.
        assert_eq!(
            normalize_remote("user:pass@host/org/repo/sub@dir/more"),
            None
        );
    }

    #[test]
    fn normalize_remote_output_never_contains_at_or_colon_property_matrix() {
        // Property-style sweep over a small matrix of credential positions —
        // every one of these must either normalize to a credential-free key
        // or fall all the way through to `None`. Never a partial leak.
        let inputs = [
            "user:pass@github.com/org/repo",
            "user:pa/ss@github.com/org/repo",
            "user:pa/ss/word@github.com/org/repo",
            "user:pass@host:2222/org/repo",
            "user:pa/ss@host:2222/org/repo",
            "https://user:pass@github.com/org/repo",
            "https://user:pa/ss@github.com/org/repo",
            "https://user:pa/ss/word@github.com/org/repo",
            "ssh://user:pa/ss@github.com/org/repo",
            "git@github.com:coralswarm/coralswarm.git",
            "Git@GitHub.Com:Org/Repo.git",
            "https://x-access-token:ghp_abc123@github.com/org/repo.git",
            "a:b@c/d.git",
            "token:se/cr/et@github.com/a/b/c",
        ];
        for input in inputs {
            if let Some(key) = normalize_remote(input) {
                assert!(
                    !key.contains('@') && !key.contains(':'),
                    "normalize_remote({input:?}) leaked credential-shaped output: {key:?}"
                );
            }
        }
    }

    // ── P0: query-string / fragment credential forms (CodeRabbit review on
    // PR #247, Rust parity). "repo?access_token=secret" / "repo#token=secret"
    // must never survive into a key — the final output invariant now forbids
    // '?', '#', and '=' alongside '@'/':' . ──────────────────────────────────

    #[test]
    fn rejects_query_string_credential_in_remote() {
        // The exact CodeRabbit repro: an access_token query param must not ride
        // through into the key.
        let key = normalize_remote("https://github.com/org/repo?access_token=secret");
        assert_eq!(key, None);
    }

    #[test]
    fn rejects_fragment_credential_in_remote() {
        let key = normalize_remote("https://github.com/org/repo#token=secret");
        assert_eq!(key, None);
    }

    #[test]
    fn rejects_query_and_fragment_credential_in_fallback() {
        // The fallback path is the other CodeRabbit repro
        // (normalizeFallback("repo#token=secret")). Must be None on both forms.
        assert_eq!(normalize_fallback("repo#token=secret"), None);
        assert_eq!(normalize_fallback("repo?access_token=secret"), None);
    }

    #[test]
    fn query_fragment_credentials_never_leak_property_matrix() {
        let inputs = [
            "https://github.com/org/repo?access_token=secret",
            "https://github.com/org/repo#token=secret",
            "github.com/org/repo?access_token=secret",
            "git@github.com:org/repo.git?token=abc",
            "https://x@github.com/org/repo?a=b#c=d",
        ];
        for input in inputs {
            if let Some(key) = normalize_remote(input) {
                assert!(
                    !key.contains('?') && !key.contains('#') && !key.contains('='),
                    "normalize_remote({input:?}) leaked query/fragment output: {key:?}"
                );
                assert!(!key.contains("secret") && !key.contains("token"),
                    "normalize_remote({input:?}) leaked credential text: {key:?}");
            }
        }
    }

    // ── scp-style folding ────────────────────────────────────────────────────

    #[test]
    fn folds_scp_syntax_with_userinfo() {
        let key = normalize_remote("git@github.com:coralswarm/coralswarm.git").unwrap();
        assert_eq!(key, "github.com/coralswarm/coralswarm");
    }

    #[test]
    fn folds_scp_syntax_without_userinfo() {
        let key = normalize_remote("github.example.com:team/repo.git").unwrap();
        assert_eq!(key, "github.example.com/team/repo");
    }

    #[test]
    fn scp_and_https_forms_of_the_same_remote_converge() {
        let scp = normalize_remote("git@github.com:coralswarm/coralswarm.git").unwrap();
        let https = normalize_remote("https://github.com/coralswarm/coralswarm.git").unwrap();
        assert_eq!(scp, https);
    }

    #[test]
    fn does_not_fold_a_windows_drive_letter_path() {
        // "C:\Users\..." (BACKSLASH form) must not be misparsed and must never
        // mint a key. Tightened from a conditional assert to an unconditional
        // one (CodeRabbit review on PR #246): a local filesystem path is never
        // a git remote, so the ONLY acceptable result is None.
        assert_eq!(normalize_remote(r"C:\Users\dev\repo"), None);
    }

    #[test]
    fn does_not_fold_a_forward_slash_windows_drive_letter_path() {
        // "C:/home/dev/repo" (FORWARD-slash form) is the CodeRabbit-reported
        // leak: it slipped past fold_scp_syntax's drive-letter guard and flowed
        // into the scheme-based path — split_once('/') gave authority "C:", the
        // port-strip discarded ":", and "c" was emitted as a fake host
        // ("c/home/dev/repo"). The step-0 early reject must now return None.
        assert_eq!(normalize_remote("C:/home/dev/repo"), None);
    }

    #[test]
    fn rejects_every_drive_letter_and_slash_direction() {
        for input in [
            "C:/home/dev/repo",
            r"C:\Users\dev\repo",
            "d:/proj",
            r"Z:\proj",
            "a:/x",
        ] {
            assert_eq!(normalize_remote(input), None, "drive path must be None: {input}");
        }
    }

    #[test]
    fn rejects_scheme_prefixed_windows_drive_letter_path() {
        // CodeRabbit re-review on PR #247: the raw-input drive-letter guard
        // misses a scheme-prefixed drive path — strip_scheme peels "file://"
        // and the leftover "C:/home/dev/repo" was minted as fake host "c".
        // The step-2b re-check (after scheme stripping) must return None.
        assert_eq!(normalize_remote("file://C:/home/dev/repo"), None);
        assert_eq!(normalize_remote("file://c:/home/dev/repo"), None);
    }

    #[test]
    fn rejects_scheme_prefixed_drive_letter_matrix() {
        for input in [
            "file://C:/home/dev/repo",
            "file://c:/home/dev/repo",
            r"file://D:\proj",
            "ssh://C:/x",
            "git://Z:/repo",
        ] {
            assert_eq!(
                normalize_remote(input),
                None,
                "scheme-prefixed drive path must be None: {input}"
            );
        }
    }

    #[test]
    fn rejects_userinfo_hidden_drive_letter_path() {
        // Round-3 CodeRabbit finding: a drive letter cloaked behind userinfo.
        assert_eq!(normalize_remote("ssh://user@C:/x"), None);
        assert_eq!(normalize_remote("user@C:/x"), None);
        assert_eq!(normalize_remote(r"user@C:\x"), None);
        assert_eq!(normalize_remote(r"ssh://user@C:\x"), None);
        assert_eq!(normalize_remote("https://user:pass@C:/x"), None);
    }

    #[test]
    fn drive_letter_combinatorial_matrix_all_rejected() {
        // {scheme} × {userinfo} × {drive+slash} × {path suffix} — every
        // combination is a local drive path, never a git remote: all must be
        // None. This pins the whole class (bare / scheme / userinfo / both)
        // against a round 4.
        let schemes = ["", "file://", "ssh://", "https://", "git://"];
        let userinfos = ["", "user@", "user:pass@", "x-access-token:ghp_secret@"];
        let drives = ["C:/", r"C:\", "c:/", r"Z:\", "d:/"];
        let suffixes = ["", "x", "Users/dev/repo", "repo.git"];
        for s in schemes {
            for u in userinfos {
                for d in drives {
                    for suf in suffixes {
                        let input = format!("{s}{u}{d}{suf}");
                        assert_eq!(
                            normalize_remote(&input),
                            None,
                            "drive path must be None: {input:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn rejects_bracketed_ipv6_authority() {
        // v5 CodeRabbit Data-Integrity finding on PR #246: a bracketed IPv6
        // authority minted the fake host "[2001" — the '[' survived and the
        // inner ':' was eaten by the port-split. All bracketed authorities are
        // out of scope for v1 and must be None.
        assert_eq!(normalize_remote("ssh://[2001:db8::1]/team/repo"), None);
        assert_eq!(normalize_remote("[::1]/x"), None);
        assert_eq!(normalize_remote("[2001:db8::1]:22/a/b"), None);
        assert_eq!(normalize_remote("ssh://user@[2001:db8::1]/team/repo"), None);
        assert_eq!(normalize_remote("https://[::1]/x/y"), None);
        assert_eq!(normalize_remote("git://[fe80::1]/repo.git"), None);
    }

    #[test]
    fn ipv6_authority_combinatorial_matrix_all_rejected() {
        // {scheme} × {userinfo} × {bracketed IPv6 literal} × {path suffix} —
        // every combination is a bracketed authority, never a git-hosting
        // identity: all must be None.
        let schemes = ["", "ssh://", "https://", "git://"];
        let userinfos = ["", "user@", "user:pass@"];
        let hosts = ["[::1]", "[2001:db8::1]", "[fe80::1]", "[2001:db8::1]:2222"];
        let suffixes = ["/x", "/team/repo", "/a/b/c.git"];
        for s in schemes {
            for u in userinfos {
                for h in hosts {
                    for suf in suffixes {
                        let input = format!("{s}{u}{h}{suf}");
                        assert_eq!(
                            normalize_remote(&input),
                            None,
                            "bracketed IPv6 authority must be None: {input:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn positive_sanity_cases_still_normalize_after_structural_refactor() {
        // Guard the other direction: the v4 restructure (userinfo stripped
        // before splitting) must not regress any real remote.
        assert_eq!(
            normalize_remote("https://github.com/coralswarm/coralswarm.git"),
            Some("github.com/coralswarm/coralswarm".to_string())
        );
        assert_eq!(
            normalize_remote("git@github.com:coralswarm/coralswarm.git"),
            Some("github.com/coralswarm/coralswarm".to_string())
        );
        assert_eq!(
            normalize_remote("ssh://git@github.com:2222/coralswarm/coralswarm.git"),
            Some("github.com/coralswarm/coralswarm".to_string())
        );
        assert_eq!(
            normalize_remote("https://x-access-token:ghp_TOKEN@github.com/org/repo.git"),
            Some("github.com/org/repo".to_string())
        );
        assert_eq!(
            normalize_remote("https://user@name:p@ssw0rd@github.com/o/r.git"),
            Some("github.com/o/r".to_string())
        );
    }

    // ── ssh:// scheme ────────────────────────────────────────────────────────

    #[test]
    fn normalizes_ssh_scheme_remote() {
        let key = normalize_remote("ssh://git@github.com/coralswarm/coralswarm.git").unwrap();
        assert_eq!(key, "github.com/coralswarm/coralswarm");
    }

    #[test]
    fn normalizes_ssh_remote_with_explicit_port() {
        let key = normalize_remote("ssh://git@github.com:2222/coralswarm/coralswarm.git").unwrap();
        assert_eq!(key, "github.com/coralswarm/coralswarm");
    }

    #[test]
    fn normalizes_git_protocol_scheme() {
        let key = normalize_remote("git://github.com/coralswarm/coralswarm.git").unwrap();
        assert_eq!(key, "github.com/coralswarm/coralswarm");
    }

    // ── port stripping (https) ───────────────────────────────────────────────

    #[test]
    fn strips_explicit_https_port() {
        let key = normalize_remote("https://github.company.com:8443/team/repo.git").unwrap();
        assert_eq!(key, "github.company.com/team/repo");
    }

    // ── trailing .git / trailing slash ───────────────────────────────────────

    #[test]
    fn strips_trailing_git_suffix() {
        let key = normalize_remote("https://github.com/org/repo.git").unwrap();
        assert_eq!(key, "github.com/org/repo");
    }

    #[test]
    fn works_without_trailing_git_suffix() {
        let key = normalize_remote("https://github.com/org/repo").unwrap();
        assert_eq!(key, "github.com/org/repo");
    }

    #[test]
    fn strips_trailing_slash() {
        let key = normalize_remote("https://github.com/org/repo/").unwrap();
        assert_eq!(key, "github.com/org/repo");
    }

    #[test]
    fn strips_trailing_slash_and_git_suffix_together() {
        let key = normalize_remote("https://github.com/org/repo.git/").unwrap();
        assert_eq!(key, "github.com/org/repo");
    }

    // ── host casing ──────────────────────────────────────────────────────────

    #[test]
    fn lowercases_host_but_preserves_path_case() {
        let key = normalize_remote("https://GitHub.COM/CoralSwarm/CoralSwarm.git").unwrap();
        assert_eq!(key, "github.com/CoralSwarm/CoralSwarm");
    }

    #[test]
    fn weird_mixed_casing_scp_form() {
        let key = normalize_remote("Git@GitHub.Com:Org/Repo.git").unwrap();
        assert_eq!(key, "github.com/Org/Repo");
    }

    // ── self-hosted / nested paths ───────────────────────────────────────────

    #[test]
    fn handles_self_hosted_gitlab_with_nested_group_path() {
        let key = normalize_remote("https://gitlab.internal.acme.com/platform/backend/core.git").unwrap();
        assert_eq!(key, "gitlab.internal.acme.com/platform/backend/core");
    }

    #[test]
    fn handles_azure_devops_style_path() {
        let key = normalize_remote("https://dev.azure.com/myorg/myproject/_git/myrepo").unwrap();
        assert_eq!(key, "dev.azure.com/myorg/myproject/_git/myrepo");
    }

    // ── idempotency: normalizing an already-normalized key is a no-op ───────

    #[test]
    fn normalizing_twice_is_idempotent() {
        let once = normalize_remote("https://x:y@GitHub.com/Org/Repo.git/").unwrap();
        let twice = normalize_remote(&once).unwrap();
        assert_eq!(once, twice);
    }

    // ── empty / degenerate input ──────────────────────────────────────────────

    #[test]
    fn empty_input_returns_none() {
        assert_eq!(normalize_remote(""), None);
        assert_eq!(normalize_remote("   "), None);
    }

    #[test]
    fn bare_host_with_no_path_returns_none() {
        assert_eq!(normalize_remote("https://github.com"), None);
        assert_eq!(normalize_remote("https://github.com/"), None);
    }

    // ── fallback (basename) ──────────────────────────────────────────────────

    #[test]
    fn fallback_sanitizes_plain_basename() {
        assert_eq!(normalize_fallback("CoralSwarm"), Some("coralswarm".to_string()));
    }

    #[test]
    fn fallback_trims_whitespace_and_slashes() {
        assert_eq!(normalize_fallback("  my-project/  "), Some("my-project".to_string()));
    }

    #[test]
    fn fallback_strips_trailing_git_suffix() {
        assert_eq!(normalize_fallback("myrepo.git"), Some("myrepo".to_string()));
    }

    #[test]
    fn fallback_empty_returns_none() {
        assert_eq!(normalize_fallback(""), None);
        assert_eq!(normalize_fallback("   "), None);
        assert_eq!(normalize_fallback("/"), None);
    }

    #[test]
    fn fallback_never_attempts_url_parsing() {
        // A basename with no '@'/':' passes through sanitized (lowercased),
        // NOT host/path-parsed the way normalize_remote would.
        let out = normalize_fallback("Weird-Project_Name.42").unwrap();
        assert_eq!(out, "weird-project_name.42");
    }

    #[test]
    fn fallback_rejects_input_containing_at_or_colon_defense_in_depth() {
        // P0 defense in depth: even though normalize_fallback never attempts
        // URL parsing, input containing '@' or ':' is refused outright
        // rather than passed through — guards against a future caller
        // mistakenly feeding a raw credential-bearing remote URL into this
        // fallback path instead of normalize_remote.
        assert_eq!(normalize_fallback("weird@name:thing"), None);
        assert_eq!(normalize_fallback("user:pass@host"), None);
        assert_eq!(normalize_fallback("just-a-colon:here"), None);
        assert_eq!(normalize_fallback("just-an-at@sign"), None);
    }

    // ── version pin sanity ────────────────────────────────────────────────────

    #[test]
    fn version_is_pinned_at_5() {
        // 1 -> 2 drive-letter reject + query/fragment invariant; 2 -> 3
        // post-scheme-strip re-check; 3 -> 4 structural post-userinfo-strip
        // drive-letter guard; 4 -> 5 bracketed-IPv6-authority rejection
        // (CodeRabbit reviews on PRs #246/#247). Must match the JS mirror in
        // hooks/project-key.mjs in github.com/CoralSwarm/coralswarm-connect.
        assert_eq!(PROJECT_KEY_NORMALIZER_VERSION, 5);
    }
}

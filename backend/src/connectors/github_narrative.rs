//! GitHub narrative atom fetchers + normalisers (docs/connectors/github.md
//! §3.5, the four PR-A source types: `github_pr`, `github_issue`,
//! `github_release`, `github_commit`).
//!
//! Split deliberately into two halves per type: a `fetch_*` orchestrator
//! (needs the HTTP client + installation token) and a `normalize_*` pure
//! function (`&serde_json::Value` in, [`NormalizedAtom`] out) — the pure half
//! is what `#[cfg(test)]` below exercises against small inline JSON fixtures,
//! never real repo data.
//!
//! # Body cap (§3.11 "never fetch full diffs" + a sane atom size)
//!
//! [`MAX_ATOM_BODY_BYTES`] bounds every assembled body. When a PR/issue's
//! comment thread would exceed it, the OLDEST comments are dropped first
//! (never the header — title/description/diff-stat is always kept) with a
//! visible `"… N earlier comment(s) omitted for size …"` marker, and a
//! `tracing::info!` log line so truncation is never silent.
//!
//! # HTML comments
//!
//! GitHub issue/PR templates routinely leave `<!-- ... -->` instruction
//! blocks in the body. [`strip_html_comments`] removes them before a body is
//! ever embedded/chunked — markdown itself is kept verbatim.

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::github::{self, GithubApiError, github_api_base};

/// Cap on an atom's assembled body, in bytes (§3.11 sizing note in this
/// module's own doc comment).
pub(crate) const MAX_ATOM_BODY_BYTES: usize = 64 * 1024;

/// One normalised GitHub narrative atom, shaped to feed straight into
/// `ingest::atom::IngestAtomRequest` (the caller — `connectors::
/// github_backfill`/`connectors::github_event_ingest` — fills in `graph_id`
/// and the rest of the request envelope; this struct carries only what the
/// fetch+normalise step actually produces).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedAtom {
    pub(crate) source_type: &'static str,
    pub(crate) source_id: String,
    /// Human title — informational (also embedded as the body's first line);
    /// `atoms.title` itself is derived server-side by `services::atom_title`.
    pub(crate) title: String,
    pub(crate) content: String,
    pub(crate) occurred_at: DateTime<Utc>,
    pub(crate) author_id: Option<String>,
    pub(crate) author_name: Option<String>,
    pub(crate) participants: Vec<String>,
    /// Repo-relative file path, for a `github_code` atom. Read by
    /// `github_backfill::chunk_atom`, which routes on it to
    /// `ChunkStrategy::code_for_path`. No production path sets it any more:
    /// the connector stopped capturing source code on 2026-10-27, and every
    /// type this module produces (PR, issue, release, commit) leaves it
    /// `None`. Kept so the shared write path still chunks a code atom the
    /// way the existing ones were chunked.
    pub(crate) code_path: Option<String>,
}

// ── Small JSON helpers ──────────────────────────────────────────────────────

fn s<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn opt_s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string)
}

fn login_of(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|u| u.get("login")).and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string)
}

fn parse_dt(v: &Value, key: &str) -> Option<DateTime<Utc>> {
    v.get(key)
        .and_then(Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
}

/// `occurred_at` for a GitHub capture that has NO source time: a payload with
/// no commit date and no `updated_at`/`created_at` (never a well-formed
/// GitHub response; this keeps a malformed one ingestible), or a summary
/// rewrite that did not read the head commit. It is the wall clock of the
/// FIRST capture, and it stays that:
/// `source_evidence` does not mint a version for a GitHub capture that differs
/// only in `occurred_at`, so a later re-capture keeps the item's earliest
/// known time rather than restamping it.
pub(crate) fn no_source_time() -> DateTime<Utc> {
    Utc::now()
}

/// Strip `<!-- ... -->` HTML-comment blocks (GitHub issue/PR templates
/// routinely leave these behind) — a plain scan rather than a regex
/// dependency, correct for the non-nesting HTML-comment grammar.
pub(crate) fn strip_html_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("<!--") {
        out.push_str(&rest[..start]);
        match rest[start..].find("-->") {
            Some(end) => rest = &rest[start + end + 3..],
            None => {
                rest = "";
                break;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Byte-safe truncation at or before `max_bytes` — never splits a UTF-8
/// scalar (stable Rust has no `str::floor_char_boundary`, so this hand-rolls
/// the same walk-back-to-a-boundary loop).
pub(crate) fn safe_truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut idx = max_bytes;
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    &s[..idx]
}

/// Assemble a capped body from a `header` (title/description/diff-stat —
/// always kept whole) and an ORDERED (oldest-first) list of comment-like
/// `sections`. Drops the OLDEST sections first when the assembled body would
/// exceed [`MAX_ATOM_BODY_BYTES`], leaving a visible marker; logs the drop
/// count. Falls back to a hard byte-truncation of the header itself only in
/// the degenerate case where the header alone already exceeds the cap.
pub(crate) fn assemble_capped_body(header: &str, sections: &[String]) -> String {
    let full = join_body(header, sections);
    if full.len() <= MAX_ATOM_BODY_BYTES {
        return full;
    }

    let mut remaining = sections;
    let mut dropped = 0usize;
    while !remaining.is_empty() {
        remaining = &remaining[1..];
        dropped += 1;
        let marker = format!("\n\n… {dropped} earlier comment(s) omitted for size …");
        let candidate = format!("{header}{marker}{}", join_body("", remaining));
        if candidate.len() <= MAX_ATOM_BODY_BYTES {
            tracing::info!(dropped, "github_narrative: truncated atom body, oldest comments dropped");
            return candidate;
        }
    }

    // Even the bare header (no sections left) is over the cap — hard
    // byte-truncate it as a last resort so the atom is never rejected purely
    // on size.
    tracing::warn!(
        header_len = header.len(),
        "github_narrative: header alone exceeds MAX_ATOM_BODY_BYTES — hard-truncating"
    );
    format!("{}\n\n… truncated …", safe_truncate(header, MAX_ATOM_BODY_BYTES.saturating_sub(32)))
}

fn join_body(header: &str, sections: &[String]) -> String {
    let mut out = header.to_string();
    for section in sections {
        out.push_str("\n\n");
        out.push_str(section);
    }
    out
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s)
}

// ── github_pr ───────────────────────────────────────────────────────────────

/// Fetch every part a PR atom needs (detail, reviews, review comments, issue
/// comments, commits) and normalise it. `pr_number` is enough — the detail
/// fetch is what carries `additions`/`deletions`/`changed_files`/`merged`,
/// none of which the list endpoint's summary objects include.
pub(crate) async fn fetch_pr_atom(
    http: &reqwest::Client,
    token: &str,
    owner: &str,
    repo: &str,
    repo_id: i64,
    pr_number: i64,
) -> Result<NormalizedAtom, GithubApiError> {
    let base = format!("{}/repos/{owner}/{repo}", github_api_base());
    let detail = github::get_one(http, token, &format!("{base}/pulls/{pr_number}")).await?;
    let reviews = github::get_paginated(http, token, &format!("{base}/pulls/{pr_number}/reviews?per_page=100"), 5).await?;
    let review_comments =
        github::get_paginated(http, token, &format!("{base}/pulls/{pr_number}/comments?per_page=100"), 5).await?;
    let issue_comments =
        github::get_paginated(http, token, &format!("{base}/issues/{pr_number}/comments?per_page=100"), 5).await?;
    let commits = github::get_paginated(http, token, &format!("{base}/pulls/{pr_number}/commits?per_page=100"), 5).await?;

    Ok(normalize_pr(repo_id, pr_number, &detail, &reviews, &review_comments, &issue_comments, &commits))
}

pub(crate) fn normalize_pr(
    repo_id: i64,
    pr_number: i64,
    detail: &Value,
    reviews: &[Value],
    review_comments: &[Value],
    issue_comments: &[Value],
    commits: &[Value],
) -> NormalizedAtom {
    let title = s(detail, "title").to_string();
    let body = strip_html_comments(s(detail, "body"));
    let author = login_of(detail, "user");
    let state = s(detail, "state");
    let merged = detail.get("merged").and_then(Value::as_bool).unwrap_or(false);
    let base_ref = detail.get("base").map(|b| s(b, "ref")).unwrap_or_default();
    let head_ref = detail.get("head").map(|h| s(h, "ref")).unwrap_or_default();
    let additions = detail.get("additions").and_then(Value::as_i64).unwrap_or(0);
    let deletions = detail.get("deletions").and_then(Value::as_i64).unwrap_or(0);
    let changed_files = detail.get("changed_files").and_then(Value::as_i64).unwrap_or(0);
    let labels: Vec<String> = detail
        .get("labels")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|l| l.get("name").and_then(Value::as_str)).map(str::to_string).collect())
        .unwrap_or_default();

    let merged_suffix = if merged { " (merged)" } else { "" };
    let header = format!(
        "PR #{pr_number}: {title}\nAuthor: {}\nState: {state}{merged_suffix}\n{base_ref} <- {head_ref}\nLabels: {}\nDiff: +{additions}/-{deletions} across {changed_files} file(s)\n\n{body}",
        author.as_deref().unwrap_or("unknown"),
        if labels.is_empty() { "(none)".to_string() } else { labels.join(", ") },
    );

    let mut events: Vec<(DateTime<Utc>, String)> = Vec::new();
    for r in reviews {
        let Some(when) = parse_dt(r, "submitted_at") else { continue };
        let who = login_of(r, "user").unwrap_or_else(|| "unknown".to_string());
        let review_state = s(r, "state");
        let rbody = strip_html_comments(s(r, "body"));
        let text = if rbody.is_empty() {
            format!("Review by {who}: {review_state}")
        } else {
            format!("Review by {who} ({review_state}): {rbody}")
        };
        events.push((when, text));
    }
    for c in review_comments {
        let Some(when) = parse_dt(c, "created_at") else { continue };
        let who = login_of(c, "user").unwrap_or_else(|| "unknown".to_string());
        let path = opt_s(c, "path").unwrap_or_else(|| "?".to_string());
        let cbody = strip_html_comments(s(c, "body"));
        events.push((when, format!("Review comment by {who} on {path}: {cbody}")));
    }
    for c in issue_comments {
        let Some(when) = parse_dt(c, "created_at") else { continue };
        let who = login_of(c, "user").unwrap_or_else(|| "unknown".to_string());
        let cbody = strip_html_comments(s(c, "body"));
        events.push((when, format!("Comment by {who}: {cbody}")));
    }
    events.sort_by_key(|(when, _)| *when);
    let mut sections: Vec<String> = events.into_iter().map(|(_, text)| text).collect();

    if !commits.is_empty() {
        let mut lines = Vec::with_capacity(commits.len());
        for c in commits {
            let sha = s(c, "sha");
            let short = &sha[..sha.len().min(7)];
            let msg = c.get("commit").map(|cm| first_line(s(cm, "message"))).unwrap_or_default();
            lines.push(format!("- {short}: {msg}"));
        }
        sections.push(format!("Commits ({}):\n{}", commits.len(), lines.join("\n")));
    }

    let content = assemble_capped_body(&header, &sections);
    let occurred_at = parse_dt(detail, "merged_at")
        .or_else(|| parse_dt(detail, "updated_at"))
        .or_else(|| parse_dt(detail, "created_at"))
        .unwrap_or_else(no_source_time);

    let mut participants: Vec<String> = vec![author.clone().unwrap_or_default()];
    for r in reviews {
        if let Some(who) = login_of(r, "user") {
            participants.push(who);
        }
    }
    participants.retain(|p| !p.is_empty());
    participants.sort();
    participants.dedup();

    NormalizedAtom {
        source_type: "github_pr",
        source_id: format!("{repo_id}:pr:{pr_number}"),
        title: format!("PR #{pr_number}: {title}"),
        content,
        occurred_at,
        author_id: author.clone(),
        author_name: author,
        participants,
        code_path: None,
    }
}

// ── github_issue ────────────────────────────────────────────────────────────

/// `true` when this `/issues` list item is actually a pull request (GitHub's
/// issues endpoint returns both — §3.5's own note).
pub(crate) fn is_actually_a_pull_request(issue_json: &Value) -> bool {
    issue_json.get("pull_request").is_some()
}

pub(crate) async fn fetch_issue_atom(
    http: &reqwest::Client,
    token: &str,
    owner: &str,
    repo: &str,
    repo_id: i64,
    issue_json: &Value,
) -> Result<NormalizedAtom, GithubApiError> {
    let number = issue_json.get("number").and_then(Value::as_i64).unwrap_or(0);
    let base = format!("{}/repos/{owner}/{repo}", github_api_base());
    let comments = github::get_paginated(http, token, &format!("{base}/issues/{number}/comments?per_page=100"), 5).await?;
    Ok(normalize_issue(repo_id, issue_json, &comments))
}

pub(crate) fn normalize_issue(repo_id: i64, issue_json: &Value, comments: &[Value]) -> NormalizedAtom {
    let number = issue_json.get("number").and_then(Value::as_i64).unwrap_or(0);
    let title = s(issue_json, "title").to_string();
    let body = strip_html_comments(s(issue_json, "body"));
    let author = login_of(issue_json, "user");
    let state = s(issue_json, "state");
    let labels: Vec<String> = issue_json
        .get("labels")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|l| l.get("name").and_then(Value::as_str)).map(str::to_string).collect())
        .unwrap_or_default();

    let header = format!(
        "Issue #{number}: {title}\nAuthor: {}\nState: {state}\nLabels: {}\n\n{body}",
        author.as_deref().unwrap_or("unknown"),
        if labels.is_empty() { "(none)".to_string() } else { labels.join(", ") },
    );

    let mut sections = Vec::with_capacity(comments.len());
    let mut comment_pairs: Vec<(DateTime<Utc>, String)> = Vec::new();
    for c in comments {
        let Some(when) = parse_dt(c, "created_at") else { continue };
        let who = login_of(c, "user").unwrap_or_else(|| "unknown".to_string());
        let cbody = strip_html_comments(s(c, "body"));
        comment_pairs.push((when, format!("Comment by {who}: {cbody}")));
    }
    comment_pairs.sort_by_key(|(when, _)| *when);
    sections.extend(comment_pairs.into_iter().map(|(_, text)| text));

    let content = assemble_capped_body(&header, &sections);
    let occurred_at = parse_dt(issue_json, "closed_at")
        .or_else(|| parse_dt(issue_json, "updated_at"))
        .or_else(|| parse_dt(issue_json, "created_at"))
        .unwrap_or_else(no_source_time);

    let mut participants: Vec<String> = vec![author.clone().unwrap_or_default()];
    participants.retain(|p| !p.is_empty());
    participants.sort();
    participants.dedup();

    NormalizedAtom {
        source_type: "github_issue",
        source_id: format!("{repo_id}:issue:{number}"),
        title: format!("Issue #{number}: {title}"),
        content,
        occurred_at,
        author_id: author.clone(),
        author_name: author,
        participants,
        code_path: None,
    }
}

// ── github_release ──────────────────────────────────────────────────────────

/// No secondary fetch needed — a release list item already carries every
/// field this atom needs. `prev_tag` (the NEXT-oldest release in the same
/// newest-first listing, when known) drives the compare link; `None` is a
/// legitimate value (first release ever, or the cross-page boundary case
/// where the previous release fell on the NEXT page — accepted, rare, noted
/// in docs/connectors/github.md §9).
pub(crate) fn normalize_release(repo_id: i64, owner: &str, repo: &str, release_json: &Value, prev_tag: Option<&str>) -> NormalizedAtom {
    let id = release_json.get("id").and_then(Value::as_i64).unwrap_or(0);
    let tag = s(release_json, "tag_name");
    let name = {
        let n = s(release_json, "name");
        if n.is_empty() { tag } else { n }
    };
    let body = strip_html_comments(s(release_json, "body"));
    let author = login_of(release_json, "author");
    let draft = release_json.get("draft").and_then(Value::as_bool).unwrap_or(false);
    let prerelease = release_json.get("prerelease").and_then(Value::as_bool).unwrap_or(false);

    let compare_link = prev_tag
        .filter(|p| !p.is_empty())
        .map(|p| format!("https://github.com/{owner}/{repo}/compare/{p}...{tag}"));

    let mut header = format!("Release {name} ({tag})\nAuthor: {}\n", author.as_deref().unwrap_or("unknown"));
    if draft {
        header.push_str("Draft\n");
    }
    if prerelease {
        header.push_str("Prerelease\n");
    }
    if let Some(link) = &compare_link {
        header.push_str(&format!("Compare: {link}\n"));
    }
    header.push('\n');
    header.push_str(&body);

    let content = assemble_capped_body(&header, &[]);
    let occurred_at = parse_dt(release_json, "published_at")
        .or_else(|| parse_dt(release_json, "created_at"))
        .unwrap_or_else(no_source_time);

    NormalizedAtom {
        source_type: "github_release",
        source_id: format!("{repo_id}:release:{id}"),
        title: format!("Release {name}"),
        content,
        occurred_at,
        author_id: author.clone(),
        author_name: author,
        participants: Vec::new(),
        code_path: None,
    }
}

// ── github_commit ───────────────────────────────────────────────────────────

/// A trailing `(#123)` squash-merge suffix on a commit message names an
/// already-covered PR — matching this lets the backfill/event workers skip
/// the association-check API call entirely for the common squash-merge case
/// (§3.5's own instruction: "skip... before hitting the association
/// endpoint"). Returns the referenced PR number when the pattern matches.
pub(crate) fn squash_merge_pr_number(commit_message_first_line: &str) -> Option<i64> {
    let line = commit_message_first_line.trim_end();
    let line = line.strip_suffix(')')?;
    let open = line.rfind(" (#")?;
    let digits = &line[open + 3..];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// GitHub's own merge-commit message for a PR merged with "Create a merge
/// commit": `Merge pull request #123 from owner/branch`. Returns the PR
/// number when the first line has that shape.
///
/// This exists because the association endpoint
/// (`GET .../commits/{sha}/pulls`) lags a just-merged PR by seconds, and a
/// push webhook arrives inside that window. Between 2026-09-30 and
/// 2026-10-04 all 20 `github_commit` atoms the webhook path wrote in prod
/// were PR merge commits or PR branch commits that slipped through that
/// window; the endpoint reports a PR for every one of them now.
pub(crate) fn pr_merge_commit_pr_number(commit_message_first_line: &str) -> Option<i64> {
    let rest = commit_message_first_line.strip_prefix("Merge pull request #")?;
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    if end == 0 || !rest[end..].starts_with(" from ") {
        return None;
    }
    rest[..end].parse().ok()
}

/// `true` when a commit listing item (compare or `/commits` list, both carry
/// `parents`) is a PR's merge commit: two or more parents and GitHub's
/// `Merge pull request #N from ...` message.
pub(crate) fn is_pr_merge_commit(item: &Value) -> bool {
    let parents = item.get("parents").and_then(Value::as_array).map(Vec::len).unwrap_or(0);
    let message = item.get("commit").and_then(|c| c.get("message")).and_then(Value::as_str).unwrap_or_default();
    parents >= 2 && pr_merge_commit_pr_number(first_line(message)).is_some()
}

/// The commits of one compare range that a PR brought in: everything
/// reachable, inside the range, from a non-first parent of a PR merge
/// commit ([`is_pr_merge_commit`]), minus the first-parent chain of `after`
/// (commits pushed straight to the default branch, even when a PR branch was
/// later cut from them). Decided from the compare listing alone, so it does
/// not depend on the association endpoint having caught up.
pub(crate) fn commits_merged_in_by_prs(commits: &[Value], after: &str) -> std::collections::HashSet<String> {
    use std::collections::{HashMap, HashSet};

    let parents_of = |c: &Value| -> Vec<String> {
        c.get("parents")
            .and_then(Value::as_array)
            .map(|ps| ps.iter().filter_map(|p| p.get("sha").and_then(Value::as_str)).map(str::to_string).collect())
            .unwrap_or_default()
    };
    let by_sha: HashMap<&str, &Value> = commits.iter().filter_map(|c| c.get("sha").and_then(Value::as_str).map(|s| (s, c))).collect();

    let mut first_parent_chain: HashSet<String> = HashSet::new();
    let mut cursor = by_sha.get(after).copied();
    while let Some(c) = cursor {
        let sha = c.get("sha").and_then(Value::as_str).unwrap_or_default();
        if !first_parent_chain.insert(sha.to_string()) {
            break;
        }
        cursor = parents_of(c).first().and_then(|p| by_sha.get(p.as_str()).copied());
    }

    let mut merged_in: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = commits.iter().filter(|c| is_pr_merge_commit(c)).flat_map(|c| parents_of(c).into_iter().skip(1)).collect();
    while let Some(sha) = stack.pop() {
        if first_parent_chain.contains(&sha) || merged_in.contains(&sha) {
            continue;
        }
        let Some(c) = by_sha.get(sha.as_str()) else { continue };
        merged_in.insert(sha);
        stack.extend(parents_of(c));
    }
    merged_in
}

/// `true` when `GET /repos/{o}/{r}/commits/{sha}/pulls` reports at least one
/// associated PR — the commit is already covered by the PR path, so the
/// direct-commit path must skip it (§3.5).
pub(crate) fn has_pr_association(association_response: &[Value]) -> bool {
    !association_response.is_empty()
}

/// Full commit detail (`GET /repos/{o}/{r}/commits/{sha}`, NOT the list
/// endpoint) — the only shape that carries `stats`/`files`.
pub(crate) fn normalize_commit(repo_id: i64, commit_detail: &Value) -> NormalizedAtom {
    let sha = s(commit_detail, "sha").to_string();
    let commit = commit_detail.get("commit").cloned().unwrap_or(Value::Null);
    let message = s(&commit, "message");
    let title = first_line(message).to_string();
    let author_login = login_of(commit_detail, "author");
    let git_author_name = commit.get("author").map(|a| s(a, "name")).unwrap_or_default().to_string();
    let additions = commit_detail.get("stats").and_then(|st| st.get("additions")).and_then(Value::as_i64).unwrap_or(0);
    let deletions = commit_detail.get("stats").and_then(|st| st.get("deletions")).and_then(Value::as_i64).unwrap_or(0);
    let files: Vec<String> = commit_detail
        .get("files")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|f| f.get("filename").and_then(Value::as_str)).map(str::to_string).collect())
        .unwrap_or_default();

    let short = &sha[..sha.len().min(7)];
    let header = format!(
        "Commit {short}\nAuthor: {}\nStat: +{additions}/-{deletions} across {} file(s)\n\n{message}\n\nFiles:\n{}",
        if git_author_name.is_empty() { author_login.clone().unwrap_or_else(|| "unknown".to_string()) } else { git_author_name },
        files.len(),
        if files.is_empty() { "(none)".to_string() } else { files.iter().map(|f| format!("- {f}")).collect::<Vec<_>>().join("\n") },
    );

    let content = assemble_capped_body(&header, &[]);
    let occurred_at = commit
        .get("author")
        .and_then(|a| parse_dt(a, "date"))
        .or_else(|| commit.get("committer").and_then(|c| parse_dt(c, "date")))
        .unwrap_or_else(no_source_time);

    NormalizedAtom {
        source_type: "github_commit",
        source_id: format!("{repo_id}:commit:{sha}"),
        title: format!("Commit {short}: {title}"),
        content,
        occurred_at,
        author_id: author_login.clone(),
        author_name: author_login,
        participants: Vec::new(),
        code_path: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── strip_html_comments ────────────────────────────────────────────────

    #[test]
    fn strip_html_comments_removes_a_single_block() {
        let input = "before <!-- instructions --> after";
        assert_eq!(strip_html_comments(input), "before  after");
    }

    #[test]
    fn strip_html_comments_removes_multiple_blocks() {
        let input = "<!--a-->one<!--b-->two<!--c-->";
        assert_eq!(strip_html_comments(input), "onetwo");
    }

    #[test]
    fn strip_html_comments_leaves_plain_markdown_untouched() {
        let input = "## Heading\n\n- item one\n- item two\n";
        assert_eq!(strip_html_comments(input), input);
    }

    #[test]
    fn strip_html_comments_unterminated_block_drops_the_rest() {
        let input = "keep this <!-- never closed";
        assert_eq!(strip_html_comments(input), "keep this ");
    }

    // ── assemble_capped_body ────────────────────────────────────────────────

    #[test]
    fn assemble_capped_body_under_cap_keeps_everything() {
        let header = "Header text";
        let sections = vec!["comment one".to_string(), "comment two".to_string()];
        let body = assemble_capped_body(header, &sections);
        assert!(body.contains("Header text"));
        assert!(body.contains("comment one"));
        assert!(body.contains("comment two"));
    }

    #[test]
    fn assemble_capped_body_drops_oldest_sections_first() {
        let header = "H";
        // Each section ~ MAX_ATOM_BODY_BYTES / 2, so keeping both blows the cap.
        let big = "x".repeat(MAX_ATOM_BODY_BYTES / 2 + 100);
        let sections = vec![format!("OLDEST-{big}"), format!("NEWEST-{big}")];
        let body = assemble_capped_body(header, &sections);
        assert!(body.len() <= MAX_ATOM_BODY_BYTES, "body must respect the cap: {}", body.len());
        assert!(!body.contains("OLDEST"), "the oldest section must be dropped first");
        assert!(body.contains("NEWEST"), "the newest section must survive");
        assert!(body.contains("omitted for size"), "a visible truncation marker must be present");
    }

    #[test]
    fn assemble_capped_body_huge_header_hard_truncates() {
        let header = "H".repeat(MAX_ATOM_BODY_BYTES * 2);
        let body = assemble_capped_body(&header, &[]);
        assert!(body.len() <= MAX_ATOM_BODY_BYTES + 64, "hard-truncated body must stay near the cap: {}", body.len());
    }

    // ── normalize_pr ────────────────────────────────────────────────────────

    fn sample_pr_detail() -> Value {
        json!({
            "number": 42,
            "title": "Add widgets",
            "body": "<!-- template --> This adds widgets.",
            "user": {"login": "alice"},
            "state": "closed",
            "merged": true,
            "merged_at": "2026-01-02T03:04:05Z",
            "updated_at": "2026-01-02T03:04:05Z",
            "created_at": "2026-01-01T00:00:00Z",
            "base": {"ref": "main"},
            "head": {"ref": "alice/widgets"},
            "additions": 10,
            "deletions": 2,
            "changed_files": 3,
            "labels": [{"name": "enhancement"}]
        })
    }

    #[test]
    fn normalize_pr_builds_header_and_source_id() {
        let detail = sample_pr_detail();
        let atom = normalize_pr(555, 42, &detail, &[], &[], &[], &[]);
        assert_eq!(atom.source_type, "github_pr");
        assert_eq!(atom.source_id, "555:pr:42");
        assert!(atom.content.contains("PR #42: Add widgets"));
        assert!(atom.content.contains("Author: alice"));
        assert!(atom.content.contains("merged"));
        assert!(atom.content.contains("+10/-2"));
        assert!(atom.content.contains("This adds widgets."));
        assert!(!atom.content.contains("template"), "HTML comment must be stripped");
        assert_eq!(atom.author_name.as_deref(), Some("alice"));
        assert_eq!(atom.occurred_at.to_rfc3339(), "2026-01-02T03:04:05+00:00");
    }

    #[test]
    fn normalize_pr_orders_reviews_and_comments_chronologically() {
        let detail = sample_pr_detail();
        let reviews = vec![json!({
            "user": {"login": "bob"}, "state": "APPROVED", "body": "lgtm",
            "submitted_at": "2026-01-01T12:00:00Z"
        })];
        let issue_comments = vec![json!({
            "user": {"login": "carol"}, "body": "question", "created_at": "2026-01-01T09:00:00Z"
        })];
        let atom = normalize_pr(555, 42, &detail, &reviews, &[], &issue_comments, &[]);
        let carol_pos = atom.content.find("carol").unwrap();
        let bob_pos = atom.content.find("bob").unwrap();
        assert!(carol_pos < bob_pos, "the earlier comment (carol, 09:00) must appear before the later one (bob, 12:00)");
    }

    #[test]
    fn normalize_pr_folds_in_commits() {
        let detail = sample_pr_detail();
        let commits = vec![json!({
            "sha": "abcdef1234567890",
            "commit": {"message": "first commit\n\nmore detail"}
        })];
        let atom = normalize_pr(555, 42, &detail, &[], &[], &[], &commits);
        assert!(atom.content.contains("abcdef1"));
        assert!(atom.content.contains("first commit"));
        assert!(!atom.content.contains("more detail"), "only the commit message's first line is folded in");
    }

    // ── normalize_issue ─────────────────────────────────────────────────────

    #[test]
    fn is_actually_a_pull_request_detects_the_pull_request_key() {
        assert!(is_actually_a_pull_request(&json!({"number": 1, "pull_request": {"url": "x"}})));
        assert!(!is_actually_a_pull_request(&json!({"number": 1})));
    }

    #[test]
    fn normalize_issue_builds_header_and_source_id() {
        let issue = json!({
            "number": 7,
            "title": "Bug report",
            "body": "It crashes",
            "user": {"login": "dave"},
            "state": "open",
            "labels": [{"name": "bug"}],
            "updated_at": "2026-02-01T00:00:00Z"
        });
        let atom = normalize_issue(555, &issue, &[]);
        assert_eq!(atom.source_type, "github_issue");
        assert_eq!(atom.source_id, "555:issue:7");
        assert!(atom.content.contains("Issue #7: Bug report"));
        assert!(atom.content.contains("bug"));
        assert!(atom.content.contains("It crashes"));
    }

    #[test]
    fn normalize_issue_orders_comments_chronologically() {
        let issue = json!({"number": 7, "title": "t", "body": "b", "user": {"login": "dave"}, "state": "open"});
        let comments = vec![
            json!({"user": {"login": "later"}, "body": "second", "created_at": "2026-02-02T00:00:00Z"}),
            json!({"user": {"login": "earlier"}, "body": "first", "created_at": "2026-02-01T00:00:00Z"}),
        ];
        let atom = normalize_issue(555, &issue, &comments);
        let earlier_pos = atom.content.find("earlier").unwrap();
        let later_pos = atom.content.find("later").unwrap();
        assert!(earlier_pos < later_pos);
    }

    // ── normalize_release ───────────────────────────────────────────────────

    #[test]
    fn normalize_release_with_previous_tag_builds_compare_link() {
        let release = json!({
            "id": 99,
            "tag_name": "v2.0.0",
            "name": "Version 2.0.0",
            "body": "Notes here",
            "author": {"login": "erin"},
            "published_at": "2026-03-01T00:00:00Z"
        });
        let atom = normalize_release(555, "acme", "repo", &release, Some("v1.0.0"));
        assert_eq!(atom.source_type, "github_release");
        assert_eq!(atom.source_id, "555:release:99");
        assert!(atom.content.contains("https://github.com/acme/repo/compare/v1.0.0...v2.0.0"));
        assert!(atom.content.contains("Notes here"));
    }

    #[test]
    fn normalize_release_without_previous_tag_has_no_compare_link() {
        let release = json!({
            "id": 1, "tag_name": "v1.0.0", "name": "First", "body": "", "author": {"login": "erin"},
            "published_at": "2026-01-01T00:00:00Z"
        });
        let atom = normalize_release(555, "acme", "repo", &release, None);
        assert!(!atom.content.contains("Compare:"));
    }

    // ── commit helpers ──────────────────────────────────────────────────────

    #[test]
    fn squash_merge_pr_number_matches_a_trailing_suffix() {
        assert_eq!(squash_merge_pr_number("Add widgets (#123)"), Some(123));
        assert_eq!(squash_merge_pr_number("Fix bug (#4)"), Some(4));
    }

    #[test]
    fn squash_merge_pr_number_rejects_non_matching_messages() {
        assert_eq!(squash_merge_pr_number("Add widgets"), None);
        assert_eq!(squash_merge_pr_number("See issue #123 for details"), None);
        assert_eq!(squash_merge_pr_number("Release (#abc)"), None);
    }

    #[test]
    fn pr_merge_commit_pr_number_matches_githubs_merge_message() {
        assert_eq!(pr_merge_commit_pr_number("Merge pull request #1116 from CoralSwarm/feat/teams"), Some(1116));
        assert_eq!(pr_merge_commit_pr_number("Merge pull request #1116"), None, "no ` from ` part");
        assert_eq!(pr_merge_commit_pr_number("Merge pull request # from x"), None);
        assert_eq!(pr_merge_commit_pr_number("Merge branch 'main' into feat"), None, "a local merge is not a PR merge");
        assert_eq!(pr_merge_commit_pr_number("Revert \"Merge pull request #5 from a/b\""), None);
    }

    fn listed(sha: &str, message: &str, parents: &[&str]) -> Value {
        json!({"sha": sha, "commit": {"message": message}, "parents": parents.iter().map(|p| json!({"sha": p})).collect::<Vec<_>>()})
    }

    #[test]
    fn is_pr_merge_commit_needs_two_parents_and_the_message() {
        assert!(is_pr_merge_commit(&listed("m", "Merge pull request #7 from a/b\n\nTitle", &["p1", "p2"])));
        assert!(!is_pr_merge_commit(&listed("m", "Merge pull request #7 from a/b", &["p1"])), "one parent: not a merge commit");
        assert!(!is_pr_merge_commit(&listed("m", "Merge branch 'x'", &["p1", "p2"])));
    }

    /// The 2026-10-04 prod shape (PR #1116): a direct commit `d1`, then a PR
    /// branch `b1 <- b2` cut from `d1`, merged by `m`, then direct `d2` on
    /// top. Only `b1` and `b2` came in through the PR; `d1` sits on the
    /// first-parent chain even though the branch descends from it.
    #[test]
    fn commits_merged_in_by_prs_is_the_merged_branch_minus_the_first_parent_chain() {
        let commits = vec![
            listed("d1", "Direct fix", &["before"]),
            listed("b1", "Branch work", &["d1"]),
            listed("b2", "More branch work", &["b1"]),
            listed("m", "Merge pull request #1116 from CoralSwarm/feat/x", &["d1", "b2"]),
            listed("d2", "Direct follow-up", &["m"]),
        ];
        let merged = commits_merged_in_by_prs(&commits, "d2");
        let mut got: Vec<&str> = merged.iter().map(String::as_str).collect();
        got.sort();
        assert_eq!(got, vec!["b1", "b2"]);

        let local_merge = vec![
            listed("f1", "Feature", &["before"]),
            listed("lm", "Merge branch 'feature'", &["before", "f1"]),
        ];
        assert!(commits_merged_in_by_prs(&local_merge, "lm").is_empty(), "a non-PR merge leaves its commits to the association check");
    }

    #[test]
    fn has_pr_association_detects_non_empty() {
        assert!(!has_pr_association(&[]));
        assert!(has_pr_association(&[json!({"number": 1})]));
    }

    #[test]
    fn normalize_commit_builds_header_and_source_id() {
        let detail = json!({
            "sha": "0123456789abcdef",
            "commit": {
                "message": "Fix the thing\n\nlonger body",
                "author": {"name": "Frank", "date": "2026-04-01T00:00:00Z"}
            },
            "author": {"login": "frank"},
            "stats": {"additions": 5, "deletions": 1},
            "files": [{"filename": "src/lib.rs"}, {"filename": "README.md"}]
        });
        let atom = normalize_commit(555, &detail);
        assert_eq!(atom.source_type, "github_commit");
        assert_eq!(atom.source_id, "555:commit:0123456789abcdef");
        assert!(atom.content.contains("+5/-1"));
        assert!(atom.content.contains("src/lib.rs"));
        assert!(atom.content.contains("README.md"));
        assert!(atom.content.contains("Fix the thing"));
        assert!(atom.content.contains("longer body"), "the full commit message body is kept for a standalone commit atom");
        assert!(atom.title.contains("Fix the thing"));
        assert!(!atom.title.contains("longer body"), "the atom TITLE uses only the message's first line");
    }
}

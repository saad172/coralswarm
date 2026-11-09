//! Org domain verification (CS-443, org-onboarding plan §3 item 37,
//! `docs/plans/org-onboarding-from-the-invite.md`, D11) — everything below
//! `routes::org_domains` that is not HTTP shape: normalisation, the claim
//! and removal writes, the two proofs (DNS TXT over DoH, and a signed
//! confirmation link mailed to an address AT the claimed domain), and the
//! ONE read the rest of the system gates on
//! ([`verified_domains`] / [`matching_verified_domain`]).
//!
//! # Why this module exists at all
//!
//! D11: *"auto-add applies only to addresses matching a **verified** org
//! domain"*, and *"**Verified matters**, or it is a text field someone types
//! `gmail.com` into."* Before migration 0242 there was no org-domain concept
//! in this codebase whatsoever — `email_domain` is a PII-redaction helper
//! for log lines. Two consumers read this module today:
//!
//!   - [`crate::connectors::slack::verified_org_domain`] — D2's guest /
//!     Slack-Connect anchor domain (item 1, amended by D21). A
//!     claimed-but-UNVERIFIED domain answering `Some(...)` there would
//!     silently widen the guest rule for anyone who can type a domain into
//!     a settings box, so every read in this module that feeds a GATE
//!     filters `verified_at IS NOT NULL`.
//!   - [`crate::services::assertion_ladder`] — D16's rung 2 ("full workspace
//!     member **and** verified domain match").
//!
//! # One org per verified domain, globally (founder ruling, 2026-09-22)
//!
//! CS-447/migration 0247, amending D11 a second time. 0242 chose
//! `UNIQUE (account_id, domain)` and argued explicitly AGAINST a global
//! index; that argument held only while nothing read these rows to spend
//! money. Item 9 (auto-add) changed the premise: an address at a
//! doubly-verified domain matches BOTH orgs, so the system would be
//! choosing which org a stranger silently joins **while consuming a paid
//! slot**. Decided: **first to verify wins.**
//!
//! ```text
//! claim  acme.com  -> per-account, two orgs may both claim it
//! verify acme.com  -> GLOBAL, the first org to prove it takes it
//! ```
//!
//! Enforced by a partial unique index
//! (`org_domains_verified_domain_global_idx ... WHERE verified_at IS NOT
//! NULL`) and surfaced by [`mark_verified`] as
//! [`VerifyOutcome::TakenByAnotherOrg`]. The refusal is derived from the
//! WRITE's own unique violation rather than from a pre-read, for two
//! reasons that both matter: the other org's row is invisible under 0242's
//! `account_isolation` policy and must stay that way, and a pre-read would
//! be a check-then-act window two orgs verifying at once could both pass.
//!
//! # RLS
//!
//! `org_domains` (0242) enables RLS with a strict `app.account_id`
//! predicate, and `state.control_db()` is `coralswarm_app`
//! (`rolbypassrls = f`). EVERY read and write below opens
//! [`crate::tenancy::scope::begin_account_scoped`] first — there is no
//! unscoped path to this table in production code, which is the (a) half of
//! the permanent fix 0238's own header records CodeRabbit having to force
//! for the identical table shape. The (b) half — `org_domains` on
//! `scripts/check_rls_coverage.sh`'s hand-curated `EXTRA_RLS_TABLES` and on
//! `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` — ships in this same
//! PR.
//!
//! # The two proofs
//!
//! **DNS TXT** ([`check_dns_txt`]) publishes
//! `_coralswarm-verification.<domain> IN TXT
//! "coralswarm-domain-verification=<token>"`. There is no DNS resolver crate
//! in this workspace and this PR deliberately does not add one: the lookup
//! rides the existing `reqwest` client over DNS-over-HTTPS, with the
//! resolver endpoint behind [`doh_resolver_url`] — the same
//! `RESEND_API_BASE_URL` / `SLACK_API_BASE` override shape, for the same
//! reason (a unit test can point one scenario at a stub without a live
//! resolver, and it is never set in production).
//!
//! **Could not ask ≠ not published.** [`check_dns_txt`] returns
//! `Err(reason)` for a transport failure, a non-JSON body, or a resolver
//! SERVFAIL, and `Ok(DnsProof::Missing)` only when the resolver actually
//! answered and the record was genuinely absent. The route maps the two to
//! different statuses (502 vs. 409) precisely so an admin is never told
//! "your DNS record is wrong" when the truth is "we could not reach a
//! resolver" — the same discipline
//! `connectors::google_chat::fetch_domain_directory` states for its own
//! zero-results case, and the one D16 calls "the trap in the strongest
//! signal."
//!
//! **Email confirmation** mails a signed, short-lived HS256 JWT
//! ([`mint_confirmation_token`]) to an address the SERVER has checked is at
//! the claimed domain ([`email_is_at_domain`]). The token binds the row id,
//! the account, the domain, the address, AND the row's own
//! `verification_token`, so a link minted before a domain was removed cannot
//! verify a later re-claim of the same name.
//!
//! # The `gmail.com` gap, and the founder ruling that closed it
//!
//! D11 argues that requiring verification is what stops the domain box being
//! "a text field someone types `gmail.com` into". That is true of the DNS
//! proof and FALSE of the email proof: anybody with a `@gmail.com` address
//! can click a link mailed to their own `@gmail.com` address and thereby
//! "prove" control of `gmail.com`, after which every Gmail user in the world
//! would match that org's domain rule.
//!
//! **Founder ruling, 2026-09-22 — "both, belt and braces"**, amending D11:
//!
//! 1. [`is_public_email_domain`] refuses the well-known consumer providers
//!    at CLAIM time, for **both** methods. Nobody can claim `gmail.com` as
//!    an org domain at all. The list is necessarily partial.
//! 2. **Only a DNS-verified domain may arm auto-add.**
//!    ([`VerificationMethod::may_arm_auto_add`].) An email-confirmed domain
//!    still proves control and still anchors D2's guest rule, but it never
//!    satisfies D16 rung 2's domain match.
//!
//! ```text
//! verified by DNS   -> D2 guest rule + D16 rung 2 / auto-add
//! verified by email -> D2 guest rule only
//! unverified        -> neither
//! ```
//!
//! The reasoning behind (2) is that an individual mailbox cannot prove org
//! control while a TXT record can — so the curated list in (1) stops being
//! the only thing standing between an org and auto-adding strangers, which
//! is more weight than a hand-maintained blocklist should ever carry.
//!
//! Concretely: [`verified_domains`] (D2's anchor, read by
//! `connectors::slack::verified_org_domain`) accepts **either** method and
//! is deliberately NOT narrowed;
//! [`verified_domains_with_method`]/[`matching_verified_domain`] carry the
//! method through so `services::assertion_ladder::domain_signal` can filter
//! rung 2 to `dns_txt`. Where an org has one of each, only the DNS-verified
//! one counts toward rung 2.

use serde_json::Value as Json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::scope::begin_account_scoped;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OrgDomainError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
}

/// How a verified row was proved — D11's *"the org's settings show which one
/// proved it"*. Serialised to `org_domains.method`'s own two-value `CHECK`
/// domain (0242) and rendered straight back to the admin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerificationMethod {
    DnsTxt,
    Email,
}

impl VerificationMethod {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::DnsTxt => "dns_txt",
            Self::Email => "email",
        }
    }

    /// Parse `org_domains.method` back. `None` for anything outside the
    /// column's own two-value `CHECK` domain — unreachable through this
    /// module's own writes, and fails closed (the row is dropped from every
    /// gate's view) rather than guessing if a future migration ever widens
    /// the domain without teaching this function about it.
    pub(crate) fn from_column(raw: &str) -> Option<Self> {
        match raw {
            "dns_txt" => Some(Self::DnsTxt),
            "email" => Some(Self::Email),
            _ => None,
        }
    }

    /// May a domain proved this way arm **auto-add** (D16 rung 2)?
    ///
    /// **`false` for [`VerificationMethod::Email`]** — founder ruling,
    /// 2026-09-22, closing the gap CS-443 flagged in D11. An emailed
    /// confirmation proves somebody holds *one mailbox* at the domain; a TXT
    /// record proves control of the *domain itself*. Only the second is a
    /// safe basis for adding strangers to an org without an admin looking:
    /// the curated consumer-provider blocklist
    /// ([`is_public_email_domain`]) must not be the only thing standing
    /// between an org and auto-adding everyone at a free mail provider it
    /// happens to hold an address at.
    ///
    /// D2's guest rule is unaffected and still accepts either method — see
    /// [`verified_domains`].
    pub(crate) fn may_arm_auto_add(self) -> bool {
        matches!(self, Self::DnsTxt)
    }
}

/// One `org_domains` row, presentation-shaped for the admin list.
#[derive(Debug, Clone)]
pub(crate) struct OrgDomain {
    pub id: Uuid,
    pub domain: String,
    pub verification_token: String,
    pub verified_at: Option<chrono::DateTime<chrono::Utc>>,
    /// `"dns_txt"` / `"email"` / `None` while unverified — read back as the
    /// raw column rather than re-parsed into [`VerificationMethod`], because
    /// the ONLY consumer is a JSON field an admin reads.
    pub method: Option<String>,
    pub claimed_by: Option<Uuid>,
    pub claimed_at: chrono::DateTime<chrono::Utc>,
    pub confirmation_email: Option<String>,
    pub confirmation_sent_at: Option<chrono::DateTime<chrono::Utc>>,
}

type DomainRow = (
    Uuid,
    String,
    String,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<String>,
    Option<Uuid>,
    chrono::DateTime<chrono::Utc>,
    Option<String>,
    Option<chrono::DateTime<chrono::Utc>>,
);

const DOMAIN_SELECT: &str = "\
    SELECT id, domain, verification_token, verified_at, method, claimed_by, claimed_at, \
           confirmation_email, confirmation_sent_at \
      FROM org_domains";

fn domain_from_row(row: DomainRow) -> OrgDomain {
    OrgDomain {
        id: row.0,
        domain: row.1,
        verification_token: row.2,
        verified_at: row.3,
        method: row.4,
        claimed_by: row.5,
        claimed_at: row.6,
        confirmation_email: row.7,
        confirmation_sent_at: row.8,
    }
}

// ── Normalisation ─────────────────────────────────────────────────────────

/// Why a domain string was refused at claim time. Every variant is rendered
/// to the admin verbatim — unlike the token/enumeration surfaces elsewhere
/// in this crate there is nothing to hide here: the caller is an
/// authenticated admin of the org and typed the string themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DomainRejection {
    Empty,
    /// Contains a scheme, a path, whitespace, or an `@` in the middle — the
    /// admin pasted a URL or an email address rather than a domain.
    NotADomain,
    /// A single label (`localhost`, `com`) — never an email domain.
    NoDot,
    /// A label is empty, over-long, or carries a character that is not
    /// `[a-z0-9-]` after normalisation.
    BadLabel,
    /// A well-known consumer mail provider — see this module's own doc
    /// comment for why the email proof alone cannot be trusted to keep
    /// these out.
    PublicEmailProvider,
}

impl DomainRejection {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Empty => "a domain is required",
            Self::NotADomain => "that is not a domain — enter it as acme.com, with no scheme, path, or @",
            Self::NoDot => "a domain must have at least two labels, e.g. acme.com",
            Self::BadLabel => "that domain contains a label that is not a valid hostname label",
            Self::PublicEmailProvider => {
                "that is a public email provider — an org domain must be one your org controls"
            }
        }
    }
}

/// Consumer mail providers whose addresses prove nothing about an employer.
/// Deliberately a short, hand-curated list of the ones a real admin might
/// plausibly type, not an attempt at completeness — see this module's own
/// doc comment ("a mitigation, not a closure").
const PUBLIC_EMAIL_DOMAINS: &[&str] = &[
    "aol.com",
    "duck.com",
    "fastmail.com",
    "gmail.com",
    "gmx.com",
    "gmx.de",
    "gmx.net",
    "googlemail.com",
    "hey.com",
    "hotmail.co.uk",
    "hotmail.com",
    "icloud.com",
    "live.com",
    "mail.com",
    "mail.ru",
    "me.com",
    "msn.com",
    "outlook.com",
    "pm.me",
    "proton.me",
    "protonmail.com",
    "qq.com",
    "yahoo.co.uk",
    "yahoo.com",
    "yandex.com",
    "yandex.ru",
    "zoho.com",
];

/// `true` for a normalised domain on [`PUBLIC_EMAIL_DOMAINS`].
pub(crate) fn is_public_email_domain(domain: &str) -> bool {
    PUBLIC_EMAIL_DOMAINS.contains(&domain)
}

/// Trim, lowercase, strip a leading `@` and a trailing root dot, then
/// validate. Pure — unit-tested directly, no DB.
///
/// The normalised form is what 0242's `(account_id, domain)` unique index
/// and every downstream comparison ([`matching_verified_domain`], the
/// assertion ladder's rung-2 check) use, so "Acme.COM" claimed twice is one
/// row, and an address at `@ACME.com` matches it.
pub(crate) fn normalize_domain(raw: &str) -> Result<String, DomainRejection> {
    let trimmed = raw.trim().trim_start_matches('@').trim_end_matches('.').trim();
    if trimmed.is_empty() {
        return Err(DomainRejection::Empty);
    }
    if trimmed.contains("://")
        || trimmed.contains('/')
        || trimmed.contains('@')
        || trimmed.contains(char::is_whitespace)
        || trimmed.contains(':')
    {
        return Err(DomainRejection::NotADomain);
    }
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.contains('.') {
        return Err(DomainRejection::NoDot);
    }
    if lowered.len() > 253 {
        return Err(DomainRejection::BadLabel);
    }
    for label in lowered.split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(DomainRejection::BadLabel);
        }
        if label.starts_with('-') || label.ends_with('-') {
            return Err(DomainRejection::BadLabel);
        }
        if !label.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
            return Err(DomainRejection::BadLabel);
        }
    }
    if is_public_email_domain(&lowered) {
        return Err(DomainRejection::PublicEmailProvider);
    }
    Ok(lowered)
}

/// Is this a local part we are willing to hand to an email provider?
///
/// Deliberately conservative, and NOT an attempt at RFC 5321 — a quoted
/// local part may legally contain almost anything, and this function refuses
/// those outright. Two reasons (CodeRabbit round 1, #1031):
///
///   1. **Correctness.** `"ops x@acme.com"` passed the old non-empty check,
///      reached Resend, was rejected as malformed, and surfaced to the admin
///      as a 502 "could not send" — a confusing answer to what is really a
///      typo in their own input. It is now a 400 naming the problem.
///   2. **Safety.** A CR or LF in an address is the classic header-injection
///      shape, and a comma or angle bracket is how one recipient becomes
///      several. Neither should ever reach an outbound-mail payload, whoever
///      the caller is.
///
/// The refused shapes are not addresses any real org's confirmation mailbox
/// uses, so the practical cost of this narrowness is nil.
fn local_part_is_sane(local: &str) -> bool {
    !local.is_empty()
        && local.len() <= 64
        && !local.starts_with('.')
        && !local.ends_with('.')
        && !local.contains("..")
        && local
            .chars()
            .all(|c| !c.is_whitespace() && !c.is_control() && !"<>,;:\\\"()[]".contains(c))
}

/// The domain half of an email address, normalised the same way
/// [`normalize_domain`] normalises a claimed domain (lowercased, trimmed,
/// trailing root dot stripped) — `None` for an address with no `@`, an
/// empty local part, or an empty domain part.
///
/// Deliberately NOT `normalize_domain`: this one must not apply the
/// public-provider refusal (a `@gmail.com` address is a perfectly real
/// address; it simply never matches a claimed domain, because `gmail.com`
/// can never BE one).
pub(crate) fn email_domain_part(email: &str) -> Option<String> {
    let trimmed = email.trim();
    let (local, domain) = trimmed.rsplit_once('@')?;
    if !local_part_is_sane(local) {
        return None;
    }
    let domain = domain.trim().trim_end_matches('.').to_ascii_lowercase();
    if domain.is_empty() || !domain.contains('.') {
        return None;
    }
    Some(domain)
}

/// D11's server-side gate on the email proof: a confirmation link may only
/// ever be mailed to an address **at the claimed domain**.
///
/// Exact equality on the normalised domain part — NOT a suffix test. A
/// suffix test would accept `ops@notacme.com` for a claim of `acme.com`
/// (and `ops@acme.com.evil.test` for good measure), which is the whole
/// proof, inverted. Subdomains are also refused: `ops@mail.acme.com` does
/// not confirm `acme.com`, because the org that controls a subdomain's mail
/// is not necessarily the org that controls the apex — the admin can claim
/// `mail.acme.com` as its own row if that is what they control.
pub(crate) fn email_is_at_domain(email: &str, domain: &str) -> bool {
    match email_domain_part(email) {
        Some(d) => d == domain.trim().to_ascii_lowercase(),
        None => false,
    }
}

/// Domains that admit a guest under D2/D21 (plan §3 item 30).
///
/// Every verified org domain is an anchor — a post-acquisition org on
/// several domains invites a guest on any of them. The connecting member's
/// domain is the bootstrap, and only while this lookup succeeded and found
/// nothing. A failed lookup admits nobody: could not ask is not "nothing is
/// verified", so a database error must not widen the rule onto the
/// connecting member's domain.
pub(crate) fn guest_anchor_domains(
    verified: Result<Vec<String>, String>,
    connecting_member_domain: Option<String>,
) -> Vec<String> {
    match verified {
        Ok(domains) if !domains.is_empty() => domains,
        Ok(_) => connecting_member_domain
            .map(|domain| domain.trim().to_ascii_lowercase())
            .filter(|domain| !domain.is_empty())
            .into_iter()
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Exact domain equality via [`email_is_at_domain`], against every anchor.
/// An empty anchor list admits nobody.
pub(crate) fn address_on_guest_anchor(email: &str, anchors: &[String]) -> bool {
    anchors.iter().any(|domain| email_is_at_domain(email, domain))
}

// ── The DNS TXT proof ─────────────────────────────────────────────────────

/// The TXT record NAME an admin must publish for `domain`.
pub(crate) fn dns_record_name(domain: &str) -> String {
    format!("_coralswarm-verification.{domain}")
}

/// The TXT record VALUE an admin must publish, for a row's
/// `verification_token`.
pub(crate) fn dns_record_value(token: &str) -> String {
    format!("coralswarm-domain-verification={token}")
}

/// A fresh, unguessable `verification_token` — 32 hex characters from the
/// same `Uuid::new_v4()` CSPRNG source every other opaque token in this
/// crate uses (`mint_unsubscribe_token`, `mint_prefill_token`).
pub(crate) fn mint_verification_token() -> String {
    Uuid::new_v4().simple().to_string()
}

/// The DNS-over-HTTPS endpoint. `DOH_RESOLVER_URL` overrides it, exactly
/// like `RESEND_API_BASE_URL` (`routes::org_invites::resend_base_url`) and
/// `SLACK_API_BASE` (`connectors::slack::slack_api_base`) — that override is
/// what makes [`check_dns_txt`] testable against a local stub, since this
/// workspace has no DNS resolver crate and this PR deliberately does not add
/// one.
///
/// **Never set this in production.** Read per call rather than cached in a
/// `OnceLock`, for the same reason `slack_api_base` gives: a test must be
/// able to point one scenario at a stub without leaking the override into
/// the rest of the process.
///
/// The default is Google's JSON DoH endpoint. Cloudflare's
/// (`https://cloudflare-dns.com/dns-query`) speaks the same JSON shape and
/// works as a drop-in override — [`check_dns_txt`] sends
/// `accept: application/dns-json`, which Cloudflare requires and Google
/// ignores.
pub(crate) fn doh_resolver_url() -> String {
    std::env::var("DOH_RESOLVER_URL").unwrap_or_else(|_| "https://dns.google/resolve".to_string())
}

/// What a completed DNS lookup found. `Err` from [`check_dns_txt`] is the
/// third, genuinely different outcome — "we could not ask" — and is never
/// folded into [`DnsProof::Missing`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DnsProof {
    /// The expected `coralswarm-domain-verification=<token>` string was
    /// present in a TXT record at the verification name.
    Found,
    /// The resolver answered and the record was genuinely not there.
    /// `records_seen` is how many TXT strings WERE at that name — surfaced
    /// to the admin as "we found N records, none of them yours", which is a
    /// materially different message from "we found nothing at all".
    Missing { records_seen: usize },
}

/// Flatten one DoH JSON body into the TXT strings it carries.
///
/// Google/Cloudflare return each TXT record's wire form in `data`, with the
/// character-strings quoted: a single-string record is `"\"abc\""`, and a
/// record split across the 255-byte wire limit is `"\"part1\" \"part2\""`,
/// which per RFC 1035 §3.3.14 concatenates with NO separator. Answers of
/// other types (a CNAME in the chain, type 5) are skipped rather than
/// misread as TXT payloads.
///
/// Pure — unit-tested directly against captured resolver bodies.
pub(crate) fn parse_txt_answers(body: &Json) -> Vec<String> {
    let Some(answers) = body.get("Answer").and_then(Json::as_array) else {
        return Vec::new();
    };
    answers
        .iter()
        .filter(|a| a.get("type").and_then(Json::as_i64) == Some(16))
        .filter_map(|a| a.get("data").and_then(Json::as_str))
        .map(concat_txt_chunks)
        .collect()
}

/// `"\"part1\" \"part2\""` → `part1part2`; an unquoted body is returned
/// trimmed as-is (some resolvers omit the quotes for a single short string).
fn concat_txt_chunks(data: &str) -> String {
    if !data.contains('"') {
        return data.trim().to_string();
    }
    let mut out = String::with_capacity(data.len());
    let mut in_quotes = false;
    let mut escaped = false;
    for ch in data.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            _ if in_quotes => out.push(ch),
            _ => {}
        }
    }
    out
}

/// Does any of `records` carry this row's expected value? Exact match on the
/// whole string after trimming — a TXT record that merely CONTAINS the token
/// as a substring is not accepted, so an unrelated record that happens to
/// quote the value (a copy-pasted support thread, say) cannot verify a
/// domain.
pub(crate) fn txt_records_prove(records: &[String], token: &str) -> bool {
    let expected = dns_record_value(token);
    records.iter().any(|r| r.trim() == expected)
}

/// Ask a DoH resolver for `_coralswarm-verification.<domain>`'s TXT records
/// and report whether this row's token is among them.
///
/// `Err(reason)` means **we could not ask** — a transport failure, a non-2xx
/// resolver response, a body that is not JSON, or a resolver `Status` that
/// is neither NOERROR (0) nor NXDOMAIN (3). NXDOMAIN specifically IS an
/// answer ("there is no such record"), so it maps to
/// [`DnsProof::Missing`], not to `Err`.
pub(crate) async fn check_dns_txt(
    http: &reqwest::Client,
    domain: &str,
    token: &str,
) -> Result<DnsProof, String> {
    let name = dns_record_name(domain);
    let url = doh_resolver_url();
    let send = http
        .get(&url)
        .header("accept", "application/dns-json")
        .query(&[("name", name.as_str()), ("type", "TXT")])
        .send();
    let resp = match tokio::time::timeout(std::time::Duration::from_secs(10), send).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => return Err(format!("DNS-over-HTTPS request failed: {e}")),
        Err(_) => return Err("DNS-over-HTTPS request timed out".to_string()),
    };
    if !resp.status().is_success() {
        return Err(format!("DNS-over-HTTPS resolver returned {}", resp.status()));
    }
    let body: Json = resp
        .json()
        .await
        .map_err(|e| format!("DNS-over-HTTPS resolver returned a non-JSON body: {e}"))?;
    match body.get("Status").and_then(Json::as_i64) {
        // NOERROR — a real answer, possibly with zero TXT records.
        Some(0) => {}
        // NXDOMAIN — also a real answer: the name does not exist.
        Some(3) => return Ok(DnsProof::Missing { records_seen: 0 }),
        Some(other) => return Err(format!("DNS-over-HTTPS resolver reported status {other}")),
        None => return Err("DNS-over-HTTPS resolver body carried no Status".to_string()),
    }
    let records = parse_txt_answers(&body);
    if txt_records_prove(&records, token) {
        Ok(DnsProof::Found)
    } else {
        Ok(DnsProof::Missing { records_seen: records.len() })
    }
}

// ── The email-confirmation proof ──────────────────────────────────────────

/// Lifetime of a domain-confirmation link. Short on purpose — unlike
/// `JOIN_NOTICE_TOKEN_TTL_SECS`' 30 days (a person being invited to an org
/// may reasonably read the mail a fortnight late), this token is clicked by
/// an admin who has just asked for it, and it is a *proof of control*: the
/// shorter it lives, the smaller the window in which a forwarded or leaked
/// message can verify a domain for someone who no longer has that mailbox.
pub(crate) const CONFIRMATION_TOKEN_TTL_SECS: i64 = 60 * 60 * 24 * 3;

/// How long before a second confirmation email may be sent for the same row
/// — also the window a failed send does NOT consume, because the claim is
/// rolled back (see [`claim_confirmation_send`]).
pub(crate) const CONFIRMATION_RESEND_COOLDOWN: &str = "10 minutes";

/// Claims in a domain-confirmation link. Short field names, the same
/// convention `oauth::state::StateClaims` and
/// `services::unmatched_recipient_email::JoinNoticeClaims` use.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ConfirmationClaims {
    /// `org_domains.id`.
    sub: String,
    /// `accounts.id` — the scope every read of that row opens.
    acc: String,
    /// The claimed domain, echoed for the confirmation page's copy.
    dom: String,
    /// The address this link was mailed to.
    eml: String,
    /// The row's `verification_token` at mint time. Re-checked against the
    /// live row before the row is marked verified, so a link minted for one
    /// claim can never verify a LATER re-claim of the same domain name
    /// (remove + re-add mints a fresh token, which invalidates every
    /// outstanding link for the old one).
    vtk: String,
    iat: i64,
    exp: i64,
}

/// The confirmation-link signing secret. `None` when
/// `DOMAIN_VERIFICATION_SECRET` is unset or empty — every caller treats that
/// as "this proof is unavailable right now", never a guess and never a
/// panic, the same contract
/// `services::unmatched_recipient_email::join_notice_secret` states for
/// `JOIN_NOTICE_SECRET` (CS-440): the SEND side refuses rather than mailing
/// a dead link, and the CONSUME side refuses rather than trusting an
/// unverifiable one. The DNS proof is unaffected and stays available.
pub(crate) fn domain_verification_secret() -> Option<String> {
    std::env::var("DOMAIN_VERIFICATION_SECRET").ok().filter(|s| !s.is_empty())
}

/// What a valid confirmation token verifies to.
#[derive(Debug, Clone)]
pub(crate) struct ConfirmationContext {
    pub domain_id: Uuid,
    pub account_id: Uuid,
    pub domain: String,
    pub email: String,
    pub verification_token: String,
}

/// Mint a signed confirmation link token. Pure (no DB, no I/O), secret taken
/// explicitly — the same `oauth::state::sign_state` shape, testable without
/// env mutation.
pub(crate) fn mint_confirmation_token(
    secret: &str,
    domain_id: Uuid,
    account_id: Uuid,
    domain: &str,
    email: &str,
    verification_token: &str,
) -> Result<String, String> {
    let now = chrono::Utc::now().timestamp();
    let claims = ConfirmationClaims {
        sub: domain_id.to_string(),
        acc: account_id.to_string(),
        dom: domain.to_string(),
        eml: email.to_string(),
        vtk: verification_token.to_string(),
        iat: now,
        exp: now + CONFIRMATION_TOKEN_TTL_SECS,
    };
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| format!("mint_confirmation_token: {e}"))
}

/// Verify a confirmation token. `None` for ANY failure — bad signature,
/// tampering, expiry, a non-UUID `sub`/`acc` — without distinguishing the
/// cause, the same "don't build an oracle" discipline
/// `oauth::state::verify_state` and `verify_join_notice_token` both state.
pub(crate) fn verify_confirmation_token(secret: &str, token: &str) -> Option<ConfirmationContext> {
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.validate_exp = true;
    validation.set_required_spec_claims(&["exp"]);
    let data = jsonwebtoken::decode::<ConfirmationClaims>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .ok()?;
    let c = data.claims;
    Some(ConfirmationContext {
        domain_id: Uuid::parse_str(&c.sub).ok()?,
        account_id: Uuid::parse_str(&c.acc).ok()?,
        domain: c.dom,
        email: c.eml,
        verification_token: c.vtk,
    })
}

// ── Writes ────────────────────────────────────────────────────────────────

/// How many domains one account may have claimed at once.
///
/// CodeRabbit round 1 (#1031): without a cap, one authenticated org admin
/// can create unbounded `org_domains` rows and — since
/// [`CONFIRMATION_RESEND_COOLDOWN`] is per-ROW, not per-account — send one
/// branded confirmation email per row. The shared admission limiter does not
/// close this: `AdmissionConfig::default` leaves `rate_enforce` false, so its
/// per-account ceiling is observe-only unless `RATE_LIMIT_ENFORCE=true`, and
/// even enabled it counts all requests rather than confirmation sends.
///
/// Capping the ROWS bounds the outbound fan-out too, because a confirmation
/// can only ever be aimed at an address at an already-claimed domain and each
/// row carries its own cooldown. Generous against any real org — D16 names
/// the post-acquisition company running on three domains; twenty-five leaves
/// room for an order of magnitude more than that — and an admin who genuinely
/// needs more can remove a stale claim.
pub(crate) const MAX_DOMAINS_PER_ACCOUNT: i64 = 25;

/// What [`claim_domain`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimOutcome {
    /// A fresh row.
    Claimed,
    /// This org had already claimed this domain and it is still unverified —
    /// the SAME `verification_token` is returned, never a fresh one, so a
    /// second click on "add domain" cannot invalidate the DNS record the
    /// admin already published.
    AlreadyClaimed,
    /// Already claimed AND already verified — nothing to do.
    AlreadyVerified,
    /// This account already holds [`MAX_DOMAINS_PER_ACCOUNT`] domains and
    /// this one is not among them — nothing was written. Re-claiming a
    /// domain the account ALREADY holds is unaffected by the cap (it writes
    /// no new row), so an admin at the ceiling can still re-read the TXT
    /// record for a domain they are mid-way through verifying.
    LimitReached,
}

/// Claim `domain` for `account_id`. Idempotent per (account, domain), and
/// capped at [`MAX_DOMAINS_PER_ACCOUNT`] rows per account.
pub(crate) async fn claim_domain(
    control: &PgPool,
    account_id: Uuid,
    claimed_by: Uuid,
    domain: &str,
) -> Result<(Option<OrgDomain>, ClaimOutcome), OrgDomainError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let token = mint_verification_token();
    // The cap is a `WHERE` on the INSERT itself, inside the same scoped
    // transaction, rather than a read-then-write — so it cannot be widened
    // by whatever happens between the two. Two genuinely concurrent claims
    // under READ COMMITTED can still each see a count below the ceiling and
    // both land, overshooting by a row; that is accepted, and is why
    // [`MAX_DOMAINS_PER_ACCOUNT`] is set generously rather than tightly.
    // `ON CONFLICT DO NOTHING` still covers the re-claim case below.
    let inserted: Option<DomainRow> = sqlx::query_as(
        "INSERT INTO org_domains (account_id, domain, verification_token, claimed_by) \
         SELECT $1, $2, $3, $4 \
          WHERE (SELECT count(*) FROM org_domains WHERE account_id = $1) < $5 \
         ON CONFLICT (account_id, domain) DO NOTHING \
         RETURNING id, domain, verification_token, verified_at, method, claimed_by, claimed_at, \
                   confirmation_email, confirmation_sent_at",
    )
    .bind(account_id)
    .bind(domain)
    .bind(&token)
    .bind(claimed_by)
    .bind(MAX_DOMAINS_PER_ACCOUNT)
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(row) = inserted {
        tx.commit().await?;
        return Ok((Some(domain_from_row(row)), ClaimOutcome::Claimed));
    }

    // Nothing was inserted, for one of two quite different reasons. Read the
    // existing row back rather than re-deriving it, exactly like
    // `org_join_requests::enqueue_join_request` reads its own `AlreadyOpen`
    // row off the same conflict — and `fetch_optional`, not `fetch_one`,
    // because its ABSENCE is the signal that the cap (not the conflict)
    // suppressed this write.
    let existing: Option<DomainRow> = sqlx::query_as(&format!(
        "{DOMAIN_SELECT} WHERE account_id = $1 AND domain = $2"
    ))
    .bind(account_id)
    .bind(domain)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    let Some(existing) = existing.map(domain_from_row) else {
        return Ok((None, ClaimOutcome::LimitReached));
    };
    let outcome = if existing.verified_at.is_some() {
        ClaimOutcome::AlreadyVerified
    } else {
        ClaimOutcome::AlreadyClaimed
    };
    Ok((Some(existing), outcome))
}

/// Every claimed domain for this org, oldest claim first — the admin list.
pub(crate) async fn list_domains(control: &PgPool, account_id: Uuid) -> Result<Vec<OrgDomain>, OrgDomainError> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control,account_id).await?;
    let value = list_domains_tx(&mut tx,account_id).await?;
    tx.rollback().await?;
    Ok(value)
}

/// One row, scoped to `account_id` — an admin route must never resolve a
/// domain id belonging to a DIFFERENT org, so a mismatched `account_id`
/// reads back `None` and the route's own 404 covers both cases (same guard
/// `org_join_requests::get_open` documents for itself).
pub(crate) async fn get_domain(
    control: &PgPool,
    account_id: Uuid,
    id: Uuid,
) -> Result<Option<OrgDomain>, OrgDomainError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let row = sqlx::query_as::<_, DomainRow>(&format!(
        "{DOMAIN_SELECT} WHERE id = $1 AND account_id = $2"
    ))
    .bind(id)
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row.map(domain_from_row))
}

/// Remove a claimed (or verified) domain. Returns `false` when the id is
/// unknown to this org.
///
/// Deleting the row deletes the proof with it: any outstanding confirmation
/// link stops working (its `vtk` no longer matches any live row), and a
/// later re-claim of the same name mints a fresh token and starts over
/// UNVERIFIED — so removing a domain genuinely narrows the guest rule and
/// the auto-add gate rather than leaving a stale verification behind.
pub(crate) async fn remove_domain(control: &PgPool, account_id: Uuid, id: Uuid) -> Result<bool, OrgDomainError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let result = sqlx::query("DELETE FROM org_domains WHERE id = $1 AND account_id = $2")
        .bind(id)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(result.rows_affected() > 0)
}

/// Stamp `verified_at`/`method` — the ONE write that arms every gate reading
/// this table.
///
/// Guarded `WHERE verified_at IS NULL OR (method <> 'dns_txt' AND $4 =
/// 'dns_txt')` — two cases, not one:
///
///   - **First proof** (`verified_at IS NULL`): idempotent under a double
///     click, the same guard-clause-instead-of-a-lock shape
///     `org_join_requests::mark_decided` uses — a second call affects 0 rows
///     and returns [`VerifyOutcome::NothingToVerify`] rather than re-dating
///     a verification or overwriting the method that actually proved it.
///   - **UPGRADE, email → dns_txt** (`verified_at IS NOT NULL`, existing
///     `method <> 'dns_txt'`, incoming `method = 'dns_txt'`): the DNS proof
///     is strictly stronger (see this module's own "belt and braces" doc
///     section) and the row's method may move to it. `verified_at` is
///     bumped to the DNS proof's own `now()`, deliberately NOT left at the
///     original email-proof time: the row stores exactly ONE
///     `(verified_at, method)` pair, 0242's own `CHECK ((verified_at IS
///     NULL) = (method IS NULL))` couples them, and this module's doc
///     comment calls that pair "the method that proved it" — singular.
///     Leaving the OLD timestamp under the NEW method would read as "DNS
///     TXT has proven this since `<email-proof-date>`", which is false: no
///     TXT record existed as proof until today. Nothing downstream reads
///     `verified_at` as an audit trail of the FIRST time an org proved
///     control by any means (every other consumer in this crate reads only
///     `verified_at IS NOT NULL`, or uses it purely to order MULTIPLE
///     distinct domains, never to date one row's own history) — see
///     `routes::org_domains::verify_dns`'s own doc comment.
///
/// Every OTHER combination is refused, in both directions:
///
///   - An already-`dns_txt` row re-verified with `method = 'dns_txt'`
///     matches neither arm (existing method already equals `'dns_txt'`) —
///     0 rows, [`VerifyOutcome::NothingToVerify`]. (In practice
///     `routes::org_domains::verify_dns` short-circuits this case before
///     ever calling here, to avoid the wasted resolver round trip; this
///     guard is the second, DB-level belt for the same buckle.)
///   - **A `dns_txt` row can NEVER be downgraded to `email`.** The upgrade
///     arm only fires for `$4 = 'dns_txt'`, so a call with `method =
///     VerificationMethod::Email` (the ONLY caller is
///     `routes::org_domains::confirm_post`, consuming a mailed link) can
///     only ever match the first arm — meaning it writes exclusively to a
///     row that is still `verified_at IS NULL`. A `dns_txt`-verified row,
///     or even an `email`-verified one re-submitting the SAME method, both
///     fail every arm and return `NothingToVerify` untouched.
///     `routes::org_domains::request_confirmation` also refuses to mint a
///     fresh link for any row with `verified_at.is_some()` regardless of
///     method, so this is genuinely belt-and-braces, not the only guard.
///
/// `expected_token` pins the write to the CLAIM it was started for: a
/// confirmation link, or a `verify` call raced against a remove-and-re-add,
/// must not verify a row whose token has since been re-minted.
pub(crate) async fn mark_verified(
    control: &PgPool,
    account_id: Uuid,
    id: Uuid,
    expected_token: &str,
    method: VerificationMethod,
) -> Result<VerifyOutcome, OrgDomainError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let result = sqlx::query(
        "UPDATE org_domains SET verified_at = now(), method = $4 \
          WHERE id = $1 AND account_id = $2 AND verification_token = $3 \
            AND (verified_at IS NULL OR (method <> 'dns_txt' AND $4 = 'dns_txt'))",
    )
    .bind(id)
    .bind(account_id)
    .bind(expected_token)
    .bind(method.as_str())
    .execute(&mut *tx)
    .await;
    let result = match result {
        Ok(r) => r,
        Err(e) if is_global_verified_domain_conflict(&e) => {
            // The partial unique index refused this write: some OTHER
            // account already has a verified row for this exact domain.
            // Nothing is committed.
            tx.rollback().await.ok();
            return Ok(VerifyOutcome::TakenByAnotherOrg);
        }
        Err(e) => return Err(e.into()),
    };
    tx.commit().await?;
    Ok(if result.rows_affected() > 0 { VerifyOutcome::Verified } else { VerifyOutcome::NothingToVerify })
}

/// What [`mark_verified`] actually did — three outcomes, none of them an
/// error, because all three are things an admin can be told.
///
/// This replaced a bare `bool` when the founder's 2026-09-22 ruling made a
/// THIRD answer possible (migration 0247). A `bool` would have had to fold
/// "somebody else owns this domain" into either `false` ("nothing to
/// confirm") — which tells an admin their DNS record is wrong when it is
/// perfectly correct — or into `Err` ("we cannot confirm this right now"),
/// which tells them to retry forever. Neither is actionable, and the
/// founder's ruling asks specifically for "a message an admin can act on".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerifyOutcome {
    /// The row is now verified by `method`.
    Verified,
    /// No row matched: unknown id, wrong account, a re-minted
    /// `verification_token`, or already verified. One outcome for all four
    /// deliberately — see [`crate::routes::org_domains::confirm_post`]'s own
    /// "not an oracle" comment.
    NothingToVerify,
    /// **Founder decision, 2026-09-22 (migration 0247, amending D11): a
    /// domain may be verified by ONE org, globally, and the first to verify
    /// wins.** Another account already holds a verified row for this exact
    /// domain. The claim here is untouched and still unverified — claiming
    /// stays per-account, only proof is exclusive.
    TakenByAnotherOrg,
}

/// Is this the global-verified-domain index refusing the write (migration
/// 0247), as opposed to any other database error?
///
/// Matched on the CONSTRAINT NAME, not on the SQLSTATE alone:
/// `org_domains` carries a second unique index
/// (`org_domains_account_domain_idx`, 0242's per-account claim uniqueness),
/// and a 23505 from THAT one would mean something entirely different — it
/// is unreachable from this particular `UPDATE`, which touches neither
/// `account_id` nor `domain`, and treating every 23505 as "another org owns
/// it" would turn a future schema change into a wrong, confident message to
/// an admin. Fails closed in the other direction too: an unrecognised error
/// stays an error rather than being reported as a domain conflict.
fn is_global_verified_domain_conflict(e: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db) = e else {
        return false;
    };
    db.code().as_deref() == Some("23505")
        && db.constraint() == Some("org_domains_verified_domain_global_idx")
}

/// Claim the right to send a confirmation email for this row, inside a
/// transaction the CALLER holds open across the send and commits only on
/// success — `workers::org_join_request_digest::process_account`'s own
/// claim-vs-send order, for the identical reason stated there: a failed send
/// must not consume the cooldown that would otherwise let the admin retry
/// immediately.
///
/// Returns `false` when the row is unknown, already verified, or still
/// inside [`CONFIRMATION_RESEND_COOLDOWN`].
pub(crate) async fn claim_confirmation_send(
    tx: &mut sqlx::PgConnection,
    account_id: Uuid,
    id: Uuid,
    email: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(&format!(
        "UPDATE org_domains \
            SET confirmation_email = $3, confirmation_sent_at = now() \
          WHERE id = $1 AND account_id = $2 AND verified_at IS NULL \
            AND (confirmation_sent_at IS NULL \
                 OR confirmation_sent_at < now() - INTERVAL '{CONFIRMATION_RESEND_COOLDOWN}')"
    ))
    .bind(id)
    .bind(account_id)
    .bind(email)
    .execute(&mut *tx)
    .await?;
    Ok(result.rows_affected() > 0)
}

// ── The read every gate in the system depends on ──────────────────────────

/// Every VERIFIED domain for this org **with the method that proved it**,
/// earliest verification first — the base read both gates go through.
///
/// **`verified_at IS NOT NULL` is one half of the whole point of this
/// function.** A claimed-but-unverified row is a string an admin typed and
/// nothing more; letting one out of here would widen D2's guest rule
/// (`connectors::slack::resolve_anchor_domain`) and arm D16's rung 2 on no
/// evidence at all.
///
/// **Carrying `method` is the other half** (founder ruling, 2026-09-22 — see
/// this module's own "belt and braces" doc section). The two gates this read
/// feeds do NOT accept the same methods:
///
/// | verified by | D2's guest rule | D16 rung 2 / auto-add |
/// |---|---|---|
/// | `dns_txt`   | yes | yes |
/// | `email`     | yes | **no** |
/// | (unverified)| no  | no |
///
/// Returning bare `String`s would make those two indistinguishable at every
/// call site, which is exactly how the distinction gets refactored away.
/// A caller that genuinely does not care takes [`verified_domains`].
pub(crate) async fn verified_domains_with_method(
    control: &PgPool,
    account_id: Uuid,
) -> Result<Vec<(String, VerificationMethod)>, OrgDomainError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT domain, method FROM org_domains \
          WHERE account_id = $1 AND verified_at IS NOT NULL \
          ORDER BY verified_at ASC, domain ASC",
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    // 0242's `CHECK ((verified_at IS NULL) = (method IS NULL))` makes a
    // verified row with an unreadable method impossible, and its own
    // two-value `CHECK` makes an unknown string impossible — but a row that
    // somehow carried one is dropped rather than guessed at, which fails
    // closed for BOTH gates.
    Ok(rows
        .into_iter()
        .filter_map(|(domain, method)| VerificationMethod::from_column(&method).map(|m| (domain, m)))
        .collect())
}

/// Every VERIFIED domain, **by either method** — D2's guest-rule anchor
/// (`connectors::slack::verified_org_domain`) and nothing else.
///
/// Deliberately method-blind: an email-confirmed domain still proves the org
/// controls a mailbox there, which is all D2's rule needs to decide whether
/// a guest or Slack-Connect participant is one of this org's own people.
/// Auto-add asks a strictly harder question and reads
/// [`verified_domains_with_method`] instead.
pub(crate) async fn verified_domains(control: &PgPool, account_id: Uuid) -> Result<Vec<String>, OrgDomainError> {
    Ok(verified_domains_with_method(control, account_id)
        .await?
        .into_iter()
        .map(|(domain, _method)| domain)
        .collect())
}

/// The verified domain `email` belongs to, **and how it was proved** — the
/// reason [`verified_domains_with_method`] returns a LIST rather than one
/// string is D16's own text, which names the post-acquisition org that
/// legitimately runs on three domains: a member on any one of them matches.
///
/// The method comes back with it, never stripped, so no caller can treat an
/// email-confirmed domain as a DNS-verified one by accident (founder ruling,
/// 2026-09-22). `services::assertion_ladder::domain_signal` is the caller
/// that acts on the difference.
pub(crate) async fn matching_verified_domain(
    control: &PgPool,
    account_id: Uuid,
    email: &str,
) -> Result<Option<(String, VerificationMethod)>, OrgDomainError> {
    let Some(candidate) = email_domain_part(email) else {
        return Ok(None);
    };
    Ok(verified_domains_with_method(control, account_id)
        .await?
        .into_iter()
        .find(|(d, _)| *d == candidate))
}

#[cfg(test)]
mod tests {
    //! Pure, DB-and-network-free: normalisation, the at-the-domain gate, the
    //! DoH body parser, and the confirmation-token round trip (its secret is
    //! taken explicitly, so no env mutation or lock is needed — same
    //! reasoning `unmatched_recipient_email::join_notice_token_tests` gives).

    use super::*;
    use serde_json::json;

    #[test]
    fn normalizes_case_at_sign_and_trailing_dot() {
        assert_eq!(normalize_domain("  Acme.COM  ").unwrap(), "acme.com");
        assert_eq!(normalize_domain("@acme.com").unwrap(), "acme.com");
        assert_eq!(normalize_domain("acme.com.").unwrap(), "acme.com");
        assert_eq!(normalize_domain("mail.acme.co.uk").unwrap(), "mail.acme.co.uk");
    }

    #[test]
    fn refuses_urls_addresses_single_labels_and_bad_labels() {
        assert_eq!(normalize_domain(""), Err(DomainRejection::Empty));
        assert_eq!(normalize_domain("   "), Err(DomainRejection::Empty));
        assert_eq!(normalize_domain("https://acme.com"), Err(DomainRejection::NotADomain));
        assert_eq!(normalize_domain("acme.com/path"), Err(DomainRejection::NotADomain));
        assert_eq!(normalize_domain("dana@acme.com"), Err(DomainRejection::NotADomain));
        assert_eq!(normalize_domain("acme .com"), Err(DomainRejection::NotADomain));
        assert_eq!(normalize_domain("localhost"), Err(DomainRejection::NoDot));
        assert_eq!(normalize_domain("acme..com"), Err(DomainRejection::BadLabel));
        assert_eq!(normalize_domain("-acme.com"), Err(DomainRejection::BadLabel));
        assert_eq!(normalize_domain("ac_me.com"), Err(DomainRejection::BadLabel));
    }

    /// The gap D11 does not close: the EMAIL proof would happily let anyone
    /// with a `@gmail.com` address "verify" `gmail.com`, after which every
    /// Gmail user on earth matches that org's domain rule. Refused at claim
    /// time, for both methods.
    #[test]
    fn refuses_public_email_providers_at_claim_time() {
        for d in ["gmail.com", "GMail.com", "outlook.com", "proton.me", "yahoo.co.uk"] {
            assert_eq!(
                normalize_domain(d),
                Err(DomainRejection::PublicEmailProvider),
                "{d} must never be claimable as an org domain"
            );
        }
        // A company that merely CONTAINS a provider name is unaffected.
        assert_eq!(normalize_domain("gmail.acme.com").unwrap(), "gmail.acme.com");
    }

    /// NEGATIVE TEST 4 (CS-443): a confirmation aimed at an address OUTSIDE
    /// the claimed domain is refused. Exact equality, never a suffix test —
    /// `ops@notacme.com` and `ops@acme.com.evil.test` both end with a string
    /// a naive `ends_with` would accept, and a subdomain address does not
    /// confirm the apex.
    #[test]
    fn confirmation_address_outside_the_claimed_domain_is_refused() {
        assert!(email_is_at_domain("ops@acme.com", "acme.com"));
        assert!(email_is_at_domain("  OPS@Acme.COM ", "acme.com"));

        assert!(!email_is_at_domain("ops@notacme.com", "acme.com"));
        assert!(!email_is_at_domain("ops@acme.com.evil.test", "acme.com"));
        assert!(!email_is_at_domain("ops@mail.acme.com", "acme.com"));
        assert!(!email_is_at_domain("person@example.com", "acme.com"));
        assert!(!email_is_at_domain("person@example.com", "acme.com"));
        assert!(!email_is_at_domain("not-an-address", "acme.com"));
        assert!(!email_is_at_domain("@acme.com", "acme.com"));
    }

    /// **CodeRabbit round 1, #1031.** A malformed local part is refused
    /// HERE, not by Resend's validator two calls later — which turns a
    /// confusing 502 ("could not send") into a 400 that names the problem,
    /// and keeps a CR/LF or a comma out of an outbound mail payload
    /// entirely (one recipient must not be able to become several).
    #[test]
    fn a_malformed_local_part_is_refused_before_any_send() {
        for bad in [
            "ops x@acme.com",
            "ops\t@acme.com",
            "ops\r\nbcc:someone@evil.test@acme.com",
            "ops\u{0}@acme.com",
            "ops,eve@acme.com",
            "ops;eve@acme.com",
            "<ops>@acme.com",
            "\"ops\"@acme.com",
            ".ops@acme.com",
            "ops.@acme.com",
            "o..ps@acme.com",
        ] {
            assert!(
                !email_is_at_domain(bad, "acme.com"),
                "{bad:?} must never reach an outbound mail payload"
            );
            assert_eq!(email_domain_part(bad), None, "{bad:?} must not even yield a domain");
        }

        // The ordinary shapes a real confirmation mailbox uses all survive.
        for good in ["ops@acme.com", "ops+domains@acme.com", "first.last@acme.com", "ops_team-1@acme.com"] {
            assert!(email_is_at_domain(good, "acme.com"), "{good:?} is a perfectly ordinary address");
        }
    }

    /// D2/D21 item 30. A guest address on any verified org domain is
    /// admitted, including a second domain verified later. The connecting
    /// member's domain is not an anchor once a verified domain exists.
    #[test]
    fn a_guest_whose_address_matches_a_verified_org_domain_is_admitted() {
        let anchors = guest_anchor_domains(
            Ok(vec!["acme.com".to_string(), "acquired.com".to_string()]),
            Some("installer.test".to_string()),
        );
        assert!(
            address_on_guest_anchor("pat@acme.com", &anchors),
            "the earliest verified domain admits"
        );
        assert!(
            address_on_guest_anchor("  Pat@Acquired.COM ", &anchors),
            "a later verified domain admits too, case-insensitively"
        );
        assert!(
            !address_on_guest_anchor("pat@installer.test", &anchors),
            "the connecting member's domain stops being an anchor once a verified domain exists"
        );
    }

    /// The other half of the same rule: an address on none of the verified
    /// domains is not admitted, including a suffix and a subdomain.
    #[test]
    fn a_guest_whose_address_does_not_match_a_verified_org_domain_is_not_admitted() {
        let anchors = guest_anchor_domains(Ok(vec!["acme.com".to_string()]), Some("installer.test".to_string()));
        assert!(!address_on_guest_anchor("pat@client.example", &anchors));
        assert!(!address_on_guest_anchor("pat@notacme.com", &anchors));
        assert!(!address_on_guest_anchor("pat@acme.com.evil.test", &anchors));
        assert!(!address_on_guest_anchor("pat@mail.acme.com", &anchors));
        assert!(!address_on_guest_anchor("pat@installer.test", &anchors));
    }

    /// D21: the connecting member's domain bootstraps the guest rule only
    /// until a verified org domain exists. A failed verified-domain lookup
    /// does not bootstrap either.
    #[test]
    fn the_connecting_members_domain_bootstraps_only_until_a_verified_domain_exists() {
        let bootstrap_only = guest_anchor_domains(Ok(vec![]), Some("  Installer.TEST ".to_string()));
        assert!(address_on_guest_anchor("pat@installer.test", &bootstrap_only));
        assert!(!address_on_guest_anchor("pat@client.example", &bootstrap_only));

        let verified = guest_anchor_domains(
            Ok(vec!["acme.com".to_string()]),
            Some("installer.test".to_string()),
        );
        assert!(address_on_guest_anchor("pat@acme.com", &verified));
        assert!(
            !address_on_guest_anchor("pat@installer.test", &verified),
            "bootstrap does not survive the first verified domain"
        );

        let failed = guest_anchor_domains(Err("connection reset".to_string()), Some("installer.test".to_string()));
        assert!(
            failed.is_empty(),
            "a failed lookup must not fall through to the connecting member's domain"
        );
        assert!(!address_on_guest_anchor("pat@installer.test", &failed));
    }

    #[test]
    fn parses_quoted_split_and_non_txt_doh_answers() {
        let body = json!({
            "Status": 0,
            "Answer": [
                {"name": "_coralswarm-verification.acme.com.", "type": 5, "data": "alias.example.net."},
                {"name": "_coralswarm-verification.acme.com.", "type": 16, "data": "\"v=spf1 -all\""},
                {"name": "_coralswarm-verification.acme.com.", "type": 16,
                 "data": "\"coralswarm-domain-verification=\" \"abc123\""}
            ]
        });
        let records = parse_txt_answers(&body);
        assert_eq!(records, vec!["v=spf1 -all", "coralswarm-domain-verification=abc123"]);
        assert!(txt_records_prove(&records, "abc123"));
        assert!(!txt_records_prove(&records, "someone-elses-token"));
    }

    #[test]
    fn a_record_that_merely_contains_the_token_does_not_prove_it() {
        let records = vec![format!("see also {}", dns_record_value("abc123"))];
        assert!(!txt_records_prove(&records, "abc123"));
    }

    #[test]
    fn no_answer_key_parses_as_no_records() {
        assert!(parse_txt_answers(&json!({"Status": 0})).is_empty());
    }

    #[test]
    fn confirmation_token_round_trips_and_rejects_tampering() {
        const SECRET: &str = "cs443-test-domain-secret-0123456789";
        let domain_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        let token = mint_confirmation_token(SECRET, domain_id, account_id, "acme.com", "ops@acme.com", "vtk-1")
            .expect("mint");
        let ctx = verify_confirmation_token(SECRET, &token).expect("verify");
        assert_eq!(ctx.domain_id, domain_id);
        assert_eq!(ctx.account_id, account_id);
        assert_eq!(ctx.domain, "acme.com");
        assert_eq!(ctx.email, "ops@acme.com");
        assert_eq!(ctx.verification_token, "vtk-1");

        assert!(verify_confirmation_token("a-different-secret", &token).is_none());
        assert!(verify_confirmation_token(SECRET, "not-a-jwt").is_none());
    }
}

// ── Live-Postgres suite ──────────────────────────────────────────────────
//
// `RELINFER_TEST_DATABASE_URL`-gated against a PERSISTENT, pre-migrated
// database — the same convention `routes::org_join_requests::pg_tests` and
// `clerk_provision::pg_tests` use (createdb-per-run buys nothing here: every
// seeded row carries a fresh `Uuid::new_v4()`, so this suite is safe
// alongside every other suite sharing that database).
//
// In-crate rather than a `backend/tests/*.rs` binary because every function
// under test is `pub(crate)`, and because the assertion ladder's own
// `domain_signal` — which these tests drive through the SAME rows — is
// `pub(crate)` too.
//
//   RELINFER_TEST_DATABASE_URL=postgres://alex@localhost:5432/cs443 \
//     cargo test -p coralswarm-backend --lib "services::org_domains::pg_tests::" -- --test-threads=1 --nocapture
#[cfg(test)]
mod pg_tests {
    use super::*;
    use crate::services::assertion_ladder::{self, DomainSignal, Verdict};

    /// "Did that call actually verify the row?" — CS-447 widened
    /// [`mark_verified`] from `bool` to [`VerifyOutcome`], and every
    /// assertion below predates the third answer.
    ///
    /// Written as an exhaustive `match` rather than
    /// `matches!(o, VerifyOutcome::Verified)` on purpose: a fourth outcome
    /// added later must be classified deliberately here, not absorbed into
    /// `false` by a wildcard. In particular
    /// [`VerifyOutcome::TakenByAnotherOrg`] answers `false` — a test that
    /// expected a verification and silently got "another org owns this
    /// domain" must FAIL, not pass with a shrug.
    fn verified(outcome: VerifyOutcome) -> bool {
        match outcome {
            VerifyOutcome::Verified => true,
            VerifyOutcome::NothingToVerify => false,
            VerifyOutcome::TakenByAnotherOrg => false,
        }
    }

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    async fn seed_org(pool: &PgPool) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users (id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
        )
        .bind(format!("cs443-dom-{}@example.test", Uuid::new_v4().simple()))
        .bind(format!("clerk_cs443_dom_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(format!("org_cs443_dom_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert account");
        (account_id, user_id)
    }

    fn fresh_domain() -> String {
        format!("cs443-{}.example", Uuid::new_v4().simple())
    }

    /// The central property of item 37, stated at the level every consumer
    /// reads it: a CLAIMED domain is invisible to every gate until it is
    /// VERIFIED. Proven across all three readers at once — the raw list, the
    /// per-address match, and the assertion ladder's own rung-2 domain
    /// signal — because a filter that is right in one of them and wrong in
    /// another is exactly how this leaks.
    #[tokio::test]
    async fn an_unverified_claim_is_invisible_to_every_gate() {
        let Some(pool) = pg_test_pool("an_unverified_claim_is_invisible_to_every_gate").await else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = fresh_domain();
        let address = format!("dana@{domain}");

        let (row, outcome) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
        let row = row.expect("a fresh claim always returns its row");
        assert_eq!(outcome, ClaimOutcome::Claimed);
        assert!(row.verified_at.is_none() && row.method.is_none());

        assert!(
            verified_domains(&pool, account_id).await.expect("read").is_empty(),
            "a claimed-but-unverified domain must not appear in the verified list"
        );
        assert_eq!(
            matching_verified_domain(&pool, account_id, &address).await.expect("match"),
            None,
            "an address on a claimed-but-unverified domain must not match"
        );
        assert!(
            verified_domains_with_method(&pool, account_id).await.expect("read").is_empty(),
            "nor may it reach the method-carrying read the ladder uses"
        );
        assert_eq!(
            assertion_ladder::domain_signal(&pool, account_id, &address).await,
            DomainSignal::NoVerifiedDomain,
            "the ladder must not see a rung-2 domain signal from an unverified claim"
        );

        // Verify it, and every one of the three flips — which is what proves
        // the assertions above came from the verified filter rather than
        // from three lookups that never worked.
        assert!(
            verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::DnsTxt).await.expect("verify"))
        );
        assert_eq!(verified_domains(&pool, account_id).await.expect("read"), vec![domain.clone()]);
        assert_eq!(
            matching_verified_domain(&pool, account_id, &address).await.expect("match"),
            Some((domain.clone(), VerificationMethod::DnsTxt)),
            "the method rides along with the match — no caller may strip it (founder ruling 2026-09-22)"
        );
        assert_eq!(
            assertion_ladder::domain_signal(&pool, account_id, &address).await,
            DomainSignal::Match(domain.clone()),
            "DNS-verified, so it is a rung-2-eligible match"
        );

        // D11's "which one proved it" survives the round trip.
        let fresh = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");
        assert_eq!(fresh.method.as_deref(), Some("dns_txt"));
        assert!(fresh.verified_at.is_some());

        // An address on a DIFFERENT domain still does not match, now that a
        // verified domain exists — `NoMatch`, which is a different fact from
        // `NoVerifiedDomain` and is recorded as one.
        assert_eq!(
            assertion_ladder::domain_signal(&pool, account_id, "dana@somewhere-else.example").await,
            DomainSignal::NoMatch
        );
    }

    /// Rung 2 needs BOTH halves. A verified domain match with no workspace
    /// signal is rung 3 — approval only — driven here through the real
    /// database rather than the pure unit test, so the wiring from
    /// `org_domains` into `evaluate_for_email` is exercised end to end.
    #[tokio::test]
    async fn the_ladder_reads_a_verified_domain_but_still_refuses_to_admit_on_it_alone() {
        let Some(pool) =
            pg_test_pool("the_ladder_reads_a_verified_domain_but_still_refuses_to_admit_on_it_alone").await
        else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = fresh_domain();
        let address = format!("dana@{domain}");
        let (row, _) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
        let row = row.expect("a fresh claim always returns its row");
        // DNS-verified deliberately: this test is about "one signal alone is
        // rung 3", so the domain half must be the STRONGEST kind of match
        // there is. An email-verified domain would also fail to admit, but
        // for a second, different reason (the founder's 2026-09-22 method
        // ruling), and a test that could pass for either reason proves
        // neither — that case has its own test, `connectors::slack::
        // domain_seam_pg_tests::an_email_verified_domain_anchors_the_guest_
        // rule_but_not_rung_two`.
        assert!(verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::DnsTxt).await.expect("verify")));

        let outcome = assertion_ladder::evaluate_for_email(
            &pool,
            account_id,
            &address,
            Err(assertion_ladder::NO_DIRECTORY_AVAILABLE),
            None,
        )
        .await;
        assert_eq!(outcome.verdict, Verdict::ApprovalOnly);
        assert!(!outcome.verdict.may_auto_add(), "a verified domain ALONE must never auto-add");
        assert_eq!(outcome.evidence()["domain"], serde_json::json!("match"));
        assert_eq!(outcome.evidence()["workspace_membership"], serde_json::json!("unknown"));
        assert_eq!(outcome.evidence()["directory"], serde_json::json!("unavailable"));
    }

    /// **CodeRabbit round 1, #1031.** The per-account domain cap. Without
    /// it one authenticated admin can create unbounded rows and — the
    /// confirmation cooldown being per-ROW — send one branded email per row.
    ///
    /// Asserts the cap refuses a NEW domain at the ceiling while still
    /// letting the admin re-claim one they ALREADY hold (they may be
    /// mid-verification and need the TXT record read back), and that
    /// removing one frees a slot.
    #[tokio::test]
    async fn the_per_account_domain_cap_refuses_new_claims_but_not_re_claims() {
        let Some(pool) = pg_test_pool("the_per_account_domain_cap_refuses_new_claims_but_not_re_claims").await
        else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;

        let mut first_domain = None;
        for _ in 0..MAX_DOMAINS_PER_ACCOUNT {
            let domain = fresh_domain();
            let (row, outcome) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
            assert_eq!(outcome, ClaimOutcome::Claimed);
            assert!(row.is_some());
            first_domain.get_or_insert(domain);
        }
        let first_domain = first_domain.expect("the cap is at least 1");

        // One more NEW domain: refused, and nothing written.
        let over = fresh_domain();
        let (row, outcome) = claim_domain(&pool, account_id, user_id, &over).await.expect("claim");
        assert_eq!(outcome, ClaimOutcome::LimitReached);
        assert!(row.is_none());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM org_domains WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, MAX_DOMAINS_PER_ACCOUNT, "the refused claim wrote no row");

        // Re-claiming one they already hold still works at the ceiling — it
        // adds no row, and an admin mid-verification needs its TXT record.
        let (row, outcome) = claim_domain(&pool, account_id, user_id, &first_domain).await.expect("re-claim");
        assert_eq!(outcome, ClaimOutcome::AlreadyClaimed);
        assert_eq!(row.expect("the existing row").domain, first_domain);

        // Removing one frees a slot.
        let removed_id: Uuid = sqlx::query_scalar("SELECT id FROM org_domains WHERE account_id = $1 LIMIT 1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(remove_domain(&pool, account_id, removed_id).await.expect("remove"));
        let (row, outcome) = claim_domain(&pool, account_id, user_id, &over).await.expect("claim");
        assert_eq!(outcome, ClaimOutcome::Claimed);
        assert!(row.is_some());
    }

    /// Re-claiming a still-unverified domain returns the SAME token — an
    /// admin who has already published the TXT record must not have it
    /// invalidated by a second click on "add domain".
    #[tokio::test]
    async fn re_claiming_keeps_the_published_token() {
        let Some(pool) = pg_test_pool("re_claiming_keeps_the_published_token").await else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = fresh_domain();

        let (first, outcome) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
        let first = first.expect("a fresh claim always returns its row");
        assert_eq!(outcome, ClaimOutcome::Claimed);
        let (second, outcome) = claim_domain(&pool, account_id, user_id, &domain).await.expect("re-claim");
        let second = second.expect("a re-claim returns the existing row");
        assert_eq!(outcome, ClaimOutcome::AlreadyClaimed);
        assert_eq!(second.id, first.id);
        assert_eq!(second.verification_token, first.verification_token);

        assert!(verified(mark_verified(&pool, account_id, first.id, &first.verification_token, VerificationMethod::DnsTxt).await.expect("verify")));
        let (third, outcome) = claim_domain(&pool, account_id, user_id, &domain).await.expect("re-claim");
        let third = third.expect("a re-claim returns the existing row");
        assert_eq!(outcome, ClaimOutcome::AlreadyVerified);
        assert!(third.verified_at.is_some());
    }

    /// `mark_verified` is pinned to the token the claim was started with,
    /// and re-verifying by the SAME method a row is already verified by is
    /// idempotent — a double click must not re-date the row. A stale
    /// confirmation link (minted before a remove-and-re-add) verifies
    /// nothing.
    ///
    /// **Deliberately does NOT test the cross-method case** (email → DNS)
    /// here — that used to be folded into this test as a second "must not
    /// re-date" assertion, but it named a DIFFERENT defect: CS-450 found
    /// that a `dns_txt` attempt on an email-verified row was being refused
    /// the SAME way a genuine double-click is, which left an org that had
    /// only ever proved control by email with no path to auto-add short of
    /// deleting and re-claiming the domain. See
    /// [`an_email_verified_row_upgrades_to_dns_but_never_downgrades`] for
    /// that behaviour now.
    #[tokio::test]
    async fn mark_verified_is_pinned_to_the_claim_token_and_is_idempotent() {
        let Some(pool) = pg_test_pool("mark_verified_is_pinned_to_the_claim_token_and_is_idempotent").await
        else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = fresh_domain();
        let (row, _) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
        let row = row.expect("a fresh claim always returns its row");

        assert!(
            !verified(mark_verified(&pool, account_id, row.id, "a-token-from-some-older-claim", VerificationMethod::Email).await.expect("verify")),
            "a token that is not this row's own must verify nothing"
        );
        assert!(verified_domains(&pool, account_id).await.expect("read").is_empty());

        assert!(verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::Email).await.expect("verify")));
        let first = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");

        assert!(
            !verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::Email).await.expect("verify again")),
            "a second verification by the SAME method must not re-date the row"
        );
        let second = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");
        assert_eq!(second.method.as_deref(), Some("email"));
        assert_eq!(second.verified_at, first.verified_at, "a same-method double click must not re-date the row");
    }

    /// **CS-450, defect 1.** An email-verified domain is not a dead end: a
    /// SUBSEQUENT `dns_txt` proof UPGRADES it (arming D16 rung 2 / auto-add,
    /// which an `email`-verified row never does — see
    /// [`VerificationMethod::may_arm_auto_add`]), and `verified_at` moves to
    /// the DNS proof's own time — the row stores exactly one
    /// `(verified_at, method)` pair, and leaving the OLD email-proof
    /// timestamp under the NEW `dns_txt` method would claim DNS had proven
    /// the domain since before any TXT record existed.
    ///
    /// The reverse must never happen: once `dns_txt`, a `email` call is
    /// refused exactly like a stale/foreign token would be — proven here by
    /// asserting the row's `method` survives a same-domain `email` attempt
    /// unchanged, the identical shape
    /// `mark_verified_is_pinned_to_the_claim_token_and_is_idempotent` uses
    /// for "this write did not happen".
    #[tokio::test]
    async fn an_email_verified_row_upgrades_to_dns_but_never_downgrades() {
        let Some(pool) =
            pg_test_pool("an_email_verified_row_upgrades_to_dns_but_never_downgrades").await
        else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = fresh_domain();
        let (row, _) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
        let row = row.expect("a fresh claim always returns its row");

        assert!(verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::Email).await.expect("verify by email")));
        let email_verified = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");
        assert_eq!(email_verified.method.as_deref(), Some("email"));
        let email_verified_at = email_verified.verified_at.expect("verified by email");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::DnsTxt).await.expect("upgrade to dns_txt")),
            "a dns_txt proof on an email-verified row must succeed — this is the upgrade path"
        );
        let dns_verified = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");
        assert_eq!(dns_verified.method.as_deref(), Some("dns_txt"), "the method flips to dns_txt");
        assert!(
            dns_verified.verified_at.expect("still verified") > email_verified_at,
            "verified_at is bumped to the DNS proof's own time, not left at the original email-proof time"
        );
        assert_eq!(
            assertion_ladder::domain_signal(&pool, account_id, &format!("dana@{domain}")).await,
            DomainSignal::Match(domain.clone()),
            "the upgrade is what arms rung 2 — an email-only row never reaches DomainSignal::Match"
        );

        // A `dns_txt` row must never be downgraded back to `email`.
        assert!(
            !verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::Email).await.expect("refused downgrade")),
            "a dns_txt row must refuse an email re-verification, exactly like a stale token would"
        );
        let after_downgrade_attempt = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");
        assert_eq!(after_downgrade_attempt.method.as_deref(), Some("dns_txt"), "dns_txt must survive an email attempt");
        assert_eq!(after_downgrade_attempt.verified_at, dns_verified.verified_at, "and its verified_at must not move either");

        // A repeat dns_txt call is ALSO idempotent — the row is already at
        // the strongest method, so this is the same "double click" shape as
        // the email/email case above, not a second upgrade.
        assert!(
            !verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::DnsTxt).await.expect("repeat dns_txt")),
            "a repeat dns_txt call on an already dns_txt-verified row must be a no-op"
        );
        let unchanged = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");
        assert_eq!(unchanged.verified_at, dns_verified.verified_at);
    }

    /// **CS-447 negative test 5 — founder decision, 2026-09-22 (migration
    /// 0247, amending D11): a domain may be verified by ONE org, globally.**
    ///
    /// Two orgs, one domain. BOTH may claim it — each gets its own row and
    /// its own `verification_token`, because a claim is a statement of
    /// intent and proves nothing. Only the FIRST to verify takes it; the
    /// second's verification is refused with
    /// [`VerifyOutcome::TakenByAnotherOrg`], and its claim survives,
    /// untouched and still unverified.
    ///
    /// Both halves are asserted, not just the refusal: a global
    /// `UNIQUE (domain)` (rather than the partial index this ships) would
    /// also produce the refusal below while breaking the claim above, and
    /// that would turn domain-claiming into a land grab any signed-up
    /// account could run against every domain it can spell.
    ///
    /// The last assertion is the one that would catch a "fix" that simply
    /// swallowed the error: the loser's gates must stay DISARMED. A refusal
    /// that left the row verified anyway would be worse than no index at
    /// all.
    #[tokio::test]
    async fn only_one_org_can_verify_a_domain_while_both_can_claim_it() {
        let Some(pool) = pg_test_pool("only_one_org_can_verify_a_domain_while_both_can_claim_it").await else {
            return;
        };
        let (first_account, first_user) = seed_org(&pool).await;
        let (second_account, second_user) = seed_org(&pool).await;
        let domain = fresh_domain();

        // Both orgs CLAIM it — per-account, unaffected by 0247.
        let (first_row, first_outcome) =
            claim_domain(&pool, first_account, first_user, &domain).await.expect("first claim");
        let first_row = first_row.expect("a fresh claim always returns its row");
        assert_eq!(first_outcome, ClaimOutcome::Claimed);
        let (second_row, second_outcome) =
            claim_domain(&pool, second_account, second_user, &domain).await.expect("second claim");
        let second_row = second_row.expect("a second org may claim the same domain");
        assert_eq!(
            second_outcome,
            ClaimOutcome::Claimed,
            "claiming stays PER-ACCOUNT — only verification is exclusive"
        );
        assert_ne!(
            first_row.verification_token, second_row.verification_token,
            "each claim proves control with its OWN token, or the second org could verify with \
             a record the first org published"
        );

        // First to verify wins.
        assert!(verified(
            mark_verified(&pool, first_account, first_row.id, &first_row.verification_token, VerificationMethod::DnsTxt)
                .await
                .expect("first verify")
        ));

        // The second is refused — by the index, on the write, not by a
        // pre-read it could race.
        let refused = mark_verified(
            &pool,
            second_account,
            second_row.id,
            &second_row.verification_token,
            VerificationMethod::DnsTxt,
        )
        .await
        .expect("a refused verification is an OUTCOME, never an Err");
        assert_eq!(
            refused,
            VerifyOutcome::TakenByAnotherOrg,
            "first org to verify wins, globally (founder, 2026-09-22)"
        );

        // …and the loser is left genuinely unverified: its claim row
        // survives, and every gate that reads this module stays disarmed
        // for it.
        let loser = get_domain(&pool, second_account, second_row.id)
            .await
            .expect("reload the loser's row")
            .expect("the losing CLAIM must survive the refused verification");
        assert!(loser.verified_at.is_none(), "a refused verification must not leave the row verified");
        assert!(loser.method.is_none());
        assert!(
            verified_domains(&pool, second_account).await.expect("read").is_empty(),
            "the losing org must have NO verified domain — D2's guest anchor stays disarmed"
        );
        assert_eq!(
            verified_domains(&pool, first_account).await.expect("read"),
            vec![domain.clone()],
            "the winner keeps it"
        );

        // The email method is refused for the same reason — the index is on
        // the row, not on how it was proved.
        let refused_by_email =
            mark_verified(&pool, second_account, second_row.id, &second_row.verification_token, VerificationMethod::Email)
                .await
                .expect("a refused verification is an OUTCOME, never an Err");
        assert_eq!(refused_by_email, VerifyOutcome::TakenByAnotherOrg);
    }

    /// Removing a domain disarms every gate that read it, and a later
    /// re-claim starts over unverified with a fresh token.
    #[tokio::test]
    async fn removing_a_domain_disarms_the_gate() {
        let Some(pool) = pg_test_pool("removing_a_domain_disarms_the_gate").await else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = fresh_domain();
        let (row, _) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
        let row = row.expect("a fresh claim always returns its row");
        assert!(verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::DnsTxt).await.expect("verify")));
        assert_eq!(verified_domains(&pool, account_id).await.expect("read"), vec![domain.clone()]);

        assert!(remove_domain(&pool, account_id, row.id).await.expect("remove"));
        assert!(verified_domains(&pool, account_id).await.expect("read").is_empty());
        assert!(!remove_domain(&pool, account_id, row.id).await.expect("remove again"));

        let (re_claimed, outcome) = claim_domain(&pool, account_id, user_id, &domain).await.expect("re-claim");
        let re_claimed = re_claimed.expect("a fresh claim always returns its row");
        assert_eq!(outcome, ClaimOutcome::Claimed);
        assert!(re_claimed.verified_at.is_none(), "a re-claim starts over UNVERIFIED");
        assert_ne!(
            re_claimed.verification_token, row.verification_token,
            "a re-claim mints a fresh token, which invalidates every outstanding link for the old one"
        );
    }

    /// The claim-then-send-then-commit order: a rolled-back transaction
    /// leaves the cooldown unconsumed, so a failed send costs an immediate
    /// retry rather than a ten-minute wait.
    #[tokio::test]
    async fn a_rolled_back_confirmation_claim_consumes_no_cooldown() {
        let Some(pool) = pg_test_pool("a_rolled_back_confirmation_claim_consumes_no_cooldown").await else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = fresh_domain();
        let address = format!("ops@{domain}");
        let (row, _) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
        let row = row.expect("a fresh claim always returns its row");

        // The "send failed" path: claim inside a transaction, roll back.
        let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
        assert!(claim_confirmation_send(&mut tx, account_id, row.id, &address).await.expect("claim send"));
        tx.rollback().await.expect("rollback");
        let after_failure = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");
        assert!(
            after_failure.confirmation_sent_at.is_none(),
            "a failed send must consume no cooldown state at all"
        );

        // The "send succeeded" path: claim, commit — and the cooldown now
        // refuses an immediate second send.
        let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
        assert!(claim_confirmation_send(&mut tx, account_id, row.id, &address).await.expect("claim send"));
        tx.commit().await.expect("commit");
        let after_success = get_domain(&pool, account_id, row.id).await.expect("reload").expect("row");
        assert_eq!(after_success.confirmation_email.as_deref(), Some(address.as_str()));
        assert!(after_success.confirmation_sent_at.is_some());

        let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
        assert!(
            !claim_confirmation_send(&mut tx, account_id, row.id, &address).await.expect("claim send"),
            "a second send inside the cooldown must be refused"
        );
        tx.rollback().await.expect("rollback");

        // And a VERIFIED row refuses a confirmation send outright — which is
        // what keeps `confirmation_email` meaningful as the address that
        // proved an `method = 'email'` row.
        assert!(verified(mark_verified(&pool, account_id, row.id, &row.verification_token, VerificationMethod::Email).await.expect("verify")));
        let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
        assert!(
            !claim_confirmation_send(&mut tx, account_id, row.id, &address).await.expect("claim send"),
            "an already-verified domain must refuse further confirmation sends"
        );
        tx.rollback().await.expect("rollback");
    }

    /// 0242's paired-nullability invariant, at the DB layer: "verified but by
    /// no method" and "proved by DNS but not verified" are states this table
    /// refuses to represent, rather than states every writer has to remember
    /// to avoid.
    #[tokio::test]
    async fn the_database_refuses_a_half_verified_row() {
        let Some(pool) = pg_test_pool("the_database_refuses_a_half_verified_row").await else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;
        let domain = fresh_domain();
        let (row, _) = claim_domain(&pool, account_id, user_id, &domain).await.expect("claim");
        let row = row.expect("a fresh claim always returns its row");

        let verified_with_no_method = sqlx::query("UPDATE org_domains SET verified_at = now() WHERE id = $1")
            .bind(row.id)
            .execute(&pool)
            .await;
        assert!(verified_with_no_method.is_err(), "verified_at without a method must be refused");

        let method_with_no_timestamp = sqlx::query("UPDATE org_domains SET method = 'dns_txt' WHERE id = $1")
            .bind(row.id)
            .execute(&pool)
            .await;
        assert!(method_with_no_timestamp.is_err(), "a method without verified_at must be refused");

        let bogus_method =
            sqlx::query("UPDATE org_domains SET verified_at = now(), method = 'vibes' WHERE id = $1")
                .bind(row.id)
                .execute(&pool)
                .await;
        assert!(bogus_method.is_err(), "method is a two-value domain");
    }
}

/// Test-only harness for the process-global `DOH_RESOLVER_URL` override.
///
/// The lock lives HERE, not inside `doh_stub_tests`, because TWO in-crate
/// test modules point that one var at a stub — this module's own resolver
/// tests and `routes::org_domains::pg_tests`' 409-vs-502 route tests — and
/// they are compiled into the SAME `--lib` test binary, which the Rust
/// harness runs concurrently by default. A module-local mutex would
/// serialize each module against itself and nothing against the other,
/// which is exactly the shape `connectors::slack::SLACK_API_BASE_ENV_LOCK`
/// was created to fix after two modules shared `SLACK_API_BASE` behind
/// nothing but a doc comment asking for `--test-threads=1`.
#[cfg(test)]
pub(crate) mod test_doh {
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// A stub resolver that answers every query with one canned body +
    /// status. Returns its `/resolve` URL.
    pub(crate) async fn spawn_resolver(
        status: axum::http::StatusCode,
        body: serde_json::Value,
    ) -> String {
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let body = body.clone();
            async move { (status, [("content-type", "application/json")], body.to_string()) }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind resolver stub");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}/resolve")
    }

    /// Points `DOH_RESOLVER_URL` at `url` for this guard's lifetime, holding
    /// [`ENV_LOCK`] throughout (including across a panic/unwind, via `Drop`).
    pub(crate) struct DohEnvGuard(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

    impl DohEnvGuard {
        pub(crate) fn set(url: &str) -> Self {
            let guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // SAFETY: serialized by `guard`, held for this struct's whole
            // lifetime.
            unsafe {
                std::env::set_var("DOH_RESOLVER_URL", url);
            }
            Self(guard)
        }
    }

    impl Drop for DohEnvGuard {
        fn drop(&mut self) {
            // SAFETY: see `set`.
            unsafe {
                std::env::remove_var("DOH_RESOLVER_URL");
            }
        }
    }
}

#[cfg(test)]
mod doh_stub_tests {
    //! [`check_dns_txt`] against a LOCAL stub resolver, reached through
    //! `DOH_RESOLVER_URL` — the whole reason that override exists (this
    //! workspace has no DNS resolver crate, by design).
    //!
    //! `DOH_RESOLVER_URL` is process-global, so these tests go through
    //! [`super::test_doh`]'s shared guard — see that module's own doc
    //! comment for why the lock does NOT live here.

    use super::test_doh::{spawn_resolver, DohEnvGuard};
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn a_published_record_verifies() {
        let url = spawn_resolver(
            axum::http::StatusCode::OK,
            json!({
                "Status": 0,
                "Answer": [
                    {"name": "_coralswarm-verification.acme.test.", "type": 16,
                     "data": "\"coralswarm-domain-verification=tok-abc\""}
                ]
            }),
        )
        .await;
        let _env = DohEnvGuard::set(&url);
        let out = check_dns_txt(&reqwest::Client::new(), "acme.test", "tok-abc").await;
        assert_eq!(out, Ok(DnsProof::Found));
    }

    #[tokio::test]
    async fn another_orgs_record_at_the_same_name_does_not_verify_this_one() {
        let url = spawn_resolver(
            axum::http::StatusCode::OK,
            json!({
                "Status": 0,
                "Answer": [
                    {"name": "_coralswarm-verification.acme.test.", "type": 16,
                     "data": "\"coralswarm-domain-verification=somebody-elses-token\""}
                ]
            }),
        )
        .await;
        let _env = DohEnvGuard::set(&url);
        let out = check_dns_txt(&reqwest::Client::new(), "acme.test", "tok-abc").await;
        assert_eq!(out, Ok(DnsProof::Missing { records_seen: 1 }));
    }

    #[tokio::test]
    async fn nxdomain_is_an_answer_not_a_failure() {
        let url = spawn_resolver(axum::http::StatusCode::OK, json!({"Status": 3})).await;
        let _env = DohEnvGuard::set(&url);
        let out = check_dns_txt(&reqwest::Client::new(), "acme.test", "tok-abc").await;
        assert_eq!(
            out,
            Ok(DnsProof::Missing { records_seen: 0 }),
            "NXDOMAIN IS an answer — 'there is no such record' — not 'we could not ask'"
        );
    }

    /// "Could not ask" must never be reported as "not published". The route
    /// maps this arm to 502 and the `Missing` arms to 409, so an admin is
    /// never told to go and fix a DNS record that was fine.
    #[tokio::test]
    async fn a_resolver_failure_is_an_error_never_a_missing_record() {
        let url = spawn_resolver(axum::http::StatusCode::INTERNAL_SERVER_ERROR, json!({"Status": 2})).await;
        let _env = DohEnvGuard::set(&url);
        let out = check_dns_txt(&reqwest::Client::new(), "acme.test", "tok-abc").await;
        assert!(out.is_err(), "a non-2xx resolver response is 'could not ask', got {out:?}");

        drop(_env);
        let url = spawn_resolver(axum::http::StatusCode::OK, json!({"Status": 2})).await;
        let _env = DohEnvGuard::set(&url);
        let out = check_dns_txt(&reqwest::Client::new(), "acme.test", "tok-abc").await;
        assert!(out.is_err(), "SERVFAIL is 'could not ask', got {out:?}");

        drop(_env);
        let url = spawn_resolver(axum::http::StatusCode::OK, json!({"Answer": []})).await;
        let _env = DohEnvGuard::set(&url);
        let out = check_dns_txt(&reqwest::Client::new(), "acme.test", "tok-abc").await;
        assert!(out.is_err(), "a body with no Status at all is 'could not ask', got {out:?}");
    }
}

pub(crate) async fn list_domains_tx(conn: &mut sqlx::PgConnection, account_id: Uuid) -> Result<Vec<OrgDomain>, OrgDomainError> {
    let rows = sqlx::query_as::<_, DomainRow>(&format!(
        "{DOMAIN_SELECT} WHERE account_id = $1 ORDER BY claimed_at ASC, domain ASC"
    ))
    .bind(account_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(domain_from_row).collect())
}

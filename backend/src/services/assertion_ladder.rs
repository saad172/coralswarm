//! D16's assertion ladder (CS-443, org-onboarding plan §3 item 6,
//! `docs/plans/org-onboarding-from-the-invite.md`, D11/D16/D21) — the
//! ordered evidence that answers *"does this person work here"*, and the
//! only thing allowed to say "yes" strongly enough to arm auto-add.
//!
//! ```text
//! | Rung | Assertion                                              | Verdict       |
//! |------|--------------------------------------------------------|---------------|
//! | 1    | In the org's own directory                             | auto-add      |
//! | 2    | Full workspace member AND DNS-verified domain match     | auto-add      |
//! | 3    | Either one alone                                       | approval only |
//! | —    | Neither available                                      | not armed     |
//! ```
//!
//! ("DNS-verified" in rung 2 is the founder's 2026-09-22 amendment to D11 —
//! see "Only a DNS-verified domain arms rung 2" below.)
//!
//! # What this module is NOT
//!
//! It does not add anyone, and it does not decide anything about seats.
//! Item 9 (auto-add) and item 38 (the toggle) are the next wave and must not
//! exist before this merges — D16's ladder is only the *evidence*. The one
//! live caller today is `routes::org_join_requests::create`, which records
//! [`LadderOutcome::evidence`] into `org_join_requests.evidence` (migration
//! 0238) so the admin deciding a request can see what the ladder made of the
//! person in front of them. A ladder with no caller is code that is "built
//! but never called"; evidence an admin can see is the honest minimum.
//!
//! # Rung 2 is the rule, rung 1 is an upgrade (D16, as amended by D21)
//!
//! Rung 1 needs four things at once — the org has Google connected and not
//! only Slack, a member consented after the `directory.readonly` widening,
//! that grant is still live, and the domain admin enabled profile sharing —
//! so at the moment we most want a directory it is usually absent, and for a
//! Slack-only org it is absent permanently. Rung 2's credential, per D21's
//! amendment at the top of D16, is a **connected member's own token**, not a
//! workspace bot token; it is available from the first connection.
//!
//! # The trap in the strongest signal — the single most important property here
//!
//! `connectors::google_chat::fetch_domain_directory` returns HTTP 200 with
//! zero people when the domain admin has not enabled profile sharing, and it
//! **already folds that into `Err`** (read its doc comment — it is the whole
//! argument). This module **inherits** that discipline rather than
//! re-deriving it: [`directory_signal`] takes that function's own
//! `Result` — it never re-reads the wire, never re-checks emptiness, and
//! never invents its own notion of "empty directory".
//!
//! `Err` means **could not ask**, which is *not* **does not work here**.
//! [`DirectorySignal`] is therefore three-way —
//! `Listed` / `NotListed` / `Unavailable` — mirroring
//! `connectors::google_chat::ChatVerifiedIdentity` and
//! `connectors::slack::SlackIdentity`, for the reason all three share: a
//! caller that flattens `Unavailable` into "not in the directory" turns the
//! strongest rung into the most dangerous one, and does it through the
//! signal we trust most.
//!
//! `Unavailable` never admits and never excludes. It falls through to rung
//! 2 exactly as an absent Google connection would, and it is recorded
//! verbatim in the evidence so an admin reading a queued request can tell
//! "Acme's directory says she is not staff" apart from "we could not reach
//! Acme's directory."
//!
//! # The rungs are OR, not veto — DECIDED, do not re-litigate
//!
//! D16's table is an ordered ladder that "fires on the top two rungs" and
//! does not itself say whether a definitive rung-1 `NotListed` should VETO a
//! rung-2 admission. CS-443 raised that as an open question; the **founder
//! ruled OR on 2026-09-22**, and this module implements it: each rung is
//! evaluated independently and the highest match wins.
//!
//! > **The directory can only ever ADD people, never subtract them.**
//!
//! So a person the org's directory does not list, who is nonetheless a full
//! workspace member on a DNS-verified domain, is admitted at rung 2 — the
//! `NotListed` is recorded in the evidence and changes nothing. This is a
//! settled decision, not an assumption the next reader should revisit: the
//! more conservative veto reading was considered and rejected.
//!
//! # Only a DNS-verified domain arms rung 2 (founder ruling, 2026-09-22)
//!
//! Amending D11. An emailed confirmation proves somebody holds *one mailbox*
//! at a domain; a TXT record proves control of the *domain itself*. Only the
//! second is a safe basis for adding strangers without an admin looking.
//!
//! ```text
//! verified by DNS   -> D2 guest rule + rung 2 / auto-add
//! verified by email -> D2 guest rule only
//! unverified        -> neither
//! ```
//!
//! [`DomainSignal`] is four-way for exactly this reason:
//! [`DomainSignal::MatchEmailOnly`] is a real match that must not admit, and
//! folding it into `NoMatch` would both lie to the admin reading the
//! evidence and make the two methods indistinguishable in the code. D2's
//! anchor (`connectors::slack::verified_org_domain`) reads a DIFFERENT,
//! deliberately method-blind function and is NOT narrowed. See
//! `services::org_domains`' own "belt and braces" doc section.

use serde_json::{json, Value as Json};
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google_chat::DirectoryEntry;
use crate::connectors::slack::MembershipClass;
use crate::services::org_domains;

/// Rung 1 — what the org's own identity system says about this address.
///
/// Three-way on purpose; see this module's own doc comment. Derived ONLY
/// from [`directory_signal`], which takes
/// `fetch_domain_directory`'s own `Result` rather than re-reading it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DirectorySignal {
    /// The directory was successfully established AND lists this address.
    Listed,
    /// The directory was successfully established and does NOT list this
    /// address — a real answer about this one person (an external guest, a
    /// consumer address in an otherwise-Workspace org, anyone the domain
    /// genuinely does not list).
    NotListed,
    /// We could not ask: no Google connection, no live `directory.readonly`
    /// grant, profile sharing disabled, or the call failed. **Never** read
    /// as `NotListed`.
    Unavailable(String),
}

impl DirectorySignal {
    fn label(&self) -> &'static str {
        match self {
            Self::Listed => "listed",
            Self::NotListed => "not_listed",
            Self::Unavailable(_) => "unavailable",
        }
    }
}

/// Rung 2's first half — is this person a FULL member of the workspace?
///
/// `Unknown` is the fail-closed reading of
/// [`MembershipClass::Unavailable`] (a narrower `users:read` grant, a Slack
/// response-shape change) and of "we have no connector signal at this
/// moment at all", which is the live caller's ordinary case: a person
/// clicking "ask to join" from the web has no Slack `users.info` response
/// attached to them. Same rule either way — unknown never admits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceSignal {
    FullMember,
    /// A guest, a single-channel guest, or a Slack-Connect participant from
    /// another workspace — D2 already treats these as a separate class, and
    /// D16 is explicit that "full member of Acme's Slack" is the assertion
    /// worth something.
    NotFullMember,
    Unknown,
}

impl WorkspaceSignal {
    /// The fail-closed mapping from Slack's own classification (CS-426).
    /// [`MembershipClass::Unavailable`] becomes [`WorkspaceSignal::Unknown`],
    /// never `NotFullMember` and certainly never `FullMember` — see
    /// `MembershipClass`'s own doc comment for why missing data must not
    /// read as a verdict.
    pub(crate) fn from_membership_class(class: MembershipClass) -> Self {
        match class {
            MembershipClass::FullMember => Self::FullMember,
            MembershipClass::Guest | MembershipClass::SlackConnect => Self::NotFullMember,
            MembershipClass::Unavailable => Self::Unknown,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::FullMember => "full_member",
            Self::NotFullMember => "not_full_member",
            Self::Unknown => "unknown",
        }
    }
}

/// Rung 2's second half — does this address sit on a DNS-VERIFIED org domain
/// (D11 as amended by the founder's 2026-09-22 ruling, item 37)?
///
/// Four-way, not three, because "matches a domain this org proved by
/// emailed confirmation" is a real and different answer from both "matches"
/// and "does not match" — see [`DomainSignal::MatchEmailOnly`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DomainSignal {
    /// The address's domain equals one this org verified **by DNS TXT** —
    /// the only shape that counts toward rung 2. Carries WHICH one, for the
    /// evidence an admin reads.
    Match(String),
    /// The address's domain equals one this org verified **by emailed
    /// confirmation only**.
    ///
    /// **Founder ruling, 2026-09-22 (amending D11): this never admits at
    /// rung 2.** The same domain still anchors D2's guest rule — control was
    /// genuinely proved — but an individual mailbox cannot prove *org*
    /// control the way a TXT record can, and auto-add is the gate where that
    /// difference matters. Kept as its own variant rather than folded into
    /// [`DomainSignal::NoMatch`] so the evidence an admin reads says
    /// "matches acme.com, but that domain was confirmed by email" instead of
    /// the false "does not match acme.com" — and so a later refactor cannot
    /// quietly collapse the two. See
    /// `services::org_domains::VerificationMethod::may_arm_auto_add`.
    MatchEmailOnly(String),
    /// The org has at least one verified domain and this address is on none
    /// of them.
    NoMatch,
    /// The org has verified no domain at all — a different fact from "this
    /// address does not match", and the one that means D11's gate simply
    /// is not configured yet.
    NoVerifiedDomain,
}

impl DomainSignal {
    fn label(&self) -> &'static str {
        match self {
            Self::Match(_) => "match",
            Self::MatchEmailOnly(_) => "match_email_verified_only",
            Self::NoMatch => "no_match",
            Self::NoVerifiedDomain => "no_verified_domain",
        }
    }
    /// Rung 2's domain half. DNS only — see
    /// [`DomainSignal::MatchEmailOnly`].
    fn is_match(&self) -> bool {
        matches!(self, Self::Match(_))
    }
    /// The domain this address matched, by EITHER method — evidence only,
    /// never a gate. `is_match` is the gate.
    fn matched_domain(&self) -> Option<&str> {
        match self {
            Self::Match(d) | Self::MatchEmailOnly(d) => Some(d.as_str()),
            Self::NoMatch | Self::NoVerifiedDomain => None,
        }
    }
}

/// Everything the ladder reads, gathered by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LadderSignals {
    pub directory: DirectorySignal,
    pub workspace: WorkspaceSignal,
    pub domain: DomainSignal,
}

/// Which rung answered. `None` is not a rung — it is D16's fourth row,
/// "neither available".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rung {
    /// Rung 1 — the org's own directory lists them.
    Directory,
    /// Rung 2 — full workspace member AND a DNS-verified domain match
    /// (founder ruling 2026-09-22: an email-confirmed domain does not
    /// qualify — see [`DomainSignal::MatchEmailOnly`]).
    MemberAndDomain,
    /// Rung 3 — one of the two rung-2 signals alone.
    SingleSignal,
    /// Neither signal is available.
    None,
}

impl Rung {
    /// The stable string this rung is recorded under — in
    /// `org_join_requests.evidence.ladder.rung`, and (for the top two rungs
    /// only) in `org_auto_adds.rung`, whose own `CHECK` enumerates exactly
    /// `'directory'` and `'member_and_domain'`. `pub(crate)` since CS-447:
    /// the auto-add ledger must record WHICH rung admitted someone (D13 as
    /// amended 2026-09-22), and re-deriving these strings at that call site
    /// is how the column and the evidence drift apart.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::MemberAndDomain => "member_and_domain",
            Self::SingleSignal => "single_signal",
            Self::None => "none",
        }
    }
    fn number(self) -> Option<u8> {
        match self {
            Self::Directory => Some(1),
            Self::MemberAndDomain => Some(2),
            Self::SingleSignal => Some(3),
            Self::None => None,
        }
    }
}

/// What the ladder concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Rungs 1 and 2 — strong enough for item 9's auto-add to act on, IF
    /// the toggle is on and the pool and tombstone gates also pass. This
    /// verdict is not by itself permission to add anyone.
    AutoAddEligible,
    /// Rung 3 — one signal alone. D16: *"Either one alone → approval only"*.
    /// Never auto-add.
    ApprovalOnly,
    /// D16's fourth row: *"Neither available → auto-add cannot be armed."*
    NotArmed,
}

impl Verdict {
    fn label(self) -> &'static str {
        match self {
            Self::AutoAddEligible => "auto_add_eligible",
            Self::ApprovalOnly => "approval_only",
            Self::NotArmed => "not_armed",
        }
    }

    /// The ONE predicate item 9 may consult. Deliberately a method on the
    /// verdict rather than a free function over signals, so a future caller
    /// cannot re-derive "close enough" from the raw signals and skip the
    /// ladder.
    pub(crate) fn may_auto_add(self) -> bool {
        matches!(self, Self::AutoAddEligible)
    }
}

/// The verdict plus the evidence that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LadderOutcome {
    pub verdict: Verdict,
    pub rung: Rung,
    pub signals: LadderSignals,
}

impl LadderOutcome {
    /// The JSONB blob written into `org_join_requests.evidence` under
    /// `"ladder"` (migration 0238's free-form bag — nothing in it is queried
    /// by value, it is rendered back to an admin).
    ///
    /// `directory` records the three-way label, so `"unavailable"` is
    /// visible as itself and can never be read back as `"not_listed"`; the
    /// reason string rides alongside it under `directory_reason` for the
    /// same purpose.
    pub(crate) fn evidence(&self) -> Json {
        let mut obj = json!({
            "verdict": self.verdict.label(),
            "rung": self.rung.label(),
            "directory": self.signals.directory.label(),
            "workspace_membership": self.signals.workspace.label(),
            "domain": self.signals.domain.label(),
        });
        if let Some(n) = self.rung.number() {
            obj["rung_number"] = json!(n);
        }
        if let DirectorySignal::Unavailable(reason) = &self.signals.directory {
            obj["directory_reason"] = json!(reason);
        }
        if let Some(domain) = self.signals.domain.matched_domain() {
            obj["matched_domain"] = json!(domain);
        }
        obj
    }
}

/// D16's ladder, evaluated. Pure — no DB, no network, unit-tested directly.
///
/// Read top-down; the first rung that answers wins:
///
/// 1. the directory LISTS them → auto-add eligible;
/// 2. full workspace member AND a DNS-verified domain match → auto-add
///    eligible (an email-confirmed domain is [`DomainSignal::MatchEmailOnly`]
///    and does NOT satisfy this rung);
/// 3. exactly one of those two → approval only;
/// 4. neither → not armed.
///
/// [`DirectorySignal::Unavailable`] and [`DirectorySignal::NotListed`] both
/// simply fail to fire rung 1; neither is allowed to veto or to substitute
/// for a rung-2 signal (see this module's doc comment, "OR, not veto").
pub(crate) fn evaluate(signals: LadderSignals) -> LadderOutcome {
    if signals.directory == DirectorySignal::Listed {
        return LadderOutcome { verdict: Verdict::AutoAddEligible, rung: Rung::Directory, signals };
    }
    let full_member = signals.workspace == WorkspaceSignal::FullMember;
    let domain_match = signals.domain.is_match();
    let (verdict, rung) = match (full_member, domain_match) {
        (true, true) => (Verdict::AutoAddEligible, Rung::MemberAndDomain),
        (true, false) | (false, true) => (Verdict::ApprovalOnly, Rung::SingleSignal),
        (false, false) => (Verdict::NotArmed, Rung::None),
    };
    LadderOutcome { verdict, rung, signals }
}

/// Rung 1's signal, derived from
/// [`crate::connectors::google_chat::fetch_domain_directory`]'s **own
/// outcome**.
///
/// `directory` is `Err(reason)` when the domain directory could not be
/// established this pass — including the zero-people-with-HTTP-200 case that
/// function already folds into `Err` on our behalf. This function neither
/// re-reads the wire nor re-derives the emptiness check; it takes the result
/// and maps `Err` to [`DirectorySignal::Unavailable`], full stop. That
/// parameter shape is copied deliberately from
/// `connectors::google_chat::resolve_chat_directory_identity`, which takes
/// the same `Result<&…, &str>` for the same reason: the fetch happens AT
/// MOST ONCE per pass, and the discipline must live where the caller cannot
/// route around it.
///
/// The lookup is by verified email address, scanning the directory's
/// entries — the map is keyed by Google `sub`, which the population being
/// classified (people who may never have touched CoralSwarm) does not carry.
/// A linear scan is bounded by `DIRECTORY_MAX_PAGES` and runs at most once
/// per person considered.
pub(crate) fn directory_signal(
    directory: Result<&std::collections::HashMap<String, DirectoryEntry>, &str>,
    email: &str,
) -> DirectorySignal {
    let directory = match directory {
        Ok(d) => d,
        // COULD NOT ASK. Never `NotListed`.
        Err(reason) => return DirectorySignal::Unavailable(reason.to_string()),
    };
    let needle = email.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return DirectorySignal::Unavailable("no email address to look up".to_string());
    }
    if directory.values().any(|e| e.email.trim().to_ascii_lowercase() == needle) {
        DirectorySignal::Listed
    } else {
        DirectorySignal::NotListed
    }
}

/// Rung 2's domain half — one scoped read of `org_domains`
/// (`services::org_domains::verified_domains_with_method`, which filters
/// `verified_at IS NOT NULL`) **plus the method filter the founder's
/// 2026-09-22 ruling added**: only a `dns_txt`-verified domain produces
/// [`DomainSignal::Match`]. An `email`-verified one produces
/// [`DomainSignal::MatchEmailOnly`], which never admits at rung 2 while
/// still being reported honestly in the evidence.
///
/// Where an org has one of each, only the DNS-verified one counts here. D2's
/// guest rule is read through a DIFFERENT function
/// (`org_domains::verified_domains`, method-blind) and is unaffected.
///
/// A database failure yields [`DomainSignal::NoVerifiedDomain`], the
/// fail-closed reading: an unreachable domain table must never look like a
/// match. It is logged rather than swallowed silently.
pub(crate) async fn domain_signal(control: &PgPool, account_id: Uuid, email: &str) -> DomainSignal {
    let verified = match org_domains::verified_domains_with_method(control, account_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id,
                "assertion_ladder: verified-domain read failed — treating the org as having no \
                 verified domain (fail closed)"
            );
            return DomainSignal::NoVerifiedDomain;
        }
    };
    let any_verified_domain = !verified.is_empty();
    let matched = org_domains::email_domain_part(email)
        .and_then(|candidate| verified.into_iter().find(|(d, _)| *d == candidate));
    classify_domain_match(matched, any_verified_domain)
}

/// The pure decision inside [`domain_signal`]: given what the database
/// found, which signal is it?
///
/// **Extracted so the METHOD FILTER is testable without a database** — the
/// founder's 2026-09-22 ruling lives in the `may_arm_auto_add` arm below,
/// and a unit test that constructs a [`DomainSignal::MatchEmailOnly`] by
/// hand and asserts the ladder refuses it does NOT exercise that arm. It
/// tests the ladder's handling of the variant, which is a different
/// property: flip `may_arm_auto_add` to always-`true` and such a test stays
/// green while the ruling is gone.
///
/// That is the failure this function exists to prevent, and it is a shape
/// this repo has been bitten by twice in one day: **when a test asserts an
/// outcome that two mechanisms can produce, it pins neither, and its name is
/// not evidence about which one it guards.** So the mapping is a named,
/// database-free function, and
/// [`tests::an_email_verified_domain_never_satisfies_rung_two`] drives it
/// from a `VerificationMethod::Email` through the SAME path production
/// takes — which makes that test die with the ruling, not merely alongside
/// it.
///
/// `any_verified_domain` distinguishes the two `None` arms: "this org has
/// verified domains and the address is on none of them" (`NoMatch`) is a
/// different fact from "this org has verified nothing at all"
/// (`NoVerifiedDomain`), and only the second means D11's gate is simply
/// unconfigured.
pub(crate) fn classify_domain_match(
    matched: Option<(String, org_domains::VerificationMethod)>,
    any_verified_domain: bool,
) -> DomainSignal {
    match matched {
        // Founder ruling, 2026-09-22: DNS proves the DOMAIN, an emailed
        // confirmation proves one MAILBOX at it. Only the first arms rung 2.
        Some((domain, method)) if method.may_arm_auto_add() => DomainSignal::Match(domain),
        Some((domain, _email_only)) => DomainSignal::MatchEmailOnly(domain),
        None if any_verified_domain => DomainSignal::NoMatch,
        None => DomainSignal::NoVerifiedDomain,
    }
}

/// The whole ladder for one person, with the domain half read from the
/// database and the other two supplied by the caller.
///
/// `membership` is `None` when the caller genuinely has no connector signal
/// at this moment — which is the live caller's ordinary case
/// (`routes::org_join_requests::create`: someone clicking "ask to join" on
/// the web is not accompanied by a Slack `users.info` response). `None` and
/// [`MembershipClass::Unavailable`] both become
/// [`WorkspaceSignal::Unknown`], which never admits.
///
/// `directory` is `Err(reason)` for every caller that has no live Google
/// directory to consult — the same value a failed fetch produces, because
/// they are the same fact: we could not ask.
pub(crate) async fn evaluate_for_email(
    control: &PgPool,
    account_id: Uuid,
    email: &str,
    directory: Result<&std::collections::HashMap<String, DirectoryEntry>, &str>,
    membership: Option<MembershipClass>,
) -> LadderOutcome {
    let signals = LadderSignals {
        directory: directory_signal(directory, email),
        workspace: membership.map_or(WorkspaceSignal::Unknown, WorkspaceSignal::from_membership_class),
        domain: domain_signal(control, account_id, email).await,
    };
    evaluate(signals)
}

/// The `Err` reason a caller with no Google connection at all passes to
/// [`evaluate_for_email`] — named rather than spelled out at each call site,
/// so the evidence an admin reads carries one consistent sentence for the
/// commonest case, and so nobody is tempted to pass `Ok(&empty_map)` (which
/// would be a lie: an empty map means "asked, and nobody is listed").
pub(crate) const NO_DIRECTORY_AVAILABLE: &str =
    "no live Google Workspace directory for this org (item 6: could not ask, which is not the \
     same as not listed)";

/// The inverse of [`LadderOutcome::evidence`] — CS-449 (item 38, D11's "ask
/// once", and the drain). Both re-run an ALREADY-QUEUED request through
/// [`decide`](crate::services::org_auto_add::decide) without a live
/// Slack/Google call: rung 2's workspace half came from a join-notice
/// token this server no longer holds once the row is sitting in the queue
/// (see `routes::org_join_requests::create`'s own "no live directory read
/// from a requester-facing POST" reasoning, which applies just as much to
/// a re-check minutes or days later). The STORED verdict from request time
/// is therefore the only evidence available, and this function reconstructs
/// it byte-for-byte from what [`LadderOutcome::evidence`] wrote.
///
/// Takes the `evidence.ladder` sub-object directly (not the whole
/// `org_join_requests.evidence` row) — callers already have it at hand from
/// whichever key they stored it under.
///
/// Returns `None` for anything the labels do not recognise: a request
/// queued before this evidence shape existed (there was no auto-add before
/// CS-447), or genuinely malformed JSON. A caller MUST treat that the same
/// as any other refusal — never as a pass — the same fail-closed posture
/// `domain_signal`'s own database-failure branch takes.
pub(crate) fn outcome_from_evidence(ladder_evidence: &Json) -> Option<LadderOutcome> {
    let verdict = match ladder_evidence.get("verdict")?.as_str()? {
        "auto_add_eligible" => Verdict::AutoAddEligible,
        "approval_only" => Verdict::ApprovalOnly,
        "not_armed" => Verdict::NotArmed,
        _ => return None,
    };
    let rung = match ladder_evidence.get("rung")?.as_str()? {
        "directory" => Rung::Directory,
        "member_and_domain" => Rung::MemberAndDomain,
        "single_signal" => Rung::SingleSignal,
        "none" => Rung::None,
        _ => return None,
    };
    let matched_domain = ladder_evidence.get("matched_domain").and_then(|v| v.as_str()).map(str::to_string);
    let domain = match ladder_evidence.get("domain")?.as_str()? {
        "match" => DomainSignal::Match(matched_domain?),
        "match_email_verified_only" => DomainSignal::MatchEmailOnly(matched_domain?),
        "no_match" => DomainSignal::NoMatch,
        "no_verified_domain" => DomainSignal::NoVerifiedDomain,
        _ => return None,
    };
    let directory = match ladder_evidence.get("directory")?.as_str()? {
        "listed" => DirectorySignal::Listed,
        "not_listed" => DirectorySignal::NotListed,
        "unavailable" => DirectorySignal::Unavailable(
            ladder_evidence.get("directory_reason").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
        ),
        _ => return None,
    };
    let workspace = match ladder_evidence.get("workspace_membership")?.as_str()? {
        "full_member" => WorkspaceSignal::FullMember,
        "not_full_member" => WorkspaceSignal::NotFullMember,
        "unknown" => WorkspaceSignal::Unknown,
        _ => return None,
    };
    Some(LadderOutcome { verdict, rung, signals: LadderSignals { directory, workspace, domain } })
}

#[cfg(test)]
mod tests {
    //! Pure, DB-and-network-free. The ladder itself
    //! ([`evaluate`]/[`directory_signal`]) takes every input by value, so
    //! the four D16 rows and the `Err`-is-not-absence property are all
    //! testable without Postgres or a Google call.

    use super::*;
    use std::collections::HashMap;

    fn entry(email: &str) -> DirectoryEntry {
        DirectoryEntry { email: email.to_string(), display_name: "Someone".to_string() }
    }

    fn signals(directory: DirectorySignal, workspace: WorkspaceSignal, domain: DomainSignal) -> LadderSignals {
        LadderSignals { directory, workspace, domain }
    }

    #[test]
    fn rung_one_admits_when_the_directory_lists_them() {
        let out = evaluate(signals(
            DirectorySignal::Listed,
            WorkspaceSignal::Unknown,
            DomainSignal::NoVerifiedDomain,
        ));
        assert_eq!(out.verdict, Verdict::AutoAddEligible);
        assert_eq!(out.rung, Rung::Directory);
        assert!(out.verdict.may_auto_add());
    }

    #[test]
    fn rung_two_admits_on_full_member_and_a_verified_domain_match() {
        let out = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::FullMember,
            DomainSignal::Match("acme.com".to_string()),
        ));
        assert_eq!(out.verdict, Verdict::AutoAddEligible);
        assert_eq!(out.rung, Rung::MemberAndDomain);
        assert_eq!(out.evidence()["matched_domain"], json!("acme.com"));
    }

    /// NEGATIVE TEST 3 (CS-443). D16: *"Either one alone → approval only."*
    /// Neither a full workspace member with no verified domain match, nor a
    /// verified domain match on someone who is not a full member, may
    /// auto-add. The contractor on `@acme.com` who is a guest, and the
    /// fractional exec who is a full member of four workspaces, are exactly
    /// the two people D11 named.
    #[test]
    fn rung_three_one_signal_alone_never_admits() {
        let member_only = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::FullMember,
            DomainSignal::NoMatch,
        ));
        assert_eq!(member_only.verdict, Verdict::ApprovalOnly);
        assert_eq!(member_only.rung, Rung::SingleSignal);
        assert!(!member_only.verdict.may_auto_add(), "a full member alone must never auto-add");

        let member_only_no_domain_configured = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::FullMember,
            DomainSignal::NoVerifiedDomain,
        ));
        assert_eq!(member_only_no_domain_configured.verdict, Verdict::ApprovalOnly);
        assert!(!member_only_no_domain_configured.verdict.may_auto_add());

        let domain_only = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::Unknown,
            DomainSignal::Match("acme.com".to_string()),
        ));
        assert_eq!(domain_only.verdict, Verdict::ApprovalOnly);
        assert_eq!(domain_only.rung, Rung::SingleSignal);
        assert!(!domain_only.verdict.may_auto_add(), "a domain match alone must never auto-add");

        let guest_on_the_domain = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::NotFullMember,
            DomainSignal::Match("acme.com".to_string()),
        ));
        assert_eq!(guest_on_the_domain.verdict, Verdict::ApprovalOnly);
        assert!(!guest_on_the_domain.verdict.may_auto_add());
    }

    /// **NEGATIVE TEST 5 (CS-443, founder ruling 2026-09-22), database-free
    /// half — and it drives the RULING, not just the variant.**
    ///
    /// The signal is derived from a `VerificationMethod::Email` through
    /// [`classify_domain_match`], the same mapping `domain_signal` uses in
    /// production, rather than by constructing a
    /// [`DomainSignal::MatchEmailOnly`] by hand. That distinction is the
    /// whole point: an earlier cut of this test built the variant directly,
    /// and **stayed green when `may_arm_auto_add` was mutated to always-
    /// `true`** — it was pinning the ladder's handling of a variant, while
    /// claiming to pin the ruling that produces it. Two mechanisms can
    /// produce "an email-verified domain does not admit", and a test that
    /// asserts only the outcome pins neither.
    ///
    /// Now a mutation of the method filter kills this test AND
    /// `connectors::slack::domain_seam_pg_tests::
    /// an_email_verified_domain_anchors_the_guest_rule_but_not_rung_two`,
    /// which is the live-database half (it additionally proves the SEAM
    /// still returns the domain, so D2's guest rule stays armed).
    #[test]
    fn an_email_verified_domain_never_satisfies_rung_two() {
        use crate::services::org_domains::VerificationMethod;

        // The mapping itself: an `email` row is a match the ladder may not
        // act on; the SAME domain on a `dns_txt` row is one it may.
        let email_signal =
            classify_domain_match(Some(("acme.com".to_string(), VerificationMethod::Email)), true);
        assert_eq!(
            email_signal,
            DomainSignal::MatchEmailOnly("acme.com".to_string()),
            "a mailbox confirmation must not map to a rung-2-eligible match"
        );
        assert_eq!(
            classify_domain_match(Some(("acme.com".to_string(), VerificationMethod::DnsTxt)), true),
            DomainSignal::Match("acme.com".to_string()),
            "a TXT record does"
        );

        let email_only = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::FullMember,
            email_signal,
        ));
        assert_eq!(
            email_only.verdict,
            Verdict::ApprovalOnly,
            "a mailbox confirmation proves one mailbox, not the domain — never rung 2"
        );
        assert_eq!(email_only.rung, Rung::SingleSignal);
        assert!(!email_only.verdict.may_auto_add());

        // Honest, and distinguishable from "does not match".
        let ev = email_only.evidence();
        assert_eq!(ev["domain"], json!("match_email_verified_only"));
        assert_eq!(ev["matched_domain"], json!("acme.com"), "the admin still sees WHICH domain matched");
        assert_ne!(ev["domain"], json!("no_match"), "an email-verified match is not a non-match");

        // The SAME pairing, differing ONLY in the proving method — derived
        // through the same mapping, so the method really is the only
        // variable — does admit. Without this half the test above could pass
        // because the pairing never admits for some unrelated reason.
        let dns = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::FullMember,
            classify_domain_match(Some(("acme.com".to_string(), VerificationMethod::DnsTxt)), true),
        ));
        assert_eq!(dns.verdict, Verdict::AutoAddEligible);
        assert_eq!(dns.rung, Rung::MemberAndDomain);

        // Rung 1 still outranks everything: the directory can only ADD.
        let listed = evaluate(signals(
            DirectorySignal::Listed,
            WorkspaceSignal::Unknown,
            classify_domain_match(Some(("acme.com".to_string(), VerificationMethod::Email)), true),
        ));
        assert_eq!(listed.verdict, Verdict::AutoAddEligible);
        assert_eq!(listed.rung, Rung::Directory);
    }

    /// The mapping's two `None` arms. "This org has verified domains and the
    /// address is on none of them" and "this org has verified nothing at
    /// all" produce the same VERDICT, so no verdict assertion can tell them
    /// apart — only the signal can, and the evidence an admin reads is
    /// materially different ("not one of your domains" vs "you have not set
    /// this up"). Asserted on the signal for exactly that reason.
    #[test]
    fn no_match_and_no_verified_domain_are_different_answers() {
        assert_eq!(classify_domain_match(None, true), DomainSignal::NoMatch);
        assert_eq!(classify_domain_match(None, false), DomainSignal::NoVerifiedDomain);
    }

    /// The founder's OR ruling (2026-09-22), asserted rather than assumed:
    /// a directory that definitively does NOT list someone must not veto a
    /// rung-2 admission. The directory can only ever add people.
    #[test]
    fn a_not_listed_directory_does_not_veto_rung_two() {
        let out = evaluate(signals(
            DirectorySignal::NotListed,
            WorkspaceSignal::FullMember,
            DomainSignal::Match("acme.com".to_string()),
        ));
        assert_eq!(
            out.verdict,
            Verdict::AutoAddEligible,
            "OR, not veto — decided by the founder 2026-09-22; do not re-litigate"
        );
        assert_eq!(out.rung, Rung::MemberAndDomain);
        assert_eq!(out.evidence()["directory"], json!("not_listed"), "and it is still recorded");
    }

    #[test]
    fn neither_signal_means_auto_add_cannot_be_armed() {
        let out = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::Unknown,
            DomainSignal::NoVerifiedDomain,
        ));
        assert_eq!(out.verdict, Verdict::NotArmed);
        assert_eq!(out.rung, Rung::None);
        assert!(!out.verdict.may_auto_add());
    }

    /// NEGATIVE TEST 2 (CS-443), and the single most important property in
    /// this module. `fetch_domain_directory` returns `Err` for the HTTP-200-
    /// with-zero-people case (profile sharing off, or a pre-widening grant)
    /// as well as for a hard failure. That `Err` must become
    /// [`DirectorySignal::Unavailable`] — never `NotListed`, and never a
    /// rung-1 admission.
    #[test]
    fn a_directory_error_is_unavailable_never_not_listed() {
        let sig = directory_signal(Err("people.listDirectoryPeople returned zero entries"), "dana@acme.com");
        assert!(
            matches!(sig, DirectorySignal::Unavailable(_)),
            "an Err from fetch_domain_directory must be Unavailable, got {sig:?}"
        );
        assert_ne!(sig, DirectorySignal::NotListed, "could not ask is NOT 'does not work here'");

        // And it must not be admitted at rung 1 either: with no other
        // signal the verdict is NotArmed, not an admission.
        let out = evaluate(signals(sig.clone(), WorkspaceSignal::Unknown, DomainSignal::NoVerifiedDomain));
        assert_eq!(out.rung, Rung::None);
        assert!(!out.verdict.may_auto_add());

        // The evidence an admin reads keeps the distinction visible.
        let ev = out.evidence();
        assert_eq!(ev["directory"], json!("unavailable"));
        assert!(ev["directory_reason"].is_string());

        // A SUCCESSFUL fetch that simply does not list this person is the
        // genuinely different answer, and is labelled differently.
        let mut dir = HashMap::new();
        dir.insert("sub-1".to_string(), entry("someone.else@acme.com"));
        assert_eq!(directory_signal(Ok(&dir), "dana@acme.com"), DirectorySignal::NotListed);
        assert_eq!(directory_signal(Ok(&dir), "SOMEONE.ELSE@Acme.com"), DirectorySignal::Listed);
    }

    /// `MembershipClass::Unavailable` — a narrower `users:read` grant, or a
    /// Slack response-shape change — must never read as a full member, and
    /// must not read as "definitely a guest" either.
    #[test]
    fn an_unavailable_membership_class_is_unknown_and_never_admits() {
        assert_eq!(
            WorkspaceSignal::from_membership_class(MembershipClass::Unavailable),
            WorkspaceSignal::Unknown
        );
        assert_eq!(
            WorkspaceSignal::from_membership_class(MembershipClass::FullMember),
            WorkspaceSignal::FullMember
        );
        assert_eq!(
            WorkspaceSignal::from_membership_class(MembershipClass::Guest),
            WorkspaceSignal::NotFullMember
        );
        assert_eq!(
            WorkspaceSignal::from_membership_class(MembershipClass::SlackConnect),
            WorkspaceSignal::NotFullMember
        );

        let out = evaluate(signals(
            DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
            WorkspaceSignal::from_membership_class(MembershipClass::Unavailable),
            DomainSignal::Match("acme.com".to_string()),
        ));
        assert_eq!(out.verdict, Verdict::ApprovalOnly, "unknown membership + a domain match is rung 3");
    }

    #[test]
    fn evidence_names_the_rung_and_every_signal() {
        let out = evaluate(signals(
            DirectorySignal::Listed,
            WorkspaceSignal::FullMember,
            DomainSignal::Match("acme.com".to_string()),
        ));
        let ev = out.evidence();
        assert_eq!(ev["verdict"], json!("auto_add_eligible"));
        assert_eq!(ev["rung"], json!("directory"));
        assert_eq!(ev["rung_number"], json!(1));
        assert_eq!(ev["directory"], json!("listed"));
        assert_eq!(ev["workspace_membership"], json!("full_member"));
        assert_eq!(ev["domain"], json!("match"));
        assert_eq!(ev["matched_domain"], json!("acme.com"));

        let not_armed = evaluate(signals(
            DirectorySignal::NotListed,
            WorkspaceSignal::Unknown,
            DomainSignal::NoVerifiedDomain,
        ));
        let ev = not_armed.evidence();
        assert_eq!(ev["rung"], json!("none"));
        assert!(ev.get("rung_number").is_none(), "there is no rung 4");
        assert_eq!(ev["directory"], json!("not_listed"));
        assert!(ev.get("directory_reason").is_none());
    }

    /// **CS-449 (item 38)**: [`outcome_from_evidence`] is the round trip
    /// [`LadderOutcome::evidence`] needs to survive — the queued-preview,
    /// admit, and drain paths all reconstruct a stored request's ladder
    /// outcome from exactly this JSON and feed it straight back into
    /// `decide`. Every rung/verdict/signal combination that can actually
    /// occur (per [`evaluate`]'s own four rows) must come back byte-for-byte
    /// equal to what produced it, or a re-gate could admit — or refuse — on
    /// a different verdict than the one an admin was shown.
    #[test]
    fn outcome_from_evidence_round_trips_every_reachable_combination() {
        let cases = vec![
            evaluate(signals(DirectorySignal::Listed, WorkspaceSignal::Unknown, DomainSignal::NoVerifiedDomain)),
            evaluate(signals(
                DirectorySignal::Unavailable(NO_DIRECTORY_AVAILABLE.to_string()),
                WorkspaceSignal::FullMember,
                DomainSignal::Match("acme.test".to_string()),
            )),
            evaluate(signals(
                DirectorySignal::NotListed,
                WorkspaceSignal::FullMember,
                DomainSignal::MatchEmailOnly("acme.test".to_string()),
            )),
            evaluate(signals(
                DirectorySignal::Unavailable("no connection".to_string()),
                WorkspaceSignal::NotFullMember,
                DomainSignal::Match("acme.test".to_string()),
            )),
            evaluate(signals(DirectorySignal::NotListed, WorkspaceSignal::Unknown, DomainSignal::NoMatch)),
        ];
        for original in cases {
            let round_tripped = outcome_from_evidence(&original.evidence())
                .unwrap_or_else(|| panic!("must parse back: {:?}", original.evidence()));
            assert_eq!(round_tripped, original, "evidence: {:?}", original.evidence());
        }
    }

    #[test]
    fn outcome_from_evidence_refuses_unrecognised_json() {
        assert!(outcome_from_evidence(&json!({})).is_none(), "a request queued before this shape existed");
        assert!(outcome_from_evidence(&json!({"verdict": "auto_add_eligible", "rung": "directory"})).is_none());
        assert!(
            outcome_from_evidence(&json!({
                "verdict": "not_a_real_verdict", "rung": "directory", "directory": "listed",
                "workspace_membership": "unknown", "domain": "no_verified_domain",
            }))
            .is_none()
        );
    }
}

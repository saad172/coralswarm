//! Recipient discovery for "email the unmatched" (chat-per-user plan PR 6,
//! `docs/plans/chat-per-user-ingest-and-in-chat-ask.md` §8 row 6 / §11
//! corrections 4-6).
//!
//! The spec (step 5): "We email the people in those containers who have not
//! linked yet." Under §5.3 there is no separate "link" act any more — a
//! person creates a CoralSwarm account, their verified address matches, and
//! content starts reaching them. This module answers exactly one question:
//! **for one Slack channel the agent is in, which verified email addresses
//! belong to people who have NOT done that yet?**
//!
//! # Reuses the existing roster walk — does not invent a second one
//!
//! [`connectors::slack::resolve_channel_members`] already walks
//! `fetch_conversation_members` and classifies each member via
//! [`connectors::slack::resolve_slack_identity_detailed`], but it only ever
//! returns matched user ids and a total count — it deliberately throws the
//! `Unmatched` arm's email away, because none of its three existing callers
//! (seat preview, auto-seat, the seat reconciler) need it.
//!
//! This module walks the SAME `fetch_conversation_members` roster and
//! classifies each member through
//! [`connectors::slack::resolve_slack_identity_with_email`] — the sibling
//! that shares 100% of the classification logic with `resolve_slack_identity_
//! detailed` (same shared `_impl`, see that function's own doc comment) and
//! additionally surfaces the email `extract_verified_slack_profile` already
//! pulled out of `users.info` at classification time. One network call per
//! member either way; this is not a second roster walk, it is the same walk
//! wired to a matcher call that returns one more field.
//!
//! # `Unmatched` only — this is the whole safety property
//!
//! [`SlackIdentity::Unmatched`] is the ONLY variant this module ever collects
//! an address for:
//!
//!   - `Matched` — already has a CoralSwarm account; the entire point of this
//!     nudge does not apply to them.
//!   - `Bot` — not a person.
//!   - `Unavailable` — we could not ASK, for any reason (rate limit, HTTP
//!     error, or — the case that matters most here — a bot token confirmed
//!     missing `users:read.email`, which makes Slack omit the email field for
//!     EVERY member rather than erroring). Emailing `Unavailable` would mean
//!     a single scope-broken workspace reads as "nobody here has an account"
//!     and this module sends to the entire workspace. `backend/tests/
//!     chat_per_user_e2e_pg.rs` already pins the CLASSIFIER side of this (an
//!     unconfirmed/placeholder granted scope resolves to `Unavailable`, never
//!     `Unmatched`) — [`discover_unmatched_recipients_pg_tests`] below pins
//!     the OTHER half: that this module's own filter does not accidentally
//!     widen past what the classifier hands it.
//!
//! A roster walk that fails entirely (`fetch_conversation_members` errors,
//! or any one member's lookup comes back `Unavailable`) returns `Err` rather
//! than a partial list — same "an unknown roster is never treated as an
//! empty/complete one" discipline `resolve_channel_members` documents for
//! itself, for the identical reason: a caller here would otherwise queue a
//! genuine batch of nudge emails off a roster we only partially trust.
//!
//! # Guest / Slack-Connect filter (CS-426, D2, D21)
//!
//! An `Unmatched` member is not automatically invited any more. Slack's
//! `users.info` also tells us whether this member is a full workspace
//! member, a guest (`is_restricted`/`is_ultra_restricted`), or visiting from
//! another workspace via Slack Connect (`team_id` mismatch) — see
//! `connectors::slack::MembershipClass`. D2's rule, applied here:
//!
//!   - [`MembershipClass::FullMember`] — invited unconditionally, as before
//!     this PR.
//!   - [`MembershipClass::Guest`] / [`MembershipClass::SlackConnect`] —
//!     invited ONLY when their verified email's domain matches one of this
//!     account's anchor domains (`connectors::slack::resolve_anchor_domain`,
//!     D21, item 30) — every verified org domain when any exist, else the
//!     domain of the CoralSwarm user who authorised this Slack connection.
//!     Otherwise skipped, with a debug log naming the class (never the
//!     email — see "Digest, not payload" conventions elsewhere in this crate).
//!   - [`MembershipClass::Unavailable`] — the `users.info` response omitted
//!     one of the three fields the classification needs. NEVER read as
//!     `FullMember`: skipped, exactly like a foreign-domain guest.
//!
//! The anchor domain is resolved ONCE per channel walk, before the
//! per-member loop — not once per member — so this filter costs exactly one
//! extra `users.info` call per `discover_unmatched_recipients` invocation,
//! not one per roster member.
//!
//! # The Matched-not-member audience (D19, CS-440, org-onboarding plan §3
//! item 29)
//!
//! This walk ALSO collects a second, narrower audience: a roster member who
//! classifies `Matched` — they already have a CoralSwarm account — but has
//! no `account_members` row for THIS account. D19's rule: they get a "join
//! this org" email into `org_join_requests`, not the signup nudge above.
//! This does not weaken "`Unmatched` only" above — that sentence is about
//! the SIGNUP nudge's own safety property, which still holds unchanged; the
//! join-this-org audience is a second, separately-gated collection living on
//! the same walk so the roster is never fetched twice (see
//! [`DiscoveryOutcome::matched_not_members`]'s own doc comment).
//!
//! **Ordinary orgs only (D19: "personal orgs never receive join
//! requests").** A personal org has one member by construction, so
//! `is_personal_org` is checked ONCE per walk (same "once per walk, not once
//! per member" discipline as the anchor domain above) and, when `true`,
//! nothing is ever added to this second audience — not even for a
//! `Matched`-and-not-member roster entry, which cannot occur for a real
//! personal org anyway (one member) but is guarded here defensively rather
//! than assumed. A direct inline `EXISTS` query against `personal_orgs`,
//! not a call into `services::org_join_requests::is_personal_org` —
//! `connectors` has no existing dependency on `services` anywhere in this
//! crate, and this predicate is one indexed `EXISTS` check, cheap enough to
//! keep local rather than invert that layering for. On a query failure this
//! defaults to `true` (treat as personal, collect nothing) — the "ask,
//! don't guess" discipline this whole module already applies to the anchor
//! domain (`None` on failure never admits a guest) and the roster-fetch
//! error path above, applied to the one binary fact that gates this
//! audience's very existence: never guess "ordinary org" when the query
//! that would prove it failed.
//!
//! **Membership and the guest/Slack-Connect filter are resolved in ONE
//! batched pass after the roster loop, not per member.** The loop itself
//! only ever collects a `Matched` candidate's raw facts (`user_id`, the
//! roster-supplied email if the classification branch had one, the Slack
//! external id, and the membership class); `account_members` membership and
//! a `users.email` fallback (for a `Matched` verdict reached via the
//! in-process cache or the `connector_identities` directory — see
//! [`super::slack::resolve_slack_identity_with_email`]'s own doc comment:
//! neither fast path re-fetches `users.info`, so the roster email that call
//! returns is `None` on those two paths even though the person is real and
//! known) are both resolved with ONE `= ANY($1)` query apiece, mirroring
//! `workers::slack_unmatched_email::accounts_whose_roster_holds`'s identical
//! batching discipline for the exact same reason: a channel can have
//! thousands of members, and this module's whole design principle is that
//! the guest-domain filter and now this membership check cost a constant
//! number of extra round trips per WALK, never one per member.
//!
//! **The guest/Slack-Connect domain filter (D2/D21) applies to this
//! audience too** — a client's guest account visible in a shared channel
//! must not be invited to join the org any more than they should get the
//! signup nudge; same reasoning, same anchor domain, same
//! `guests_skipped`/`slack_connect_skipped`/`unavailable_membership_skipped`
//! counters (shared across BOTH audiences deliberately — the task this
//! module implements names only [`DiscoveryOutcome::matched_member_skipped`]
//! and [`DiscoveryOutcome::matched_not_member_found`] as new counters,
//! which is this module's own signal that the three guest-filter counters
//! were meant to be reused, not forked). **One accepted, documented
//! trade-off**: `MembershipClass::Unavailable` skips the candidate exactly
//! like it does for the signup-nudge audience — but for a `Matched` person
//! reached via the cache/directory fast path, the class is ALWAYS the
//! filler `Unavailable` (see [`super::slack::resolve_slack_identity_with_
//! email`]'s own doc comment on the third element), meaning a
//! Matched-not-member is only reliably caught on a walk that resolves them
//! fresh over the network — typically the FIRST walk after they became
//! `Matched` in this workspace, or any later walk of a DIFFERENT channel/
//! workspace pair the in-process cache and the `connector_identities`
//! directory have not yet recorded. Accepted rather than special-cased: it
//! is the identical trade this module's `Unavailable` handling already
//! makes for the signup-nudge audience, just interacting with a DB-backed
//! directory (persistent across restarts) instead of network-only
//! uncertainty.

use sqlx::PgPool;
use uuid::Uuid;

use super::slack::{resolve_anchor_domain, resolve_slack_identity_with_email, MembershipClass, SlackIdentity};

/// One address discovered on this walk. A one-field struct rather than a bare
/// `String` so a future caller (a second platform's discovery module) has a
/// named place to add its own fields without every call site re-destructuring
/// a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnmatchedRecipient {
    /// Lowercase + trimmed (matches `connectors::slack::normalize_email`'s
    /// own rule — deliberately NOT provider canonicalisation, see that
    /// function's doc comment: a `+tag` alias must not collapse into its
    /// base address here either, for the identical reason).
    pub email: String,
}

/// One roster member discovered `Matched` but not an `account_members` row
/// for this account — D19's "join this org" audience (CS-440, org-onboarding
/// plan §3 item 29). See this module's own doc comment ("The Matched-not-
/// member audience") for the full collection contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MatchedNotMember {
    /// This person's existing CoralSwarm account — `users.id`. Kept, not
    /// re-derived, by every downstream caller (the tombstone check, the
    /// existing-join-request read, `enqueue_join_request`'s own second
    /// argument all want a `user_id` directly, not another `users` lookup
    /// keyed off the email below).
    pub user_id: Uuid,
    /// This person's own verified CoralSwarm account email — lowercase +
    /// trimmed, same normalisation as [`UnmatchedRecipient::email`].
    /// Sourced from the roster's own `users.info` profile when this walk
    /// resolved them fresh (the common case for a first sighting); when the
    /// classification instead came back off the in-process cache or the
    /// `connector_identities` directory (no email attached — see this
    /// module's own doc comment), the post-loop batch falls back to
    /// `users.email` for this SAME `user_id` — the account's own verified
    /// address either way, never a second, unverified source.
    pub email: String,
    /// The Slack workspace member id this person was resolved from — carried
    /// for observability (logging/debugging a specific walk's decision)
    /// only; never itself used for matching (the `user_id` above already is
    /// the verified link) and not part of `org_join_requests.evidence`,
    /// which records `workspace_id`/`channel_id` — the CONTAINER — rather
    /// than this per-person Slack id.
    pub slack_user_id: String,
    /// **CS-447.** How this walk classified them in the workspace — the
    /// verdict the guest filter just above this push applied, KEPT rather
    /// than discarded.
    ///
    /// It is D16 rung 2's workspace half, and this walk (on a connected
    /// member's own token, per D21) is the only place in the system that
    /// establishes it. `workers::slack_unmatched_email::
    /// process_matched_not_members` signs it into the join-notice token so
    /// `routes::org_join_requests::create` can read it back without firing
    /// a live Slack call from a requester-facing POST.
    ///
    /// **Never `Unavailable` in practice** — that class is filtered out
    /// above and never reaches this struct — but carried as the full enum
    /// rather than a `bool`, because "full member" and "guest on the anchor
    /// domain" are the two things this audience deliberately mixes, and a
    /// `bool` at this boundary is exactly how they get confused.
    pub class: MembershipClass,
}

/// The outcome of one channel's roster walk (CS-426, item 3) — a small,
/// named place for the guest/Slack-Connect filter's skip counts alongside
/// the recipients it DID collect, so a caller does not have to re-derive
/// "how many did we skip and why" from anything else. This is the smallest
/// extension of this module's existing "outcome" shape (a bare
/// `Vec<UnmatchedRecipient>`) that keeps the decision itself — not a log
/// line — the thing a test or a future admin-facing count reads.
/// `workers::slack_unmatched_email::process_job` logs the three counts as a
/// per-job summary; a durable, cross-restart aggregate (an actual "N guests
/// skipped this month" surface) is intentionally left as follow-up rather
/// than folding this into `slack_unmatched_email_notices` (whose row
/// lifetime is a per-recipient SEND cooldown — reusing it for a decision
/// that never sends anything would make a future legitimate send read as
/// still "cooling down") or into `connector_audit_log` (a curated
/// compliance trail for discrete lifecycle events, not a per-roster-member
/// counter that would fire on every walk of a large channel).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DiscoveryOutcome {
    pub recipients: Vec<UnmatchedRecipient>,
    pub guests_skipped: usize,
    pub slack_connect_skipped: usize,
    pub unavailable_membership_skipped: usize,
    /// CS-428 (`docs/plans/org-onboarding-from-the-invite.md` §3 item 3,
    /// D12) — how many of this walk's `Unmatched`, filter-cleared addresses
    /// were never actually invited because this org already removed that
    /// person (`org_membership_tombstones`). Unlike the other three fields,
    /// this one is populated by `workers::slack_unmatched_email::process_
    /// job` AFTER this function returns, not by `discover_unmatched_
    /// recipients` itself: the org whose tombstone applies is the D4-
    /// selected SENDING account, which is only known once `select_notice_
    /// account` has run for a given recipient — strictly after this walk
    /// completes (see that loop's own comment for why the winner is always
    /// this job's own `account_id` by the time it checks). It lives on this
    /// shared "outcome" shape anyway, alongside the other three skip
    /// counts, because it answers the identical question a caller asks of
    /// them: how many addresses did this job discover but never invite, and
    /// why.
    pub tombstoned_skipped: usize,
    /// D19 (CS-440, org-onboarding plan §3 item 29) — every roster member
    /// this walk classified `Matched`, cleared the guest/Slack-Connect
    /// filter, and has no `account_members` row for THIS account. See this
    /// module's own "The Matched-not-member audience" doc section for the
    /// full collection contract. Empty, unconditionally, for a personal-org
    /// account (D19: "personal orgs never receive join requests") and for
    /// every roster this walk could not fully trust (this function still
    /// returns `Err` in that case — see the module doc's roster-trust
    /// section — so an empty `Vec` here is never reachable alongside a
    /// partial roster the caller might otherwise mistake for "nobody
    /// matched").
    pub matched_not_members: Vec<MatchedNotMember>,
    /// How many `Matched` roster members this walk found ALREADY a member
    /// of this account — the ordinary case for most of a channel's roster
    /// once an org has been live a while, and not itself an error or a
    /// notable skip; counted for the same "every decision this walk makes
    /// is visible somewhere, not silently absorbed" reasoning the other
    /// counters on this struct already follow.
    pub matched_member_skipped: usize,
    /// `matched_not_members.len()` at the moment this walk finished —
    /// tracked as its own field, not derived by the caller from the `Vec`'s
    /// length after the fact, for the same reason `recipients.len()` isn't
    /// how `workers::slack_unmatched_email::process_job` reports "how many
    /// did this walk find": the caller drains/iterates `matched_not_members`
    /// while processing it, and a count captured at discovery time is a
    /// stable fact about the WALK, independent of whatever the caller does
    /// with the list afterward.
    pub matched_not_member_found: usize,
}

/// Whether this roster member clears D2's guest filter.
///
/// A full member always does. A guest or Slack-Connect participant does
/// only when [`crate::services::org_domains::address_on_guest_anchor`] says
/// their address sits on one of `anchors` — every verified org domain, or
/// the connecting member's domain while none is verified. `Unavailable`
/// never clears: a missing classification must not read as a full member.
fn clears_guest_domain_filter(class: MembershipClass, email: &str, anchors: &[String]) -> bool {
    match class {
        MembershipClass::FullMember => true,
        MembershipClass::Guest | MembershipClass::SlackConnect => {
            crate::services::org_domains::address_on_guest_anchor(email, anchors)
        }
        MembershipClass::Unavailable => false,
    }
}

/// Walk one Slack channel's roster and return the verified email of every
/// member classified `Unmatched` AND cleared by the guest/Slack-Connect
/// filter (D2, D21) — deduplicated (a person can appear once; Slack's own
/// roster has no duplicate member ids, but defensive dedup costs nothing and
/// protects a future caller that merges several channels' results before
/// enqueuing).
///
/// `Err` means the roster could not be fully trusted (see this module's own
/// doc comment) — the caller must change nothing rather than act on a
/// partial answer.
pub(crate) async fn discover_unmatched_recipients(
    control_pool: &PgPool,
    identity_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
    channel_id: &str,
) -> Result<DiscoveryOutcome, String> {
    let external_ids =
        super::slack::fetch_conversation_members(http, bot_token, channel_id).await?;

    // Once per walk, not once per member — see this module's own doc
    // comment.
    let anchor_domains = resolve_anchor_domain(control_pool, http, account_id, workspace_id, bot_token).await;

    // D19/CS-440 — once per walk, same discipline as the anchor domain
    // above. Fails closed (`true`, i.e. "treat as personal, collect
    // nothing") on a query error — see this module's own "Matched-not-
    // member audience" doc section for why.
    let is_personal_org: bool = match sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS ( \
             SELECT 1 FROM personal_orgs po \
             JOIN accounts a ON a.clerk_org_id = po.clerk_org_id \
             WHERE a.id = $1 \
         )",
    )
    .bind(account_id)
    .fetch_one(control_pool)
    .await
    {
        Ok(v) => v,
        // CodeRabbit round 1 (#1026): a persistent failure here silently
        // suppresses the whole join-this-org audience, walk after walk,
        // with nothing to alert on — log it, same "every skip is visible
        // somewhere" discipline this module already follows for its other
        // three skip counters.
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id, workspace_id, channel_id,
                "CS-440: personal-org check failed — treating this account as personal and \
                 collecting nothing for the join-this-org audience rather than guessing"
            );
            true
        }
    };

    let mut seen = std::collections::HashSet::new();
    let mut outcome = DiscoveryOutcome::default();
    // Raw `Matched` facts collected during the walk, resolved to a real
    // membership/email/class verdict in ONE batched pass after the loop —
    // see this module's own doc comment for why.
    struct MatchedCandidate {
        user_id: Uuid,
        slack_user_id: String,
        roster_email: Option<String>,
        class: MembershipClass,
    }
    let mut matched_candidates: Vec<MatchedCandidate> = Vec::new();

    for external_id in external_ids {
        let (identity, email, class) = resolve_slack_identity_with_email(
            control_pool,
            identity_pool,
            http,
            account_id,
            workspace_id,
            &external_id,
            bot_token,
        )
        .await;

        match identity {
            SlackIdentity::Unmatched => {
                // `extract_verified_slack_profile` guarantees a non-empty,
                // trimmed string whenever `resolve_slack_identity_with_email`
                // pairs it with `Unmatched` — see that function's own doc
                // comment ("`Some` only on the branch that actually
                // extracted one"). A `None` here means this member never
                // reached that branch at all (e.g. the early `workspace_id`/
                // `external_user_id`-empty guard) — nothing to email, and
                // NOT a reason to fail the whole walk.
                let Some(email) = email else { continue };
                let normalized = email.trim().to_lowercase();
                if normalized.is_empty() {
                    continue;
                }

                match class {
                    MembershipClass::FullMember => {}
                    MembershipClass::Guest | MembershipClass::SlackConnect => {
                        if !clears_guest_domain_filter(class, &normalized, &anchor_domains) {
                            if class == MembershipClass::Guest {
                                outcome.guests_skipped += 1;
                            } else {
                                outcome.slack_connect_skipped += 1;
                            }
                            tracing::debug!(
                                workspace_id,
                                channel_id,
                                external_id = %external_id,
                                class = ?class,
                                "CS-426: skipping unmatched Slack member outside the account's \
                                 anchor domain — not invited (D2/D21 guest/Slack-Connect rule)"
                            );
                            continue;
                        }
                    }
                    MembershipClass::Unavailable => {
                        outcome.unavailable_membership_skipped += 1;
                        tracing::debug!(
                            workspace_id,
                            channel_id,
                            external_id = %external_id,
                            "CS-426: users.info omitted is_restricted/is_ultra_restricted/team_id \
                             for this member — membership class Unavailable, not invited (absence \
                             must never read as full member)"
                        );
                        continue;
                    }
                }

                if seen.insert(normalized.clone()) {
                    outcome.recipients.push(UnmatchedRecipient { email: normalized });
                }
            }
            // D19/CS-440 — collected raw here (a personal org collects
            // nothing at all, per this module's own doc comment); resolved
            // to a real verdict in the batched pass below the loop.
            SlackIdentity::Matched(user_id) => {
                if !is_personal_org {
                    matched_candidates.push(MatchedCandidate {
                        user_id,
                        slack_user_id: external_id.clone(),
                        roster_email: email,
                        class,
                    });
                }
            }
            SlackIdentity::Bot => continue,
            // Same "the whole roster is abandoned rather than reported
            // short" discipline `resolve_channel_members` applies to its own
            // callers — see this module's doc comment for why an
            // `Unavailable` member must never be silently skipped here: that
            // is exactly the shape of bug that would let a scope-broken
            // workspace email real people it never actually asked about.
            SlackIdentity::Unavailable(why) => {
                return Err(format!(
                    "could not resolve Slack member {external_id} in {channel_id}: {why}"
                ));
            }
        }
    }

    // D19/CS-440 — the batched membership + email-fallback + guest-filter
    // pass. See this module's own "Matched-not-member audience" doc section
    // for the full reasoning; every DB read here is best-effort (a failure
    // logs and narrows this audience for THIS walk only — it never fails
    // the whole roster walk, unlike the `Unavailable`-identity `Err` above,
    // because the SIGNUP-nudge audience this function primarily exists for
    // is unaffected either way).
    if !matched_candidates.is_empty() {
        let mut seen_matched = std::collections::HashSet::new();
        matched_candidates.retain(|c| seen_matched.insert(c.user_id));

        let candidate_ids: Vec<Uuid> = matched_candidates.iter().map(|c| c.user_id).collect();
        let member_ids: std::collections::HashSet<Uuid> = sqlx::query_scalar(
            "SELECT user_id FROM account_members WHERE account_id = $1 AND user_id = ANY($2)",
        )
        .bind(account_id)
        .bind(&candidate_ids)
        .fetch_all(control_pool)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(
                error = %e, account_id = %account_id, workspace_id, channel_id,
                "CS-440: account_members membership check failed for the join-this-org audience \
                 — treating every candidate as already a member rather than guessing an invite"
            );
            candidate_ids.clone()
        })
        .into_iter()
        .collect();

        let not_members: Vec<&MatchedCandidate> =
            matched_candidates.iter().filter(|c| !member_ids.contains(&c.user_id)).collect();
        outcome.matched_member_skipped = matched_candidates.len() - not_members.len();

        let need_email_ids: Vec<Uuid> = not_members
            .iter()
            .filter(|c| c.roster_email.is_none())
            .map(|c| c.user_id)
            .collect();
        let email_by_id: std::collections::HashMap<Uuid, String> = if need_email_ids.is_empty() {
            std::collections::HashMap::new()
        } else {
            sqlx::query_as::<_, (Uuid, String)>(
                "SELECT id, email FROM users WHERE id = ANY($1) AND deleted_at IS NULL",
            )
            .bind(&need_email_ids)
            .fetch_all(control_pool)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    error = %e, account_id = %account_id, workspace_id, channel_id,
                    "CS-440: users.email fallback lookup failed for the join-this-org audience — \
                     candidates with no roster-supplied email are skipped this walk rather than \
                     guessed"
                );
                Vec::new()
            })
            .into_iter()
            .collect()
        };

        for c in not_members {
            let Some(raw_email) = c.roster_email.clone().or_else(|| email_by_id.get(&c.user_id).cloned())
            else {
                // No roster email (a cache/directory hit — see this module's
                // own doc comment) AND no `users.email` fallback (a lookup
                // failure, or a raced-deleted `users` row) — cannot invite
                // someone with no address. Never a reason to fail the whole
                // walk.
                continue;
            };
            let normalized = raw_email.trim().to_lowercase();
            if normalized.is_empty() {
                continue;
            }

            // The identical D2/D21 guest/Slack-Connect domain filter the
            // Unmatched arm above applies, sharing its three skip counters
            // (this module's own doc comment explains why they are shared,
            // not forked, for this audience).
            match c.class {
                MembershipClass::FullMember => {}
                MembershipClass::Guest | MembershipClass::SlackConnect => {
                    if !clears_guest_domain_filter(c.class, &normalized, &anchor_domains) {
                        if c.class == MembershipClass::Guest {
                            outcome.guests_skipped += 1;
                        } else {
                            outcome.slack_connect_skipped += 1;
                        }
                        tracing::debug!(
                            workspace_id,
                            channel_id,
                            external_id = %c.slack_user_id,
                            class = ?c.class,
                            "CS-440: skipping a Matched-not-member outside the account's anchor \
                             domain — not invited to join (D2/D21 guest/Slack-Connect rule)"
                        );
                        continue;
                    }
                }
                MembershipClass::Unavailable => {
                    outcome.unavailable_membership_skipped += 1;
                    tracing::debug!(
                        workspace_id,
                        channel_id,
                        external_id = %c.slack_user_id,
                        "CS-440: membership class Unavailable for a Matched-not-member (a cache/ \
                         directory hit, or a genuinely narrower users.info grant) — not invited to \
                         join this walk"
                    );
                    continue;
                }
            }

            outcome.matched_not_members.push(MatchedNotMember {
                user_id: c.user_id,
                email: normalized,
                slack_user_id: c.slack_user_id.clone(),
                // The class the `match` immediately above just cleared —
                // taken from the SAME value that decided the filter, so
                // the two can never disagree (CS-447).
                class: c.class,
            });
        }
        outcome.matched_not_member_found = outcome.matched_not_members.len();
    }

    Ok(outcome)
}

/// One CoralSwarm account that could plausibly send the "email the
/// unmatched" nudge to a SPECIFIC recipient — the candidate set
/// [`select_notice_account`] chooses among, per D4
/// (`docs/plans/org-onboarding-from-the-invite.md` §3 item 2 / D4 "One
/// email, one account, chosen deterministically", CS-427).
///
/// Every fact here is precomputed by the caller (a DB read) so the
/// selection rule itself stays pure and DB-free — see [`select_notice_
/// account`]'s own doc comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NoticeAccountCandidate {
    pub(crate) account_id: Uuid,
    /// D4 tier 1 — this account's own member roster (`account_members` ⋈
    /// `users`) already has a verified user at the recipient's exact
    /// (normalized) address, i.e. the recipient already has standing with
    /// this specific org even though no CoralSwarm identity has linked to
    /// THIS Slack account yet.
    ///
    /// **Unreachable from this module's own call site, by construction —
    /// wired anyway because D4 states it as the general rule.**
    /// [`SlackIdentity::Unmatched`] is only ever produced when
    /// `connectors::slack::match_and_persist_verified_email`'s `users`
    /// lookup — GLOBAL, not scoped to any one account (see that function's
    /// own doc comment) — found NO row anywhere with this email. A `true`
    /// here requires exactly the `users` row that outcome guarantees does
    /// NOT exist, so for a genuinely `Unmatched` recipient this field can
    /// never be `true` in practice. It is still implemented and unit-tested
    /// below (and the org-onboarding plan's OTHER "one notice, one account"
    /// paths — a person who already has a CoralSwarm account but has not
    /// connected a given platform to a specific org, D19/job 49 in that
    /// plan — reuse this identical selection rule against a candidate set
    /// where tier 1 genuinely does fire).
    pub(crate) roster_holds_verified_address: bool,
    /// D4 tier 2 — this account's own connection to the Slack workspace is
    /// the one this pass treats as having authorised the agent's presence
    /// here (see [`select_notice_account`]'s callers for how this is
    /// computed as a fact shared by every account's independent worker
    /// tick, not "whichever account happens to be running right now").
    pub(crate) authorized_this_channel: bool,
}

/// D4 (`docs/plans/org-onboarding-from-the-invite.md` §3 item 2): "One
/// email, one account, chosen deterministically" — prefer the org whose
/// roster already holds the recipient's verified address; otherwise the
/// account that authorised *this* channel; never more than one.
///
/// Pure and DB-free by construction: every fact this needs already sits on
/// [`NoticeAccountCandidate`], computed by the caller. Deterministic given
/// the same candidate slice — every one of possibly several accounts'
/// independent worker ticks that discovers the SAME recipient converges on
/// the SAME answer, rather than "whichever job's `claim_notice_slot` call
/// happens to reach Postgres first" (a race the persisted, per-recipient
/// cooldown — migration `0230` — still makes SAFE, never a double send, but
/// not policy-driven: without this function, the "winner" would be
/// whichever account's tick scheduling happened to run earliest, not the
/// account D4 actually prefers).
///
/// Ties within a tier resolve to the FIRST matching candidate in
/// `candidates`' own order — callers that care about a specific tie-break
/// (e.g. more than one sibling account's roster somehow holds the address)
/// must order the slice accordingly; this function does not re-sort.
///
/// `None` only when `candidates` is empty. Every real call site includes at
/// least the channel-authorising account, so this is a defensive floor, not
/// a reachable outcome in practice — a caller must never guess an account
/// when this returns `None`, exactly like every other "ask, don't guess"
/// refusal in this feature (`discover_unmatched_recipients`'s own doc
/// comment states the identical discipline for a roster it cannot trust).
pub(crate) fn select_notice_account(candidates: &[NoticeAccountCandidate]) -> Option<Uuid> {
    candidates
        .iter()
        .find(|c| c.roster_holds_verified_address)
        .or_else(|| candidates.iter().find(|c| c.authorized_this_channel))
        .map(|c| c.account_id)
}

#[cfg(test)]
mod select_notice_account_tests {
    //! Pure, DB-and-network-free tests — [`select_notice_account`] is a
    //! plain function over already-computed booleans, same reasoning as
    //! `services::unmatched_recipient_email`'s own `copy_tests` module.

    use super::{select_notice_account, NoticeAccountCandidate};
    use uuid::Uuid;

    fn candidate(roster_holds: bool, authorized: bool) -> NoticeAccountCandidate {
        NoticeAccountCandidate {
            account_id: Uuid::new_v4(),
            roster_holds_verified_address: roster_holds,
            authorized_this_channel: authorized,
        }
    }

    /// FAILS WITHOUT THE FIX: an empty candidate slice must never be guessed
    /// at — `None` is the only honest answer (see this function's own doc
    /// comment).
    #[test]
    fn empty_candidates_yields_none() {
        assert_eq!(select_notice_account(&[]), None);
    }

    /// The ordinary case for THIS module's own call site (tier 1 is
    /// unreachable for a genuinely `Unmatched` recipient — see
    /// [`NoticeAccountCandidate::roster_holds_verified_address`]'s own doc
    /// comment): with nobody's roster holding the address, the account that
    /// authorised the channel wins.
    #[test]
    fn falls_back_to_the_authorizing_account_when_no_roster_matches() {
        let authorized = candidate(false, true);
        let sibling = candidate(false, false);
        let candidates = [sibling, authorized];
        assert_eq!(select_notice_account(&candidates), Some(candidates[1].account_id));
    }

    /// FAILS WITHOUT THE FIX: D4's stated preference order is tier 1 BEFORE
    /// tier 2 — a sibling account whose roster already holds the verified
    /// address wins even though it did not authorise this channel.
    #[test]
    fn roster_match_outranks_the_authorizing_account() {
        let authorized = candidate(false, true);
        let roster_match = candidate(true, false);
        let candidates = [authorized, roster_match];
        assert_eq!(
            select_notice_account(&candidates),
            Some(candidates[1].account_id),
            "tier 1 (roster already holds the verified address) must win over tier 2 \
             (authorised this channel), per D4's stated order"
        );
    }

    /// Neither tier matches (e.g. a sibling account list resolved without
    /// ever including the actual authorising account, a defensive
    /// caller-side bug) — `None`, never a guess.
    #[test]
    fn neither_tier_matching_yields_none() {
        let candidates = [candidate(false, false), candidate(false, false)];
        assert_eq!(select_notice_account(&candidates), None);
    }

    /// Two candidates both claim a roster match (should not happen in
    /// practice — a verified email belongs to at most one org's roster —
    /// but this pins the documented, deterministic tie-break: the FIRST
    /// match in the given order, not an arbitrary or a "last write wins"
    /// pick).
    #[test]
    fn a_tie_within_a_tier_resolves_to_the_first_candidate_in_order() {
        let first = candidate(true, false);
        let second = candidate(true, false);
        let candidates = [first, second];
        assert_eq!(select_notice_account(&candidates), Some(candidates[0].account_id));
    }

    /// A single candidate that matches neither tier (the channel-
    /// authorising account itself, somehow not marked as such — e.g. a
    /// caller bug) still yields `None` rather than defaulting to "the only
    /// one in the list", which would silently reintroduce a guess.
    #[test]
    fn a_single_non_matching_candidate_still_yields_none() {
        assert_eq!(select_notice_account(&[candidate(false, false)]), None);
    }
}

#[cfg(test)]
mod discover_unmatched_recipients_pg_tests {
    //! DB-and-HTTP-backed proof that THIS module's own filter — not the
    //! classifier it calls — only ever collects `Unmatched` addresses.
    //!
    //! Mirrors `backend/tests/chat_per_user_e2e_pg.rs`'s mock-Slack pattern
    //! exactly (a real `axum` server on an ephemeral loopback port, reached
    //! via the `SLACK_API_BASE` seam — §11 correction 7) rather than
    //! asserting against data handed to the function directly, so what is
    //! actually proven is "this module asks Slack the right two questions and
    //! survives the answers it gets", not merely "this match arm compiles".
    //!
    //! Gated on `ORGT_TEST_BASE_URL`, migrating the CONTROL tree (same schema
    //! every real caller of `discover_unmatched_recipients` runs against on
    //! the consumer plane). Requires an EXPLICIT `cargo test --lib`
    //! invocation: the plain workspace `cargo test` job has no Postgres
    //! service and silently skips every test here. `SLACK_API_BASE` is
    //! process-global (same requirement `chat_per_user_e2e_pg.rs` states for
    //! the identical reason), but within THIS `--lib` binary the two tests
    //! below no longer depend on a hand-remembered `--test-threads=1`: both
    //! serialize on `crate::connectors::slack::SLACK_API_BASE_ENV_LOCK`
    //! (CodeRabbit round 1, #1005, Thread 24), same as
    //! `history_backfill.rs`'s three tests that mutate the identical var.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib discover_unmatched_recipients_pg_tests \
    //!     -- --test-threads=1

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::discover_unmatched_recipients;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A minimal, owned account — `accounts.owner_user_id` is `NOT NULL`, so
    /// a throwaway owner user comes first, same order every other pg suite
    /// in this crate seeds them in.
    async fn seed_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Same mock-Slack shape as `chat_per_user_e2e_pg.rs::spawn_mock_slack`,
    /// extended with `conversations.members` (that suite only needed
    /// `users.info` — this module's own roster walk needs both). One fixed
    /// member id, `U1`, whose `users.info` behaviour is configurable per
    /// test:`with_email = None` reproduces Slack's exact "ok:true, email
    /// field simply absent" shape a `users:read.email`-less bot token
    /// produces; `Some(addr)` reproduces a normal, real profile — CS-426:
    /// carrying `is_restricted`/`is_ultra_restricted`/`team_id` so `U1`
    /// classifies `FullMember` (see `connectors::slack::MembershipClass`)
    /// and this suite's existing "collected" assertions are unaffected by
    /// the guest/Slack-Connect filter; `team_id` is hardcoded to
    /// `"T_CONFIRMED"`, the one workspace id this mock's own caller
    /// (`unmatched_member_with_confirmed_scope_is_collected_pg`) uses.
    async fn spawn_mock_slack(with_email: Option<&'static str>) -> String {
        use axum::{routing::get, Router};

        async fn members() -> axum::Json<serde_json::Value> {
            axum::Json(serde_json::json!({"ok": true, "members": ["U1"]}))
        }

        let users_info = move || async move {
            let body = match with_email {
                Some(email) => serde_json::json!({
                    "ok": true,
                    "user": {
                        "id": "U1",
                        "is_restricted": false, "is_ultra_restricted": false, "team_id": "T_CONFIRMED",
                        "profile": {"email": email, "display_name": "New Person"}
                    }
                }),
                None => serde_json::json!({
                    "ok": true,
                    "user": {"id": "U1", "profile": {"display_name": "No Email"}}
                }),
            };
            axum::Json(body)
        };

        let app = Router::new()
            .route("/conversations.members", get(members))
            .route("/users.info", get(users_info));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock slack listener");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    /// FAILS WITHOUT THE FIX: a hypothetical regression that widened this
    /// module's match arm to also collect `SlackIdentity::Unavailable(_)` —
    /// exactly the class of bug PR 1 exists to prevent one layer down, and
    /// the one this PR's own brief calls out by name. A workspace with NO
    /// `oauth_tokens` row at all (so `slack_granted_scope` returns `None`,
    /// the confirmed-missing-or-unknown case) resolves its one member to
    /// `Unavailable` — proving **a workspace with no confirmable
    /// `users:read.email` grant sends zero emails.**
    #[tokio::test]
    async fn scope_broken_workspace_sends_zero_emails_pg() {
        let Some((admin_conn, pool)) = setup("cs_test_unmatched_email_scope_broken").await else {
            eprintln!("skipping scope_broken_workspace_sends_zero_emails_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;

        let base = spawn_mock_slack(None).await;
        // CodeRabbit round 1 (#1005, Thread 24): held for this test's whole
        // body, alongside every other in-process test that mutates this
        // SAME process-global var (see
        // `crate::connectors::slack::SLACK_API_BASE_ENV_LOCK`'s own doc).
        let _slack_api_base_lock =
            crate::connectors::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _slack_api_base_guard = crate::connectors::slack::SlackApiBaseTestGuard::set(&base);

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let result = discover_unmatched_recipients(
            &pool,
            &pool,
            &http,
            account_id,
            "T_SCOPE_BROKEN",
            "xoxb-fake",
            "C123",
        )
        .await;

        drop(_slack_api_base_guard);
        drop(_slack_api_base_lock);

        assert!(
            result.is_err(),
            "a workspace with no confirmable users:read.email grant must fail closed \
             (Unavailable), never resolve to a sendable Unmatched list — got {result:?}"
        );

        teardown(admin_conn, pool, "cs_test_unmatched_email_scope_broken").await;
    }

    /// The positive case, same mock family: a member whose `users.info`
    /// response carries a real, previously-unseen email and no
    /// `connector_identities`/`users` match at all resolves `Unmatched` and
    /// IS collected — proving the filter's happy path isn't accidentally
    /// empty too (a test that only ever asserts `Err`/`is_empty` on the
    /// negative case would pass just as well with the whole function
    /// stubbed to always fail).
    #[tokio::test]
    async fn unmatched_member_with_confirmed_scope_is_collected_pg() {
        let Some((admin_conn, pool)) = setup("cs_test_unmatched_email_happy_path").await else {
            eprintln!(
                "skipping unmatched_member_with_confirmed_scope_is_collected_pg — no ORGT_TEST_BASE_URL"
            );
            return;
        };
        let account_id = seed_account(&pool).await;

        // A confirmed `users:read.email` grant on file for this workspace —
        // `slack_granted_scope` reads exactly this row.
        sqlx::query(
            "INSERT INTO oauth_tokens \
             (account_id, provider, scope, access_token, provider_workspace_id, owner_user_id) \
             VALUES ($1, 'slack', 'channels:read users:read.email', '\\x00'::bytea, $2, NULL)",
        )
        .bind(account_id)
        .bind("T_CONFIRMED")
        .execute(&pool)
        .await
        .unwrap();

        let base = spawn_mock_slack(Some("new-person@example.com")).await;
        // CodeRabbit round 1 (#1005, Thread 24): see
        // `crate::connectors::slack::SLACK_API_BASE_ENV_LOCK`'s own doc.
        let _slack_api_base_lock =
            crate::connectors::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _slack_api_base_guard = crate::connectors::slack::SlackApiBaseTestGuard::set(&base);

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let result = discover_unmatched_recipients(
            &pool,
            &pool,
            &http,
            account_id,
            "T_CONFIRMED",
            "xoxb-fake",
            "C123",
        )
        .await;

        drop(_slack_api_base_guard);
        drop(_slack_api_base_lock);

        let outcome = result.expect("a confirmed-scope workspace must resolve, not error");
        assert_eq!(
            outcome.recipients.iter().map(|r| r.email.as_str()).collect::<Vec<_>>(),
            vec!["new-person@example.com"],
            "an Unmatched member with a real email must be the exactly-one collected recipient"
        );
        assert_eq!(outcome.guests_skipped, 0);
        assert_eq!(outcome.slack_connect_skipped, 0);
        assert_eq!(outcome.unavailable_membership_skipped, 0);

        teardown(admin_conn, pool, "cs_test_unmatched_email_happy_path").await;
    }

    // ── D19/CS-440 — the Matched-not-member audience ────────────────────

    /// An ORDINARY-org account (`clerk_org_id` set) — `seed_account` above
    /// seeds a PERSONAL account, which is wrong for these tests: D19's
    /// audience collects nothing at all on a personal plane (see the
    /// dedicated test below), so every other test needs a real org.
    async fn seed_org_account(pool: &PgPool, clerk_org_id: &str) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(owner)
            .bind(clerk_org_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_user(pool: &PgPool, email: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// PR F (seat lifecycle, migration `0176`) requires every `account_
    /// members` writer to pass `seat_tier_id` explicitly — same lookup
    /// `routes::org_join_requests::pg_tests::seed_org` uses for the
    /// identical reason.
    async fn seed_membership(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
            .fetch_one(pool)
            .await
            .expect("standard seat tier must be seeded (0165)");
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(tier_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_confirmed_scope(pool: &PgPool, account_id: Uuid, workspace_id: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens \
             (account_id, provider, scope, access_token, provider_workspace_id, owner_user_id) \
             VALUES ($1, 'slack', 'channels:read users:read.email', '\\x00'::bytea, $2, NULL)",
        )
        .bind(account_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A mock Slack roster of exactly three members, `U1`/`U2`/`U3`, each
    /// full workspace members (no guest flags) so this module's own D2/D21
    /// filter never interferes with the D19 assertions these tests make —
    /// that filter has its own dedicated suite below
    /// (`guest_slack_connect_filter_pg_tests`).
    /// `workspace_id` becomes BOTH the URL this mock answers under AND the
    /// `team_id` every member's `users.info` profile carries — it MUST be
    /// UNIQUE per test (never reused across two different `#[tokio::test]`
    /// functions in this same suite), for a reason with nothing to do with
    /// HTTP routing: [`super::super::slack::verified_identity_cache`] is a
    /// PROCESS-GLOBAL, in-memory cache keyed `(workspace_id,
    /// external_user_id)`, shared by every test in this binary regardless
    /// of which scratch Postgres database each one otherwise uses. Two
    /// tests reusing the SAME `(workspace_id, "U1")` pair would let the
    /// SECOND test's lookup silently return the FIRST test's cached
    /// `user_id` — a real cross-test contamination bug this suite hit
    /// once already (a shared `"T_CS440"` constant across two tests) before
    /// this parameter existed. Every OTHER pre-existing mock-Slack helper in
    /// this file already follows this same "one workspace id per test"
    /// discipline (`"T_GUEST1"`, `"T_GUEST2"`, `"T_HOST"`, `"T_FULL"`,
    /// `"T_NARROW"`, `"T_CONFIRMED"`, `"T_SCOPE_BROKEN"` — no two tests
    /// share one); this helper now enforces it structurally instead of by
    /// convention alone.
    async fn spawn_mock_slack_three(
        workspace_id: &'static str,
        u1_email: &'static str,
        u2_email: &'static str,
        u3_email: &'static str,
    ) -> String {
        use axum::{
            extract::{Query, State},
            routing::get,
            Json, Router,
        };
        use std::collections::HashMap;

        async fn members() -> Json<serde_json::Value> {
            Json(serde_json::json!({"ok": true, "members": ["U1", "U2", "U3"]}))
        }
        async fn users_info(
            State((team_id, emails)): State<(&'static str, [&'static str; 3])>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<serde_json::Value> {
            let user = q.get("user").map(String::as_str).unwrap_or("");
            let email = match user {
                "U1" => emails[0],
                "U2" => emails[1],
                "U3" => emails[2],
                _ => return Json(serde_json::json!({"ok": false, "error": "user_not_found"})),
            };
            Json(serde_json::json!({
                "ok": true,
                "user": {
                    "id": user,
                    "is_restricted": false, "is_ultra_restricted": false, "team_id": team_id,
                    "profile": {"email": email, "display_name": user}
                }
            }))
        }

        let state = (workspace_id, [u1_email, u2_email, u3_email]);
        let app = Router::new()
            .route("/conversations.members", get(members))
            .route("/users.info", get(users_info))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock slack listener");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    /// FAILS WITHOUT THE FIX: the whole audience predicate, on one shared
    /// roster walk — `U1` (an existing CoralSwarm user, NOT a member of
    /// this account) lands in `matched_not_members`; `U2` (an existing
    /// CoralSwarm user who IS already a member) is counted in
    /// `matched_member_skipped` and appears in neither list; `U3` (no
    /// CoralSwarm account at all) is the one and only `Unmatched` recipient.
    /// Never a second roster fetch — one `discover_unmatched_recipients`
    /// call classifies all three.
    #[tokio::test]
    async fn one_walk_classifies_all_three_audiences_pg() {
        let db = "cs440_discover_three_audiences";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping one_walk_classifies_all_three_audiences_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs440_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        // A workspace id used by NO other test in this suite — see
        // `spawn_mock_slack_three`'s own doc comment for why that matters
        // (a process-global identity cache keyed on this value).
        let workspace_id = "T_CS440_THREE_AUDIENCES";
        seed_confirmed_scope(&pool, account_id, workspace_id).await;

        let not_member_email = "not-a-member@example.com";
        let member_email = "already-a-member@example.com";
        let unmatched_email = "brand-new@example.com";
        let not_member_user = seed_user(&pool, not_member_email).await;
        let member_user = seed_user(&pool, member_email).await;
        seed_membership(&pool, account_id, member_user).await;

        let base = spawn_mock_slack_three(workspace_id, not_member_email, member_email, unmatched_email).await;
        let _slack_api_base_lock =
            crate::connectors::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _slack_api_base_guard = crate::connectors::slack::SlackApiBaseTestGuard::set(&base);

        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build().unwrap();
        let result =
            discover_unmatched_recipients(&pool, &pool, &http, account_id, workspace_id, "xoxb-fake", "C1").await;

        drop(_slack_api_base_guard);
        drop(_slack_api_base_lock);

        let outcome = result.expect("a confirmed-scope workspace must resolve, not error");

        teardown(admin_conn, pool, db).await;

        assert_eq!(
            outcome.recipients.iter().map(|r| r.email.as_str()).collect::<Vec<_>>(),
            vec![unmatched_email],
            "only the genuinely Unmatched member is a signup-nudge recipient"
        );
        assert_eq!(
            outcome.matched_not_members.iter().map(|m| m.email.as_str()).collect::<Vec<_>>(),
            vec![not_member_email],
            "only the Matched-but-not-member goes into the join-this-org audience"
        );
        assert_eq!(
            outcome.matched_not_members[0].user_id, not_member_user,
            "the collected user_id must be the not-member's own users.id"
        );
        assert_eq!(outcome.matched_member_skipped, 1, "the already-a-member must be counted, not silently dropped");
        assert_eq!(outcome.matched_not_member_found, 1);
    }

    /// FAILS WITHOUT THE FIX (D19: "personal orgs never receive join
    /// requests"): the IDENTICAL roster as above, but the account is a
    /// PERSONAL org — `matched_not_members` must be empty even though `U1`
    /// classifies exactly the same way (Matched, not a member).
    #[tokio::test]
    async fn a_personal_org_collects_nothing_for_the_join_audience_pg() {
        let db = "cs440_discover_personal_org";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_personal_org_collects_nothing_for_the_join_audience_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs440_personal_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        // A DIFFERENT workspace id than `one_walk_classifies_all_three_
        // audiences_pg` uses — see `spawn_mock_slack_three`'s own doc
        // comment.
        let workspace_id = "T_CS440_PERSONAL_ORG";
        seed_confirmed_scope(&pool, account_id, workspace_id).await;
        sqlx::query("INSERT INTO personal_orgs(clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&clerk_org_id)
            .bind(format!("clerk_user_cs440_{}", Uuid::new_v4().simple()))
            .execute(&pool)
            .await
            .unwrap();

        let not_member_email = "not-a-member@example.com";
        let member_email = "already-a-member@example.com";
        let unmatched_email = "brand-new@example.com";
        seed_user(&pool, not_member_email).await;
        let member_user = seed_user(&pool, member_email).await;
        seed_membership(&pool, account_id, member_user).await;

        let base = spawn_mock_slack_three(workspace_id, not_member_email, member_email, unmatched_email).await;
        let _slack_api_base_lock =
            crate::connectors::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _slack_api_base_guard = crate::connectors::slack::SlackApiBaseTestGuard::set(&base);

        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build().unwrap();
        let result =
            discover_unmatched_recipients(&pool, &pool, &http, account_id, workspace_id, "xoxb-fake", "C1").await;

        drop(_slack_api_base_guard);
        drop(_slack_api_base_lock);

        let outcome = result.expect("a confirmed-scope workspace must resolve, not error");

        teardown(admin_conn, pool, db).await;

        assert!(
            outcome.matched_not_members.is_empty(),
            "a personal org must collect nothing for the join-this-org audience: {:?}",
            outcome.matched_not_members
        );
        assert_eq!(outcome.matched_not_member_found, 0);
        assert_eq!(
            outcome.matched_member_skipped, 0,
            "a personal org never even reaches the membership check for this audience"
        );
        // The signup-nudge audience is UNCHANGED by D19 — still exactly the
        // one genuinely Unmatched member.
        assert_eq!(
            outcome.recipients.iter().map(|r| r.email.as_str()).collect::<Vec<_>>(),
            vec![unmatched_email]
        );
    }
}

#[cfg(test)]
mod guest_slack_connect_filter_pg_tests {
    //! CS-426 (`docs/plans/org-onboarding-from-the-invite.md` §3 item 1, D2,
    //! D21) — the guest/Slack-Connect domain filter inside
    //! [`discover_unmatched_recipients`]. Same mock-Slack-over-real-HTTP
    //! pattern as [`super::discover_unmatched_recipients_pg_tests`] (a real
    //! `axum` server on an ephemeral loopback port, reached via
    //! `SLACK_API_BASE`) plus a real Postgres for the `oauth_tokens` anchor
    //! lookup — extended to route `users.info` by the queried Slack id, so
    //! one mock server can answer BOTH the roster member under test (`U1`)
    //! and the authorising member [`connectors::slack::resolve_anchor_
    //! domain`] looks up (`UADMIN`) with independent profiles.
    //!
    //! Every assertion here is on the DECISION —
    //! `DiscoveryOutcome.recipients`/`.guests_skipped`/`.slack_connect_
    //! skipped`/`.unavailable_membership_skipped` — never on log output or
    //! call order, per the story's own instruction: "Assert the decision,
    //! not the log position."
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib guest_slack_connect_filter_pg_tests \
    //!     -- --test-threads=1

    use std::collections::HashMap;
    use std::sync::Arc;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::{discover_unmatched_recipients, DiscoveryOutcome};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A minimal, owned account — same seeding order every sibling pg suite
    /// in this crate uses (`accounts.owner_user_id` is `NOT NULL`).
    async fn seed_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Seeds the account-wide `oauth_tokens` row `resolve_anchor_domain`
    /// reads — `provider_user_id = "UADMIN"` is the authorising member's
    /// Slack id (`routes::slack::oauth_callback`'s `authed_user.id`, in
    /// production), the same id this test's mock answers with the anchor
    /// profile.
    async fn seed_oauth_token(pool: &PgPool, account_id: Uuid, workspace_id: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens \
             (account_id, provider, scope, access_token, provider_workspace_id, provider_user_id, owner_user_id) \
             VALUES ($1, 'slack', 'channels:read users:read.email', '\\x00'::bytea, $2, 'UADMIN', NULL)",
        )
        .bind(account_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A mock Slack serving `conversations.members` (fixed: `["U1"]`) and a
    /// `users.info` that answers per queried `?user=` id from `users` —
    /// real Slack answers two different ids' worth of profile independently
    /// the same way. An id with no entry answers `user_not_found`, matching
    /// Slack's own shape for an id nobody recognises.
    async fn spawn_mock_slack(users: HashMap<&'static str, serde_json::Value>) -> String {
        use axum::{extract::Query, extract::State, routing::get, Json, Router};

        async fn members() -> Json<serde_json::Value> {
            Json(serde_json::json!({"ok": true, "members": ["U1"]}))
        }
        async fn users_info(
            State(users): State<Arc<HashMap<&'static str, serde_json::Value>>>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<serde_json::Value> {
            let user = q.get("user").map(String::as_str).unwrap_or("");
            let body = users
                .get(user)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({"ok": false, "error": "user_not_found"}));
            Json(body)
        }

        let state = Arc::new(users);
        let app = Router::new()
            .route("/conversations.members", get(members))
            .route("/users.info", get(users_info))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock slack listener");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    fn admin_profile(email: &'static str) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "user": {"id": "UADMIN", "profile": {"email": email, "display_name": "Admin"}}
        })
    }

    /// Runs one roster walk against a freshly spawned mock, under the
    /// SAME `SLACK_API_BASE_ENV_LOCK` discipline every other in-process
    /// test mutating that process-global var uses (CodeRabbit round 1,
    /// #1005, Thread 24 — see `crate::connectors::slack::
    /// SLACK_API_BASE_ENV_LOCK`'s own doc comment).
    async fn run_walk(
        pool: &PgPool,
        account_id: Uuid,
        workspace_id: &str,
        users: HashMap<&'static str, serde_json::Value>,
    ) -> Result<DiscoveryOutcome, String> {
        let base = spawn_mock_slack(users).await;
        let _lock =
            crate::connectors::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = crate::connectors::slack::SlackApiBaseTestGuard::set(&base);

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let result =
            discover_unmatched_recipients(pool, pool, &http, account_id, workspace_id, "xoxb-fake", "C123")
                .await;

        drop(_guard);
        drop(_lock);
        result
    }

    /// A full workspace member (no guest flag, `team_id` matches) is
    /// invited exactly as before this PR — the anchor domain is irrelevant
    /// to them.
    #[tokio::test]
    async fn full_member_is_invited_pg() {
        let db = "cs_test_guest_filter_full_member";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping full_member_is_invited_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        seed_oauth_token(&pool, account_id, "T_FULL").await;

        let mut users = HashMap::new();
        users.insert("UADMIN", admin_profile("person@example.com"));
        users.insert(
            "U1",
            serde_json::json!({
                "ok": true,
                "user": {
                    "id": "U1",
                    "is_restricted": false, "is_ultra_restricted": false, "team_id": "T_FULL",
                    "profile": {"email": "colleague@other-domain.com", "display_name": "Colleague"}
                }
            }),
        );

        let outcome = run_walk(&pool, account_id, "T_FULL", users).await.expect("must resolve");
        assert_eq!(
            outcome.recipients.iter().map(|r| r.email.as_str()).collect::<Vec<_>>(),
            vec!["colleague@other-domain.com"],
            "a full member must be invited even when their domain does NOT match the anchor"
        );
        assert_eq!(outcome.guests_skipped, 0);
        assert_eq!(outcome.slack_connect_skipped, 0);
        assert_eq!(outcome.unavailable_membership_skipped, 0);

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX: a single-channel guest (`is_ultra_restricted`)
    /// whose email domain does NOT match the anchor (the authorising
    /// member's domain, since no verified org domain exists yet) must NOT
    /// be invited — this is D2's whole reason for existing: without it, a
    /// client's staff on a guest account would be emailed that their
    /// conversations are landing in a customer's CoralSwarm.
    #[tokio::test]
    async fn single_channel_guest_with_foreign_domain_is_not_invited_pg() {
        let db = "cs_test_guest_filter_ultra_restricted_foreign";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!(
                "skipping single_channel_guest_with_foreign_domain_is_not_invited_pg — no \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let account_id = seed_account(&pool).await;
        seed_oauth_token(&pool, account_id, "T_GUEST1").await;

        let mut users = HashMap::new();
        users.insert("UADMIN", admin_profile("person@example.com"));
        users.insert(
            "U1",
            serde_json::json!({
                "ok": true,
                "user": {
                    "id": "U1",
                    "is_restricted": false, "is_ultra_restricted": true, "team_id": "T_GUEST1",
                    "profile": {"email": "contractor@client-domain.com", "display_name": "Contractor"}
                }
            }),
        );

        let outcome = run_walk(&pool, account_id, "T_GUEST1", users).await.expect("must resolve");
        assert!(
            outcome.recipients.is_empty(),
            "a single-channel guest on a foreign domain must not be invited — got {:?}",
            outcome.recipients
        );
        assert_eq!(outcome.guests_skipped, 1);
        assert_eq!(outcome.slack_connect_skipped, 0);
        assert_eq!(outcome.unavailable_membership_skipped, 0);

        teardown(admin_conn, pool, db).await;
    }

    /// The positive guest case: a multi-channel guest (`is_restricted`)
    /// whose email domain MATCHES the anchor (the authorising member's own
    /// domain, since no verified org domain exists yet) IS invited — the
    /// founder's stated exception: a colleague on a guest account must not
    /// be skipped just because Slack marked them restricted.
    #[tokio::test]
    async fn multi_channel_guest_with_anchor_domain_is_invited_pg() {
        let db = "cs_test_guest_filter_restricted_anchor";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!(
                "skipping multi_channel_guest_with_anchor_domain_is_invited_pg — no \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let account_id = seed_account(&pool).await;
        seed_oauth_token(&pool, account_id, "T_GUEST2").await;

        let mut users = HashMap::new();
        users.insert("UADMIN", admin_profile("person@example.com"));
        users.insert(
            "U1",
            serde_json::json!({
                "ok": true,
                "user": {
                    "id": "U1",
                    "is_restricted": true, "is_ultra_restricted": false, "team_id": "T_GUEST2",
                    "profile": {"email": "person@example.com", "display_name": "Colleague"}
                }
            }),
        );

        let outcome = run_walk(&pool, account_id, "T_GUEST2", users).await.expect("must resolve");
        assert_eq!(
            outcome.recipients.iter().map(|r| r.email.as_str()).collect::<Vec<_>>(),
            vec!["person@example.com"],
            "a multi-channel guest on the SAME domain as the anchor must be invited"
        );
        assert_eq!(outcome.guests_skipped, 0);
        assert_eq!(outcome.slack_connect_skipped, 0);
        assert_eq!(outcome.unavailable_membership_skipped, 0);

        teardown(admin_conn, pool, db).await;
    }

    /// A Slack Connect participant (`team_id` mismatch) from another
    /// workspace, on a foreign domain, must not be invited — the same rule
    /// as a guest, kept as a distinct classification and a distinct test
    /// because the plan calls it out as a separate case.
    #[tokio::test]
    async fn slack_connect_member_with_foreign_domain_is_not_invited_pg() {
        let db = "cs_test_guest_filter_slack_connect_foreign";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!(
                "skipping slack_connect_member_with_foreign_domain_is_not_invited_pg — no \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let account_id = seed_account(&pool).await;
        seed_oauth_token(&pool, account_id, "T_HOST").await;

        let mut users = HashMap::new();
        users.insert("UADMIN", admin_profile("person@example.com"));
        users.insert(
            "U1",
            serde_json::json!({
                "ok": true,
                "user": {
                    "id": "U1",
                    "is_restricted": true, "is_ultra_restricted": false, "team_id": "T_VISITOR",
                    "profile": {"email": "partner@other-company.com", "display_name": "Partner"}
                }
            }),
        );

        let outcome = run_walk(&pool, account_id, "T_HOST", users).await.expect("must resolve");
        assert!(
            outcome.recipients.is_empty(),
            "a Slack Connect participant on a foreign domain must not be invited — got {:?}",
            outcome.recipients
        );
        assert_eq!(outcome.guests_skipped, 0);
        assert_eq!(outcome.slack_connect_skipped, 1);
        assert_eq!(outcome.unavailable_membership_skipped, 0);

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX: `users.info` omitting
    /// `is_restricted`/`is_ultra_restricted`/`team_id` (a narrower
    /// `users:read` grant, or a Slack response-shape change) must classify
    /// `Unavailable` and NOT be invited — absence must never read as "full
    /// member". Domain matches the anchor exactly, so a regression that
    /// defaulted a missing flag to `false` (reading as `FullMember`) would
    /// make this test pass for the wrong reason if it only checked the
    /// domain; asserting `unavailable_membership_skipped == 1` pins the
    /// actual classification, not just the outcome.
    #[tokio::test]
    async fn missing_membership_fields_is_unavailable_and_not_invited_pg() {
        let db = "cs_test_guest_filter_fields_absent";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!(
                "skipping missing_membership_fields_is_unavailable_and_not_invited_pg — no \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let account_id = seed_account(&pool).await;
        seed_oauth_token(&pool, account_id, "T_NARROW").await;

        let mut users = HashMap::new();
        users.insert("UADMIN", admin_profile("person@example.com"));
        users.insert(
            "U1",
            serde_json::json!({
                "ok": true,
                "user": {
                    "id": "U1",
                    "profile": {"email": "person@example.com", "display_name": "No Guest Fields"}
                }
            }),
        );

        let outcome = run_walk(&pool, account_id, "T_NARROW", users).await.expect("must resolve");
        assert!(
            outcome.recipients.is_empty(),
            "a member whose users.info omits the membership fields must not be invited — got {:?}",
            outcome.recipients
        );
        assert_eq!(outcome.guests_skipped, 0);
        assert_eq!(outcome.slack_connect_skipped, 0);
        assert_eq!(outcome.unavailable_membership_skipped, 1);

        teardown(admin_conn, pool, db).await;
    }
}

#[cfg(test)]
mod guest_domain_admit_tests {
    use super::{clears_guest_domain_filter, MembershipClass};
    use crate::services::org_domains::guest_anchor_domains;

    #[test]
    fn a_guest_whose_address_matches_a_verified_org_domain_is_admitted() {
        let anchors = guest_anchor_domains(
            Ok(vec!["acme.com".into(), "acquired.com".into()]),
            Some("installer.test".into()),
        );
        assert!(clears_guest_domain_filter(
            MembershipClass::Guest,
            "pat@acquired.com",
            &anchors
        ));
        assert!(clears_guest_domain_filter(
            MembershipClass::SlackConnect,
            "pat@acme.com",
            &anchors
        ));
        assert!(
            !clears_guest_domain_filter(MembershipClass::Guest, "pat@installer.test", &anchors),
            "the connecting member's domain is not an anchor once a verified domain exists"
        );
    }

    #[test]
    fn a_guest_whose_address_does_not_match_a_verified_org_domain_is_not_admitted() {
        let anchors = guest_anchor_domains(Ok(vec!["acme.com".into()]), Some("installer.test".into()));
        assert!(!clears_guest_domain_filter(
            MembershipClass::Guest,
            "contractor@client.example",
            &anchors
        ));
        assert!(!clears_guest_domain_filter(
            MembershipClass::SlackConnect,
            "partner@other-company.com",
            &anchors
        ));
        assert!(
            !clears_guest_domain_filter(MembershipClass::Unavailable, "pat@acme.com", &anchors),
            "a missing membership classification never reads as admitted"
        );
        assert!(clears_guest_domain_filter(
            MembershipClass::FullMember,
            "colleague@other-domain.com",
            &anchors
        ));
    }

    #[test]
    fn the_connecting_members_domain_bootstraps_only_until_a_verified_domain_exists() {
        let bootstrap = guest_anchor_domains(Ok(vec![]), Some("installer.test".into()));
        assert!(clears_guest_domain_filter(
            MembershipClass::Guest,
            "colleague@installer.test",
            &bootstrap
        ));
        assert!(!clears_guest_domain_filter(
            MembershipClass::Guest,
            "contractor@client.example",
            &bootstrap
        ));

        let verified = guest_anchor_domains(Ok(vec!["acme.com".into()]), Some("installer.test".into()));
        assert!(clears_guest_domain_filter(MembershipClass::Guest, "pat@acme.com", &verified));
        assert!(!clears_guest_domain_filter(
            MembershipClass::Guest,
            "colleague@installer.test",
            &verified
        ));
    }
}

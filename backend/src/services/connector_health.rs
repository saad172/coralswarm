//! D16's connector-health record (CS-446, org-onboarding plan §3 item 16,
//! `docs/plans/org-onboarding-from-the-invite.md`) — the durable answer to
//! one sentence in D16's own build note:
//!
//! > *"if the member whose token backs the directory leaves, the whole org
//! > silently drops from rung 1 to rung 2 unless that raises a
//! > `connector_health` warning."*
//!
//! # This module is NOT the `connector_health` tracing field
//!
//! Read this first; the two share a name deliberately and are different
//! things. `connector_health = true` already exists as a **tracing field**
//! on a handful of `warn!` lines (`clerk_provision.rs`,
//! `connectors::google_chat::auto_seat_space_members`,
//! `connectors/github_*.rs`), required on every `graphs.is_empty()` fan-out
//! branch by the CI grep-guard `scripts/check_silent_drop_is_alertable.sh`.
//! That marker makes a condition visible to an **operator tailing logs**.
//!
//! D16's sentence is about an **admin**, who does not read our logs. A
//! `warn!` IS the "silently" that sentence objects to. So item 16 builds a
//! record (`connector_health`, migration 0243) and a surface
//! (`routes::connector_health` → the connectors page's own notice). The
//! tracing field keeps its meaning, its guard and its call sites exactly as
//! they are — nothing in this module extends a log line.
//!
//! # The one condition this records, and everything deliberately excluded
//!
//! **Recorded: [`CONDITION_BACKING_MEMBER_DEPARTED`].** The member whose
//! Google grant carried `directory.readonly` is no longer a member of this
//! org, and no remaining member's grant carries it — so the org that could
//! reach D16's rung 1 yesterday cannot today, and auto-add now decides on
//! the weaker rung 2. That is the exact event D16 names.
//!
//! **Not recorded — a directory fetch that failed this minute.**
//! `connectors::google_chat::fetch_domain_directory` returns `Err` for a
//! transport error, a 403, and (deliberately) for the HTTP-200-with-zero-
//! people case; `services::assertion_ladder::DirectorySignal::Unavailable`
//! already carries that, per person, per pass. It is *"could not ask"*, the
//! transient D16's own trap paragraph is about — *"could not ask ≠ does not
//! work here"*. A transient written into a durable table reads, an hour
//! later, exactly like a permanent loss, which is how the strongest rung's
//! best-known failure mode would become the alarm nobody believes. The
//! detection below therefore **never touches the wire**: it is derived
//! entirely from membership and stored grants, which is also why it can run
//! at the instant of departure rather than waiting for a sweep to notice.
//!
//! **Not recorded — a revoked or expired token, or a re-consent that
//! dropped the scope.** A different condition with a different remedy, and
//! one that is *already surfaced*: CS-431 computes `missing_scopes` off the
//! stored grant and `ReconsentNotice` renders it on the very page this
//! feature's own notice renders on, with the remedy ("Reconnect") attached
//! to the person who can press it. Recording it here too would put two
//! notices on one screen for one problem. Departure is the case that has no
//! Reconnect button, because the credential's owner is gone; that asymmetry
//! is the whole reason it needs its own record.
//!
//! **Not recorded — the backing member DISCONNECTING Google Chat while
//! staying in the org.** The org loses rung 1 just as completely, and this
//! was real residue CS-446 flagged rather than settled. The argument for
//! leaving it out: the person is still present and their own connectors
//! page already shows them disconnected with a Connect button, so the
//! condition is both visible and self-serviceable by its own cause — unlike
//! a departure.
//!
//! **CS-448 settled it, and the answer was still not to record it.** What
//! was missing was never the fact — both halves of it are already stored —
//! but the LINK between them: an admin could see "this member hasn't
//! connected a tool" (item 49, the members page) and could see join
//! requests being decided on the weaker rung, with nothing saying those are
//! the same fact. So the fix is a READ, [`directory_backing`], joining
//! `oauth_tokens`/`account_members` (who backs rung 1 now) to
//! `connector_audit_log` (who disconnected and stayed). No new `condition`
//! value, no new writer, no migration: a derived fact that can be recomputed
//! exactly from stored ones has no business being stored a third time, where
//! it could only go stale. See that function's own doc for the one claim it
//! deliberately refuses to make.
//!
//! # Absence of a capability is not loss of one
//!
//! The discipline that keeps this from firing on every org in the world.
//! Most orgs — every Slack-only org, which D16 says is the MVP path — have
//! never had a directory-capable member and never will. "No directory
//! today" is their steady state, not a degradation. So a row is **only ever
//! created by [`note_member_departure`]**, which knows both halves: that
//! the departing member *did* carry the grant, and that nobody left does.
//! No sweep, no read path and no reconciler may create one from "this
//! account has no directory-capable member" alone — see
//! [`refresh_open_degradations`], which can refresh and resolve but never
//! open.

use serde_json::{json, Value as Json};
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::scope::begin_account_scoped;

/// The `oauth_tokens.provider` literal for Google Chat
/// (`connectors::google::Surface::Chat::token_provider`), recorded on the
/// row so a surface can key its notice to the same connector tile a member
/// would act on.
pub(crate) const PROVIDER_GOOGLE_CHAT: &str = "google_chat";

/// D16's rung 1 — the org's own Workspace domain directory, reached with a
/// member's delegated `directory.readonly` grant. A capability of the
/// connector, not the connector itself: Chat capture can be perfectly
/// healthy while this one is gone.
pub(crate) const CAPABILITY_DOMAIN_DIRECTORY: &str = "domain_directory";

/// Google Chat space reader — the elected member whose grant reads a
/// `mode='fanout'` / container space (see `elect_space_reader`).
pub(crate) const CAPABILITY_CHAT_SPACE_READER: &str = "chat_space_reader";

/// Teams per-channel/chat push subscription owner (Phase G item 56).
/// [`on_backer_lost`] re-elects through [`elect_container_owner_hook`].
pub(crate) const CAPABILITY_TEAMS_CONTAINER_PUSH: &str = "teams_container_push";

/// Account-wide Slack bot that posts the cold-start message
/// (`oauth_tokens.provider = 'slack' AND owner_user_id IS NULL`). A member's
/// `slack_user` disconnect is NOT loss of this capability.
pub(crate) const CAPABILITY_SLACK_COLD_START_BOT: &str = "slack_cold_start_bot";

/// Member whose grant backed the capability left the org, and nobody left
/// can replace them. Migration 0243's original (and still primary) condition.
pub(crate) const CONDITION_BACKING_MEMBER_DEPARTED: &str = "backing_member_departed";

/// Last eligible backer's grant died (credential_invalid / definite revoke)
/// and nobody else can take over — a *shared* capability loss. Not a
/// member's own ReconsentNotice tile (founder Phase G decision 4 / CS-448).
pub(crate) const CONDITION_BACKING_CREDENTIAL_LOST: &str = "backing_credential_lost";

/// Re-election ran and found nobody eligible. Distinct from departure so an
/// admin can tell "someone left" from "election exhausted".
pub(crate) const CONDITION_NO_ELIGIBLE_BACKER: &str = "no_eligible_backer";

/// Per-member Teams transcript watch (Phase G item 51). Not the shared
/// channel doorbell. Recorded under [`CONDITION_NO_ELIGIBLE_BACKER`] because
/// `connector_health.condition` is a closed CHECK (0243 / 0254) and this is
/// a skip, not a new column: the tenant disabled Graph transcript access.
/// `instance_key` is one member, so one member's skip does not touch
/// another's row. Callers must not also fire [`on_backer_lost`] — this is
/// not a shared capability losing its last backer.
pub(crate) const CAPABILITY_TEAMS_TRANSCRIPTS: &str = "teams_transcripts";

/// Why [`on_backer_lost`] was invoked. Drives which kinds run and which
/// `condition` value is written when nobody can take over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackerLostCause {
    /// Member left the org (`account_members` already deleted).
    Departure,
    /// Member disconnected a connector but stayed in the org.
    /// `domain_directory` deliberately does NOT open a row for this cause —
    /// CS-448's derived [`directory_backing`] read covers it.
    Disconnect,
    /// A definite dead-credential signal marked `credential_invalid_at`.
    CredentialDeath,
}

/// Outcome of one [`on_backer_lost`] pass across every registered kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BackerLostReport {
    pub directory: Option<DepartureOutcome>,
    pub chat_spaces_reelected: usize,
    pub chat_spaces_orphaned: usize,
    pub slack_bot_opened: bool,
    pub teams_container_hook_invoked: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnectorHealthError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
}

/// One CURRENT degradation, as the admin surface reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Degradation {
    pub provider: String,
    pub capability: String,
    pub condition: String,
    pub backing_user_id: Option<Uuid>,
    pub backing_email: Option<String>,
    pub detail: Json,
    pub first_observed_at: chrono::DateTime<chrono::Utc>,
    pub last_observed_at: chrono::DateTime<chrono::Utc>,
}

/// What one member's stored Google Chat grant says about rung 1.
///
/// Three-way for the same reason
/// [`crate::services::assertion_ladder::DirectorySignal`] is: the answer
/// *"we cannot tell from what we stored"* is not either verdict, and
/// folding it into one of them is how a gate starts lying.
/// [`crate::connectors::google::chat_missing_scopes`] already returns
/// `None` rather than a guessed `Vec` for a stored scope string it cannot
/// trust (a blank one — the shape a Google refresh response that omits
/// `scope` would leave behind), and this preserves that refusal instead of
/// re-deciding it here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectoryCredential {
    /// A live `google_chat` grant that includes `directory.readonly`.
    /// Other Chat scopes may be missing — that is CS-431's re-consent
    /// notice, not a directory-backing question.
    Carries,
    /// A `google_chat` grant that does not include `directory.readonly`,
    /// or no `google_chat` grant at all.
    Lacks,
    /// A `google_chat` grant whose stored scope string cannot answer the
    /// question.
    Unknown,
}

impl DirectoryCredential {
    /// Classify one stored `oauth_tokens.scope` string. `None` = the member
    /// has no `google_chat` row at all.
    ///
    /// Directory backing is `directory.readonly`, read off the same
    /// [`crate::connectors::google::chat_missing_scopes`] set CS-431 uses
    /// so the required-scope list stays in one place. A grant that still
    /// carries that scope while missing a different Chat scope is not
    /// `Lacks`: folding Chat incompleteness into this answer is how a
    /// disconnect by someone else becomes a false directory-loss
    /// attribution. Unreadable grants stay `Unknown`.
    fn from_stored_scope(stored: Option<&str>) -> Self {
        let Some(scope) = stored else {
            return Self::Lacks;
        };
        match crate::connectors::google::chat_missing_scopes(scope) {
            None => Self::Unknown,
            Some(missing) if missing.iter().any(|s| s.ends_with("/directory.readonly")) => {
                Self::Lacks
            }
            Some(_) => Self::Carries,
        }
    }

    /// May this credential be BLAMED for a capability the org has lost?
    ///
    /// Only a definite `Carries` — `directory.readonly` present, regardless
    /// of whether every other Chat scope is too. See
    /// [`Self::might_still_back_the_directory`] for why the same `Unknown`
    /// answers the *other* question differently.
    fn definitely_backed_the_directory(self) -> bool {
        matches!(self, Self::Carries)
    }

    /// May this credential SPARE the alarm?
    ///
    /// `Carries` or `Unknown`. The asymmetry with
    /// [`Self::definitely_backed_the_directory`] is deliberate and is the
    /// point of modelling `Unknown` at all: the two questions are not the
    /// same question, so the same uncertainty does not answer them the same
    /// way.
    ///
    /// * To blame a departure we must be sure the leaver *did* carry it —
    ///   otherwise we invent a backer.
    /// * To raise the alarm we must be sure nobody *else* does — otherwise
    ///   we tell an admin their directory is gone while a grant we simply
    ///   could not parse is still serving it.
    ///
    /// Both readings resolve the same way: **an uncertainty never produces
    /// a row.** The alarm fires only when both halves are definite.
    fn might_still_back_the_directory(self) -> bool {
        matches!(self, Self::Carries | Self::Unknown)
    }
}

/// What [`note_member_departure`] concluded. Returned (rather than only
/// logged) so its caller and its tests can tell the three outcomes apart —
/// the difference between them is the whole of this item's detection logic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DepartureOutcome {
    /// This member's grant did not carry `directory.readonly` (or they had
    /// no Google Chat connection at all), so their leaving took nothing
    /// away. NOTHING is written — including no row for an org that simply
    /// never had a directory.
    NotTheDirectoryBacker,
    /// They did back it, but at least one CURRENT member still could — the
    /// org keeps rung 1. Any open degradation for this capability is
    /// RESOLVED (the `bool` says whether one actually was).
    StillCovered { resolved_existing: bool },
    /// They backed it and nobody left does: rung 1 is gone. Recorded, or
    /// re-observed if it was already open.
    Recorded { newly_opened: bool },
}

/// Every CURRENT member of `account_id` whose stored `google_chat` grant
/// might still serve D16's rung 1, and the same for the whole roster's
/// worth of unreadable ones.
///
/// **This is the ladder's rung-1 credential, resolved from state — never
/// from the wire.** `connectors::google_chat::elect_space_reader` is the
/// production shape this mirrors: a CURRENT `account_members` row joined to
/// a `google_chat` `oauth_tokens` row. The one thing added on top is the
/// scope filter, because election does not care about `directory.readonly`
/// and rung 1 is nothing without it. That filter asks only whether
/// `directory.readonly` is among the scopes
/// [`crate::connectors::google::chat_missing_scopes`] reports missing —
/// the SAME required-set CS-431's re-consent notice reads — not whether
/// every Chat scope is present. Chat incompleteness is a different
/// notice; collapsing it here invents a directory loss.
///
/// The `account_members` gate is load-bearing for the identical reason
/// `elect_space_reader`'s is (security review H1): offboarding deletes the
/// membership row and never touches `oauth_tokens`, so a departed member's
/// Chat grant outlives their membership. Counting it here would mean the
/// org "still has a directory" forever, backed by a credential nobody may
/// use — the exact silence this item exists to break.
async fn remaining_directory_credentials(
    control: &PgPool,
    account_id: Uuid,
    excluding_user_id: Uuid,
) -> Result<Vec<DirectoryCredential>, ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT ot.scope FROM oauth_tokens ot \
          WHERE ot.account_id = $1 AND ot.provider = $2 \
            AND ot.owner_user_id IS NOT NULL \
            AND ot.owner_user_id <> $3 \
            AND ot.credential_invalid_at IS NULL \
            AND EXISTS ( \
                  SELECT 1 FROM account_members am \
                  WHERE am.account_id = $1 AND am.user_id = ot.owner_user_id \
                )",
    )
    .bind(account_id)
    .bind(PROVIDER_GOOGLE_CHAT)
    .bind(excluding_user_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows
        .into_iter()
        .map(|(scope,)| DirectoryCredential::from_stored_scope(Some(&scope)))
        .collect())
}

/// One member's own stored Google Chat grant, read WITHOUT a membership
/// gate.
///
/// Deliberately ungated, and this is the one read in the module that is:
/// [`note_member_departure`] runs AFTER
/// `clerk_provision::remove_org_membership` has already deleted the
/// `account_members` row, so a membership-gated read would report every
/// departing member as having had no credential and this feature would
/// never fire at all. The account scope is still enforced
/// (`begin_account_scoped` + `account_id = $1`); what is dropped is only
/// the "is this person still a member" predicate, whose answer is known —
/// no — and is the reason we are here.
async fn stored_directory_credential(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<DirectoryCredential, ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT scope FROM oauth_tokens \
          WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3 \
          ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(PROVIDER_GOOGLE_CHAT)
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(DirectoryCredential::from_stored_scope(row.as_ref().map(|(s,)| s.as_str())))
}

/// **D16's detection.** Call this when a member leaves `account_id`, with
/// their `users.id` and the address on file at that moment.
///
/// Runs after the membership row is gone (see
/// [`stored_directory_credential`]) and is safe to call for EVERY
/// departure: an org with no Google connection, a Slack-only org, a
/// personal org and a member who never connected Chat all take the
/// [`DepartureOutcome::NotTheDirectoryBacker`] arm and write nothing at
/// all.
///
/// Idempotent by construction. A re-delivered Clerk `.deleted` webhook (or
/// any other double call) for the same departure re-runs the same three
/// reads and lands on the same `ON CONFLICT DO UPDATE`, which moves
/// `last_observed_at` and nothing else — never a second row, because
/// migration 0243's PRIMARY KEY is the condition, not the observation.
pub(crate) async fn note_member_departure(
    control: &PgPool,
    account_id: Uuid,
    departing_user_id: Uuid,
    departing_email: Option<&str>,
) -> Result<DepartureOutcome, ConnectorHealthError> {
    // 1. Did THIS member back rung 1? Anything short of a definite yes ends
    //    here, having written nothing: an org that never had a directory
    //    must not acquire a degradation row the first time anyone leaves.
    let departing = stored_directory_credential(control, account_id, departing_user_id).await?;
    if !departing.definitely_backed_the_directory() {
        return Ok(DepartureOutcome::NotTheDirectoryBacker);
    }

    // 2. Is anyone left who might still serve it? `might_still_back_the_
    //    directory` — not `definitely_` — on purpose; see its doc comment.
    let remaining = remaining_directory_credentials(control, account_id, departing_user_id).await?;
    if remaining.iter().copied().any(DirectoryCredential::might_still_back_the_directory) {
        let resolved_existing = resolve_condition(
            control,
            account_id,
            PROVIDER_GOOGLE_CHAT,
            CAPABILITY_DOMAIN_DIRECTORY,
            CONDITION_BACKING_MEMBER_DEPARTED,
            "",
        )
        .await?;
        return Ok(DepartureOutcome::StillCovered { resolved_existing });
    }

    // 3. The org could reach rung 1 and now cannot.
    let detail = json!({
        "rung": 1,
        "ladder_effect": "auto_add_now_decides_on_rung_2",
        "remaining_google_chat_connections": remaining.len(),
    });
    let newly_opened = upsert_degradation(
        control,
        account_id,
        PROVIDER_GOOGLE_CHAT,
        CAPABILITY_DOMAIN_DIRECTORY,
        CONDITION_BACKING_MEMBER_DEPARTED,
        "",
        Some(departing_user_id),
        departing_email,
        detail,
    )
    .await?;
    Ok(DepartureOutcome::Recorded { newly_opened })
}

/// Single entry for "someone who backed a shared capability is gone" —
/// departure, disconnect, and credential death (CS-456 / Phase G S6).
///
/// Tries re-election where the kind allows it, writes an audit-friendly
/// outcome on success, and opens a `connector_health` row ONLY when nobody
/// can take over. A sweep may resolve or refresh rows but never open them
/// (see [`refresh_open_degradations`]). Absence of a capability is not loss
/// of one — this function is only called when a known backer was lost.
///
/// `content_pool` is required for Chat space re-election (watched spaces live
/// on the content/tenant plane). Pass `None` when unavailable; Chat then
/// skips election and does not invent a health row from incomplete evidence.
///
/// `instance_key` defaults to `''` for account-scoped capabilities
/// (`domain_directory`, `slack_cold_start_bot`); Chat may open one row per
/// orphaned space id.
///
/// `user_id` is `Some` for a member-owned grant and `None` for an
/// account-wide credential (Slack bot). Directory / Chat / Teams paths
/// require a member and skip when it is `None`. `extra_detail` is merged
/// into the Slack loss row (e.g. `disconnected_by`) and is not used as
/// `backing_user_id`.
pub(crate) async fn on_backer_lost(
    control: &PgPool,
    content_pool: Option<&PgPool>,
    account_id: Uuid,
    user_id: Option<Uuid>,
    provider: &str,
    cause: BackerLostCause,
    backing_email: Option<&str>,
    extra_detail: Option<Json>,
) -> Result<BackerLostReport, ConnectorHealthError> {
    let mut report = BackerLostReport::default();

    // ── domain_directory ──────────────────────────────────────────────────
    // Departure: existing detection (`note_member_departure`).
    // Disconnect-while-staying: still the derived `directory_backing` read —
    // do NOT open a stored row (CS-448).
    // Credential death of a still-present sole directory backer: shared
    // capability loss → `backing_credential_lost` (not the member's own
    // ReconsentNotice tile).
    //
    // Directory bookkeeping is best-effort: a query failure must not cancel
    // Chat reader re-election below.
    if let Some(uid) = user_id {
        if provider == PROVIDER_GOOGLE_CHAT || provider.is_empty() {
            match cause {
                BackerLostCause::Departure => {
                    match note_member_departure(control, account_id, uid, backing_email).await {
                        Ok(o) => report.directory = Some(o),
                        Err(e) => tracing::warn!(
                            error = %e, %account_id, user_id = %uid,
                            "on_backer_lost: directory departure check failed — continuing with other kinds"
                        ),
                    }
                }
                BackerLostCause::Disconnect => {
                    // Derived read only — intentionally no write.
                }
                BackerLostCause::CredentialDeath => {
                    match note_directory_credential_death(control, account_id, uid, backing_email)
                        .await
                    {
                        Ok(o) => report.directory = Some(o),
                        Err(e) => tracing::warn!(
                            error = %e, %account_id, user_id = %uid,
                            "on_backer_lost: directory credential-death check failed — continuing with other kinds"
                        ),
                    }
                }
            }
        }
    }

    // ── chat_space_reader ─────────────────────────────────────────────────
    // Calls the existing `reelect_reader_for_departing_member` (which itself
    // calls `elect_space_reader`) — no Chat rewrite. Opens
    // `no_eligible_backer` per orphaned space when election returns nobody.
    if let Some(uid) = user_id {
        if (provider == PROVIDER_GOOGLE_CHAT || provider.is_empty())
            && matches!(
                cause,
                BackerLostCause::Departure
                    | BackerLostCause::Disconnect
                    | BackerLostCause::CredentialDeath
            )
        {
            if let Some(content) = content_pool {
                match crate::connectors::google_chat::reelect_reader_for_departing_member(
                    control, content, account_id, uid,
                )
                .await
                {
                    Ok((reelected, orphaned)) => {
                        report.chat_spaces_reelected = reelected;
                        report.chat_spaces_orphaned = orphaned;
                        if orphaned > 0 {
                            // One account-scoped row (instance_key '') — per-space
                            // keys would need the space id list reelect doesn't
                            // return today; wave-later refinement.
                            let condition = match cause {
                                BackerLostCause::Departure => CONDITION_BACKING_MEMBER_DEPARTED,
                                BackerLostCause::CredentialDeath => {
                                    CONDITION_BACKING_CREDENTIAL_LOST
                                }
                                BackerLostCause::Disconnect => CONDITION_NO_ELIGIBLE_BACKER,
                            };
                            let _ = upsert_degradation(
                                control,
                                account_id,
                                PROVIDER_GOOGLE_CHAT,
                                CAPABILITY_CHAT_SPACE_READER,
                                condition,
                                "",
                                Some(uid),
                                backing_email,
                                json!({
                                    "orphaned_spaces": orphaned,
                                    "reelected_spaces": reelected,
                                    "cause": format!("{cause:?}"),
                                }),
                            )
                            .await;
                        } else {
                            // Re-election left no orphaned spaces — close any
                            // open Chat reader rows even if the sweep cannot
                            // see the content pool later.
                            let _ = resolve_chat_space_reader_conditions(control, account_id).await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e, %account_id, user_id = %uid,
                            "on_backer_lost: chat space reader re-election failed — not inventing a \
                             health row from incomplete evidence"
                        );
                    }
                }
            }
        }
    }

    // ── teams_container_push ──────────────────────────────────────────────
    if let Some(uid) = user_id {
        if provider == "microsoft" || provider.is_empty() {
            report.teams_container_hook_invoked = true;
            elect_container_owner_hook(control, account_id, uid, cause).await;
        }
    }

    // ── slack_cold_start_bot ──────────────────────────────────────────────
    // Only the account-wide bot install (`owner_user_id IS NULL`). A member's
    // `slack_user` disconnect must NOT open this.
    if provider == "slack"
        && matches!(
            cause,
            BackerLostCause::Disconnect | BackerLostCause::CredentialDeath | BackerLostCause::Departure
        )
    {
        report.slack_bot_opened = note_slack_cold_start_bot_loss(
            control,
            account_id,
            user_id,
            cause,
            backing_email,
            extra_detail,
        )
        .await?;
    }

    Ok(report)
}

/// Re-elect every Teams doorbell `user_id` owns (S5b). Departure and
/// disconnect also drop that member's watcher rows only. Credential death
/// leaves the watch in place so a reconnect can be elected again.
/// Graph PATCH/delete, when a token is still in hand, is
/// [`crate::connectors::teams_doorbell::prepare_owner_departure`] and runs
/// before this hook on those paths.
async fn elect_container_owner_hook(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    cause: BackerLostCause,
) {
    let drop_own_watch = matches!(
        cause,
        BackerLostCause::Departure | BackerLostCause::Disconnect
    );
    if let Err(e) = crate::connectors::teams_doorbell::reelect_after_owner_lost(
        control,
        account_id,
        user_id,
        drop_own_watch,
    )
    .await
    {
        tracing::warn!(
            error = %e, %account_id, %user_id,
            "on_backer_lost: teams container re-election failed — not inventing extra health rows"
        );
    }
}

/// `connector_health.instance_key` for one Teams doorbell.
pub(crate) fn teams_container_instance_key(ms_tenant_id: &str, resource: &str) -> String {
    format!("{ms_tenant_id}\n{resource}")
}

/// Open `no_eligible_backer` for `teams_container_push` on every account that
/// still watches the container. Names the last owner. A successful later
/// election resolves the row via [`note_teams_push_recovered`].
pub(crate) async fn note_teams_push_lost(
    control: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
    reason: &str,
) -> Result<(), ConnectorHealthError> {
    let owner: Option<(Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
        "SELECT owner_account_id, owner_user_id FROM teams_container_subscriptions \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_optional(control)
    .await?;
    let (owner_account, owner_user) = owner.unwrap_or((None, None));
    let mut accounts: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT account_id FROM teams_container_watchers \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_all(control)
    .await?;
    if accounts.is_empty() {
        if let Some(account_id) = owner_account {
            accounts.push(account_id);
        }
    }
    let email: Option<String> = match owner_user {
        Some(uid) => sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
            .bind(uid)
            .fetch_optional(control)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let still_member = match (owner_account, owner_user) {
        (Some(account_id), Some(uid)) => {
            let found: Option<i32> = sqlx::query_scalar(
                "SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2 LIMIT 1",
            )
            .bind(account_id)
            .bind(uid)
            .fetch_optional(control)
            .await
            .ok()
            .flatten();
            found.is_some()
        }
        _ => false,
    };
    let remedy = if still_member {
        match email.as_deref() {
            Some(addr) if !addr.is_empty() => format!("{addr} can reconnect"),
            _ => "any member of this channel can connect".to_string(),
        }
    } else {
        "any member of this channel can connect".to_string()
    };
    let instance_key = teams_container_instance_key(ms_tenant_id, resource);
    for account_id in accounts {
        upsert_degradation(
            control,
            account_id,
            "microsoft",
            CAPABILITY_TEAMS_CONTAINER_PUSH,
            CONDITION_NO_ELIGIBLE_BACKER,
            &instance_key,
            owner_user,
            email.as_deref(),
            json!({
                "remedy": remedy,
                "instance_key": instance_key,
                "ms_tenant_id": ms_tenant_id,
                "resource": resource,
                "reason": reason,
                "last_owner_user_id": owner_user,
            }),
        )
        .await?;
    }
    Ok(())
}

fn transcript_instance_key(user_id: Option<Uuid>) -> String {
    match user_id {
        Some(id) => id.to_string(),
        None => "account-wide".to_string(),
    }
}

/// Record that Graph refused transcripts for this member
/// (`GraphAccessToTranscriptsDisabled`). Other members of the account are
/// not written.
pub(crate) async fn note_transcript_access_disabled(
    control: &PgPool,
    account_id: Uuid,
    user_id: Option<Uuid>,
) -> Result<(), ConnectorHealthError> {
    upsert_degradation(
        control,
        account_id,
        "microsoft",
        CAPABILITY_TEAMS_TRANSCRIPTS,
        CONDITION_NO_ELIGIBLE_BACKER,
        &transcript_instance_key(user_id),
        user_id,
        None,
        json!({
            "reason": "GraphAccessToTranscriptsDisabled",
            "remedy": "A Teams administrator can re-enable Graph API access to transcripts",
        }),
    )
    .await?;
    Ok(())
}

/// `true` when a transcript skip is already open for this member. A read
/// failure is `false` so a blip retries next tick rather than sticking forever;
/// the check still stops a retry storm once the row is written.
pub(crate) async fn transcript_access_disabled(
    control: &PgPool,
    account_id: Uuid,
    user_id: Option<Uuid>,
) -> bool {
    let key = transcript_instance_key(user_id);
    let mut tx = match begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "transcript skip lookup failed");
            return false;
        }
    };
    // Skip only for six hours after the last refusal. After that the next
    // tick calls Graph again, so an administrator who re-enables transcripts
    // is observed. A success resolves the row (`note_transcript_access_recovered`).
    let open: Option<bool> = match sqlx::query_scalar(
        "SELECT resolved_at IS NULL AND last_observed_at > now() - interval '6 hours' \
           FROM connector_health \
         WHERE account_id = $1 AND provider = 'microsoft' AND capability = $2 \
           AND condition = $3 AND instance_key = $4",
    )
    .bind(account_id)
    .bind(CAPABILITY_TEAMS_TRANSCRIPTS)
    .bind(CONDITION_NO_ELIGIBLE_BACKER)
    .bind(&key)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "transcript skip lookup failed");
            tx.rollback().await.ok();
            return false;
        }
    };
    tx.rollback().await.ok();
    open.unwrap_or(false)
}

/// Close the transcript-access skip after a successful read or subscription,
/// so a re-enabled tenant does not stay dark.
pub(crate) async fn note_transcript_access_recovered(
    control: &PgPool,
    account_id: Uuid,
    user_id: Option<Uuid>,
) -> Result<(), ConnectorHealthError> {
    resolve_condition(
        control,
        account_id,
        "microsoft",
        CAPABILITY_TEAMS_TRANSCRIPTS,
        CONDITION_NO_ELIGIBLE_BACKER,
        &transcript_instance_key(user_id),
    )
    .await?;
    Ok(())
}

/// Close an open `teams_container_push` / `no_eligible_backer` row for every
/// account that watches this container. A missing doorbell (watch ended)
/// resolves too — the sweep and a successful election both use this, and
/// neither opens a row.
pub(crate) async fn note_teams_push_recovered(
    control: &PgPool,
    ms_tenant_id: &str,
    resource: &str,
) -> Result<(), ConnectorHealthError> {
    let instance_key = teams_container_instance_key(ms_tenant_id, resource);
    let mut accounts: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT account_id FROM teams_container_watchers \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_all(control)
    .await?;
    let owner_account: Option<Uuid> = sqlx::query_scalar(
        "SELECT owner_account_id FROM teams_container_subscriptions \
         WHERE ms_tenant_id = $1 AND resource = $2",
    )
    .bind(ms_tenant_id)
    .bind(resource)
    .fetch_optional(control)
    .await?
    .flatten();
    if let Some(account_id) = owner_account {
        if !accounts.contains(&account_id) {
            accounts.push(account_id);
        }
    }
    for account_id in accounts {
        resolve_condition(
            control,
            account_id,
            "microsoft",
            CAPABILITY_TEAMS_CONTAINER_PUSH,
            CONDITION_NO_ELIGIBLE_BACKER,
            &instance_key,
        )
        .await?;
    }
    Ok(())
}

/// Credential death of a still-present directory backer, with nobody left
/// who might replace them → open `backing_credential_lost`. Same "both
/// halves" discipline as [`note_member_departure`], but the member is still
/// in the org (Reconnect exists for THEM; this row is for the SHARED
/// capability admins must act on — founder Phase G decision 4).
async fn note_directory_credential_death(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    backing_email: Option<&str>,
) -> Result<DepartureOutcome, ConnectorHealthError> {
    let departing = stored_directory_credential(control, account_id, user_id).await?;
    // After mark_credential_invalid the grant may still carry scopes; we
    // treat "this user's grant was the one that just died" as the caller
    // having already established they were a backer. Require they still
    // look like a directory carrier OR we cannot tell (Unknown) — if they
    // Lacks, they were never the directory backer.
    if matches!(departing, DirectoryCredential::Lacks) {
        return Ok(DepartureOutcome::NotTheDirectoryBacker);
    }
    let remaining = remaining_directory_credentials(control, account_id, user_id).await?;
    if remaining
        .iter()
        .copied()
        .any(DirectoryCredential::might_still_back_the_directory)
    {
        let resolved_existing = resolve_condition(
            control,
            account_id,
            PROVIDER_GOOGLE_CHAT,
            CAPABILITY_DOMAIN_DIRECTORY,
            CONDITION_BACKING_CREDENTIAL_LOST,
            "",
        )
        .await?;
        let _ = resolve_condition(
            control,
            account_id,
            PROVIDER_GOOGLE_CHAT,
            CAPABILITY_DOMAIN_DIRECTORY,
            CONDITION_BACKING_MEMBER_DEPARTED,
            "",
        )
        .await?;
        return Ok(DepartureOutcome::StillCovered { resolved_existing });
    }
    let newly_opened = upsert_degradation(
        control,
        account_id,
        PROVIDER_GOOGLE_CHAT,
        CAPABILITY_DOMAIN_DIRECTORY,
        CONDITION_BACKING_CREDENTIAL_LOST,
        "",
        Some(user_id),
        backing_email,
        json!({
            "rung": 1,
            "ladder_effect": "auto_add_now_decides_on_rung_2",
            "cause": "credential_death",
            "remaining_google_chat_connections": remaining.len(),
        }),
    )
    .await?;
    Ok(DepartureOutcome::Recorded { newly_opened })
}

/// Open a health row when the account-wide Slack bot install is gone and no
/// replacement `owner_user_id IS NULL` row remains. Never fires for a
/// member-scoped `slack` / `slack_user` token.
async fn note_slack_cold_start_bot_loss(
    control: &PgPool,
    account_id: Uuid,
    backing_user_id: Option<Uuid>,
    cause: BackerLostCause,
    backing_email: Option<&str>,
    extra_detail: Option<Json>,
) -> Result<bool, ConnectorHealthError> {
    let remaining = live_account_wide_slack_bots(control, account_id).await?;
    if remaining > 0 {
        // Someone else's bot install (or this one still live) — resolve any
        // open row, never open.
        resolve_slack_cold_start_conditions(control, account_id).await?;
        return Ok(false);
    }
    // Only open when we know a bot existed and was lost. Heuristic: the
    // caller named provider=slack with AccountWide semantics — Departure of
    // a member does not delete the bot row, so skip Departure unless the
    // count is already zero AND cause is Disconnect/CredentialDeath.
    if matches!(cause, BackerLostCause::Departure) {
        return Ok(false);
    }
    let condition = match cause {
        BackerLostCause::CredentialDeath => CONDITION_BACKING_CREDENTIAL_LOST,
        _ => CONDITION_NO_ELIGIBLE_BACKER,
    };
    let mut detail = extra_detail.unwrap_or_else(|| json!({}));
    if let Some(obj) = detail.as_object_mut() {
        obj.insert("cause".into(), json!(format!("{cause:?}")));
    }
    upsert_degradation(
        control,
        account_id,
        "slack",
        CAPABILITY_SLACK_COLD_START_BOT,
        condition,
        "",
        backing_user_id,
        backing_email,
        detail,
    )
    .await
}

async fn live_account_wide_slack_bots(
    control: &PgPool,
    account_id: Uuid,
) -> Result<i64, ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM oauth_tokens \
          WHERE account_id = $1 AND provider = 'slack' AND owner_user_id IS NULL \
            AND credential_invalid_at IS NULL",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(remaining)
}

async fn resolve_slack_cold_start_conditions(
    control: &PgPool,
    account_id: Uuid,
) -> Result<(), ConnectorHealthError> {
    for cond in [CONDITION_NO_ELIGIBLE_BACKER, CONDITION_BACKING_CREDENTIAL_LOST] {
        resolve_condition(
            control,
            account_id,
            "slack",
            CAPABILITY_SLACK_COLD_START_BOT,
            cond,
            "",
        )
        .await?;
    }
    Ok(())
}

async fn resolve_chat_space_reader_conditions(
    control: &PgPool,
    account_id: Uuid,
) -> Result<(), ConnectorHealthError> {
    for cond in [
        CONDITION_NO_ELIGIBLE_BACKER,
        CONDITION_BACKING_MEMBER_DEPARTED,
        CONDITION_BACKING_CREDENTIAL_LOST,
    ] {
        resolve_condition(
            control,
            account_id,
            PROVIDER_GOOGLE_CHAT,
            CAPABILITY_CHAT_SPACE_READER,
            cond,
            "",
        )
        .await?;
    }
    Ok(())
}

/// Container spaces (`owner_user_id IS NULL`) whose reader seat is empty.
async fn orphaned_chat_container_spaces(
    content: &PgPool,
    account_id: Uuid,
) -> Result<i64, ConnectorHealthError> {
    let mut tx = begin_account_scoped(content, account_id).await?;
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM google_chat_watched_spaces \
          WHERE account_id = $1 AND owner_user_id IS NULL AND reader_user_id IS NULL",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(n)
}

/// The idempotent write. Returns `true` only when this call OPENED the
/// condition (a first-ever row, or the re-opening of a resolved one); a
/// re-observation of an already-open condition returns `false` and moves
/// only `last_observed_at`.
///
/// The `CASE WHEN connector_health.resolved_at IS NOT NULL THEN now() ELSE
/// connector_health.first_observed_at END` is the same shape
/// `clerk_provision::remove_org_membership`'s `member_ocean_desired_state`
/// upsert uses for `offboarded_at`, for the same reason: a NEW episode must
/// not inherit a PREVIOUS episode's start date, and a redundant
/// re-observation of the SAME episode must not move it either. `resolved_at`
/// is evaluated BEFORE it is cleared in this same `SET` list, so the two
/// cases are distinguishable at all.
///
/// `newly_opened` is read from a `SELECT ... FOR UPDATE` in the SAME
/// transaction rather than inferred from the upsert's `RETURNING`.
/// `RETURNING` on an `ON CONFLICT DO UPDATE` reports the row as it is
/// AFTER the update, where a fresh insert and a re-opening and (given a
/// coarse enough clock) a re-observation can all look alike; asking the
/// prior state directly is the version that cannot be wrong for a subtle
/// reason. `FOR UPDATE` serialises two concurrent observers of the same
/// condition, so only one of them can report having opened it. A row that
/// does not exist yet locks nothing — two racing first-ever writers would
/// both report `true`, which is the harmless direction (a notifier would
/// send one notice twice, never none) and is why the insert is still an
/// upsert.
#[allow(clippy::too_many_arguments)]
async fn upsert_degradation(
    control: &PgPool,
    account_id: Uuid,
    provider: &str,
    capability: &str,
    condition: &str,
    instance_key: &str,
    backing_user_id: Option<Uuid>,
    backing_email: Option<&str>,
    detail: Json,
) -> Result<bool, ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let already_open: Option<bool> = sqlx::query_scalar(
        "SELECT resolved_at IS NULL FROM connector_health \
          WHERE account_id = $1 AND provider = $2 AND capability = $3 AND condition = $4 \
            AND instance_key = $5 \
          FOR UPDATE",
    )
    .bind(account_id)
    .bind(provider)
    .bind(capability)
    .bind(condition)
    .bind(instance_key)
    .fetch_optional(&mut *tx)
    .await?;
    let newly_opened = !matches!(already_open, Some(true));

    sqlx::query(
        "INSERT INTO connector_health \
             (account_id, provider, capability, condition, instance_key, backing_user_id, backing_email, \
              detail, first_observed_at, last_observed_at, resolved_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now(), NULL) \
         ON CONFLICT (account_id, provider, capability, condition, instance_key) DO UPDATE \
           SET last_observed_at  = now(), \
               first_observed_at = CASE \
                   WHEN connector_health.resolved_at IS NOT NULL THEN now() \
                   ELSE connector_health.first_observed_at \
                 END, \
               resolved_at       = NULL, \
               backing_user_id   = EXCLUDED.backing_user_id, \
               backing_email     = EXCLUDED.backing_email, \
               detail            = EXCLUDED.detail",
    )
    .bind(account_id)
    .bind(provider)
    .bind(capability)
    .bind(condition)
    .bind(instance_key)
    .bind(backing_user_id)
    .bind(backing_email)
    .bind(detail)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(newly_opened)
}

/// Close one condition if it is open. `true` when a row actually moved.
async fn resolve_condition(
    control: &PgPool,
    account_id: Uuid,
    provider: &str,
    capability: &str,
    condition: &str,
    instance_key: &str,
) -> Result<bool, ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let done = sqlx::query(
        "UPDATE connector_health SET resolved_at = now(), last_observed_at = now() \
          WHERE account_id = $1 AND provider = $2 AND capability = $3 AND condition = $4 \
            AND instance_key = $5 \
            AND resolved_at IS NULL",
    )
    .bind(account_id)
    .bind(provider)
    .bind(capability)
    .bind(condition)
    .bind(instance_key)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(done > 0)
}

/// Re-check every OPEN degradation for this account and close the ones that
/// are no longer true, then return what is left.
///
/// **This function can resolve and refresh; it can never open.** That is
/// the "absence is not loss" rule from the module doc, expressed as a
/// capability of the code rather than as a comment: it takes no departing
/// member, so it has no way to know that a capability was ever present, and
/// therefore no standing to claim one was lost. Only
/// [`note_member_departure`] holds both halves.
///
/// Called on the read path (`routes::connector_health::list`) so a
/// degradation an admin has already fixed — by connecting their own Google
/// Chat with `directory.readonly`, which is the remedy the notice names —
/// stops being shown the moment they look, without a second write hook in
/// the OAuth callback. `last_observed_at` advances on every such check,
/// which is what makes "still current" a re-derived fact rather than a
/// stale one; a read that observes the same ongoing condition every 15
/// minutes still produces exactly one row, because the PK is the condition.
pub(crate) async fn refresh_open_degradations(
    control: &PgPool,
    content_pool: Option<&PgPool>,
    account_id: Uuid,
) -> Result<Vec<Degradation>, ConnectorHealthError> {
    let open = list_open(control, account_id).await?;
    if open.is_empty() {
        return Ok(open);
    }

    // `Uuid::nil()` as the exclusion: nobody is departing here, so nothing
    // is excluded — a real `users.id` is never nil. Invalid grants are
    // already dropped by `remaining_directory_credentials`.
    let directory_open = open
        .iter()
        .any(|d| d.provider == PROVIDER_GOOGLE_CHAT && d.capability == CAPABILITY_DOMAIN_DIRECTORY);
    if directory_open {
        let remaining = remaining_directory_credentials(control, account_id, Uuid::nil()).await?;
        if remaining.iter().copied().any(DirectoryCredential::might_still_back_the_directory) {
            resolve_condition(
                control,
                account_id,
                PROVIDER_GOOGLE_CHAT,
                CAPABILITY_DOMAIN_DIRECTORY,
                CONDITION_BACKING_MEMBER_DEPARTED,
                "",
            )
            .await?;
            resolve_condition(
                control,
                account_id,
                PROVIDER_GOOGLE_CHAT,
                CAPABILITY_DOMAIN_DIRECTORY,
                CONDITION_BACKING_CREDENTIAL_LOST,
                "",
            )
            .await?;
        } else {
            touch_open(control, account_id, PROVIDER_GOOGLE_CHAT, CAPABILITY_DOMAIN_DIRECTORY)
                .await?;
        }
    }

    let slack_open = open
        .iter()
        .any(|d| d.provider == "slack" && d.capability == CAPABILITY_SLACK_COLD_START_BOT);
    if slack_open {
        match live_account_wide_slack_bots(control, account_id).await {
            Ok(remaining) if remaining > 0 => {
                resolve_slack_cold_start_conditions(control, account_id).await?;
            }
            Ok(_) => {
                touch_open(control, account_id, "slack", CAPABILITY_SLACK_COLD_START_BOT).await?;
            }
            Err(e) => tracing::warn!(
                error = %e, %account_id,
                "refresh_open_degradations: slack bot recovery check failed — leaving rows"
            ),
        }
    }

    let chat_open = open
        .iter()
        .any(|d| d.provider == PROVIDER_GOOGLE_CHAT && d.capability == CAPABILITY_CHAT_SPACE_READER);
    if chat_open {
        match content_pool {
            Some(content) => match orphaned_chat_container_spaces(content, account_id).await {
                Ok(0) => resolve_chat_space_reader_conditions(control, account_id).await?,
                Ok(_) => {
                    touch_open(
                        control,
                        account_id,
                        PROVIDER_GOOGLE_CHAT,
                        CAPABILITY_CHAT_SPACE_READER,
                    )
                    .await?;
                }
                Err(e) => tracing::warn!(
                    error = %e, %account_id,
                    "refresh_open_degradations: chat orphan check failed — leaving rows"
                ),
            },
            None => tracing::debug!(
                %account_id,
                "refresh_open_degradations: no content pool — chat reader rows left for reconnect/re-election"
            ),
        }
    }

    if open.iter().any(|d| {
        d.provider == "microsoft" && d.capability == CAPABILITY_TEAMS_CONTAINER_PUSH
    }) {
        refresh_teams_container_rows(control, account_id).await?;
    }

    list_open(control, account_id).await
}

/// Resolve Teams push rows whose doorbell is live again, and refresh the
/// ones still polling. Never opens a row — a sweep does not know that a
/// capability was lost, only that an already-open condition ended.
async fn refresh_teams_container_rows(
    control: &PgPool,
    account_id: Uuid,
) -> Result<(), ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let keys: Vec<String> = sqlx::query_scalar(
        "SELECT instance_key FROM connector_health \
         WHERE account_id = $1 AND provider = 'microsoft' AND capability = $2 \
           AND resolved_at IS NULL AND instance_key <> ''",
    )
    .bind(account_id)
    .bind(CAPABILITY_TEAMS_CONTAINER_PUSH)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    let mut still_open = false;
    for key in keys {
        let Some((ms_tenant_id, resource)) = key.split_once('\n') else {
            continue;
        };
        let state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM teams_container_subscriptions \
             WHERE ms_tenant_id = $1 AND resource = $2",
        )
        .bind(ms_tenant_id)
        .bind(resource)
        .fetch_optional(control)
        .await?;
        if state.as_deref() == Some("live") || state.is_none() {
            resolve_condition(
                control,
                account_id,
                "microsoft",
                CAPABILITY_TEAMS_CONTAINER_PUSH,
                CONDITION_NO_ELIGIBLE_BACKER,
                &key,
            )
            .await?;
        } else {
            still_open = true;
        }
    }
    if still_open {
        touch_open(control, account_id, "microsoft", CAPABILITY_TEAMS_CONTAINER_PUSH).await?;
    }
    Ok(())
}

/// "Still true as of now" — moves `last_observed_at` on the open rows for
/// one capability and nothing else. Never inserts.
async fn touch_open(
    control: &PgPool,
    account_id: Uuid,
    provider: &str,
    capability: &str,
) -> Result<(), ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    sqlx::query(
        "UPDATE connector_health SET last_observed_at = now() \
          WHERE account_id = $1 AND provider = $2 AND capability = $3 AND resolved_at IS NULL",
    )
    .bind(account_id)
    .bind(provider)
    .bind(capability)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// The raw tuple [`OPEN_SELECT`]'s eight columns decode into — named once
/// rather than repeated inline (clippy `type_complexity`), the same way
/// `services::org_join_requests::SummaryRow` names its own.
type DegradationRow = (
    String,
    String,
    String,
    Option<Uuid>,
    Option<String>,
    Json,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
);

const OPEN_SELECT: &str = "\
    SELECT provider, capability, condition, backing_user_id, backing_email, detail, \
           first_observed_at, last_observed_at \
      FROM connector_health \
     WHERE account_id = $1 AND resolved_at IS NULL \
     ORDER BY first_observed_at ASC, provider ASC, capability ASC";

/// The raw open-rows read. Oldest first — a degradation that has been true
/// for a week is the one an admin should see at the top.
pub(crate) async fn list_open(
    control: &PgPool,
    account_id: Uuid,
) -> Result<Vec<Degradation>, ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let rows = sqlx::query_as::<_, DegradationRow>(OPEN_SELECT)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(rows
        .into_iter()
        .map(|r| Degradation {
            provider: r.0,
            capability: r.1,
            condition: r.2,
            backing_user_id: r.3,
            backing_email: r.4,
            detail: r.5,
            first_observed_at: r.6,
            last_observed_at: r.7,
        })
        .collect())
}

// ── CS-448: attribution, not a second record ────────────────────────────────
//
// Everything below this line is a READ. No function here takes an HTTP
// client, a `DirectoryEntry`, or any other directory-fetch result, and none
// of them writes a row — the same signature-level discipline D16's trap
// paragraph asks of the writers above, applied to the reader so a future
// edit cannot quietly turn "could not ask" into a durable record. The
// attribution is re-derived on every read and stored nowhere.

/// One CURRENT member of this org who disconnected Google Chat and has not
/// reconnected.
///
/// "Has not reconnected" is expressed as *no live `google_chat` grant right
/// now*, which also makes this set disjoint from CS-431's: a member who
/// reconnected with a narrower grant HAS a row, so they fall to
/// `missing_scopes`/`ReconsentNotice` (which already names them, on this
/// same page, with Reconnect attached) and never appear here. One problem,
/// one notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DisconnectedBacker {
    pub user_id: Uuid,
    pub email: String,
    /// The most recent `connector_audit_log` `disconnect` row for
    /// `google_chat` naming this member as actor. The audit log is the
    /// append-only compliance record (migration 0193) — this reads it, it
    /// does not add to it.
    pub disconnected_at: chrono::DateTime<chrono::Utc>,
}

/// Whether anyone in this org is backing D16's rung 1 right now, and — when
/// nobody is — who is still here that could be.
///
/// Three-way for the third time in this module, and for the third time for
/// the same reason: *"we cannot tell from what we stored"* is not either
/// verdict. [`DirectoryCredential`] refuses to guess at one member's grant;
/// [`crate::services::assertion_ladder::DirectorySignal`] refuses to guess
/// at one person's listing; this refuses to guess at the org.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DirectoryBacking {
    /// At least one CURRENT member's stored grant definitely carries
    /// `directory.readonly`. Rung 1 is available; there is nothing to
    /// attribute and nothing to show.
    Backed,
    /// At least one CURRENT member's stored grant is unreadable, so we
    /// cannot say the org has no backing. Attributes nothing — the same
    /// refusal [`DirectoryCredential::might_still_back_the_directory`]
    /// already makes for the alarm, made once more for the notice.
    Indeterminate,
    /// No current member's grant carries it, definitely.
    ///
    /// **`disconnected` being empty is the ordinary case, not an error.**
    /// Every Slack-only org — D16's own MVP path — is `Unbacked` with an
    /// empty list forever, because it never had a directory to lose.
    /// "Absence of a capability is not loss of one" is this module's
    /// founding rule (see its doc comment) and it is why the attribution is
    /// keyed on *somebody having disconnected*, never on the org merely
    /// having no directory.
    Unbacked { disconnected: Vec<DisconnectedBacker> },
}

/// **CS-448's whole feature.** Join two facts this codebase already
/// computes, at read time, for one org.
///
/// The gap it closes: item 16 (CS-446) records a departure, because a
/// departure has no Reconnect button — the credential's owner is gone.
/// Migration 0243 deliberately leaves out the case where the member
/// *disconnects and stays*, on the reasoning that the person is still here
/// and their own connectors page shows them disconnected. True, and still
/// not enough: item 49 shows an admin "this member hasn't connected a tool"
/// on the members page, and nothing anywhere says that this is *why* join
/// requests are now being decided on the weaker rung. The two facts are
/// each visible and the causal link between them is not. That link is what
/// this returns.
///
/// # Why this is not a new `connector_health` condition
///
/// Migration 0243's `CHECK` admits `'backing_member_departed'` and nothing
/// else, on purpose: *"a writer for a new condition has to add the value
/// here, in its own migration, with its own reasoning."* CS-448 adds no
/// writer and no value, and needs no migration, because there is nothing
/// new to remember. Both halves are already durable — `oauth_tokens` and
/// `account_members` say who backs the directory now, `connector_audit_log`
/// (0193, append-only, the compliance record) says who disconnected — and a
/// derived fact that can be recomputed exactly from two stored ones has no
/// business being stored a third time, where it could only go stale. That
/// is also why the condition needs no resolution path: reconnecting makes
/// it false on the next read, with no hook anywhere.
///
/// # What this claims, and the one thing it deliberately does not
///
/// It claims, and can prove from state: *no current member grants directory
/// access, and this named current member disconnected Google Chat on this
/// date and has not reconnected.*
///
/// It does **not** claim that the named member's grant was the one carrying
/// `directory.readonly` — and the copy on the surface must not either.
/// Disconnect DELETES the `oauth_tokens` row (`routes::oauth::
/// delete_connection`), which is the only place the granted scope string
/// ever lived, so by the time anyone can ask, the evidence is gone. Proving
/// it would mean recording the scope at disconnect time: a new column, a
/// migration, and a durable write on a path that has no business acquiring
/// one. The two facts joined are enough to act on, and each of them is
/// true; an invented third would not be. This is the same refusal
/// [`DirectoryCredential::definitely_backed_the_directory`] makes one level
/// down — *"otherwise we invent a backer"*.
pub(crate) async fn directory_backing(
    control: &PgPool,
    account_id: Uuid,
) -> Result<DirectoryBacking, ConnectorHealthError> {
    // `Uuid::nil()` excludes nobody — a real `users.id` is never nil. The
    // same call `refresh_open_degradations` already makes, so "who backs
    // rung 1" has exactly one implementation and the notice cannot disagree
    // with the record beside it.
    let current = remaining_directory_credentials(control, account_id, Uuid::nil()).await?;
    if current.iter().copied().any(DirectoryCredential::definitely_backed_the_directory) {
        return Ok(DirectoryBacking::Backed);
    }
    if current.iter().copied().any(DirectoryCredential::might_still_back_the_directory) {
        // Only `Unknown` can reach here — `Carries` returned above.
        return Ok(DirectoryBacking::Indeterminate);
    }
    Ok(DirectoryBacking::Unbacked {
        disconnected: disconnected_members(control, account_id).await?,
    })
}

/// The members named by [`DirectoryBacking::Unbacked`].
///
/// Four predicates, each one load-bearing:
///
/// * `action = 'disconnect' AND provider = 'google_chat'` — the audit row
///   for THIS connector. A member who disconnected Gmail, Slack or GitHub
///   has taken nothing from rung 1 and is never named.
/// * `actor_kind = 'user'` — `system`/`webhook` rows carry no
///   `actor_user_id` (0193's `connector_audit_log_actor_pairing` CHECK), so
///   there is nobody to name.
/// * an `account_members` row still exists — **the line between this and
///   item 16.** A member who disconnected and then LEFT is a departure, and
///   `note_member_departure` owns that case with a durable row that names
///   them; naming them here too would put two notices on one screen for one
///   problem, which is the exact thing 0243 refuses to do to CS-431.
/// * no `oauth_tokens` row for `google_chat` — they have not reconnected.
///   A member who reconnected with a narrower grant is excluded here and
///   picked up by CS-431's `missing_scopes` instead.
///
/// Newest first: if several people disconnected, the most recent one is the
/// change an admin is most likely to recognise.
async fn disconnected_members(
    control: &PgPool,
    account_id: Uuid,
) -> Result<Vec<DisconnectedBacker>, ConnectorHealthError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let rows: Vec<(Uuid, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT u.id, u.email, max(cal.occurred_at) AS disconnected_at \
           FROM connector_audit_log cal \
           JOIN users u ON u.id = cal.actor_user_id \
          WHERE cal.account_id = $1 \
            AND cal.action = 'disconnect' \
            AND cal.provider = $2 \
            AND cal.actor_kind = 'user' \
            AND EXISTS ( \
                  SELECT 1 FROM account_members am \
                  WHERE am.account_id = $1 AND am.user_id = cal.actor_user_id \
                ) \
            AND NOT EXISTS ( \
                  SELECT 1 FROM oauth_tokens ot \
                  WHERE ot.account_id = $1 AND ot.provider = $2 \
                    AND ot.owner_user_id = cal.actor_user_id \
                ) \
          GROUP BY u.id, u.email \
          ORDER BY max(cal.occurred_at) DESC, u.email ASC",
    )
    .bind(account_id)
    .bind(PROVIDER_GOOGLE_CHAT)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows
        .into_iter()
        .map(|(user_id, email, disconnected_at)| DisconnectedBacker {
            user_id,
            email,
            disconnected_at,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `Unknown` asymmetry, stated as a test rather than only as a doc
    /// comment. An unparseable grant must not be blamed for a departure AND
    /// must not be ignored when asking whether anyone is left — the two
    /// readings are opposite, and both of them refuse to produce a row.
    #[test]
    fn an_unreadable_grant_neither_blames_nor_alarms() {
        let unknown = DirectoryCredential::Unknown;
        assert!(
            !unknown.definitely_backed_the_directory(),
            "an unreadable grant must never be blamed as the departed backer"
        );
        assert!(
            unknown.might_still_back_the_directory(),
            "an unreadable grant must still spare the alarm — we do not know it is gone"
        );
    }

    /// The scope filter itself, driven through the SAME function production
    /// takes (`connectors::google::chat_missing_scopes`) rather than by
    /// constructing the variant by hand — the discipline
    /// `assertion_ladder::classify_domain_match`'s own doc comment spells
    /// out: a test that builds the answer cannot pin the mapping that
    /// produces it.
    #[test]
    fn a_pre_widening_chat_grant_does_not_carry_the_directory() {
        // The three scopes Chat requested BEFORE `directory.readonly` was
        // added (see `connectors::google::Surface::Chat::data_scope`).
        let pre_widening = "openid email \
             https://www.googleapis.com/auth/chat.messages.readonly \
             https://www.googleapis.com/auth/chat.spaces.readonly \
             https://www.googleapis.com/auth/chat.memberships.readonly";
        assert_eq!(
            DirectoryCredential::from_stored_scope(Some(pre_widening)),
            DirectoryCredential::Lacks,
            "a grant made before the directory.readonly widening cannot back rung 1"
        );

        let current = crate::connectors::google::scopes_for(crate::connectors::google::Surface::Chat);
        assert_eq!(
            DirectoryCredential::from_stored_scope(Some(&current)),
            DirectoryCredential::Carries,
            "the scope set this codebase requests today must read as carrying rung 1 — if this \
             fails, chat_missing_scopes and scopes_for have drifted apart"
        );

        assert_eq!(
            DirectoryCredential::from_stored_scope(None),
            DirectoryCredential::Lacks,
            "no google_chat connection at all is Lacks, not Unknown"
        );
        assert_eq!(
            DirectoryCredential::from_stored_scope(Some("   ")),
            DirectoryCredential::Unknown,
            "a blank stored scope is the one shape we refuse to guess at"
        );
    }

    /// Directory backing is `directory.readonly`, not Chat-scope
    /// completeness. A member who granted the directory scope but not
    /// (for example) `chat.spaces.readonly` still serves rung 1; treating
    /// them as `Lacks` is how a disconnect by someone else becomes a
    /// false attribution.
    #[test]
    fn directory_readonly_backs_rung_1_even_when_another_chat_scope_is_missing() {
        let mixed = "openid email \
             https://www.googleapis.com/auth/chat.messages.readonly \
             https://www.googleapis.com/auth/directory.readonly";
        let cred = DirectoryCredential::from_stored_scope(Some(mixed));
        assert_eq!(
            cred,
            DirectoryCredential::Carries,
            "missing a Chat scope must not drop a grant that still carries directory.readonly"
        );
        assert!(
            cred.definitely_backed_the_directory(),
            "the directory-backing read must see this grant as a definite backer"
        );
        assert!(
            cred.might_still_back_the_directory(),
            "the same grant must also spare the alarm — rung 1 is still served"
        );
    }
}

// ── Live-Postgres tests (CS-446) ────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL` — a control database with
// `migrations_control` applied through 0243. In-crate rather than a
// `backend/tests/*.rs` binary because every function under test is
// `pub(crate)`, same as `services::org_domains`' own `pg_tests`.
//
//   RELINFER_TEST_DATABASE_URL=postgresql://alex@localhost:5432/coralswarm_phase_e \
//     cargo test -p coralswarm-backend --lib "services::connector_health::pg_tests::" \
//       -- --test-threads=1 --nocapture
#[cfg(test)]
mod pg_tests {
    use super::*;

    /// The full scope string a member who consented AFTER the widening
    /// has on file — the one that carries rung 1.
    fn current_chat_scope() -> String {
        crate::connectors::google::scopes_for(crate::connectors::google::Surface::Chat)
    }

    /// What a member who connected BEFORE `directory.readonly` shipped has.
    const PRE_WIDENING_CHAT_SCOPE: &str = "openid email \
         https://www.googleapis.com/auth/chat.messages.readonly \
         https://www.googleapis.com/auth/chat.spaces.readonly \
         https://www.googleapis.com/auth/chat.memberships.readonly";

    /// `directory.readonly` plus only one of the three Chat scopes — the
    /// shape that used to collapse into `Lacks` and produce a false
    /// directory-loss attribution.
    const DIRECTORY_WITHOUT_ALL_CHAT_SCOPES: &str = "openid email \
         https://www.googleapis.com/auth/chat.messages.readonly \
         https://www.googleapis.com/auth/directory.readonly";

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

    async fn seed_user(pool: &PgPool, tag: &str) -> (Uuid, String) {
        let email = format!("cs446-{tag}-{}@example.test", Uuid::new_v4().simple());
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users (id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) \
             RETURNING id",
        )
        .bind(&email)
        .bind(format!("clerk_cs446_{tag}_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert user");
        (user_id, email)
    }

    async fn seed_org(pool: &PgPool) -> (Uuid, Uuid) {
        let (owner, _) = seed_user(pool, "owner").await;
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(format!("org_cs446_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert account");
        (account_id, owner)
    }

    /// `seat_tier_id` is passed explicitly, never defaulted: the
    /// `account_members_default_seat_tier` trigger (PR F, seat lifecycle)
    /// RAISES on an org-account insert that omits it, so a fixture that
    /// left it out would fail at the database rather than at the assertion
    /// — same shape `billing::renewal`'s own member fixture uses.
    async fn add_member(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        let tier: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
                .fetch_one(pool)
                .await
                .expect("the 'standard' seat tier must be seeded (0165)");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'member', $3) ON CONFLICT DO NOTHING",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(tier)
        .execute(pool)
        .await
        .expect("insert account_member");
    }

    async fn remove_member(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("delete account_member");
    }

    /// A `google_chat` grant for one member. `scope` decides whether it
    /// backs rung 1 — the token bytes are irrelevant to every code path
    /// under test here (nothing decrypts or calls the wire), so a literal
    /// placeholder is honest rather than a shortcut.
    async fn connect_chat(pool: &PgPool, account_id: Uuid, user_id: Uuid, scope: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens \
                 (account_id, provider, scope, access_token, owner_user_id, provider_workspace_id) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(account_id)
        .bind(PROVIDER_GOOGLE_CHAT)
        .bind(scope)
        .bind(b"cs446-not-a-real-token".to_vec())
        .bind(user_id)
        .bind(format!("ws_cs446_{}", Uuid::new_v4().simple()))
        .execute(pool)
        .await
        .expect("insert oauth_token");
    }

    async fn open_row_count(pool: &PgPool, account_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM connector_health WHERE account_id = $1 AND resolved_at IS NULL",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .expect("count open")
    }

    async fn total_row_count(pool: &PgPool, account_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM connector_health WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .expect("count all")
    }

    /// **The item.** The member whose Google grant carried
    /// `directory.readonly` leaves, nobody else has it, and the org's drop
    /// from rung 1 to rung 2 becomes a record an admin can be shown —
    /// naming WHOSE credential it was, which is the part that makes the
    /// notice actionable rather than merely true.
    #[tokio::test]
    async fn a_departing_directory_backer_is_recorded_with_the_credential_that_is_gone() {
        let Some(pool) =
            pg_test_pool("a_departing_directory_backer_is_recorded_with_the_credential_that_is_gone")
                .await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, account_id, dana).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;

        // Offboarding deletes the membership row and never touches
        // `oauth_tokens` — the real shape
        // `clerk_provision::remove_org_membership` leaves behind.
        remove_member(&pool, account_id, dana).await;

        let outcome = note_member_departure(&pool, account_id, dana, Some(&dana_email))
            .await
            .expect("note departure");
        assert_eq!(outcome, DepartureOutcome::Recorded { newly_opened: true });

        let open = list_open(&pool, account_id).await.expect("list open");
        assert_eq!(open.len(), 1, "exactly one degradation");
        let d = &open[0];
        assert_eq!(d.provider, PROVIDER_GOOGLE_CHAT);
        assert_eq!(d.capability, CAPABILITY_DOMAIN_DIRECTORY);
        assert_eq!(d.condition, CONDITION_BACKING_MEMBER_DEPARTED);
        assert_eq!(d.backing_user_id, Some(dana), "the record must name whose credential it was");
        assert_eq!(d.backing_email.as_deref(), Some(dana_email.as_str()));
        assert_eq!(
            d.detail.get("ladder_effect").and_then(|v| v.as_str()),
            Some("auto_add_now_decides_on_rung_2"),
            "the record must carry what it MEANS, not only that it happened"
        );
    }

    /// **NEGATIVE — the same ongoing condition observed twice is one row.**
    /// Migration 0243's PK is the condition, not the observation, so a
    /// sweep (or a re-delivered webhook) that notices the same degradation
    /// again must move `last_observed_at` and nothing else. The second call
    /// also reports `newly_opened: false`, which is what a future notifier
    /// would gate "tell the admin" on.
    #[tokio::test]
    async fn the_same_ongoing_condition_observed_twice_is_still_one_row() {
        let Some(pool) = pg_test_pool("the_same_ongoing_condition_observed_twice_is_still_one_row").await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, account_id, dana).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;
        remove_member(&pool, account_id, dana).await;

        let first = note_member_departure(&pool, account_id, dana, Some(&dana_email))
            .await
            .expect("first");
        assert_eq!(first, DepartureOutcome::Recorded { newly_opened: true });
        let first_seen = list_open(&pool, account_id).await.expect("list")[0].first_observed_at;

        let second = note_member_departure(&pool, account_id, dana, Some(&dana_email))
            .await
            .expect("second");
        assert_eq!(
            second,
            DepartureOutcome::Recorded { newly_opened: false },
            "a re-observation is not a new opening"
        );
        // And a third, through the READ path this time — the surface's own
        // refresh must be just as incapable of multiplying rows.
        refresh_open_degradations(&pool, None, account_id).await.expect("refresh");

        assert_eq!(total_row_count(&pool, account_id).await, 1, "three observations, one row");
        let rows = list_open(&pool, account_id).await.expect("list");
        assert_eq!(
            rows[0].first_observed_at, first_seen,
            "re-observing an ONGOING condition must not move first_observed_at"
        );
        assert!(
            rows[0].last_observed_at >= rows[0].first_observed_at,
            "but last_observed_at tracks the latest observation"
        );
    }

    /// **NEGATIVE — a transient directory failure is not this condition.**
    ///
    /// The single most important test in this file, and the one D16's own
    /// trap paragraph demands: *"could not ask ≠ does not work here."* A
    /// directory that is unreachable this minute — a 403, a transport
    /// error, or the HTTP-200-with-zero-people case
    /// `fetch_domain_directory` already folds into `Err` — is
    /// `DirectorySignal::Unavailable`, a per-pass transient. It is NOT the
    /// member-left degradation, and must never produce a row.
    ///
    /// Proven structurally rather than by mocking the wire: the detection
    /// path takes no HTTP client and no directory result at all, so there
    /// is no argument by which a fetch failure could reach it. What this
    /// test drives is the same account in the same state a failed fetch
    /// would leave it in — a live, directory-capable, CURRENT member — and
    /// asserts that nothing is recorded, on the departure path AND on the
    /// read path.
    #[tokio::test]
    async fn a_transient_directory_failure_is_never_recorded_as_a_departure() {
        let Some(pool) =
            pg_test_pool("a_transient_directory_failure_is_never_recorded_as_a_departure").await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, dana_email) = seed_user(&pool, "dana").await;
        let (sam, _sam_email) = seed_user(&pool, "sam").await;
        add_member(&pool, account_id, dana).await;
        add_member(&pool, account_id, sam).await;
        connect_chat(&pool, account_id, sam, &current_chat_scope()).await;

        // The whole org's directory could be 403-ing right now — Workspace
        // profile sharing off, a revoked grant, Google down. Nothing about
        // that is a departure, and there is no path from it into this
        // record: the read path refuses to open a row at all.
        let open = refresh_open_degradations(&pool, None, account_id).await.expect("refresh");
        assert!(open.is_empty(), "a read path may resolve and refresh, never open");
        assert_eq!(
            total_row_count(&pool, account_id).await,
            0,
            "a transient directory failure must leave no trace in connector_health"
        );

        // And a departure by someone who never backed the directory is
        // equally inert, even while a fetch would be failing.
        remove_member(&pool, account_id, dana).await;
        assert_eq!(
            note_member_departure(&pool, account_id, dana, Some(&dana_email)).await.expect("note"),
            DepartureOutcome::NotTheDirectoryBacker
        );
        assert_eq!(total_row_count(&pool, account_id).await, 0);
    }

    /// An org that NEVER had a directory does not acquire a degradation the
    /// first time anyone leaves — "absence of a capability is not loss of
    /// one". This is the Slack-only org D16 calls the MVP path, and it is
    /// every org in the product today; if this fired for them the notice
    /// would be worthless on day one.
    ///
    /// Also covers the pre-widening grant: a `google_chat` connection made
    /// before `directory.readonly` shipped never backed rung 1, so losing
    /// it takes nothing away — CS-431's re-consent prompt is that person's
    /// surface, not this one.
    #[tokio::test]
    async fn an_org_that_never_had_a_directory_records_nothing_when_someone_leaves() {
        let Some(pool) =
            pg_test_pool("an_org_that_never_had_a_directory_records_nothing_when_someone_leaves")
                .await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;

        // (a) a Slack-only org: no Google connection at all.
        let (alex, alex_email) = seed_user(&pool, "alex").await;
        add_member(&pool, account_id, alex).await;
        remove_member(&pool, account_id, alex).await;
        assert_eq!(
            note_member_departure(&pool, account_id, alex, Some(&alex_email)).await.expect("note"),
            DepartureOutcome::NotTheDirectoryBacker
        );

        // (b) a Chat connection from BEFORE the directory.readonly widening.
        let (robin, robin_email) = seed_user(&pool, "robin").await;
        add_member(&pool, account_id, robin).await;
        connect_chat(&pool, account_id, robin, PRE_WIDENING_CHAT_SCOPE).await;
        remove_member(&pool, account_id, robin).await;
        assert_eq!(
            note_member_departure(&pool, account_id, robin, Some(&robin_email)).await.expect("note"),
            DepartureOutcome::NotTheDirectoryBacker,
            "a grant that never carried directory.readonly cannot have been the backer"
        );

        assert_eq!(total_row_count(&pool, account_id).await, 0);
    }

    /// A departure that leaves rung 1 INTACT is not a degradation — and if
    /// a stale one was open, it is resolved. This is the other half of the
    /// "definite on both sides" rule: the leaver did carry it, but somebody
    /// else still does.
    #[tokio::test]
    async fn a_departure_with_another_directory_backer_left_resolves_instead_of_recording() {
        let Some(pool) = pg_test_pool(
            "a_departure_with_another_directory_backer_left_resolves_instead_of_recording",
        )
        .await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, dana_email) = seed_user(&pool, "dana").await;
        let (sam, sam_email) = seed_user(&pool, "sam").await;
        for u in [dana, sam] {
            add_member(&pool, account_id, u).await;
            connect_chat(&pool, account_id, u, &current_chat_scope()).await;
        }

        // Dana leaves — Sam still carries the grant.
        remove_member(&pool, account_id, dana).await;
        assert_eq!(
            note_member_departure(&pool, account_id, dana, Some(&dana_email)).await.expect("dana"),
            DepartureOutcome::StillCovered { resolved_existing: false }
        );
        assert_eq!(open_row_count(&pool, account_id).await, 0);

        // Now Sam leaves too, and the org really does lose rung 1.
        remove_member(&pool, account_id, sam).await;
        assert_eq!(
            note_member_departure(&pool, account_id, sam, Some(&sam_email)).await.expect("sam"),
            DepartureOutcome::Recorded { newly_opened: true }
        );
        assert_eq!(open_row_count(&pool, account_id).await, 1);
        assert_eq!(
            list_open(&pool, account_id).await.expect("list")[0].backing_user_id,
            Some(sam),
            "the record names the LAST backer, the one whose leaving actually took it away"
        );
    }

    /// Recovery, and a re-opening after it. A new member connects Chat with
    /// the directory scope, the read path notices on the next look and
    /// closes the row without a second write hook in the OAuth callback;
    /// when they in turn leave, the SAME row re-opens with a FRESH
    /// `first_observed_at` rather than reporting the old episode's start
    /// date.
    #[tokio::test]
    async fn recovery_resolves_the_row_and_a_later_relapse_reopens_it_with_a_fresh_start() {
        let Some(pool) = pg_test_pool(
            "recovery_resolves_the_row_and_a_later_relapse_reopens_it_with_a_fresh_start",
        )
        .await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, account_id, dana).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;
        remove_member(&pool, account_id, dana).await;
        note_member_departure(&pool, account_id, dana, Some(&dana_email)).await.expect("dana");
        let first_episode = list_open(&pool, account_id).await.expect("list")[0].first_observed_at;

        // The remedy the notice names: somebody still here connects.
        let (sam, sam_email) = seed_user(&pool, "sam").await;
        add_member(&pool, account_id, sam).await;
        connect_chat(&pool, account_id, sam, &current_chat_scope()).await;

        let open = refresh_open_degradations(&pool, None, account_id).await.expect("refresh");
        assert!(open.is_empty(), "the read path must stop showing a degradation that is fixed");
        assert_eq!(total_row_count(&pool, account_id).await, 1, "resolved, not deleted");

        // Sam leaves later: the SAME row re-opens, dated to THIS episode.
        remove_member(&pool, account_id, sam).await;
        assert_eq!(
            note_member_departure(&pool, account_id, sam, Some(&sam_email)).await.expect("sam"),
            DepartureOutcome::Recorded { newly_opened: true },
            "a relapse after a resolution is a NEW opening, not a re-observation"
        );
        assert_eq!(total_row_count(&pool, account_id).await, 1, "re-opened, never duplicated");
        let reopened = &list_open(&pool, account_id).await.expect("list")[0];
        assert!(
            reopened.first_observed_at > first_episode,
            "a new episode must not report the previous episode's start date"
        );
        assert_eq!(reopened.backing_user_id, Some(sam));
    }

    /// One org's degradation is invisible to another. The policy is
    /// enforced for real by `coralswarm_app` in `consumer_rls_pg`; this
    /// asserts the SCOPE PREDICATE in the queries themselves, which is what
    /// an owner-role connection (this one) actually exercises.
    #[tokio::test]
    async fn a_degradation_is_scoped_to_its_own_account() {
        let Some(pool) = pg_test_pool("a_degradation_is_scoped_to_its_own_account").await else {
            return;
        };
        let (acme, _) = seed_org(&pool).await;
        let (beta, _) = seed_org(&pool).await;
        let (dana, dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, acme, dana).await;
        connect_chat(&pool, acme, dana, &current_chat_scope()).await;
        remove_member(&pool, acme, dana).await;
        note_member_departure(&pool, acme, dana, Some(&dana_email)).await.expect("note");

        assert_eq!(list_open(&pool, acme).await.expect("acme").len(), 1);
        assert!(
            list_open(&pool, beta).await.expect("beta").is_empty(),
            "Acme's connector health is none of Beta's business"
        );
    }

    // ── CS-448: the attribution ─────────────────────────────────────────

    /// Disconnect Google Chat the way `routes::oauth::delete_connection`
    /// does: DELETE the `oauth_tokens` row, write the `disconnect` row into
    /// `connector_audit_log`, in that order.
    ///
    /// Both halves matter, and the fixture does both rather than only the
    /// one the assertion needs, because the whole feature is a JOIN of
    /// them: a deleted grant with no audit row is a member we cannot name,
    /// and an audit row with a live grant is a member who reconnected. A
    /// fixture that wrote only one would be testing a state production
    /// never produces.
    async fn disconnect_chat(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        drop_chat_grants(pool, account_id, user_id).await;
        audit_disconnect(pool, account_id, user_id, PROVIDER_GOOGLE_CHAT).await;
    }

    /// The `oauth_tokens` half of a disconnect, on its own — the exact
    /// `DELETE ... WHERE account_id AND provider AND owner_user_id`
    /// `routes::oauth::delete_connection` runs.
    async fn drop_chat_grants(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        sqlx::query(
            "DELETE FROM oauth_tokens \
              WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3",
        )
        .bind(account_id)
        .bind(PROVIDER_GOOGLE_CHAT)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("delete oauth_token");
    }

    /// Re-consent: the old grant goes, a new one with `scope` replaces it.
    /// Written as replace-not-add on purpose — `connect_chat` mints a fresh
    /// `provider_workspace_id` every call, so simply calling it twice would
    /// leave a member holding TWO Chat grants at once, a state no OAuth
    /// callback produces and one that would make this test pass for the
    /// wrong reason.
    async fn reconnect_chat(pool: &PgPool, account_id: Uuid, user_id: Uuid, scope: &str) {
        drop_chat_grants(pool, account_id, user_id).await;
        connect_chat(pool, account_id, user_id, scope).await;
    }

    /// One `connector_audit_log` `disconnect` row, provider-parameterised
    /// so a test can prove a Gmail or Slack disconnect is NOT attributed to
    /// the directory.
    async fn audit_disconnect(pool: &PgPool, account_id: Uuid, user_id: Uuid, provider: &str) {
        sqlx::query(
            "INSERT INTO connector_audit_log \
                 (account_id, actor_user_id, actor_kind, action, provider) \
             VALUES ($1, $2, 'user', 'disconnect', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(provider)
        .execute(pool)
        .await
        .expect("insert connector_audit_log disconnect");
    }

    /// **The item.** The org had directory access through one member; that
    /// member disconnects Google Chat and STAYS. Item 16 records nothing —
    /// correctly, nobody departed — and until CS-448 nothing said why join
    /// requests had quietly started being decided on the weaker rung.
    ///
    /// Note what is asserted and what is not: that the org is `Unbacked`
    /// and that Dana is named, NOT that Dana's grant was the one carrying
    /// `directory.readonly`. Disconnect deleted the row that would have
    /// said so. See `directory_backing`'s own doc comment.
    #[tokio::test]
    async fn a_still_present_member_who_disconnected_is_named() {
        let Some(pool) = pg_test_pool("a_still_present_member_who_disconnected_is_named").await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, account_id, dana).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;

        assert_eq!(
            directory_backing(&pool, account_id).await.expect("backed"),
            DirectoryBacking::Backed,
            "while Dana's grant carries it the org is backed and nothing is attributed"
        );

        disconnect_chat(&pool, account_id, dana).await;

        let DirectoryBacking::Unbacked { disconnected } =
            directory_backing(&pool, account_id).await.expect("unbacked")
        else {
            panic!("the org has no directory-capable member left");
        };
        assert_eq!(disconnected.len(), 1, "exactly one member to name");
        assert_eq!(disconnected[0].user_id, dana);
        assert_eq!(disconnected[0].email, dana_email);

        // And the durable half is untouched: this is a read, and a read
        // that wrote a row would have turned CS-448 into the new condition
        // value migration 0243 deliberately refuses.
        assert_eq!(
            total_row_count(&pool, account_id).await,
            0,
            "the attribution is derived, never recorded"
        );
    }

    /// **NEGATIVE — the case the item is explicitly about not doing.**
    ///
    /// A member disconnects Google Chat who was NOT what the org's
    /// directory was resting on: Sam still has a grant that carries
    /// `directory.readonly`, so rung 1 is intact and there is nothing to
    /// attribute to anybody. Naming Dana here would be the failure mode
    /// this whole area guards against one level down — telling an admin a
    /// capability is gone while it is still being served.
    #[tokio::test]
    async fn a_disconnect_by_someone_who_was_not_the_backing_is_not_attributed() {
        let Some(pool) =
            pg_test_pool("a_disconnect_by_someone_who_was_not_the_backing_is_not_attributed").await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, _dana_email) = seed_user(&pool, "dana").await;
        let (sam, _sam_email) = seed_user(&pool, "sam").await;
        add_member(&pool, account_id, dana).await;
        add_member(&pool, account_id, sam).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;
        connect_chat(&pool, account_id, sam, &current_chat_scope()).await;

        disconnect_chat(&pool, account_id, dana).await;

        assert_eq!(
            directory_backing(&pool, account_id).await.expect("still backed"),
            DirectoryBacking::Backed,
            "Sam still carries directory.readonly, so Dana's disconnect took nothing away \
             and must produce no attribution"
        );
    }

    /// **NEGATIVE — Chat-scope incompleteness is not directory loss.**
    ///
    /// Sam granted `directory.readonly` but is missing a different Chat
    /// scope. Dana disconnects. Naming Dana would be a false attribution:
    /// rung 1 is still served by Sam's directory grant. The incomplete
    /// Chat grant is CS-431's notice, not this one.
    #[tokio::test]
    async fn a_directory_grant_missing_another_chat_scope_still_backs_the_directory() {
        let Some(pool) = pg_test_pool(
            "a_directory_grant_missing_another_chat_scope_still_backs_the_directory",
        )
        .await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, _dana_email) = seed_user(&pool, "dana").await;
        let (sam, _sam_email) = seed_user(&pool, "sam").await;
        add_member(&pool, account_id, dana).await;
        add_member(&pool, account_id, sam).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;
        connect_chat(&pool, account_id, sam, DIRECTORY_WITHOUT_ALL_CHAT_SCOPES).await;

        disconnect_chat(&pool, account_id, dana).await;

        assert_eq!(
            directory_backing(&pool, account_id).await.expect("still backed"),
            DirectoryBacking::Backed,
            "Sam still carries directory.readonly even without every Chat scope, so Dana's \
             disconnect must produce no attribution"
        );
    }

    /// **NEGATIVE — absence of a capability is not loss of one.**
    ///
    /// This module's founding rule, restated for the read path. A
    /// Slack-only org — D16's own MVP path, and most orgs in the product —
    /// is permanently `Unbacked` and must name nobody, no matter who
    /// disconnects what. The list is empty rather than the variant being
    /// different, and the route turns an empty list into no notice at all.
    #[tokio::test]
    async fn an_org_that_never_had_a_directory_attributes_nobody() {
        let Some(pool) = pg_test_pool("an_org_that_never_had_a_directory_attributes_nobody").await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, _dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, account_id, dana).await;

        assert_eq!(
            directory_backing(&pool, account_id).await.expect("never had one"),
            DirectoryBacking::Unbacked { disconnected: vec![] },
            "an org with no Google connection at all has lost nothing and names nobody"
        );
    }

    /// **NEGATIVE — a different connector is a different fact.**
    ///
    /// Dana disconnects Gmail and Slack. Neither has anything to do with
    /// D16's rung 1, and an attribution that keyed on "somebody
    /// disconnected something" would name her for both.
    #[tokio::test]
    async fn disconnecting_another_connector_is_never_a_directory_attribution() {
        let Some(pool) =
            pg_test_pool("disconnecting_another_connector_is_never_a_directory_attribution").await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, _dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, account_id, dana).await;
        audit_disconnect(&pool, account_id, dana, "google_gmail").await;
        audit_disconnect(&pool, account_id, dana, "slack").await;

        assert_eq!(
            directory_backing(&pool, account_id).await.expect("unbacked"),
            DirectoryBacking::Unbacked { disconnected: vec![] },
            "a Gmail or Slack disconnect took nothing from the directory"
        );
    }

    /// **NEGATIVE — a departure belongs to item 16, not here.**
    ///
    /// Dana disconnects Chat AND then leaves. That is exactly the case
    /// `note_member_departure` records durably, naming her, with the
    /// remedy pointed at whoever is left. Naming her again here would put
    /// two notices on one screen for one problem — the precise thing
    /// migration 0243 refuses to do to CS-431's re-consent prompt.
    ///
    /// The membership gate is what keeps the two disjoint, and it is
    /// load-bearing for a second reason: offboarding never touches
    /// `connector_audit_log`, so a departed member's disconnect row
    /// outlives their membership forever.
    #[tokio::test]
    async fn a_member_who_disconnected_and_then_left_is_left_to_the_departure_record() {
        let Some(pool) =
            pg_test_pool("a_member_who_disconnected_and_then_left_is_left_to_the_departure_record")
                .await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, _dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, account_id, dana).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;
        disconnect_chat(&pool, account_id, dana).await;
        remove_member(&pool, account_id, dana).await;

        assert_eq!(
            directory_backing(&pool, account_id).await.expect("unbacked"),
            DirectoryBacking::Unbacked { disconnected: vec![] },
            "an ex-member is a departure, and naming them here would double the notice"
        );
    }

    /// **NEGATIVE — reconnecting ends the attribution, with no hook.**
    ///
    /// Dana disconnects, then reconnects with a PRE-WIDENING grant. She has
    /// a live `google_chat` row again, so she is not "disconnected" — she
    /// is CS-431's case, `missing_scopes`/`ReconsentNotice`, already
    /// rendered on this same page with Reconnect attached to her. The org
    /// is still unbacked (her grant `Lacks` the scope) and still names
    /// nobody, which is what keeps one problem to one notice.
    ///
    /// The audit row is still there and always will be — the audit log is
    /// append-only. Being excluded by present state rather than by deleting
    /// history is the whole reason this needs no resolution path.
    #[tokio::test]
    async fn a_member_who_reconnected_is_no_longer_attributed() {
        let Some(pool) = pg_test_pool("a_member_who_reconnected_is_no_longer_attributed").await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, _dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, account_id, dana).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;
        disconnect_chat(&pool, account_id, dana).await;

        let DirectoryBacking::Unbacked { disconnected } =
            directory_backing(&pool, account_id).await.expect("unbacked")
        else {
            panic!("expected Unbacked");
        };
        assert_eq!(disconnected.len(), 1, "named while she is disconnected");

        reconnect_chat(&pool, account_id, dana, PRE_WIDENING_CHAT_SCOPE).await;
        assert_eq!(
            directory_backing(&pool, account_id).await.expect("unbacked"),
            DirectoryBacking::Unbacked { disconnected: vec![] },
            "a live grant means she reconnected — narrowly, which is CS-431's notice, not this one"
        );

        reconnect_chat(&pool, account_id, dana, &current_chat_scope()).await;
        assert_eq!(
            directory_backing(&pool, account_id).await.expect("backed"),
            DirectoryBacking::Backed,
            "and a full reconnect ends the condition on the next read, with no write hook anywhere"
        );
    }

    /// **NEGATIVE — an unreadable grant refuses to attribute.**
    ///
    /// The `Unknown` asymmetry, carried into the third question this module
    /// asks. Sam's stored scope is blank — the shape a Google refresh
    /// response that omitted `scope` would leave behind — so we cannot say
    /// the org has no backing, and a notice claiming it does would be a
    /// guess. Same refusal `might_still_back_the_directory` already makes
    /// for the alarm.
    #[tokio::test]
    async fn an_unreadable_grant_refuses_to_attribute() {
        let Some(pool) = pg_test_pool("an_unreadable_grant_refuses_to_attribute").await else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, _dana_email) = seed_user(&pool, "dana").await;
        let (sam, _sam_email) = seed_user(&pool, "sam").await;
        add_member(&pool, account_id, dana).await;
        add_member(&pool, account_id, sam).await;
        connect_chat(&pool, account_id, dana, &current_chat_scope()).await;
        connect_chat(&pool, account_id, sam, "   ").await;

        disconnect_chat(&pool, account_id, dana).await;

        assert_eq!(
            directory_backing(&pool, account_id).await.expect("indeterminate"),
            DirectoryBacking::Indeterminate,
            "a grant we could not parse might still be serving rung 1 — never attribute over it"
        );
    }

    /// One org's attribution is none of another's business. The read opens
    /// under `begin_account_scoped` like every other in this module, and
    /// the two new tables it touches (`connector_audit_log`, `users`) must
    /// not widen that.
    #[tokio::test]
    async fn an_attribution_is_scoped_to_its_own_account() {
        let Some(pool) = pg_test_pool("an_attribution_is_scoped_to_its_own_account").await else {
            return;
        };
        let (acme, _) = seed_org(&pool).await;
        let (beta, _) = seed_org(&pool).await;
        let (dana, _dana_email) = seed_user(&pool, "dana").await;
        add_member(&pool, acme, dana).await;
        connect_chat(&pool, acme, dana, &current_chat_scope()).await;
        disconnect_chat(&pool, acme, dana).await;

        let DirectoryBacking::Unbacked { disconnected } =
            directory_backing(&pool, acme).await.expect("acme")
        else {
            panic!("expected Unbacked for Acme");
        };
        assert_eq!(disconnected.len(), 1);
        assert_eq!(
            directory_backing(&pool, beta).await.expect("beta"),
            DirectoryBacking::Unbacked { disconnected: vec![] },
            "Beta never had a directory and has never heard of Dana"
        );
    }

    /// A still-present member whose grant is marked `credential_invalid_at`
    /// is not a live directory backer. Without this filter, death of the
    /// last *valid* grant would be treated as StillCovered, and the next
    /// refresh would close `backing_credential_lost`.
    #[tokio::test]
    async fn an_invalid_remaining_grant_does_not_spare_the_directory_alarm() {
        let Some(pool) =
            pg_test_pool("an_invalid_remaining_grant_does_not_spare_the_directory_alarm").await
        else {
            return;
        };
        let (account_id, _owner) = seed_org(&pool).await;
        let (dana, dana_email) = seed_user(&pool, "dana").await;
        let (sam, _sam_email) = seed_user(&pool, "sam").await;
        for u in [dana, sam] {
            add_member(&pool, account_id, u).await;
            connect_chat(&pool, account_id, u, &current_chat_scope()).await;
        }
        sqlx::query(
            "UPDATE oauth_tokens SET credential_invalid_at = now(), \
                 credential_invalid_reason = 'revoked' \
              WHERE account_id = $1 AND owner_user_id = $2 AND provider = $3",
        )
        .bind(account_id)
        .bind(sam)
        .bind(PROVIDER_GOOGLE_CHAT)
        .execute(&pool)
        .await
        .expect("mark sam invalid");

        remove_member(&pool, account_id, dana).await;
        assert_eq!(
            note_member_departure(&pool, account_id, dana, Some(&dana_email))
                .await
                .expect("dana"),
            DepartureOutcome::Recorded { newly_opened: true },
            "Sam's invalid grant must not count as a remaining directory backer"
        );
        assert_eq!(open_row_count(&pool, account_id).await, 1);

        let open = refresh_open_degradations(&pool, None, account_id)
            .await
            .expect("refresh");
        assert_eq!(
            open.len(),
            1,
            "refresh must not close the row while the only remaining grant is invalid"
        );
        assert_eq!(open[0].condition, CONDITION_BACKING_MEMBER_DEPARTED);
    }
}

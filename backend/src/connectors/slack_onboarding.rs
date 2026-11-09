//! The ONE server-derived Slack onboarding status payload —
//! docs/connectors/slack-onboarding.md §3. Two surfaces render it:
//! `routes::slack::onboarding_status` (`GET /v1/connectors/slack/onboarding`,
//! JSON) and `routes::slack_events`' `app_home_opened` handler (Block Kit,
//! `views.publish`). Both call [`derive_status`] — the whole point of
//! shipping this as one contract instead of two independently-computed
//! checklists is that they can never drift apart on "is this set up" (see
//! that doc's §3 preamble).
//!
//! # Why the pure/impure split
//!
//! Every section below splits into (a) a pure `..._from_...` function that
//! turns already-fetched data into the section's status struct, and (b) a
//! thin async function that fetches that data (DB query or Slack API call)
//! and calls (a). This crate has no HTTP-mock harness (see
//! `connectors::slack`'s own C5 test module for the same constraint), so the
//! two rules this contract most needs tested — "unavailable" is never
//! flattened into a zero count, and huddle "skipped" is counted as normal,
//! not as an error — are tested directly against the pure functions, with no
//! network or database required. The DB-touching halves get their own
//! `ORGT_TEST_BASE_URL`-gated pg tests, same convention as
//! `connectors::slack`'s `verified_identity_matcher_pg_tests`.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::routes::slack::AccountPlane;

/// Which plane the account this status was derived for is on. A thin
/// JSON-friendly mirror of [`AccountPlane`] (kept separate rather than
/// deriving `Serialize` on `AccountPlane` itself — that type lives in
/// `routes::slack` and is `pub(crate)` route-resolution plumbing with no
/// reason to also carry a wire format).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Plane {
    Personal,
    Org,
}

impl From<AccountPlane> for Plane {
    fn from(p: AccountPlane) -> Self {
        match p {
            AccountPlane::Personal => Plane::Personal,
            AccountPlane::Org => Plane::Org,
        }
    }
}

/// Step 1's live-Slack-call outcome — see [`ChannelsState`]'s doc comment for
/// the rule this exists to enforce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelsState {
    Ok,
    Unavailable,
}

/// Step 1 — is the bot in any channel? Live `conversations.list`, never
/// `slack_watched_channels` (docs/connectors/slack-onboarding.md §4).
///
/// **`state: "unavailable"` is not `count: 0`.** A rate-limited or failed
/// `conversations.list` must render as `Unavailable` with `count: None` —
/// never as a zero count, which reads as "the bot is in no channels" and
/// sends someone to re-do work that was already done. `count` and `names`
/// are only ever populated on the `Ok` arm; see [`channels_status_from_result`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ChannelsStatus {
    pub state: ChannelsState,
    pub count: Option<u32>,
    pub names: Vec<String>,
    pub error: Option<String>,
}

/// Pure: turn [`crate::connectors::slack::fetch_bot_member_channels`]'s
/// result into a [`ChannelsStatus`]. Split out from the async caller
/// specifically so the unavailable-vs-zero distinction is unit-testable
/// without a live Slack call (see this module's header).
pub(crate) fn channels_status_from_result(
    result: Result<Vec<(String, String)>, String>,
) -> ChannelsStatus {
    match result {
        Ok(channels) => ChannelsStatus {
            state: ChannelsState::Ok,
            // `count` is authoritative — `names` is a capped preview, never
            // the other way around (docs/connectors/slack-onboarding.md §3).
            count: Some(channels.len() as u32),
            names: channels.iter().take(5).map(|(_, name)| format!("#{name}")).collect(),
            error: None,
        },
        Err(error) => ChannelsStatus {
            state: ChannelsState::Unavailable,
            count: None,
            names: Vec::new(),
            error: Some(error),
        },
    }
}

/// Step 2 — how many of the bot's channels have an EXPLICIT destination
/// (a `slack_watched_channels` row, docs/connectors/slack-onboarding.md §4).
/// `count <= channels.count` is normal — an unrouted channel still ingests
/// via the account-level default, it just has not had a human choice made
/// for it yet. `fanout` is how many of `count` chose "everyone's individual
/// oceans" (`mode = 'fanout'`) rather than a single ocean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct RoutedStatus {
    pub count: u32,
    pub fanout: u32,
}

/// Step 3 — has anything actually been captured? Counts every atom this
/// Slack workspace produced (messages AND huddle transcripts alike — both
/// ingest paths stamp `atoms.workspace_id`, migration `0058`), scoped by
/// `workspace_id` rather than by `source_type`, since `workspace_id` is only
/// ever populated by a Slack ingest path in the first place.
///
/// **`workspace_id` is matched permissively, and the NULL arm carries its own
/// source filter.** The claim above — "both ingest paths stamp
/// `workspace_id`" — is true only from migration `0058` onward. Atoms
/// written before it, and seeded history, carry NULL, and a strict `= $N`
/// did not scope those rows so much as delete them from the answer: in
/// production on 2026-09-09, 19 of one org's 35 Slack atoms were invisible
/// to this count.
///
/// But a bare `OR a.workspace_id IS NULL` is wrong in the other direction,
/// and it was shipped that way for one review round. Re-read the sentence
/// above: this count is "scoped by `workspace_id` rather than by
/// `source_type`, since `workspace_id` is only ever populated by a Slack
/// ingest path". `workspace_id` was doing DOUBLE duty — naming the workspace
/// AND standing in for "this is Slack" — so relaxing it dropped the second
/// job silently. Every Gmail message, Google Chat message and coding session
/// in the account has `workspace_id IS NULL`, and all of them began counting
/// as Slack capture. Hence `AND a.source_type = 'slack'` on the NULL arm
/// only: the stamped arm still needs no source filter, because a non-NULL
/// `workspace_id` is itself the Slack discriminator.
///
/// **Pre-`0058` huddle transcripts are OUT, deliberately.** They are
/// `source_type = 'transcript'` (`workers::huddle`), so the filter above
/// excludes them, and this count's own header says it means to include
/// huddles. Including them would mean admitting `'transcript'` on the NULL
/// arm — and `'transcript'` is also what Teams and meeting-recorder
/// transcripts write, none of which set `workspace_id` at all. That trade
/// swaps a handful of missing pre-`0058` Slack huddles for every meeting
/// transcript in the account counted as Slack capture, which is the larger
/// and more misleading error. Same decision, same reasoning, as
/// `slack_channel_management::fetch_unattributed_atom_count`.
///
/// `graphs.account_id` remains the account boundary (see
/// [`fetch_capture_status`]'s own doc comment), which is what makes the
/// workspace relaxation safe in the first place.
///
/// The same relaxation is applied in
/// `connectors::slack_channel_management::fetch_atom_stats`, and it has to
/// be: the checklist here and the channel page there describe the SAME
/// workspace side by side, and a reader who saw 16 corals on one surface and
/// 35 on the other would have no way to tell which was lying. The residue
/// that genuinely cannot be attributed to a channel is reported separately by
/// that module rather than folded into either total.
///
/// What the relaxation admits, stated plainly: on an account with TWO
/// connected Slack workspaces, a NULL-`workspace_id` atom counts toward both
/// workspaces' totals, because nothing in the row says which one it came
/// from. Over-counting a legacy atom on one checklist is the lesser error
/// against dropping it from both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct CaptureStatus {
    pub atom_count: u64,
    /// `null` exactly when `atom_count == 0` — never a made-up epoch.
    pub first_at: Option<DateTime<Utc>>,
    pub last_at: Option<DateTime<Utc>>,
}

/// One entry of [`HuddlesStatus::top_skip_reasons`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SkipReasonCount {
    pub reason: String,
    pub count: i64,
}

/// Huddle capture health, straight off the `slack_huddle_captures` ledger
/// (migration `0102`'s terminal-state design: every row is either `ingested`
/// or `skipped`, never neither).
///
/// **`skipped` is the NORMAL case, not an error.** Most huddles are too
/// short for Slack to summarise (docs/connectors/slack.md §3.1's "the canvas
/// is often empty" section) — this struct exists to make that answerable
/// ("huddles here aren't being saved, and why"), not to flag it as a fault.
/// Callers must render `skipped`/`top_skip_reasons` as information, never as
/// a warning badge.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct HuddlesStatus {
    pub seen: i64,
    pub ingested: i64,
    pub skipped: i64,
    /// Capped at 3, ordered by count desc (reason asc on ties, for a stable
    /// order across otherwise-tied reasons rather than an arbitrary hashmap
    /// iteration order).
    pub top_skip_reasons: Vec<SkipReasonCount>,
}

/// Pure: build a [`HuddlesStatus`] from raw ledger rows —
/// `(ingested, skip_reason)` per row, where `ingested` is
/// `processed_at.is_some()`. Split out for the same reason
/// [`channels_status_from_result`] is: the "skipped is normal, not an error"
/// shape is worth pinning with a test that needs no database.
pub(crate) fn huddles_status_from_rows(rows: &[(bool, Option<String>)]) -> HuddlesStatus {
    let seen = rows.len() as i64;
    let ingested = rows.iter().filter(|(is_ingested, _)| *is_ingested).count() as i64;
    // Not `seen - ingested`: migration 0102's terminal-state CHECK guarantees
    // every row is one or the other, but computing `skipped` from
    // `skip_reason` directly (rather than by subtraction) means a future
    // third state doesn't silently miscount both this and `ingested` — one
    // wrong assumption stays local to whichever count actually got it wrong.
    let skipped = rows
        .iter()
        .filter(|(is_ingested, reason)| !*is_ingested && reason.is_some())
        .count() as i64;

    // The SAME filter `skipped` uses, not just `reason.is_some()`. Migration
    // 0102's terminal-state CHECK is `processed_at IS NOT NULL OR skip_reason
    // IS NOT NULL` — an OR, so a row may legitimately carry BOTH. Such a row
    // is counted as ingested, excluded from `skipped`, and was still being
    // counted here, so the reason totals could sum ABOVE `skipped` and render
    // as "3 skipped · top reason ×4". Both fields must describe the same set
    // of rows or neither can be trusted.
    let mut counts: HashMap<String, i64> = HashMap::new();
    for (is_ingested, reason) in rows {
        if *is_ingested {
            continue;
        }
        if let Some(r) = reason {
            *counts.entry(r.clone()).or_insert(0) += 1;
        }
    }
    let mut top_skip_reasons: Vec<SkipReasonCount> = counts
        .into_iter()
        .map(|(reason, count)| SkipReasonCount { reason, count })
        .collect();
    // Count desc, then reason asc — a stable, deterministic order (a
    // `HashMap`'s iteration order is not) rather than one that changes
    // between two calls with identical input.
    top_skip_reasons.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));
    top_skip_reasons.truncate(3);

    HuddlesStatus { seen, ingested, skipped, top_skip_reasons }
}

/// Org-plane-only coverage of the verified-email matcher (capability C5,
/// docs/connectors/slack.md §1). `identified` counts CURRENT account members
/// with a confirmed `connector_identities` link for this workspace;
/// `total` is the account's current member count. Never null-on-org — a
/// zero-member or zero-identified account still reports real zeros, same
/// "never report a number we cannot stand behind" rule as [`ChannelsStatus`],
/// just with nothing that can go `Unavailable` here (both reads are plain DB
/// queries, not live Slack calls subject to rate limiting).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct IdentityCoverage {
    pub identified: u32,
    pub total: u32,
}

// ── The org-member view (docs/connectors/slack-onboarding.md §3.1) ───────────
//
// An org MEMBER's setup journey is not the admin's. The admin already
// authorised the app — only an admin can, so the three steps above are
// admin-by-construction — and the member's questions are different ones:
// "does CoralSwarm know which Slack account is me", "is the bot in the
// channels I'm actually in", "has anything of MINE been captured". The
// sections below answer exactly those, per-viewer, and are `None` for
// everyone else.

/// Who is asking. Constructed by the caller, never inferred here — see
/// [`derive_status`]'s doc comment.
///
/// A three-way enum rather than a `(role, Option<user_id>)` pair so the
/// member branch cannot be reached without the caller identity it needs, and
/// a viewer carrying that identity cannot be silently rendered as an admin:
/// both mistakes stop compiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Viewer<'a> {
    /// The whole-account view: a personal account, or an org owner/admin.
    Account { role: &'a str },
    /// One org member looking at their own slice. `user_id` is the caller's
    /// CoralSwarm user id — the member sections are scoped to it.
    Member { user_id: Uuid },
    /// A caller that resolved an ACCOUNT but never resolved a ROLE — today
    /// only `routes::slack_events`' App Home tab, which is a
    /// signature-authenticated webhook with no Clerk request context and
    /// renders one generic view for every viewer (docs/connectors/
    /// slack-onboarding.md §5.2). Serialises `viewer_role: null`, which is
    /// the honest answer: §3's "never report a number we cannot stand
    /// behind" applies to a role claim just as much as to a count, and a
    /// fabricated `"owner"` here would be exactly that.
    Unresolved,
}

impl Viewer<'_> {
    /// What goes on the wire as `viewer_role`. `None` only for
    /// [`Viewer::Unresolved`]; `GET /v1/connectors/slack/onboarding` always
    /// builds one of the other two, so that endpoint's payload always
    /// carries a role.
    fn role_label(&self) -> Option<String> {
        match self {
            Viewer::Account { role } => Some((*role).to_string()),
            // Not read off a field: a `Member` viewer IS the member role, by
            // construction. There is no way to build one that claims
            // otherwise.
            Viewer::Member { .. } => Some("member".to_string()),
            Viewer::Unresolved => None,
        }
    }
}

/// Member step 1 — does CoralSwarm know which Slack account is this person?
///
/// `linked` is true exactly when a CONFIRMED `connector_identities` row
/// exists for this workspace with `user_id` set. Migration `0104`'s
/// provenance CHECK makes `user_id`/`link_method`/`link_verified_at`
/// all-or-nothing, so a set `user_id` IS the confirmation — there is no
/// half-linked state to distinguish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct MemberLink {
    pub linked: bool,
    /// The Slack member id this person was matched to — `None` when
    /// `linked == false`.
    pub slack_user_id: Option<String>,
    /// How the link was established (`'verified_email'` today). Surfaced so
    /// the client can say "linked automatically" for a match nobody had to
    /// perform, rather than crediting the member with an action they never
    /// took.
    pub link_method: Option<String>,
}

impl MemberLink {
    fn unlinked() -> Self {
        Self { linked: false, slack_user_id: None, link_method: None }
    }
}

/// Pure: does this (viewer, plane) pair get a [`MemberStatus`] at all, and
/// if so for whom?
///
/// Split out of [`derive_status`] for this module's usual reason (see its
/// header): the rule "the member sections exist for an ORG MEMBER and nobody
/// else" is the one thing here that must not drift, and testing it inside
/// `derive_status` would need a live Slack call. `Some(user_id)` is also the
/// only way to obtain the caller identity the member reads need, so a viewer
/// who does not qualify cannot be scoped-read for by accident.
pub(crate) fn member_branch_viewer(viewer: Viewer<'_>, plane: AccountPlane) -> Option<Uuid> {
    match (viewer, plane) {
        (Viewer::Member { user_id }, AccountPlane::Org) => Some(user_id),
        // A personal account has no member oceans and no identity matcher —
        // and `routes::slack::onboarding_status` cannot build a `Member`
        // viewer there anyway (a personal role is always "owner"). Pinned
        // here regardless, rather than trusting that call site never to
        // change.
        (Viewer::Member { .. }, AccountPlane::Personal) => None,
        (Viewer::Account { .. } | Viewer::Unresolved, _) => None,
    }
}

/// Pure: member step 2's three-way outcome, from the two live lookups.
///
/// `bot_channels` is step 1's roster and `member_channels` the caller's own,
/// each `None` when its lookup failed (or was never made). Split out so the
/// two rules this step exists to keep straight are unit-testable with no
/// network:
///
/// * **unlinked is not zero, and costs no Slack call.** With no confirmed
///   identity there is no Slack id to ask about; the answer is "does not
///   apply yet", not "the bot is in none of your channels".
/// * **unavailable is not zero, in EITHER direction.** The rendered number
///   is an INTERSECTION, so losing either side loses it. Falling back to the
///   member's own channel count would silently answer a different question
///   ("how many channels are you in") in the slot reserved for this one.
fn my_channels_from_lookups(
    linked: bool,
    bot_channels: Option<&[(String, String)]>,
    member_channels: Option<&[String]>,
) -> MyChannelsStatus {
    if !linked {
        return MyChannelsStatus {
            state: MyChannelsState::Unlinked,
            count: None,
            containers: None,
        };
    }
    match (bot_channels, member_channels) {
        (Some(bot), Some(mine)) => {
            // Keep the NAME alongside the id (chat-per-user plan §4.2 step 1:
            // widen this from a count to a list). The backfill offer needs to
            // name each container a person can choose, and the intersection is
            // the only place both halves are in hand at once — step 1's bot
            // roster carries `(id, name)`, `users.conversations` carries ids
            // only. Recovering the names later would mean a second Slack call
            // per view, on an endpoint whose doc comment already warns that a
            // timer here burns workspace rate limit for every open tab.
            let bot_names: std::collections::HashMap<&str, &str> =
                bot.iter().map(|(id, name)| (id.as_str(), name.as_str())).collect();
            // Deduplicated on the member's side too: Slack paginates, and a
            // channel appearing twice across a cursor boundary must not
            // count twice.
            let mine_set: std::collections::HashSet<&str> =
                mine.iter().map(String::as_str).collect();

            let mut containers: Vec<MyContainer> = mine_set
                .iter()
                .filter_map(|id| {
                    bot_names.get(*id).map(|name| MyContainer {
                        id: (*id).to_string(),
                        name: (*name).to_string(),
                    })
                })
                .collect();
            // Deterministic order, by name then id. `mine_set` is a HashSet,
            // so without this the list arrives in a different order on every
            // request — which in a checkbox list reads to the person as rows
            // jumping around under the cursor, and makes any UI test flaky
            // for a reason nobody would guess from the diff.
            containers.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));

            // `count` stays, and stays DERIVED from the same list rather than
            // counted separately — two independent computations of "how many"
            // is exactly how a disclosure drifts from the thing it describes.
            let count = containers.len() as u32;
            MyChannelsStatus {
                state: MyChannelsState::Ok,
                count: Some(count),
                containers: Some(containers),
            }
        }
        _ => MyChannelsStatus {
            state: MyChannelsState::Unavailable,
            count: None,
            containers: None,
        },
    }
}

/// [`MyChannelsStatus`]'s three-way outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MyChannelsState {
    Ok,
    /// A live lookup failed — the SAME "unavailable is not zero" rule
    /// [`ChannelsState::Unavailable`] exists for, applied per-viewer.
    Unavailable,
    /// No confirmed identity, so there is no "my channels" to ask about and
    /// NO Slack call is made. Distinct from `Unavailable`: nothing failed,
    /// the question simply does not apply yet. Rendering this as zero would
    /// tell a member the bot is in none of their channels when we never
    /// looked.
    Unlinked,
}

/// Member step 2 — how many of the channels THIS member is in does the bot
/// also sit in? The intersection of one `users.conversations` call
/// ([`crate::connectors::slack::fetch_user_member_channel_ids`]) with step
/// 1's already-fetched bot roster — one extra Slack call per member view, not
/// a second roster walk.
/// One container this member and the agent are both in — the unit the
/// backfill offer (plan §4.2) is made per.
///
/// **No destination field, deliberately.** Under plan §2.1 every container
/// fans out into each linked member's own private ocean, so there is no
/// destination to filter on and no destination to show: every container in
/// this list is a valid offer, and the ocean it would land in is always the
/// asking member's own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct MyContainer {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct MyChannelsStatus {
    pub state: MyChannelsState,
    /// `Some` only on the `Ok` arm — never a zero standing in for
    /// "unavailable" or "unlinked".
    pub count: Option<u32>,
    /// The containers themselves, `Some` on exactly the same arm as `count`
    /// and always the list `count` was derived from. The three-way state is
    /// what keeps this honest: an empty `Some(vec![])` means "we looked and
    /// the agent is in none of your channels", while `None` with state
    /// `unavailable` means "we could not look". Collapsing those two into an
    /// empty list is the same class of bug as reporting a scope-broken
    /// workspace's members as unmatched — an absence rendered as an answer.
    pub containers: Option<Vec<MyContainer>>,
}

/// Everything the org-member journey needs, and nothing the admin one does.
/// `None` for personal accounts and for org owners/admins.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct MemberStatus {
    pub linked: MemberLink,
    pub my_channels: MyChannelsStatus,
    /// Member step 3 — what has landed in THIS member's own ocean from this
    /// workspace. A tenant-plane read scoped to the caller's primary
    /// `kind = 'member'` graph; a member with no member ocean yet reports a
    /// real zero, not an error.
    pub my_capture: CaptureStatus,
}

/// The full §3 payload for one connected Slack workspace.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SlackOnboardingStatus {
    pub connected: bool,
    pub workspace_id: Option<String>,
    pub workspace_name: Option<String>,
    pub plane: Plane,
    /// `None` only when `connected == false` — there is nothing to check
    /// once there is no workspace. Always `Some` once connected (never
    /// individually omitted): a failed live call still fills its slot with
    /// its own `Unavailable`/error shape rather than leaving the whole
    /// section absent, so a client can tell "not connected" apart from "one
    /// section briefly failed" at a glance.
    pub channels: Option<ChannelsStatus>,
    pub routed: Option<RoutedStatus>,
    pub capture: Option<CaptureStatus>,
    pub huddles: Option<HuddlesStatus>,
    /// Org plane only — `None` on personal (docs/connectors/
    /// slack-onboarding.md §3's own comment on this field). The personal
    /// plane never needed identity mapping at all (docs/connectors/slack.md
    /// §1's C5 row): the connecting member's own Slack identity is already
    /// proven at OAuth time.
    pub identity: Option<IdentityCoverage>,
    /// The caller's own role on this account, straight off
    /// [`Viewer::role_label`]. `null` ONLY for [`Viewer::Unresolved`] (the
    /// Slack App Home, which never serialises this payload) — the HTTP
    /// endpoint always sends a string.
    ///
    /// Present on the not-connected shape too, deliberately: the connectors
    /// page has to decide between a Connect button and "ask an admin"
    /// BEFORE anything is connected, and that decision is the one place a
    /// role is most load-bearing.
    pub viewer_role: Option<String>,
    /// Org-MEMBER viewers only — see [`MemberStatus`]. `None` for personal
    /// accounts, for org owners/admins, and whenever nothing is connected.
    pub member: Option<MemberStatus>,
}

impl SlackOnboardingStatus {
    /// The "nothing to check yet" shape — no workspace connected at all.
    /// docs/connectors/slack-onboarding.md §1's retraction starts here: a
    /// disconnected account must never look like a connected-but-idle one.
    ///
    /// `member` stays `None` even for a member viewer: every member section
    /// is derived FROM a connected workspace (its identity link, its
    /// channels, its atoms), so there is nothing to report — the same reason
    /// `channels`/`routed`/`capture` are `None` here. `viewer_role` is the
    /// one field that survives, because it is a fact about the CALLER rather
    /// than about the workspace.
    pub(crate) fn not_connected(plane: Plane, viewer: Viewer<'_>) -> Self {
        Self {
            connected: false,
            workspace_id: None,
            workspace_name: None,
            plane,
            channels: None,
            routed: None,
            capture: None,
            huddles: None,
            identity: None,
            viewer_role: viewer.role_label(),
            member: None,
        }
    }
}

// ── In-process TTL cache for the live Slack lookups ──────────────────────────
//
// `GET /v1/connectors/slack/onboarding` is polled — every mounted client
// refetches on focus/visibility, and production logs show ~60s intervals per
// client. Every one of those calls used to mean a fresh
// `conversations.list`, and the member branch added on top of that a fresh
// `users.conversations`. Two live Slack calls per client per minute per
// workspace is how a connector walks into its own rate limit while telling
// nobody anything new.
//
// A short TTL collapses that back to at most one call per key per window.
//
// ── The cost, stated plainly ──
// This DOES make the checklist stale by up to the TTL, and the step it makes
// stale is the highest-drop-off one in the whole flow ("invite the bot"). A
// user who invites the bot and immediately hits Refresh can see the old
// answer for the rest of the window. Accepted, not overlooked, on three
// grounds: the window is shorter than the poll interval it exists to
// collapse; the client's own focus-refetch floor already delays that same
// update by up to 15s (`MIN_REFETCH_INTERVAL_MS`); and the cache is
// PER-PROCESS across N machines, so a repeat request frequently misses it
// anyway. If the staleness ever bites in practice, the fix is an explicit
// cache-bypass on the manual-refresh path — NOT a shorter TTL, which would
// just trade the whole benefit away for a partial fix.
//
// In-process, so N backend machines each keep their own window rather than
// sharing one. Same deliberate trade `connectors::slack`'s
// `should_send_unmatched_notice` makes: bounded and small, versus a schema
// change or an external cache for a value whose staleness ceiling is under a
// minute.

/// How long a successful Slack lookup is reused. Under the ~60s client poll
/// interval, so a poll landing after the window still does real work — this
/// bounds concurrent/multi-client fan-out rather than halving any single
/// client's refresh rate.
const SLACK_LOOKUP_TTL: Duration = Duration::from_secs(45);

/// Read a live value back if it is still inside `ttl`, sweeping expired
/// entries as it goes (this map is keyed by workspace/viewer and would
/// otherwise grow for the lifetime of the process — the same opportunistic
/// `retain` `should_send_unmatched_notice` does).
///
/// A poisoned mutex reads as a miss: another task panicked mid-update, and
/// making a real Slack call is strictly better than serving whatever was in
/// there.
fn cache_lookup<K, V>(map: &Mutex<HashMap<K, (Instant, V)>>, key: &K, ttl: Duration) -> Option<V>
where
    K: Eq + Hash,
    V: Clone,
{
    let now = Instant::now();
    let mut guard = map.lock().ok()?;
    guard.retain(|_, (at, _)| now.duration_since(*at) < ttl);
    guard.get(key).map(|(_, v)| v.clone())
}

/// Store a value against "now". Callers only ever store SUCCESSES — see
/// [`cached_bot_channels`].
fn cache_store<K, V>(map: &Mutex<HashMap<K, (Instant, V)>>, key: K, value: V)
where
    K: Eq + Hash,
{
    if let Ok(mut guard) = map.lock() {
        guard.insert(key, (Instant::now(), value));
    }
}

/// Step 1's roster, keyed by `(account_id, workspace_id)` — the pair that
/// identifies one `slack_integrations` row, i.e. one bot token.
#[allow(clippy::type_complexity)]
fn bot_channels_cache() -> &'static Mutex<HashMap<(Uuid, String), (Instant, Vec<(String, String)>)>>
{
    static C: std::sync::OnceLock<
        Mutex<HashMap<(Uuid, String), (Instant, Vec<(String, String)>)>>,
    > = std::sync::OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The member branch's `users.conversations` result, keyed by
/// `(account_id, workspace_id, slack_user_id)` — per VIEWER, since the whole
/// answer is "the channels THIS person is in". Keying it any coarser would
/// serve one member another member's channel count.
#[allow(clippy::type_complexity)]
fn member_channels_cache()
-> &'static Mutex<HashMap<(Uuid, String, String), (Instant, Vec<String>)>> {
    static C: std::sync::OnceLock<Mutex<HashMap<(Uuid, String, String), (Instant, Vec<String>)>>> =
        std::sync::OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// [`crate::connectors::slack::fetch_bot_member_channels`], memoised for
/// [`SLACK_LOOKUP_TTL`].
///
/// **Successes only — a failure is never cached.** A cached `Err` would pin
/// `state: "unavailable"` on the checklist for the whole window after a
/// single transient rate-limit, so a workspace that recovered a second later
/// would keep being told we cannot see it. That is the mirror image of the
/// staleness `connectors::slack`'s `verified_identity_cache` refuses for the
/// same reason, and the poll interval already bounds how often a failing
/// workspace retries.
/// `pub(crate)` — reused by
/// `connectors::slack_channel_management::derive_channel_management`, which
/// called `crate::connectors::slack::fetch_bot_member_channels` DIRECTLY and
/// so spent a live `conversations.list` on every load of the channel page
/// while the checklist beside it was served from this memo. That also made
/// `slack_channel_management`'s own header claim — "calls the SAME
/// `fetch_bot_member_channels` underneath, so the two surfaces can never
/// disagree about whether the bot can currently see its channel list" —
/// false in exactly the window that matters: one surface holding a cache hit
/// while the other took a fresh rate-limit failure. Routing both through this
/// ONE memo makes the claim true within [`SLACK_LOOKUP_TTL`], and is the
/// roster half of the same one-cache-per-question rule
/// [`cached_member_channels`] already follows for membership.
pub(crate) async fn cached_bot_channels(
    http: &reqwest::Client,
    control_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
) -> Result<Vec<(String, String)>, String> {
    let key = (account_id, workspace_id.to_string());
    if let Some(hit) = cache_lookup(bot_channels_cache(), &key, SLACK_LOOKUP_TTL) {
        return Ok(hit);
    }
    let fresh = match crate::connectors::slack::fetch_bot_member_channels(http, bot_token).await {
        Ok(v) => v,
        Err(e) => {
            // `fetch_bot_member_channels` formats Slack `ok:false` as
            // "conversations.list Slack error: {code}". Classify the raw
            // code against the stored account-wide bot token.
            if let Some(code) = e.strip_prefix("conversations.list Slack error: ") {
                crate::connectors::slack::record_if_stored_slack_revoked(
                    control_pool,
                    account_id,
                    code,
                )
                .await;
            }
            return Err(e);
        }
    };
    cache_store(bot_channels_cache(), key, fresh.clone());
    Ok(fresh)
}

/// [`crate::connectors::slack::fetch_user_member_channel_ids`], memoised for
/// [`SLACK_LOOKUP_TTL`]. Successes only, for the same reason as
/// [`cached_bot_channels`].
///
/// `pub(crate)` — reused by `connectors::slack_channel_management` (both the
/// read-side member row filter and, via `resolve_member_channel_scope`, the
/// write-side gate in `routes::slack`) so a member's "which channels am I in"
/// answer is resolved through this ONE cache everywhere it is asked, rather
/// than a second `users.conversations` cache with its own TTL and its own
/// staleness window drifting from this one.
///
/// `force_fresh` skips the cache LOOKUP but still stores the result: write
/// authorization must not ride a 45-second-old membership answer (a member
/// removed from a channel could re-route it for the rest of the TTL window —
/// CWE-613 shape), so the write gate passes `true` and pays one live call per
/// action; the read page keeps the cache and inherits the fresher entry the
/// write just stored. Documented staleness: reads lag membership changes by
/// at most [`SLACK_LOOKUP_TTL`]; writes never do.
pub(crate) async fn cached_member_channels(
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
    slack_user_id: &str,
    force_fresh: bool,
) -> Result<Vec<String>, String> {
    let key = (account_id, workspace_id.to_string(), slack_user_id.to_string());
    if !force_fresh {
        if let Some(hit) = cache_lookup(member_channels_cache(), &key, SLACK_LOOKUP_TTL) {
            return Ok(hit);
        }
    }
    let fresh =
        crate::connectors::slack::fetch_user_member_channel_ids(http, bot_token, slack_user_id)
            .await?;
    cache_store(member_channels_cache(), key, fresh.clone());
    Ok(fresh)
}

/// Derive the full status for one connected Slack workspace. The caller has
/// already resolved a `slack_integrations` row (workspace id/name/bot
/// token) — this function does not check "is Slack connected at all"; see
/// [`SlackOnboardingStatus::not_connected`] for that branch, which both
/// callers (`routes::slack::onboarding_status` and `routes::slack_events`'
/// `app_home_opened` handler) take BEFORE reaching here.
///
/// # Pools
///
/// Same two-pool split as [`crate::connectors::slack::resolve_channel_destinations`]
/// / [`crate::connectors::slack::resolve_org_fanout_destinations`] — read
/// those functions' doc comments for the full rationale. Short version:
///
/// * `control_pool` — `slack_huddle_captures` (CONTROL-only; migration
///   `0102`'s header — huddle capture is consumer-only, so the tenant-tree
///   copy of this table stays inert) and `account_members` (CONTROL-only,
///   no tenant-tree equivalent at all).
/// * `tenant_pool` — `slack_watched_channels`/`graphs`/`atoms`/
///   `connector_identities` (present, and populated, on both trees — the
///   plane this workspace's content actually lands on).
///
/// On the consumer (Personal) plane, callers pass the SAME pool for both,
/// exactly like every other caller of that two-pool convention. On the Org
/// plane (`ORG_SLACK_OAUTH_ENABLED` is live in production, so this is no
/// longer a hypothetical) a caller MUST pass the account's real tenant pool
/// as `tenant_pool` — passing `control_pool` for both, as this function's
/// call sites once assumed was harmless "for every account this can reach
/// today", silently reads `slack_watched_channels`/`atoms`/`graphs` off the
/// wrong database and renders a real org's live ingest as "nothing captured
/// yet". `routes::slack::onboarding_status`'s request-path caller gets this
/// for free from the binding gate's own per-request tenant pool
/// (`Tenant::pool`); `routes::slack_events`' `app_home_opened` handler has no
/// such gate (it is a signature-authenticated webhook, not a Clerk-JWT
/// request) and resolves its own tenant pool explicitly — see that
/// function's doc comment's "Pool split" section for how.
///
/// `Err` is a hard failure of one of the DB reads — this function does not
/// itself try to keep partial results on a DB error (unlike the live Slack
/// call, which already fails soft into `ChannelsStatus::Unavailable` before
/// it ever reaches here); a DB error propagates and both callers turn it into
/// a 500 / a swallowed `warn!`, respectively.
///
/// # Viewers
///
/// `viewer` decides whether the org-member sections are derived at all, and
/// what `viewer_role` says. It is passed IN rather than resolved here for the
/// same reason `derive_channel_management` takes `viewer_role`: the role has
/// already been re-checked live against `account_members` by `require_auth`
/// on the request path (`middleware::auth`'s `live_org_membership`), and a
/// second query here could only disagree with the value the request was
/// already gated on. The member sections are derived ONLY for
/// [`Viewer::Member`] AND `plane == Org` — a personal account has no member
/// oceans and no identity matcher to be covered by.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn derive_status(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    plane: AccountPlane,
    viewer: Viewer<'_>,
    workspace_id: &str,
    workspace_name: &str,
    bot_token: &str,
) -> Result<SlackOnboardingStatus, String> {
    let channels_result =
        cached_bot_channels(http, control_pool, account_id, workspace_id, bot_token).await;
    let channels = channels_status_from_result(channels_result.clone());

    let routed = fetch_routed_status(tenant_pool, account_id, workspace_id).await?;
    let capture = fetch_capture_status(tenant_pool, account_id, workspace_id).await?;
    let huddles = fetch_huddles_status(control_pool, account_id).await?;

    let identity = match plane {
        AccountPlane::Org => {
            Some(fetch_identity_coverage(control_pool, tenant_pool, account_id, workspace_id).await?)
        }
        AccountPlane::Personal => None,
    };

    let member = match member_branch_viewer(viewer, plane) {
        Some(caller_user_id) => Some(
            derive_member_status(
                tenant_pool,
                http,
                account_id,
                caller_user_id,
                workspace_id,
                bot_token,
                channels_result.as_deref().ok(),
            )
            .await?,
        ),
        None => None,
    };

    Ok(SlackOnboardingStatus {
        connected: true,
        workspace_id: Some(workspace_id.to_string()),
        workspace_name: Some(workspace_name.to_string()),
        plane: plane.into(),
        channels: Some(channels),
        routed: Some(routed),
        capture: Some(capture),
        huddles: Some(huddles),
        identity,
        viewer_role: viewer.role_label(),
        member,
    })
}

/// Build one org member's [`MemberStatus`].
///
/// `bot_channels` is step 1's ALREADY-FETCHED roster (`None` when that live
/// call failed) — threaded in rather than re-fetched so the member branch
/// costs exactly one extra Slack call. It is also what makes
/// [`MyChannelsState::Unavailable`] correct in both directions: if we cannot
/// see the bot's channels, we cannot compute the intersection either, and
/// reporting the member's own channel count in its place would answer a
/// question nobody asked ("how many channels are you in") as though it were
/// the one on screen ("how many of them is the bot reading").
///
/// Order matters: the identity lookup runs FIRST and short-circuits. An
/// unlinked member makes NO Slack call at all — there is no Slack id to ask
/// about, and burning a rate-limit slice to produce a `null` count would be
/// pure waste on the single most common member state (day one, before the
/// matcher has ever run for them).
async fn derive_member_status(
    tenant_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    caller_user_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
    bot_channels: Option<&[(String, String)]>,
) -> Result<MemberStatus, String> {
    let link = fetch_member_link(tenant_pool, workspace_id, caller_user_id).await?;

    // `None` unless we both HAVE a Slack id to ask about and the call
    // succeeded — the two `None` reasons are told apart by
    // `my_channels_from_lookups`, which sees `linked` separately.
    let mine: Option<Vec<String>> = match &link.slack_user_id {
        None => None,
        Some(slack_user_id) => {
            match cached_member_channels(http, account_id, workspace_id, bot_token, slack_user_id, false)
                .await
            {
                Ok(mine) => Some(mine),
                Err(e) => {
                    // Logged rather than propagated: one member's channel
                    // count failing must not 500 the whole checklist, whose
                    // other sections are all fine. Same fail-soft direction
                    // step 1 already takes.
                    tracing::warn!(
                        error = %e,
                        account_id = %account_id,
                        "users.conversations failed for a member onboarding view"
                    );
                    None
                }
            }
        }
    };
    let my_channels = my_channels_from_lookups(link.linked, bot_channels, mine.as_deref());

    let my_capture =
        fetch_member_capture_status(tenant_pool, account_id, caller_user_id, workspace_id).await?;

    Ok(MemberStatus { linked: link, my_channels, my_capture })
}

/// Member step 1's DB read — the caller's own CONFIRMED `connector_identities`
/// row for this workspace, if any.
///
/// `connector_identities` carries no RLS policy at all (deliberately —
/// `migrations_tenant/0009_tenant_rls.sql`'s uncovered-tables list: no
/// account/graph column to key a policy on), so this reads `tenant_pool`
/// unscoped, filtered by `(provider, workspace_id, user_id)` — the same
/// predicate shape [`fetch_identity_coverage`] uses, narrowed to one user.
///
/// `user_id = $2` is itself the confirmation test: migration `0104`'s
/// provenance CHECK makes `user_id`/`link_method`/`link_verified_at`
/// all-or-nothing, so a row matching this predicate is confirmed by
/// construction and there is no separate "is it verified" condition to
/// forget.
/// `pub(crate)` — also the write-side gate's linking test
/// (`slack_channel_management::resolve_member_channel_scope`): "is this
/// member linked" must have exactly one definition, and it is this
/// predicate.
pub(crate) async fn fetch_member_link(
    tenant_pool: &PgPool,
    workspace_id: &str,
    caller_user_id: Uuid,
) -> Result<MemberLink, String> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT external_user_id, link_method FROM connector_identities \
         WHERE provider = 'slack' AND workspace_id = $1 AND user_id = $2 LIMIT 1",
    )
    .bind(workspace_id)
    .bind(caller_user_id)
    .fetch_optional(tenant_pool)
    .await
    .map_err(|e| format!("member link query: {e}"))?;

    Ok(match row {
        Some((external_user_id, link_method)) => {
            MemberLink { linked: true, slack_user_id: Some(external_user_id), link_method }
        }
        None => MemberLink::unlinked(),
    })
}

/// Member step 3's DB read — this workspace's atoms that landed in the
/// CALLER'S OWN member ocean.
///
/// **A TENANT-PLANE read.** `graphs`/`atoms` live on the account's tenant
/// tree; passing the control pool here would read another database entirely
/// and report an org member's real capture as zero — the same pool-split
/// mistake the App Home shipped once and `derive_status`'s own "# Pools"
/// section exists to prevent. Scoped with `begin_account_scoped` like every
/// other tenant read in this module.
///
/// The ocean is resolved by
/// [`crate::connectors::slack_channel_management::fetch_caller_member_ocean`]
/// — reused, not re-written, so this and the channel table can never disagree
/// about which ocean is "yours". A member with no primary member ocean yet
/// reports an honest zero rather than an error: that is a real, common state
/// (they have not been seated anywhere), not a failure.
///
/// The `graphs` join carries BOTH boundaries as explicit predicates — the
/// account's, and the owner's — with `begin_account_scoped`'s RLS policy as
/// the second layer, exactly as [`fetch_capture_status`]'s doc comment
/// describes for the account-wide version.
async fn fetch_member_capture_status(
    tenant_pool: &PgPool,
    account_id: Uuid,
    caller_user_id: Uuid,
    workspace_id: &str,
) -> Result<CaptureStatus, String> {
    let Some(graph_id) = crate::connectors::slack_channel_management::fetch_caller_member_ocean(
        tenant_pool,
        account_id,
        caller_user_id,
    )
    .await?
    else {
        return Ok(CaptureStatus { atom_count: 0, first_at: None, last_at: None });
    };

    let mut tx = crate::tenancy::scope::begin_tenant_scoped(tenant_pool, account_id, caller_user_id)
        .await
        .map_err(|e| format!("member capture status scope: {e}"))?;
    let row = sqlx::query(
        "SELECT count(*) AS atom_count, min(a.created_at) AS first_at, \
         max(a.created_at) AS last_at \
         FROM atoms a JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND g.id = $2 \
           AND (a.workspace_id = $3 \
                OR (a.workspace_id IS NULL AND a.source_type = 'slack'))",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(workspace_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("member capture status query: {e}"))?;
    tx.rollback().await.ok();

    let atom_count: i64 = row.get("atom_count");
    Ok(CaptureStatus {
        atom_count: atom_count as u64,
        first_at: row.get("first_at"),
        last_at: row.get("last_at"),
    })
}

/// Step 2's DB read — see [`RoutedStatus`]'s doc comment for what it counts.
///
/// Scoped: `slack_watched_channels` carries migration `0101`'s
/// `account_isolation` RLS policy, whose `current_setting('app.account_id')
/// ::uuid` cast ERRORS on an unscoped connection rather than returning zero
/// rows — the exact bug that left `post_destination_prompt` silently dead in
/// production for weeks (`routes::slack_events.rs`'s long comment at the read
/// this mirrors). `begin_account_scoped` is not optional here.
async fn fetch_routed_status(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<RoutedStatus, String> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| format!("routed status scope: {e}"))?;
    let row = sqlx::query(
        "SELECT count(*) AS total, count(*) FILTER (WHERE mode = 'fanout') AS fanout \
         FROM slack_watched_channels WHERE account_id = $1 AND workspace_id = $2",
    )
    .bind(account_id)
    .bind(workspace_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("routed status query: {e}"))?;
    tx.rollback().await.ok();

    let total: i64 = row.get("total");
    let fanout: i64 = row.get("fanout");
    Ok(RoutedStatus { count: total as u32, fanout: fanout as u32 })
}

/// Step 3's DB read — see [`CaptureStatus`]'s doc comment. Scoped exactly
/// like [`fetch_routed_status`] above: `atoms` carries a graph-mediated
/// `account_isolation` policy (migration `0083`), so this runs inside
/// `begin_account_scoped` too.
///
/// **The `graphs` join carries the account boundary; the scope wrapper is the
/// second layer, not the first.** An earlier version of this comment claimed
/// "RLS is what actually enforces the account boundary here, the
/// `workspace_id` predicate only narrows within it" and the query filtered on
/// `workspace_id` alone. Both halves of that were wrong. (a) `workspace_id`
/// is not an account discriminator at all: two DIFFERENT CoralSwarm accounts
/// can each connect the SAME Slack workspace (`slack_integrations` is
/// `UNIQUE (account_id, workspace_id)`, not unique on `workspace_id`), so
/// each account's onboarding checklist reported the other's capture count and
/// first/last-capture timestamps. (b) At the time, RLS enforced nothing:
/// the application still connected as its owner/`BYPASSRLS` role, so
/// `begin_account_scoped` set `app.account_id` and the policy was skipped
/// entirely, leaving the explicit `g.account_id = $1` predicate as the only
/// real protection. OPS-4 HAS SINCE RUN (verified in production 2026-08-04:
/// `DATABASE_URL`'s user is the non-`BYPASSRLS` `coralswarm_app` role, and an
/// unscoped query against an RLS-covered table now errors instead of
/// returning all rows) — `0083_consumer_rls`'s own header predates that and
/// is stale; see `scripts/check_rls_coverage.sh`'s header. The two layers now
/// agree: the `graphs` join carries the account boundary as the first layer,
/// and the scope wrapper makes the RLS policy a live second layer.
/// `atoms.graph_id` is `NOT NULL REFERENCES
/// graphs(id)` (migration `0003`), so the inner join drops no rows.
async fn fetch_capture_status(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<CaptureStatus, String> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| format!("capture status scope: {e}"))?;
    let row = sqlx::query(
        "SELECT count(*) AS atom_count, min(a.created_at) AS first_at, \
         max(a.created_at) AS last_at \
         FROM atoms a JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 \
           AND (a.workspace_id = $2 \
                OR (a.workspace_id IS NULL AND a.source_type = 'slack'))",
    )
    .bind(account_id)
    .bind(workspace_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("capture status query: {e}"))?;
    tx.rollback().await.ok();

    let atom_count: i64 = row.get("atom_count");
    Ok(CaptureStatus {
        atom_count: atom_count as u64,
        first_at: row.get("first_at"),
        last_at: row.get("last_at"),
    })
}

/// Huddle ledger read — see [`HuddlesStatus`]'s doc comment. `slack_huddle_captures`
/// carries `account_isolation` RLS on the CONTROL plane (migration `0083`;
/// the tenant-tree copy is deliberately inert, migration `0102`'s header), so
/// this reads `control_pool`, scoped, and NOT `tenant_pool`.
///
/// **Known limitation, not a bug**: `slack_huddle_captures` has no
/// `workspace_id` column (only `account_id`/`channel_id`/`huddle_thread_ts` —
/// migration `0024`'s original shape, unchanged by `0102`'s ledger rewrite),
/// so an account with MORE THAN ONE connected Slack workspace gets huddle
/// counts blended across all of them, unlike [`fetch_capture_status`]'s
/// `atoms.workspace_id` filter. `slack_integrations` explicitly allows this
/// (`UNIQUE (account_id, workspace_id)`, not `UNIQUE (account_id)`), so the
/// case is real, just rare and pre-existing (not introduced by this PR) —
/// flagged here rather than silently narrowed with a filter this table
/// cannot actually support.
async fn fetch_huddles_status(control_pool: &PgPool, account_id: Uuid) -> Result<HuddlesStatus, String> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| format!("huddles status scope: {e}"))?;
    let rows: Vec<(Option<DateTime<Utc>>, Option<String>)> = sqlx::query_as(
        "SELECT processed_at, skip_reason FROM slack_huddle_captures WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("huddles status query: {e}"))?;
    tx.rollback().await.ok();

    let normalized: Vec<(bool, Option<String>)> =
        rows.into_iter().map(|(processed_at, reason)| (processed_at.is_some(), reason)).collect();
    Ok(huddles_status_from_rows(&normalized))
}

/// Identity coverage read (org plane only) — see [`IdentityCoverage`]'s doc
/// comment. Mirrors `connectors::slack::resolve_org_fanout_destinations`'
/// first two steps exactly (including its read-order note below), but stops
/// short of that function's `resolve_primary_graph` pass: THIS number is
/// "coverage of the verified-email matcher" (does a confirmed link exist at
/// all), not "coverage of the matcher AND a resolvable destination ocean" —
/// a deliberately looser, display-only question, so it does not need that
/// function's per-member graph lookups.
///
/// ── READ ORDER: identities (TENANT) BEFORE members (CONTROL) ──
/// Same reasoning as `resolve_org_fanout_destinations`'s own header: two
/// reads on two separate pools have no shared snapshot, and reading members
/// LAST means a member removed mid-read is correctly absent from the
/// intersection rather than incorrectly counted as identified. This is a
/// read-only display value (no seat is granted or revoked here), so the
/// residual race is lower-stakes than in the routing path, but there is no
/// reason to accept a WORSE ordering just because the stakes are lower.
///
/// `connector_identities` carries no RLS policy at all (deliberately —
/// `migrations_tenant/0009_tenant_rls.sql`'s uncovered-tables list: no
/// account/graph column to key a policy on), so this reads `tenant_pool`
/// unscoped, filtered by `workspace_id` alone — the same predicate
/// `resolve_org_fanout_destinations` uses for the identical reason.
/// `account_members` likewise carries no RLS relevant here and is filtered
/// by `account_id` directly.
async fn fetch_identity_coverage(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<IdentityCoverage, String> {
    let identity_user_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT user_id FROM connector_identities \
         WHERE provider = 'slack' AND workspace_id = $1 AND user_id IS NOT NULL",
    )
    .bind(workspace_id)
    .fetch_all(tenant_pool)
    .await
    .map_err(|e| format!("identity coverage identities query: {e}"))?;

    let member_user_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT user_id FROM account_members WHERE account_id = $1")
            .bind(account_id)
            .fetch_all(control_pool)
            .await
            .map_err(|e| format!("identity coverage members query: {e}"))?;
    let total = member_user_ids.len();
    let member_set: std::collections::HashSet<Uuid> = member_user_ids.into_iter().collect();

    let identified = identity_user_ids.into_iter().filter(|id| member_set.contains(id)).count();

    Ok(IdentityCoverage { identified: identified as u32, total: total as u32 })
}

#[cfg(test)]
mod pure_derivation_tests {
    use super::*;

    // ── ChannelsStatus: unavailable-vs-zero (docs/connectors/
    //    slack-onboarding.md §3's headline rule) ─────────────────────────────

    #[test]
    fn channels_ok_with_zero_channels_is_a_real_zero_not_unavailable() {
        let status = channels_status_from_result(Ok(Vec::new()));
        assert_eq!(status.state, ChannelsState::Ok);
        assert_eq!(status.count, Some(0));
        assert!(status.names.is_empty());
        assert_eq!(status.error, None);
    }

    #[test]
    fn channels_error_never_renders_as_a_zero_count() {
        let status =
            channels_status_from_result(Err("conversations.list: rate-limited by Slack".to_string()));
        assert_eq!(status.state, ChannelsState::Unavailable);
        // FAILS WITHOUT THE FIX: a naive `.unwrap_or_default().len()` collapse
        // would report `count: Some(0)` here, indistinguishable from "the bot
        // really is in zero channels" — exactly the flattening
        // docs/connectors/slack-onboarding.md §3 forbids.
        assert_eq!(status.count, None);
        assert!(status.names.is_empty());
        assert!(status.error.is_some());
    }

    #[test]
    fn channels_ok_caps_names_at_five_but_count_stays_authoritative() {
        let channels: Vec<(String, String)> = (0..8)
            .map(|i| (format!("C{i}"), format!("channel-{i}")))
            .collect();
        let status = channels_status_from_result(Ok(channels));
        assert_eq!(status.state, ChannelsState::Ok);
        assert_eq!(status.count, Some(8), "count must reflect the real total, not the capped preview");
        assert_eq!(status.names.len(), 5);
        assert_eq!(status.names[0], "#channel-0");
    }

    // ── HuddlesStatus: skipped is normal, not an error ──────────────────────

    #[test]
    fn all_huddles_skipped_is_not_flagged_as_a_failure() {
        // Every huddle in this channel was too short to summarize — the
        // MODAL outcome per docs/connectors/slack.md §3.1, not a fault.
        let rows = vec![
            (false, Some("Slack placeholder: not enough to summarize".to_string())),
            (false, Some("Slack placeholder: not enough to summarize".to_string())),
            (false, Some("no mappable destination ocean".to_string())),
        ];
        let status = huddles_status_from_rows(&rows);
        assert_eq!(status.seen, 3);
        assert_eq!(status.ingested, 0);
        assert_eq!(status.skipped, 3);
        // The struct carries no "ok"/"error" discriminator at all — a caller
        // cannot accidentally render this as a failure state because there is
        // no failure state to render; this test pins that shape rather than a
        // boolean that a future change could flip.
    }

    #[test]
    fn seen_ingested_skipped_add_up_and_top_reasons_are_ranked_and_capped() {
        let rows = vec![
            (true, None),
            (true, None),
            (false, Some("short huddle".to_string())),
            (false, Some("short huddle".to_string())),
            (false, Some("short huddle".to_string())),
            (false, Some("no destination".to_string())),
            (false, Some("no destination".to_string())),
            (false, Some("automatic notes off".to_string())),
        ];
        let status = huddles_status_from_rows(&rows);
        assert_eq!(status.seen, 8);
        assert_eq!(status.ingested, 2);
        assert_eq!(status.skipped, 6);
        assert_eq!(status.top_skip_reasons.len(), 3, "capped at 3 per the contract");
        assert_eq!(status.top_skip_reasons[0], SkipReasonCount { reason: "short huddle".into(), count: 3 });
        assert_eq!(status.top_skip_reasons[1], SkipReasonCount { reason: "no destination".into(), count: 2 });
        assert_eq!(
            status.top_skip_reasons[2],
            SkipReasonCount { reason: "automatic notes off".into(), count: 1 }
        );
    }

    /// Migration 0102's terminal-state CHECK is an OR, so a row may carry
    /// BOTH `processed_at` and a `skip_reason`. Such a row is ingested, is
    /// excluded from `skipped`, and must therefore be excluded from the
    /// reason counts too — otherwise the totals sum above `skipped` and the
    /// UI renders "1 skipped · top reason ×2", which is self-contradictory.
    ///
    /// Fails without the `is_ingested` filter in `huddles_status_from_rows`:
    /// "short huddle" would count 2.
    #[test]
    fn a_reason_on_an_ingested_row_never_inflates_the_counts_above_skipped() {
        let rows = vec![
            // Ingested, but still carrying a reason — the OR case.
            (true, Some("short huddle".to_string())),
            (false, Some("short huddle".to_string())),
        ];
        let status = huddles_status_from_rows(&rows);
        assert_eq!(status.seen, 2);
        assert_eq!(status.ingested, 1);
        assert_eq!(status.skipped, 1);
        let total: i64 = status.top_skip_reasons.iter().map(|r| r.count).sum();
        assert!(
            total <= status.skipped,
            "reason counts ({total}) must never exceed skipped ({})",
            status.skipped,
        );
        assert_eq!(
            status.top_skip_reasons,
            vec![SkipReasonCount { reason: "short huddle".into(), count: 1 }],
        );
    }

    #[test]
    fn empty_ledger_reports_real_zeros_not_a_missing_section() {
        let status = huddles_status_from_rows(&[]);
        assert_eq!(status.seen, 0);
        assert_eq!(status.ingested, 0);
        assert_eq!(status.skipped, 0);
        assert!(status.top_skip_reasons.is_empty());
    }

    // ── Plane / identity: org-only field, null on personal ──────────────────

    #[test]
    fn plane_conversion_matches_account_plane() {
        assert_eq!(Plane::from(AccountPlane::Personal), Plane::Personal);
        assert_eq!(Plane::from(AccountPlane::Org), Plane::Org);
    }

    #[test]
    fn not_connected_carries_no_identity_field_regardless_of_plane() {
        // `derive_status` is the only path that ever populates `identity`
        // (org-plane-gated there); the `not_connected` shortcut never calls
        // it, so both planes must show `None` here — pinned directly rather
        // than relying on that call graph never changing.
        let owner = Viewer::Account { role: "owner" };
        assert_eq!(SlackOnboardingStatus::not_connected(Plane::Personal, owner).identity, None);
        assert_eq!(SlackOnboardingStatus::not_connected(Plane::Org, owner).identity, None);
    }

    #[test]
    fn not_connected_serializes_plane_as_personal_or_org_and_everything_else_null() {
        let json = serde_json::to_value(SlackOnboardingStatus::not_connected(
            Plane::Org,
            Viewer::Account { role: "admin" },
        ))
        .unwrap();
        assert_eq!(json["connected"], serde_json::json!(false));
        assert_eq!(json["plane"], serde_json::json!("org"));
        assert_eq!(json["workspace_id"], serde_json::Value::Null);
        assert_eq!(json["channels"], serde_json::Value::Null);
        assert_eq!(json["identity"], serde_json::Value::Null);
    }

    // ── viewer_role: the role travels even when nothing is connected ────────

    /// The connectors page decides between a Connect button and "an org admin
    /// needs to connect this" from THIS payload, and that decision only
    /// exists while `connected == false`. A `viewer_role` that only appeared
    /// on the connected arm would leave the one screen that needs it with
    /// nothing to read.
    #[test]
    fn not_connected_still_reports_the_viewer_role() {
        let json = serde_json::to_value(SlackOnboardingStatus::not_connected(
            Plane::Org,
            Viewer::Member { user_id: Uuid::new_v4() },
        ))
        .unwrap();
        assert_eq!(json["viewer_role"], serde_json::json!("member"));
        // …but no member SECTION: every member field is derived from a
        // connected workspace, so there is nothing to report yet.
        assert_eq!(json["member"], serde_json::Value::Null);
    }

    #[test]
    fn a_member_viewer_cannot_claim_any_other_role() {
        // `Member`'s label is not read off a field — there is no field. The
        // only way to be labelled "member" is to BE one, and the only way to
        // be one is to carry the caller identity the member reads need.
        assert_eq!(Viewer::Member { user_id: Uuid::new_v4() }.role_label().as_deref(), Some("member"));
        assert_eq!(Viewer::Account { role: "owner" }.role_label().as_deref(), Some("owner"));
        assert_eq!(Viewer::Account { role: "admin" }.role_label().as_deref(), Some("admin"));
    }

    /// The App Home resolves an ACCOUNT for its viewer but never a ROLE.
    /// FAILS IF `Unresolved` IS EVER GIVEN A DEFAULT: a fabricated `"owner"`
    /// here is a claim about a person we never checked — the same class of
    /// mistake as an `Unavailable` roster rendered as `count: 0`.
    #[test]
    fn an_unresolved_viewer_reports_no_role_rather_than_guessing_one() {
        assert_eq!(Viewer::Unresolved.role_label(), None);
        let json =
            serde_json::to_value(SlackOnboardingStatus::not_connected(Plane::Org, Viewer::Unresolved))
                .unwrap();
        assert_eq!(json["viewer_role"], serde_json::Value::Null);
    }

    // ── member_branch_viewer: who gets the member sections ──────────────────

    #[test]
    fn only_an_org_member_viewer_gets_the_member_sections() {
        let uid = Uuid::new_v4();
        assert_eq!(
            member_branch_viewer(Viewer::Member { user_id: uid }, AccountPlane::Org),
            Some(uid),
        );
        // An org OWNER/ADMIN sees the account-wide checklist, not a personal
        // slice — the three steps are admin-by-construction (only an admin
        // can authorise the app in the first place).
        assert_eq!(member_branch_viewer(Viewer::Account { role: "owner" }, AccountPlane::Org), None);
        assert_eq!(member_branch_viewer(Viewer::Account { role: "admin" }, AccountPlane::Org), None);
        // Personal: no member oceans, no identity matcher, no member section
        // — for ANY viewer shape, including one that somehow claimed to be a
        // member there.
        assert_eq!(
            member_branch_viewer(Viewer::Account { role: "owner" }, AccountPlane::Personal),
            None,
        );
        assert_eq!(
            member_branch_viewer(Viewer::Member { user_id: uid }, AccountPlane::Personal),
            None,
        );
        assert_eq!(member_branch_viewer(Viewer::Unresolved, AccountPlane::Org), None);
    }

    // ── my_channels_from_lookups: unlinked ≠ unavailable ≠ zero ─────────────

    fn bot(ids: &[&str]) -> Vec<(String, String)> {
        ids.iter().map(|id| ((*id).to_string(), format!("chan-{id}"))).collect()
    }

    #[test]
    fn my_channels_is_the_intersection_not_either_side_alone() {
        let b = bot(&["C1", "C2", "C3"]);
        let mine = vec!["C2".to_string(), "C3".to_string(), "C9".to_string()];
        let status = my_channels_from_lookups(true, Some(&b), Some(&mine));
        assert_eq!(status.state, MyChannelsState::Ok);
        // NOT 3 (the member's own channel count) and NOT 3 (the bot's) — C9
        // is a channel the member is in that the bot has never been invited
        // to, which is exactly what this step is asking about.
        assert_eq!(status.count, Some(2));
    }

    #[test]
    fn my_channels_counts_a_paginated_duplicate_once() {
        let b = bot(&["C1"]);
        // Slack paginates; a channel straddling a cursor boundary must not
        // inflate the count.
        let mine = vec!["C1".to_string(), "C1".to_string()];
        assert_eq!(my_channels_from_lookups(true, Some(&b), Some(&mine)).count, Some(1));
    }

    /// FAILS WITHOUT THE `linked` SHORT-CIRCUIT: an unlinked member would
    /// fall through to the intersection arm and report `Ok, count: 0` —
    /// "the bot is in none of your channels" — for someone we never looked
    /// up at all. The checklist is that person's LINKING funnel; telling
    /// them step 2 is failing sends them to invite a bot that is already
    /// there.
    #[test]
    fn an_unlinked_member_is_never_reported_as_zero_channels() {
        let b = bot(&["C1", "C2"]);
        let status = my_channels_from_lookups(false, Some(&b), None);
        assert_eq!(status.state, MyChannelsState::Unlinked);
        assert_eq!(status.count, None);
        // Even if a member-channel list somehow arrived without a link, the
        // link is what gates the answer.
        let mine = vec!["C1".to_string()];
        assert_eq!(
            my_channels_from_lookups(false, Some(&b), Some(&mine)).state,
            MyChannelsState::Unlinked,
        );
    }

    /// The rendered number is an INTERSECTION, so losing EITHER side loses
    /// it. FAILS WITHOUT THE FIX in both directions: falling back to the
    /// member's own count when the bot roster is missing (or to the bot's
    /// count when theirs is) answers a different question in this slot.
    #[test]
    fn my_channels_is_unavailable_when_either_live_lookup_failed() {
        let b = bot(&["C1", "C2"]);
        let mine = vec!["C1".to_string()];

        let bot_failed = my_channels_from_lookups(true, None, Some(&mine));
        assert_eq!(bot_failed.state, MyChannelsState::Unavailable);
        assert_eq!(bot_failed.count, None);

        let mine_failed = my_channels_from_lookups(true, Some(&b), None);
        assert_eq!(mine_failed.state, MyChannelsState::Unavailable);
        assert_eq!(mine_failed.count, None);

        let both_failed = my_channels_from_lookups(true, None, None);
        assert_eq!(both_failed.state, MyChannelsState::Unavailable);
        assert_eq!(both_failed.count, None);
    }

    #[test]
    fn a_linked_member_in_no_shared_channels_is_a_real_ok_zero() {
        // Distinct from both arms above: we DID look, and the honest answer
        // is zero. This is the one case that should send someone to
        // `/invite`.
        let b = bot(&["C1"]);
        let mine = vec!["C9".to_string()];
        let status = my_channels_from_lookups(true, Some(&b), Some(&mine));
        assert_eq!(status.state, MyChannelsState::Ok);
        assert_eq!(status.count, Some(0));
    }

    #[test]
    fn my_channels_states_serialize_as_the_three_wire_strings() {
        let json = |s: MyChannelsStatus| serde_json::to_value(s).unwrap()["state"].clone();
        assert_eq!(
            json(MyChannelsStatus {
                state: MyChannelsState::Ok,
                count: Some(0),
                containers: Some(Vec::new()),
            }),
            serde_json::json!("ok"),
        );
        assert_eq!(
            json(MyChannelsStatus {
                state: MyChannelsState::Unavailable,
                count: None,
                containers: None,
            }),
            serde_json::json!("unavailable"),
        );
        assert_eq!(
            json(MyChannelsStatus {
                state: MyChannelsState::Unlinked,
                count: None,
                containers: None,
            }),
            serde_json::json!("unlinked"),
        );
    }

    /// The list and the count must never be able to disagree, and the empty
    /// list must stay distinguishable from "we could not look" — the same
    /// absence-is-not-an-answer rule the three-way state exists for, now
    /// applied to the payload the backfill offer renders from.
    #[test]
    fn my_containers_is_the_named_intersection_and_matches_the_count() {
        let bot = vec![
            ("C2".to_string(), "zebra".to_string()),
            ("C1".to_string(), "alpha".to_string()),
            ("C9".to_string(), "not-mine".to_string()),
        ];
        let mine = vec!["C1".to_string(), "C2".to_string(), "C3".to_string()];

        let status = my_channels_from_lookups(true, Some(&bot), Some(&mine));
        let containers = status.containers.expect("Ok arm carries the list");

        // Named, intersected, and sorted by name — not in `mine`'s order and
        // not in a HashSet's order.
        assert_eq!(
            containers.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "zebra"],
        );
        assert_eq!(status.count, Some(containers.len() as u32));

        // An empty intersection is a real answer: Some(vec![]), not None.
        let disjoint = my_channels_from_lookups(
            true,
            Some(&[("C9".to_string(), "theirs".to_string())]),
            Some(&["C1".to_string()]),
        );
        assert_eq!(disjoint.state, MyChannelsState::Ok);
        assert_eq!(disjoint.containers, Some(Vec::new()));
        assert_eq!(disjoint.count, Some(0));

        // A failed lookup is NOT an empty list.
        let unavailable = my_channels_from_lookups(true, None, Some(&["C1".to_string()]));
        assert_eq!(unavailable.state, MyChannelsState::Unavailable);
        assert_eq!(unavailable.containers, None);
    }

    // ── The polling cache ───────────────────────────────────────────────────

    #[test]
    fn a_cached_value_is_reused_inside_the_ttl_and_dropped_outside_it() {
        let map: Mutex<HashMap<u8, (Instant, Vec<String>)>> = Mutex::new(HashMap::new());
        cache_store(&map, 1u8, vec!["C1".to_string()]);

        assert_eq!(
            cache_lookup(&map, &1u8, Duration::from_secs(45)),
            Some(vec!["C1".to_string()]),
            "a fresh entry must be served without a second Slack call",
        );
        // A zero TTL is the same code path an expired entry takes.
        assert_eq!(cache_lookup(&map, &1u8, Duration::ZERO), None);
        assert!(
            map.lock().unwrap().is_empty(),
            "the expiring read must also SWEEP — this map is keyed by workspace/viewer and \
             would otherwise grow for the lifetime of the process",
        );
    }

    #[test]
    fn a_cache_miss_on_an_unknown_key_is_not_another_keys_value() {
        let map: Mutex<HashMap<(u8, u8), (Instant, u32)>> = Mutex::new(HashMap::new());
        cache_store(&map, (1u8, 1u8), 7u32);
        assert_eq!(cache_lookup(&map, &(1u8, 1u8), Duration::from_secs(45)), Some(7));
        // The member cache is keyed per VIEWER for exactly this reason: a
        // coarser key would serve one member another member's channel count.
        assert_eq!(cache_lookup(&map, &(1u8, 2u8), Duration::from_secs(45)), None);
    }
}

#[cfg(test)]
mod db_derivation_pg_tests {
    //! DB-backed tests for [`fetch_routed_status`], [`fetch_capture_status`],
    //! [`fetch_huddles_status`], and [`fetch_identity_coverage`] — the four
    //! reads [`derive_status`] composes. `derive_status` itself is NOT
    //! exercised end-to-end here: its `channels` section is a live Slack API
    //! call, and this crate has no HTTP-mock harness (see this module's own
    //! header) — the unavailable-vs-zero distinction that section exists to
    //! enforce is instead pinned directly against
    //! [`channels_status_from_result`] in `pure_derivation_tests` above, with
    //! no network involved.
    //!
    //! In-crate (not `backend/tests/*_pg.rs`): every function under test is
    //! private, so an external test crate cannot drive them — same reasoning
    //! as `connectors::slack`'s own `verified_identity_matcher_pg_tests`,
    //! whose helper scaffolding this copies (there is no shared harness for
    //! in-crate `#[cfg(test)]` pg modules; every one of them carries its own
    //! `setup`/`teardown` pair by convention).
    //!
    //! These tests connect and migrate as the DATABASE OWNER, exactly like
    //! `verified_identity_matcher_pg_tests` — RLS is inert for that role, so
    //! they prove the SQL/aggregation logic each function computes, NOT the
    //! `account_isolation` scoping itself (that boundary is instead the
    //! subject of `backend/tests/consumer_rls_pg.rs`'s general `RLS_TABLES`
    //! roster, which already covers `slack_watched_channels`/`atoms`/
    //! `slack_huddle_captures`). `begin_account_scoped` still runs in every
    //! call below (it is not optional plumbing to skip in a test), it simply
    //! has nothing to enforce against an owner connection.
    //!
    //! Gated on `ORGT_TEST_BASE_URL`, migrating the CONTROL tree (where every
    //! table these functions read lives for a personal/consumer account — the
    //! schema every real caller runs against on the plane this webhook path
    //! can reach today, per `derive_status`'s own doc comment). Requires an
    //! EXPLICIT `cargo test --lib <module>` invocation (the workspace `cargo
    //! test` job has no Postgres service and silently skips every test here):
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib db_derivation_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::{
        fetch_capture_status, fetch_huddles_status, fetch_identity_coverage, fetch_member_capture_status,
        fetch_member_link, fetch_routed_status,
    };

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

    /// Seed one account (owned by a fresh user) — the minimum every test
    /// below needs before it can seed anything account-scoped.
    async fn seed_account(pool: &PgPool) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("{}@example.com", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(id, owner_user_id) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO graphs(id, account_id, name) VALUES (gen_random_uuid(), $1, 'primary') RETURNING id")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    // ── fetch_routed_status ──────────────────────────────────────────────────

    /// FAILS WITHOUT THE FIX: a query that counted every
    /// `slack_watched_channels` row for the account (no `workspace_id`
    /// filter) would report this OTHER workspace's row too.
    #[tokio::test]
    async fn routed_status_counts_only_this_workspace_and_splits_out_fanout_pg() {
        let db = "orgt_onboarding_routed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping routed_status_counts_only_this_workspace_and_splits_out_fanout_pg: \
                       set ORGT_TEST_BASE_URL");
            return;
        };

        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let user_id: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();

        // Two rows on the workspace under test: one 'single', one 'fanout'.
        sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, 'single', $3, 'T1', 'C1')",
        )
        .bind(account_id).bind(user_id).bind(graph_id)
        .execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, 'fanout', NULL, 'T1', 'C2')",
        )
        .bind(account_id).bind(user_id)
        .execute(&pool).await.unwrap();
        // A row on a DIFFERENT workspace for the SAME account — must not count.
        sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, 'single', $3, 'T2', 'C3')",
        )
        .bind(account_id).bind(user_id).bind(graph_id)
        .execute(&pool).await.unwrap();

        let status = fetch_routed_status(&pool, account_id, "T1").await.unwrap();
        assert_eq!(status.count, 2, "must count only T1's two rows, not T2's");
        assert_eq!(status.fanout, 1);

        teardown(admin_conn, pool, db).await;
    }

    #[tokio::test]
    async fn routed_status_is_a_real_zero_when_nothing_routed_yet_pg() {
        let db = "orgt_onboarding_routed_zero";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping routed_status_is_a_real_zero_when_nothing_routed_yet_pg: set \
                       ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;

        let status = fetch_routed_status(&pool, account_id, "T1").await.unwrap();
        assert_eq!(status.count, 0);
        assert_eq!(status.fanout, 0);

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_capture_status ─────────────────────────────────────────────────

    /// An atom of an arbitrary source type. `seed_atom` hardcodes `'slack'`,
    /// which is exactly why the source hole below went unnoticed: no fixture
    /// in this module could express a non-Slack atom at all.
    async fn seed_atom_of_source(
        pool: &PgPool,
        graph_id: Uuid,
        source_id: &str,
        source_type: &str,
        workspace_id: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, \
             timestamp, workspace_id) \
             VALUES (gen_random_uuid(), $1, $2, $3, 'blob-key', now(), $4)",
        )
        .bind(graph_id)
        .bind(source_type)
        .bind(source_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_atom(pool: &PgPool, graph_id: Uuid, source_id: &str, workspace_id: Option<&str>) {
        sqlx::query(
            "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, \
             timestamp, workspace_id) \
             VALUES (gen_random_uuid(), $1, 'slack', $2, 'blob-key', now(), $3)",
        )
        .bind(graph_id)
        .bind(source_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn capture_status_excludes_other_workspaces_but_keeps_pre_0058_atoms_pg() {
        let db = "orgt_onboarding_capture";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping capture_status_excludes_other_workspaces_but_keeps_pre_0058_atoms_pg: set \
                       ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;

        seed_atom(&pool, graph_id, "msg-1", Some("T1")).await;
        seed_atom(&pool, graph_id, "msg-2", Some("T1")).await;
        // A different workspace's atom — must not count toward T1's total.
        seed_atom(&pool, graph_id, "msg-3", Some("T2")).await;
        // Pre-migration-0058 atom with no workspace_id at all. This used to
        // assert "must not count either — it cannot be attributed to T1",
        // which sounded like scoping and was not: the column did not exist
        // when the row was written, so excluding it did not attribute the
        // atom elsewhere, it made the atom disappear from every Slack surface
        // in the product (19 of 35 in production, 2026-09-09). The premise is
        // inverted deliberately — see `CaptureStatus`'s doc comment for what
        // the relaxation admits on a two-workspace account.
        seed_atom(&pool, graph_id, "msg-4", None).await;
        // FAILS WITHOUT THE SOURCE FILTER. These have no `workspace_id`
        // either — every non-Slack connector leaves it NULL — so a bare
        // `OR workspace_id IS NULL` counts the account's Gmail, Google Chat
        // and coding-session content as SLACK capture, and the checklist
        // reports a workspace as ingesting when Slack has produced nothing.
        seed_atom_of_source(&pool, graph_id, "mail-1", "gmail", None).await;
        seed_atom_of_source(&pool, graph_id, "chat-1", "google_chat", None).await;
        seed_atom_of_source(&pool, graph_id, "sess-1", "coding_session", None).await;
        // A pre-0058 huddle transcript: also OUT, deliberately — see
        // `CaptureStatus`'s doc comment on why `'transcript'` cannot be
        // admitted on the NULL arm without sweeping in every meeting.
        seed_atom_of_source(&pool, graph_id, "huddle-1", "transcript", None).await;

        let status = fetch_capture_status(&pool, account_id, "T1").await.unwrap();
        assert_eq!(status.atom_count, 3, "2 stamped T1 + 1 pre-0058 Slack; never T2's, never another source's");
        assert!(status.first_at.is_some());
        assert!(status.last_at.is_some());

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX — and, unlike the `workspace_id` tests above,
    /// it fails for a reason no amount of RLS plumbing would have caught.
    ///
    /// Two SEPARATE CoralSwarm accounts each connect the SAME Slack workspace
    /// (`T1`). That is a supported shape: `slack_integrations` is
    /// `UNIQUE (account_id, workspace_id)`, not unique on `workspace_id`. The
    /// pre-fix query was `... FROM atoms WHERE workspace_id = $1` with no
    /// account predicate at all, so account A's onboarding checklist counted
    /// account B's atoms and reported B's first/last-capture timestamps.
    ///
    /// **This test would pass either way if it relied on RLS**, which is
    /// exactly why the assertion targets the WHERE clause instead: this
    /// module's header explains that these tests connect as the database
    /// OWNER, so `begin_account_scoped` runs inside `fetch_capture_status`
    /// here and enforces nothing IN THE TEST. (Production is different since
    /// the OPS-4 cutover to the non-`BYPASSRLS` `coralswarm_app` role, where
    /// the RLS policy is a live second layer — but the explicit predicate
    /// remains the first layer, and this test proves THAT layer on its own,
    /// with no policy masking a deleted `graphs` join.)
    #[tokio::test]
    async fn capture_status_does_not_blend_a_second_account_on_the_same_workspace_pg() {
        let db = "orgt_onboarding_capture_two_accounts";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping capture_status_does_not_blend_a_second_account_on_the_same_workspace_pg: \
                       set ORGT_TEST_BASE_URL");
            return;
        };
        let account_a = seed_account(&pool).await;
        let account_b = seed_account(&pool).await;
        let graph_a = seed_graph(&pool, account_a).await;
        let graph_b = seed_graph(&pool, account_b).await;

        // Account A: two atoms on T1.
        seed_atom(&pool, graph_a, "a-msg-1", Some("T1")).await;
        seed_atom(&pool, graph_a, "a-msg-2", Some("T1")).await;
        // Account B: three atoms on the SAME workspace id. Same string, a
        // different tenant — none of these may reach account A.
        seed_atom(&pool, graph_b, "b-msg-1", Some("T1")).await;
        seed_atom(&pool, graph_b, "b-msg-2", Some("T1")).await;
        seed_atom(&pool, graph_b, "b-msg-3", Some("T1")).await;

        let a = fetch_capture_status(&pool, account_a, "T1").await.unwrap();
        assert_eq!(a.atom_count, 2, "account A must not see account B's atoms on the shared workspace");
        let b = fetch_capture_status(&pool, account_b, "T1").await.unwrap();
        assert_eq!(b.atom_count, 3, "…and the isolation is symmetric, not one-directional");

        // A third account with nothing on T1 must read a real, honest zero —
        // not "everyone's atoms because the workspace matched".
        let account_c = seed_account(&pool).await;
        let c = fetch_capture_status(&pool, account_c, "T1").await.unwrap();
        assert_eq!(c.atom_count, 0);
        assert_eq!(c.first_at, None);
        assert_eq!(c.last_at, None);

        teardown(admin_conn, pool, db).await;
    }

    #[tokio::test]
    async fn capture_status_reports_null_timestamps_not_a_fabricated_epoch_when_empty_pg() {
        let db = "orgt_onboarding_capture_zero";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping capture_status_reports_null_timestamps_not_a_fabricated_epoch_when_empty_pg: \
                       set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;

        let status = fetch_capture_status(&pool, account_id, "T1").await.unwrap();
        assert_eq!(status.atom_count, 0);
        // FAILS WITHOUT THE FIX: `min()`/`max()` over zero rows in Postgres
        // already yields NULL, but a naive `.unwrap_or(Utc::now())` or
        // similar substitution at the Rust layer would fabricate a
        // plausible-looking non-null timestamp here — exactly the "no zeros
        // (or fake values) standing in for unknowns" rule §3 states.
        assert_eq!(status.first_at, None);
        assert_eq!(status.last_at, None);

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_huddles_status ─────────────────────────────────────────────────

    #[tokio::test]
    async fn huddles_status_reads_the_ledger_for_this_account_only_pg() {
        let db = "orgt_onboarding_huddles";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping huddles_status_reads_the_ledger_for_this_account_only_pg: set \
                       ORGT_TEST_BASE_URL");
            return;
        };
        let account_a = seed_account(&pool).await;
        let account_b = seed_account(&pool).await;

        sqlx::query(
            "INSERT INTO slack_huddle_captures(account_id, channel_id, huddle_thread_ts, processed_at, skip_reason) \
             VALUES ($1, 'C1', '1.1', now(), NULL)",
        )
        .bind(account_a).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO slack_huddle_captures(account_id, channel_id, huddle_thread_ts, processed_at, skip_reason) \
             VALUES ($1, 'C1', '1.2', NULL, 'too short to summarize')",
        )
        .bind(account_a).execute(&pool).await.unwrap();
        // A different account's row — must not leak into account_a's counts.
        sqlx::query(
            "INSERT INTO slack_huddle_captures(account_id, channel_id, huddle_thread_ts, processed_at, skip_reason) \
             VALUES ($1, 'C9', '9.1', now(), NULL)",
        )
        .bind(account_b).execute(&pool).await.unwrap();

        let status = fetch_huddles_status(&pool, account_a).await.unwrap();
        assert_eq!(status.seen, 2, "must not count account_b's row");
        assert_eq!(status.ingested, 1);
        assert_eq!(status.skipped, 1);
        assert_eq!(status.top_skip_reasons.len(), 1);
        assert_eq!(status.top_skip_reasons[0].reason, "too short to summarize");

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_identity_coverage ──────────────────────────────────────────────

    #[tokio::test]
    async fn identity_coverage_counts_only_current_members_with_a_confirmed_link_pg() {
        let db = "orgt_onboarding_identity";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping identity_coverage_counts_only_current_members_with_a_confirmed_link_pg: \
                       set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;

        // Three members: one confirmed on THIS workspace, one confirmed but
        // on a DIFFERENT workspace (must not count), one with no identity at
        // all.
        let mut member_ids = Vec::new();
        for _ in 0..3 {
            let user_id: Uuid = sqlx::query_scalar(
                "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
            )
            .bind(format!("{}@example.com", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
                .bind(account_id).bind(user_id)
                .execute(&pool).await.unwrap();
            member_ids.push(user_id);
        }
        // A confirmed identity for a user who is NOT a current member of this
        // account at all — must not inflate `identified`.
        let stranger_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("{}@example.com", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();

        // `user_id` carries a provenance CHECK (migration 0104): either all
        // of `user_id`/`link_method`/`link_verified_at` are NULL, or all
        // three are set — never a bare `user_id` with no provenance.
        sqlx::query(
            "INSERT INTO connector_identities \
                 (provider, workspace_id, external_user_id, user_id, canonical_name, \
                  link_method, link_verified_at) \
             VALUES ('slack', 'T1', 'U1', $1, 'Member One', 'verified_email', now())",
        )
        .bind(member_ids[0]).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO connector_identities \
                 (provider, workspace_id, external_user_id, user_id, canonical_name, \
                  link_method, link_verified_at) \
             VALUES ('slack', 'T2', 'U2', $1, 'Member Two (other workspace)', 'verified_email', now())",
        )
        .bind(member_ids[1]).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO connector_identities \
                 (provider, workspace_id, external_user_id, user_id, canonical_name, \
                  link_method, link_verified_at) \
             VALUES ('slack', 'T1', 'U9', $1, 'Stranger', 'verified_email', now())",
        )
        .bind(stranger_id).execute(&pool).await.unwrap();

        let coverage = fetch_identity_coverage(&pool, &pool, account_id, "T1").await.unwrap();
        assert_eq!(coverage.total, 3, "3 current members, regardless of identity");
        assert_eq!(
            coverage.identified, 1,
            "only member_ids[0] is both a current member AND confirmed on workspace T1"
        );

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_member_link ────────────────────────────────────────────────────

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("{}@example.com", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_identity(pool: &PgPool, workspace_id: &str, external_id: &str, user_id: Uuid) {
        // Migration 0104's provenance CHECK: `user_id`/`link_method`/
        // `link_verified_at` are all-or-nothing.
        sqlx::query(
            "INSERT INTO connector_identities \
                 (provider, workspace_id, external_user_id, user_id, canonical_name, \
                  link_method, link_verified_at) \
             VALUES ('slack', $1, $2, $3, 'A Person', 'verified_email', now())",
        )
        .bind(workspace_id)
        .bind(external_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// FAILS WITHOUT THE `workspace_id`/`user_id` PREDICATES: a member would
    /// be told they are linked because SOMEBODY is, or because they are
    /// linked on a DIFFERENT Slack workspace this account also connects
    /// (`slack_integrations` is `UNIQUE (account_id, workspace_id)`, so an
    /// account with two workspaces is a supported shape).
    #[tokio::test]
    async fn member_link_matches_this_caller_on_this_workspace_only_pg() {
        let db = "orgt_onboarding_member_link";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping member_link_matches_this_caller_on_this_workspace_only_pg: set \
                       ORGT_TEST_BASE_URL");
            return;
        };
        let caller = seed_user(&pool).await;
        let colleague = seed_user(&pool).await;

        // Nothing linked yet — the day-one state the whole member checklist
        // exists to move people out of.
        let none_yet = fetch_member_link(&pool, "T1", caller).await.unwrap();
        assert!(!none_yet.linked);
        assert_eq!(none_yet.slack_user_id, None);
        assert_eq!(none_yet.link_method, None);

        // A COLLEAGUE linked on T1, and the caller linked on a DIFFERENT
        // workspace. Neither may make the caller look linked on T1.
        seed_identity(&pool, "T1", "U-colleague", colleague).await;
        seed_identity(&pool, "T2", "U-caller-elsewhere", caller).await;
        let still_none = fetch_member_link(&pool, "T1", caller).await.unwrap();
        assert!(!still_none.linked, "neither a colleague's link nor another workspace's counts");

        seed_identity(&pool, "T1", "U-caller", caller).await;
        let linked = fetch_member_link(&pool, "T1", caller).await.unwrap();
        assert!(linked.linked);
        assert_eq!(linked.slack_user_id.as_deref(), Some("U-caller"));
        // Surfaced so the client can say "linked automatically" rather than
        // crediting the member with an action nobody performed.
        assert_eq!(linked.link_method.as_deref(), Some("verified_email"));

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_member_capture_status ──────────────────────────────────────────

    /// The member's step 3 is "what landed in MY ocean", not "what landed in
    /// this account". FAILS WITHOUT THE `g.id = $2` PREDICATE: the caller
    /// would be shown the whole account's Slack capture — including a
    /// colleague's private member ocean — which is both the wrong answer and
    /// a disclosure (`SlackChannelTable`'s own member view exists to keep
    /// exactly this boundary).
    #[tokio::test]
    async fn member_capture_counts_only_the_callers_own_member_ocean_pg() {
        let db = "orgt_onboarding_member_capture";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping member_capture_counts_only_the_callers_own_member_ocean_pg: set \
                       ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let caller = seed_user(&pool).await;
        let colleague = seed_user(&pool).await;

        // No member ocean yet — an honest zero, not an error. A member who
        // has not been seated anywhere is a real, common state.
        let no_ocean = fetch_member_capture_status(&pool, account_id, caller, "T1").await.unwrap();
        assert_eq!(no_ocean.atom_count, 0);
        assert_eq!(no_ocean.first_at, None);
        assert_eq!(no_ocean.last_at, None);

        // `graphs_member_requires_owner` (migration 0090): `kind = 'member'`
        // IFF `owner_user_id IS NOT NULL`, so both are set on the INSERT.
        let my_graph = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO graphs(id, account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, $2, 'My ocean', 'member', $3, true)",
        )
        .bind(my_graph).bind(account_id).bind(caller)
        .execute(&pool).await.unwrap();
        let their_graph = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO graphs(id, account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, $2, 'Their ocean', 'member', $3, true)",
        )
        .bind(their_graph).bind(account_id).bind(colleague)
        .execute(&pool).await.unwrap();
        // A shared/team ocean on the same account — also not "mine".
        let team_graph = seed_graph(&pool, account_id).await;

        seed_atom(&pool, my_graph, "mine-1", Some("T1")).await;
        seed_atom(&pool, my_graph, "mine-2", Some("T1")).await;
        // Same ocean, a DIFFERENT workspace — this checklist is about T1.
        seed_atom(&pool, my_graph, "mine-3", Some("T2")).await;
        // Pre-migration-0058: no workspace provenance. Counted, for the same
        // reason `fetch_capture_status` counts it — see `CaptureStatus`'s doc
        // comment. Without this the member's "what landed in MY ocean" would
        // disagree with the channel page's per-channel totals for the same
        // oceans, and neither surface could say which was right.
        seed_atom(&pool, my_graph, "mine-4", None).await;
        // Same source hole, same ocean — a member's "what landed in MY ocean"
        // must not count their own Gmail as Slack capture either.
        seed_atom_of_source(&pool, my_graph, "mine-mail", "gmail", None).await;
        seed_atom(&pool, their_graph, "theirs-1", Some("T1")).await;
        seed_atom(&pool, team_graph, "team-1", Some("T1")).await;

        let mine = fetch_member_capture_status(&pool, account_id, caller, "T1").await.unwrap();
        assert_eq!(mine.atom_count, 3, "T1 + pre-0058 atoms in the caller's OWN member ocean; never T2's");
        assert!(mine.first_at.is_some());
        assert!(mine.last_at.is_some());

        // …and symmetrically for the colleague, so the isolation is not
        // one-directional.
        let theirs = fetch_member_capture_status(&pool, account_id, colleague, "T1").await.unwrap();
        assert_eq!(theirs.atom_count, 1);

        teardown(admin_conn, pool, db).await;
    }
}

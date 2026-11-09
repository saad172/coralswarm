//! Per-channel Slack management payload — `GET /v1/connectors/slack/channels`
//! (`routes::slack::channel_management`). Answers "what is CoralSwarm actually
//! reading, where does each channel land, and are huddles being saved there" —
//! the detail page behind a connected Slack workspace tile, as opposed to
//! `slack_onboarding`'s single-number checklist.
//!
//! # Same rules as `slack_onboarding`, deliberately not re-derived
//!
//! docs/connectors/slack-onboarding.md §3's "rules the payload must obey"
//! apply here unchanged — this module exists precisely because that contract
//! generalizes to "one row per channel" without needing new rules:
//!
//! * **The row set comes from a LIVE `conversations.list`
//!   ([`crate::connectors::slack_onboarding::cached_bot_channels`], the same
//!   `SLACK_LOOKUP_TTL` memo over
//!   [`crate::connectors::slack::fetch_bot_member_channels`] the checklist
//!   uses — see that function's doc comment for why both surfaces must share
//!   ONE), never from
//!   `slack_watched_channels`.** That table only has rows for channels a
//!   human routed through the picker; an invited-but-unrouted channel has no
//!   row there at all and still ingests via the account default
//!   (docs/connectors/slack-onboarding.md §4). Reading the row set off the
//!   table would make a correctly-configured, unrouted channel disappear
//!   from this page entirely.
//! * **`state: "unavailable"` is not an empty row list.** A failed/rate-
//!   limited `conversations.list` renders as `state: "unavailable"` with the
//!   error and `rows: []` — never as "the bot is in zero channels", which
//!   reads as a setup failure. [`ChannelsSection::unavailable`] is the only
//!   way to build that shape, on purpose.
//! * **`kind: "default"` is the no-row case, not a missing/error state.** An
//!   unrouted channel ingests via the account-level default
//!   (`connectors::slack::resolve_channel_destinations`'s no-row branch) —
//!   expected, common, and rendered as a normal destination kind, not an
//!   absence.
//!
//! # Why the row-list shape is a NEW struct, not `slack_onboarding::
//! ChannelsStatus` reused
//!
//! `ChannelsStatus` (the onboarding module) intentionally throws away
//! everything but a capped `names` preview and an authoritative `count` — it
//! is a checklist step, not a table. This endpoint needs the opposite: every
//! channel, its resolved destination, its capture stats, its huddle health.
//! Reusing `ChannelsStatus` would mean bolting all of that onto a struct whose
//! whole point was to be a thin summary, so this module defines its own
//! [`ChannelsSection`]/[`ChannelRow`] pair instead — but keeps the SAME
//! `state`/`error` vocabulary and the SAME "unavailable is not zero" rule,
//! and calls the SAME `fetch_bot_member_channels` underneath, so the two
//! surfaces can never disagree about whether the bot can currently see its
//! channel list.
//!
//! # Pure/impure split
//!
//! Same reasoning as `slack_onboarding`'s own header (no HTTP-mock harness in
//! this crate): [`channel_rows_from_data`] and [`huddle_health_from_rows`]
//! are pure functions over already-fetched data, unit-tested directly with no
//! network or database. The DB/HTTP-touching halves
//! ([`derive_channel_management`] and its four `fetch_*` helpers) get their
//! own `ORGT_TEST_BASE_URL`-gated pg tests, same convention as
//! `slack_onboarding::db_derivation_pg_tests`.
//!
//! # N+1 avoidance
//!
//! Every query here is GROUPED over the whole channel-id set produced by the
//! live Slack call — watched rows, atom aggregates, per-ocean landing
//! aggregates, huddle aggregates — never one query per channel.
//! [`channel_rows_from_data`] zips them together in memory. See each
//! `fetch_*` function's own doc comment for its query shape.
//!
//! The account-level routing resolution ([`resolve_live_routing`]) obeys the
//! same rule for the same reason: a `kind: "default"` row's destination is an
//! ACCOUNT-level fact, identical for every such row on the page, so it is
//! resolved once and shared rather than resolved per row.
//!
//! # Two tenses, deliberately both
//!
//! [`ChannelDestination`] is the FUTURE tense — where the next message in
//! this channel will land. [`ChannelRow::landed_in`] is the PRESENT tense —
//! which oceans this channel's corals are in right now. They are computed
//! from completely different sources (live routing resolution vs. an
//! aggregate over `atoms`) and they legitimately disagree: re-routing a
//! channel never moves already-captured atoms
//! (`connectors::slack_channel_actions`' header), and an org fan-out that
//! reaches nobody has a destination and no landings at all. Collapsing them
//! into one "where does this go" column is exactly the conflation that let a
//! workspace ingesting into one person's private ocean read as "still
//! ingesting" with nothing further said (2026-09-09).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::connectors::slack_onboarding::Plane;
use crate::routes::slack::AccountPlane;

// ── Wire types ───────────────────────────────────────────────────────────────

/// Mirrors [`crate::connectors::slack_onboarding::ChannelsState`] — same two
/// values, same "unavailable is not zero" rule — but this module keeps its
/// own copy rather than reusing that one. Not laziness: `ChannelsSection`
/// below carries `rows: Vec<ChannelRow>` where `ChannelsStatus` carries
/// `count`/`names`, so the two payloads are shaped for genuinely different
/// questions (this module's header explains why) and sharing a `state` enum
/// across them would be the only coupling between two otherwise-independent
/// structs — not worth it for two variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelsListState {
    Ok,
    Unavailable,
    /// Org member with no confirmed Slack identity: the row set is scoped to
    /// "channels you're in", and we cannot know which those are until they
    /// link. Rows are always empty; the client renders the linking prompt.
    /// Distinct from [`ChannelsListState::Unavailable`] — nothing FAILED
    /// here, there is a specific human act that unlocks the page.
    Unlinked,
}

/// A channel's resolved destination — the read-side mirror of
/// [`crate::connectors::slack::ChannelDestination`], but shaped for display
/// rather than for ingest routing: names the ocean instead of its `Uuid`,
/// carries provenance (`chosen_at`) and a staleness flag, and — critically —
/// distinguishes "no row, defaults" from "row, single" from "row, fanout" as
/// three PEER kinds rather than the two-variant enum ingest resolution uses
/// (ingest never needs to say "this is the unrouted default" out loud; a
/// human looking at this page does).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DestinationKind {
    /// One named ocean, chosen explicitly via the picker (`mode = 'single'`).
    Single,
    /// "Everyone's individual oceans", chosen explicitly via the picker
    /// (`mode = 'fanout'`). Org plane only — see
    /// `slack_watched_channels_mode_graph_id_check`.
    Fanout,
    /// No `slack_watched_channels` row at all — the channel ingests via the
    /// account-level default (`resolve_channel_destinations`'s no-row
    /// branch: a personal account's own primary ocean, or an org account's
    /// live per-member fan-out). Normal, not missing — see this module's
    /// header.
    ///
    /// **`Default` alone does not say where anything lands**, and this kind
    /// used to be the whole answer the payload gave for such a channel. It
    /// is not one: on a PERSONAL account the default is one named ocean
    /// ([`ChannelDestination::ocean_name`] now carries it), and on an ORG
    /// account it is a live fan-out to each MAPPED member's private ocean
    /// with the shared org ocean receiving nothing at all
    /// ([`ChannelDestination::fanout`] carries how far that reaches). A
    /// client that renders `Default` as a bare "your account default" is
    /// reporting the org case wrong — that string cost this project a
    /// production misdiagnosis on 2026-09-09.
    Default,
}

/// How far a fan-out destination actually reaches RIGHT NOW — the read-side
/// projection of `connectors::slack::ChannelDestination::FanOut`, resolved by
/// `connectors::slack::resolve_org_fanout_destinations`, the SAME function
/// ingest resolves it with (never a parallel count; see
/// `connectors::slack::resolve_account_default_destination`'s doc comment).
///
/// The two numbers are a partition of the account's CURRENT member roster,
/// so `mapped_members + unmapped_members` is the roster size. There is
/// deliberately no third `total_members` field: a stored total is one more
/// thing that can disagree with the halves it is supposed to sum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct FanoutReach {
    /// Members whose messages actually land somewhere: a CONFIRMED
    /// `connector_identities` row for this workspace AND a resolvable
    /// primary member ocean. Equal to the number of destination oceans, and
    /// deduplicated by PERSON, not by ocean.
    ///
    /// **`0` is the "messages are being dropped" case**, and it renders in
    /// production today: nothing is written anywhere, and no error is raised
    /// anywhere, because a fan-out to nobody is a successful fan-out to an
    /// empty set.
    pub mapped_members: usize,
    /// Members skipped because no confirmed Slack identity maps to them
    /// (they have never sent a message, clicked an interactive element, or
    /// been enumerated by auto-seat), or because their primary member ocean
    /// cannot be resolved yet. Never guessed from display names — see
    /// `connectors::slack::resolve_org_fanout_destinations`.
    pub unmapped_members: usize,
}

/// The channel-less Slack residue, split by whether we can prove which
/// workspace it belongs to — see [`fetch_unattributed_atom_count`] for why
/// one number would have been a claim the data does not support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct UnattributedAtoms {
    /// Slack atoms stamped with THIS `workspace_id` but no `channel_id`.
    /// Attributable to the workspace this page describes, just not to any row
    /// on it.
    pub this_workspace: u64,
    /// Slack atoms with neither `workspace_id` nor `channel_id` — written
    /// before migration `0058` added workspace provenance. They belong to
    /// this ACCOUNT; which of its connected Slack workspaces produced them is
    /// unknowable from the row, so the copy must not name one.
    pub account_legacy: u64,
}

/// One ocean that this channel's corals are ACTUALLY sitting in today, with
/// how many — see [`ChannelRow::landed_in`]. Distinct from
/// [`ChannelDestination`] in tense, and that distinction is the point:
/// `destination` is where the NEXT message goes, `landed_in` is where the
/// existing ones ARE. They disagree whenever routing changed after ingest
/// (re-routing deliberately never moves already-captured atoms — see
/// `connectors::slack_channel_actions`' header), and disagree permanently
/// under an org fan-out that reaches nobody.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LandedOcean {
    pub ocean_name: String,
    /// `graphs.kind` verbatim — `"personal" | "team" | "org" | "member"`
    /// (migration `0090_member_ocean_columns`'s `graphs_kind_check`). Passed
    /// through rather than mapped to a display vocabulary here: a kind this
    /// endpoint has never heard of must reach the client as itself, not as a
    /// silently-wrong bucket.
    pub ocean_kind: String,
    pub atom_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ChannelDestination {
    pub kind: DestinationKind,
    /// The destination ocean's name. `Some` when a SINGLE named ocean is the
    /// answer and it could still be resolved:
    ///
    /// * `kind == Single` — the picked ocean.
    /// * `kind == Default` on the PERSONAL plane — the account's own primary
    ///   ocean, resolved through
    ///   `connectors::slack::resolve_account_default_destination`, i.e. the
    ///   very ocean ingest's no-row branch would pick.
    ///
    /// `None` for every fan-out-shaped destination (there is no single ocean
    /// to name — read [`ChannelDestination::fanout`] instead), and `None`
    /// when a `graph_id` no longer resolves to a graph (however that could
    /// happen given the FK's `ON DELETE CASCADE`), which degrades to `None`
    /// rather than panicking on a missing map entry.
    pub ocean_name: Option<String>,
    /// How far this channel's fan-out reaches, when the destination IS a
    /// fan-out: `kind == Fanout` (an explicit pick), or `kind == Default` on
    /// the ORG plane (the account-level default). `None` for every
    /// single-ocean destination — and also `None`, deliberately, for an
    /// ORG-MEMBER viewer on any row: fan-out roster counts are seat
    /// information about other people, which the row-visibility rule below
    /// keeps to owners and admins.
    pub fanout: Option<FanoutReach>,
    /// **This channel's next message reaches NO ocean at all.** A fan-out to
    /// an empty set is a SUCCESSFUL fan-out as far as ingest is concerned:
    /// nothing is written, and no error is raised anywhere in the system.
    /// Without this bit the only evidence is `fanout.mapped_members == 0`.
    ///
    /// Sent to EVERY viewer, including an org member whose
    /// [`ChannelDestination::fanout`] is masked. That is the deliberate,
    /// documented exception to "members get no roster facts", and the
    /// narrowest one that fixes a real harm: masking left a member's row
    /// reading "nobody chose · still ingesting" while their own messages
    /// were being dropped. One boolean about THIS CHANNEL is not a roster —
    /// it names nobody, counts nobody, and does not say whether the viewer
    /// themselves is among the unmapped. What it does say is the one thing a
    /// member needs in order to go ask an admin.
    ///
    /// Computed from the UNMASKED reach for that reason — see
    /// [`build_channel_row`], where masking happens after this is derived.
    /// Always `false` for a single named ocean.
    pub dropping: bool,
    /// When the pick was made — the watched row's `created_at`. `None`
    /// exactly when `kind == Default` (there is no row, so nothing was ever
    /// "chosen").
    pub chosen_at: Option<DateTime<Utc>>,
    /// `true` when `slack_watched_channels.display_name` (captured once, at
    /// pick time — see `connectors::slack::ensure_slack_watched_channel`'s
    /// doc comment on why it never updates on conflict) no longer matches
    /// the channel's LIVE name from `conversations.list`. Always `false` for
    /// `kind == Default`: there is no stored name to have gone stale.
    ///
    /// **Known limitation, found while building this endpoint, not fixed
    /// here (read-only endpoint, out of scope):** every current write site in
    /// `routes::slack_interactivity` that persists a channel pick passes
    /// `display_name: String::new()` — EXCEPT the "route to an existing team
    /// ocean" pick (`"cs_dest_org_team_existing"`), which passes the TEAM
    /// OCEAN's name, not the channel's. So in today's data this flag can
    /// only ever fire (a) never, for the empty-string rows, since an empty
    /// stored name is treated as "nothing to compare" below, or (b) as a
    /// near-permanent false positive for team-ocean-routed channels, whose
    /// stored "name" is a team name that will essentially never equal the
    /// channel's own live name. The field is implemented exactly per this
    /// endpoint's contract (compare stored vs. live); the write-side bug that
    /// makes it unreliable for one pick path is a `routes::slack_interactivity`
    /// fix, not a `GET` handler's.
    pub stale_name: bool,
}

/// Per-channel huddle capture health — the `slack_huddle_captures` ledger
/// rolled up to ONE channel, mirroring
/// [`crate::connectors::slack_onboarding::HuddlesStatus`]'s "skipped is
/// normal, not an error" rule, but DELIBERATELY NOT grouping `reasons` the
/// way that struct's `top_skip_reasons` groups-and-counts. Two different
/// aggregation levels call for two different shapes:
///
/// * The onboarding checklist rolls up EVERY huddle across the whole
///   workspace, where free-form prose reasons (docs/connectors/slack.md §5's
///   still-open "aggregate health surface" item — no stable code exists yet)
///   would otherwise produce a long tail of near-duplicate strings; grouping
///   and counting the top 3 is what makes that legible.
/// * This struct is scoped to ONE channel, where the row count is small
///   enough that grouping buys nothing and risks hiding a real distinction
///   between two similarly-worded but different reasons. So `reasons` is the
///   raw, uncounted, unbucketed strings — capped at 3, most-recent first —
///   exactly as they came out of `skip_reason`. Do not add grouping here; if
///   a stable skip-reason CODE ever ships (the thing §5 says aggregation is
///   waiting on), that is the point to reconsider this, not before.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct HuddleChannelHealth {
    pub seen: i64,
    pub ingested: i64,
    pub skipped: i64,
    /// Raw `skip_reason` strings from this channel's skipped huddles, most
    /// recent first, capped at 3. Never deduplicated or bucketed — see this
    /// struct's own doc comment.
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ChannelRow {
    pub channel_id: String,
    /// `#`-prefixed, from the LIVE `conversations.list` call — never
    /// `slack_watched_channels.display_name` (see [`ChannelDestination::
    /// stale_name`]'s doc comment for why that column cannot be trusted as a
    /// current name).
    pub name: String,
    pub destination: ChannelDestination,
    /// Most recent `atoms.timestamp` (the source event time, not
    /// `created_at`/ingest time — a human asking "when was this channel last
    /// active" means Slack-side activity, not when our worker happened to
    /// process it) for this channel, across both message and huddle-
    /// transcript atoms (both ingest paths stamp `channel_id`/`workspace_id`,
    /// migrations `0034`/`0058`). `None` when `atom_count == 0`.
    pub last_activity_at: Option<DateTime<Utc>>,
    pub atom_count: u64,
    /// Which oceans this channel's corals are ACTUALLY in, biggest share
    /// first — the "current state" companion to `destination`'s "future
    /// state" (see [`LandedOcean`]). Empty exactly when `atom_count == 0`
    /// for an owner/admin viewer; the two are computed from the SAME rows by
    /// the same predicate, so they can never disagree about whether anything
    /// landed.
    ///
    /// **Restricted for an ORG MEMBER viewer to oceans they own** — a member
    /// must not be handed other members' private-ocean names just for
    /// opening a settings page (the row-visibility rule below). For that
    /// viewer the entries therefore do NOT sum to `atom_count` (which stays
    /// the channel's true total), and it can be empty while `atom_count` is
    /// non-zero: "none of these are yours", not "nothing landed".
    pub landed_in: Vec<LandedOcean>,
    pub huddles: HuddleChannelHealth,
}

/// Step 1's live-Slack-call outcome for the whole row list — same
/// "unavailable is not zero" contract as
/// [`crate::connectors::slack_onboarding::ChannelsStatus`].
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ChannelsSection {
    pub state: ChannelsListState,
    pub error: Option<String>,
    /// Always empty when `state == Unavailable` — see [`ChannelsSection::
    /// unavailable`]. Never partially populated: a failed
    /// `conversations.list` means the row SET itself could not be
    /// established, not merely that one channel's detail is missing.
    pub rows: Vec<ChannelRow>,
}

impl ChannelsSection {
    fn unavailable(error: String) -> Self {
        Self { state: ChannelsListState::Unavailable, error: Some(error), rows: Vec::new() }
    }
}

/// The full payload for one connected Slack workspace's channel management
/// page.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SlackChannelManagement {
    /// Slack team id (`T…`). Returned for two reasons, neither cosmetic:
    ///
    /// 1. It is half of a Slack deep link — `https://app.slack.com/client/
    ///    {team_id}/{channel_id}` — so without it a client holding every
    ///    `channel_id` in `rows` still cannot link any of them into Slack.
    /// 2. `slack_integrations` is `UNIQUE (account_id, workspace_id)`, NOT
    ///    unique on `account_id`, so an account may legitimately have several
    ///    workspaces connected. The caller resolves ONE of them; without its
    ///    id in the payload the client cannot say which one this table
    ///    describes.
    pub workspace_id: String,
    pub workspace_name: String,
    pub plane: Plane,
    /// `"owner" | "admin" | "member"`. On the personal plane this is always
    /// `"owner"` — see [`crate::routes::slack::channel_management`]'s doc
    /// comment for why this is read straight off the already-live-checked
    /// `UserContext::role` rather than a fresh `account_members` query.
    pub viewer_role: String,
    pub channels: ChannelsSection,
    /// Slack corals that carry NO `channel_id` and therefore cannot appear in
    /// ANY row above — older ingest paths and seeded history wrote atoms
    /// without the structured `channel_id`/`workspace_id` provenance
    /// migrations `0034`/`0058` added (in production on 2026-09-09, 19 of one
    /// org's 35 Slack atoms). They are real captured content; leaving them
    /// out of every per-channel count with nothing said made the page
    /// under-report, so the residue is stated instead of hidden.
    ///
    /// `None` — never a zeroed struct — when it was not computed: the live
    /// `conversations.list` failed (`state: "unavailable"`), the viewer is an
    /// unlinked org member (`state: "unlinked"`), or the viewer is an org
    /// MEMBER, for whom a workspace-wide residue count is information about
    /// content outside their own row set. Zeros mean it really is zero.
    pub unattributed_atom_count: Option<UnattributedAtoms>,
}

// ── Row visibility (org-member restriction) ─────────────────────────────────

/// Who gets to see which rows, **and how much of each row**. Org members
/// must not receive the full workspace routing table — channel names plus
/// destinations map out org structure (which team ocean a channel feeds) and
/// seat counts (fan-out membership), both of which leak information a plain
/// member should not be handed just for opening a connector settings page.
/// Owners and admins see everything; a member sees the channels their
/// confirmed Slack identity is a member of.
///
/// This enum governs THREE rules, not one, because all three answer the same
/// question and splitting them across three parameters is how one of them
/// eventually gets forgotten:
///
/// 1. **Which channels are rows at all** ([`row_is_visible`]).
/// 2. **Whether [`ChannelDestination::fanout`] is populated** — how many of
///    the org's people a fan-out reaches is a roster fact about other
///    people, so a member gets `None`, not a number.
/// 3. **Which oceans [`ChannelRow::landed_in`] may name** — a member sees
///    only oceans they own, never a colleague's private-ocean name.
///
/// **Enforced here, in the row-building function itself — never left to the
/// caller to filter, and never expressed as a client-side hide.** The route
/// handler decides WHICH variant to pass based on `viewer_role`/`plane`, but
/// once it has decided, [`channel_rows_from_data`] is the only place rows are
/// ever produced, so there is no code path that can forget to filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowVisibility<'a> {
    /// Owner or admin (or the personal plane, which reports `"owner"` and
    /// has no "everyone" to hide anything from). Every live channel is a
    /// row, with every field populated.
    Unrestricted,
    /// An org member: exactly the channels their CONFIRMED Slack identity
    /// is a member of (one cached `users.conversations` lookup — see
    /// [`MemberChannelScope`]). Read scope = write scope, per the 2026-08-04
    /// governance decision: a member sees FULL rows (destination, health,
    /// stats) for their channels — including `Fanout` and `Default` rows,
    /// which the previous own-ocean-only filter hid even when the fan-out
    /// was actively feeding the member's ocean. Visibility keys on the
    /// member's CHANNEL membership, never on a watched row existing: a
    /// fan-out channel may have no tenant-side row at all (migration 0013's
    /// NULL-`graph_id` policy gap), and the member is IN the channel — they
    /// can already read every message in it; the routing row is not the
    /// secret.
    ///
    /// `caller_user_id` is what makes rules 2 and 3 above expressible: it is
    /// the owner every `landed_in` ocean must match. Carried IN the variant
    /// rather than passed alongside it so a restricted view cannot be
    /// constructed without saying whose it is.
    MemberChannels {
        channels: &'a std::collections::HashSet<String>,
        caller_user_id: Uuid,
    },
}

fn row_is_visible(visibility: RowVisibility<'_>, channel_id: &str) -> bool {
    match visibility {
        RowVisibility::Unrestricted => true,
        RowVisibility::MemberChannels { channels, .. } => channels.contains(channel_id),
    }
}

/// Rule 3 of [`RowVisibility`]: may this viewer be told, by name, that corals
/// landed in this ocean? Owners/admins yes, always. A member only for oceans
/// they OWN — which is their own member-private ocean and (by
/// `graphs_member_requires_owner`, migration `0090`) nothing else, since
/// `owner_user_id IS NOT NULL` holds for `kind = 'member'` rows alone.
///
/// `owner_user_id` is the graph's, and is `None` for every account-wide ocean
/// (personal/team/org) — which is exactly why the member arm must NOT read
/// "unowned" as "shared, so show it": a team ocean's NAME is the org
/// structure this restriction exists to withhold.
fn landed_ocean_is_visible(visibility: RowVisibility<'_>, owner_user_id: Option<Uuid>) -> bool {
    match visibility {
        RowVisibility::Unrestricted => true,
        RowVisibility::MemberChannels { caller_user_id, .. } => owner_user_id == Some(caller_user_id),
    }
}

/// Rule 2 of [`RowVisibility`]: may this viewer be told how many people a
/// fan-out reaches? A member may not — see the enum's own doc comment.
fn fanout_reach_is_visible(visibility: RowVisibility<'_>) -> bool {
    matches!(visibility, RowVisibility::Unrestricted)
}

/// The viewer's channel-management scope, resolved ONCE per request and
/// shared verbatim by the read side ([`derive_channel_management`]) and the
/// write side (`routes::slack`'s destination/stop-watching gates) — the two
/// must never disagree about which channels are "yours".
///
/// Fail-closed on every non-answer: an unlinked member and a failed lookup
/// both refuse rather than degrade to all-or-nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemberChannelScope {
    /// Admin/owner, or the personal plane: no member scoping applies.
    Unrestricted,
    /// A linked org member: the channel ids their Slack identity is in.
    MemberChannels(std::collections::HashSet<String>),
    /// An org member with no confirmed `connector_identities` row — the
    /// linking act is the consent gate, so there is nothing to scope to
    /// yet. Reads render the link prompt; writes 403 naming the fix.
    Unlinked,
    /// The `users.conversations` lookup failed. NOT an empty set: a failed
    /// lookup must never read as "you are in no channels" (the
    /// unavailable-vs-zero rule) — reads render `state: "unavailable"`,
    /// writes refuse with a retryable 503.
    Unavailable(String),
}

/// Resolve the caller's [`MemberChannelScope`]. `Err` is a DB failure (500).
///
/// Non-members short-circuit to `Unrestricted` without touching Slack or
/// `connector_identities`; an unlinked member short-circuits without the
/// Slack call (nothing to look up). The lookup itself goes through
/// `slack_onboarding::cached_member_channels` — the ONE `users.conversations`
/// cache — so this, the onboarding checklist, and the row filter all see the
/// same answer inside one TTL window.
///
/// `fresh_membership` — pass `true` from WRITE gates: authorization must not
/// ride a cached membership answer (a member removed from a channel could
/// otherwise act on it for the rest of the TTL window). Reads pass `false`
/// and accept at most `SLACK_LOOKUP_TTL` of display staleness; every write
/// re-verifies live and refreshes the shared cache as a side effect.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_member_channel_scope(
    http: &reqwest::Client,
    tenant_pool: &PgPool,
    account_id: Uuid,
    plane: AccountPlane,
    viewer_role: &str,
    caller_user_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
    fresh_membership: bool,
) -> Result<MemberChannelScope, String> {
    if plane != AccountPlane::Org || viewer_role != "member" {
        return Ok(MemberChannelScope::Unrestricted);
    }
    let link =
        crate::connectors::slack_onboarding::fetch_member_link(tenant_pool, workspace_id, caller_user_id)
            .await?;
    let Some(slack_user_id) = link.slack_user_id else {
        return Ok(MemberChannelScope::Unlinked);
    };
    match crate::connectors::slack_onboarding::cached_member_channels(
        http,
        account_id,
        workspace_id,
        bot_token,
        &slack_user_id,
        fresh_membership,
    )
    .await
    {
        Ok(ids) => Ok(MemberChannelScope::MemberChannels(ids.into_iter().collect())),
        Err(e) => Ok(MemberChannelScope::Unavailable(e)),
    }
}

// ── Pure derivation ──────────────────────────────────────────────────────────

/// One `slack_watched_channels` row, trimmed to what row-building needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WatchedChannelRow {
    pub mode: String,
    pub graph_id: Option<Uuid>,
    pub display_name: String,
    pub created_at: DateTime<Utc>,
}

/// One channel's atom aggregate — see [`fetch_atom_stats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChannelAtomStats {
    pub atom_count: u64,
    pub last_activity_at: Option<DateTime<Utc>>,
}

/// One `(channel, ocean)` pair's atom count as read from the database — see
/// [`fetch_landed_in`]. The pre-visibility shape: it carries the graph's
/// `owner_user_id` purely so [`landed_ocean_is_visible`] can decide whether
/// this viewer may be told the ocean's name, and that field is dropped on the
/// way into the wire-side [`LandedOcean`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LandedOceanRow {
    pub ocean_name: String,
    pub ocean_kind: String,
    /// `graphs.owner_user_id` — `Some` exactly for `kind = 'member'` oceans
    /// (migration `0090`'s `graphs_member_requires_owner`), `None` for
    /// account-wide personal/team/org oceans.
    pub owner_user_id: Option<Uuid>,
    pub atom_count: u64,
}

/// The account-level routing facts every `Default`/`Fanout` row on this page
/// needs, resolved ONCE per request (never once per row) by the SAME
/// functions ingest itself calls — `connectors::slack::
/// resolve_account_default_destination` and
/// `connectors::slack::resolve_org_fanout_destinations`. Nothing in this
/// module re-derives "where would a message land"; see
/// [`resolve_live_routing`].
///
/// Both fields default to `None` and stay that way when nothing on the page
/// needs them: a workspace whose every visible channel carries an explicit
/// `mode = 'single'` row asks neither question, and resolving them anyway
/// would put a fan-out membership computation on a page that has no fan-out
/// to describe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountDefault {
    /// PERSONAL plane: the account's own primary ocean, by `graph_id`.
    PrimaryOcean(Uuid),
    /// ORG plane: a live per-member fan-out, with its current reach.
    MemberFanOut(FanoutReach),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LiveRouting {
    /// What a channel with NO watched row ingests through. **An enum, not
    /// two independent options**, and that is the whole fix for a real bug:
    /// when these were a flat `account_primary_graph_id` plus a shared
    /// `fanout`, a PERSONAL account holding one legacy `mode = 'fanout'` row
    /// populated the shared reach, and every `Default` row on that page then
    /// carried BOTH its correct ocean name AND an org fan-out reach that had
    /// nothing to do with it — rendering a healthy personal default as
    /// "nobody's ocean · messages dropped". The account default has exactly
    /// one shape on a given plane; the type now says so.
    ///
    /// `None` when no visible row is `kind == Default`.
    pub account_default: Option<AccountDefault>,
    /// The reach an EXPLICIT `mode = 'fanout'` row resolves through — kept
    /// separate from [`LiveRouting::account_default`] for the reason above,
    /// even though on the org plane both are the same computation
    /// (`resolve_org_fanout_destinations`) and the second is answered from
    /// the first rather than re-run.
    ///
    /// Resolved on the personal plane too, where the picker cannot produce a
    /// `mode = 'fanout'` row but the table's CHECK constraint permits one:
    /// `connectors::slack::materialize_channel_destination` runs the same
    /// org fan-out for such a row REGARDLESS of plane, so describing it any
    /// other way here would describe something ingest does not do. `None`
    /// when no visible row carries such a pick.
    pub explicit_fanout: Option<FanoutReach>,
}

/// Pure: fold `(ingested, skip_reason)` rows for ONE channel — already
/// ordered most-recent-first by the caller — into a [`HuddleChannelHealth`].
/// Split out for the same testability reason
/// `slack_onboarding::huddles_status_from_rows` is: this is where "skipped is
/// normal" and "reasons are raw, not grouped" both live, and both are worth
/// pinning without a database.
pub(crate) fn huddle_health_from_rows(rows: &[(bool, Option<String>)]) -> HuddleChannelHealth {
    let seen = rows.len() as i64;
    let ingested = rows.iter().filter(|(is_ingested, _)| *is_ingested).count() as i64;
    // Same "compute from skip_reason, not by subtraction" reasoning as
    // `slack_onboarding::huddles_status_from_rows` — see that function's own
    // comment.
    let skipped =
        rows.iter().filter(|(is_ingested, reason)| !*is_ingested && reason.is_some()).count() as i64;
    let reasons: Vec<String> = rows
        .iter()
        .filter(|(is_ingested, reason)| !*is_ingested && reason.is_some())
        .filter_map(|(_, reason)| reason.clone())
        .take(3)
        .collect();
    HuddleChannelHealth { seen, ingested, skipped, reasons }
}

/// Pure: build the full row list from already-fetched data — three grouped
/// lookups (`watched`/`atom_stats`/`huddle_rows`, each keyed by
/// `channel_id`) zipped against the live channel list, one pass, no queries.
/// This is the function [`derive_channel_management`] exists to call; kept
/// separate so the "unrouted channel is `kind: default`, not missing" and
/// "org member sees only their own ocean's rows" rules are unit-testable
/// without a database or a live Slack call.
///
/// `live_channels` — `(channel_id, name)` pairs from
/// [`crate::connectors::slack::fetch_bot_member_channels`]; `name` here is
/// the RAW Slack name (no `#`), matching that function's own return shape.
/// `huddle_rows` — per channel, `(ingested, skip_reason)` pairs ALREADY
/// ordered most-recent-first (`fetch_huddle_stats` orders by `created_at
/// DESC`); this function does not re-sort them.
///
/// Output is sorted by name (case-insensitive) then `channel_id` — a live
/// `conversations.list` gives no ordering guarantee at all, and a page
/// re-rendering its rows in a different order on every load would be a worse
/// experience than a network call being merely unordered.
pub(crate) fn channel_rows_from_data(
    live_channels: &[(String, String)],
    watched: &HashMap<String, WatchedChannelRow>,
    graph_names: &HashMap<Uuid, String>,
    atom_stats: &HashMap<String, ChannelAtomStats>,
    landed: &HashMap<String, Vec<LandedOceanRow>>,
    routing: &LiveRouting,
    huddle_rows: &HashMap<String, Vec<(bool, Option<String>)>>,
    visibility: RowVisibility,
) -> Vec<ChannelRow> {
    let mut rows: Vec<ChannelRow> = live_channels
        .iter()
        .filter(|(channel_id, _)| row_is_visible(visibility, channel_id))
        .map(|(channel_id, live_name)| {
            build_channel_row(
                channel_id,
                live_name,
                watched.get(channel_id),
                graph_names,
                atom_stats,
                landed,
                routing,
                huddle_rows,
                visibility,
            )
        })
        .collect();
    rows.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()).then_with(|| a.channel_id.cmp(&b.channel_id)));
    rows
}

#[allow(clippy::too_many_arguments)]
fn build_channel_row(
    channel_id: &str,
    live_name: &str,
    watched: Option<&WatchedChannelRow>,
    graph_names: &HashMap<Uuid, String>,
    atom_stats: &HashMap<String, ChannelAtomStats>,
    landed: &HashMap<String, Vec<LandedOceanRow>>,
    routing: &LiveRouting,
    huddle_rows: &HashMap<String, Vec<(bool, Option<String>)>>,
    visibility: RowVisibility,
) -> ChannelRow {
    // ── Reach, then masking, in that order ───────────────────────────────
    //
    // `reach` is the TRUE fan-out for this row, whoever is looking. Rule 2 of
    // `RowVisibility` masks the roster NUMBERS from a member below — but
    // `dropping` is derived from `reach` BEFORE that masking, because a
    // member whose messages are going nowhere has to be told so. Deriving it
    // from the masked value instead is exactly the bug this ordering fixes:
    // it made a member's broken channel render as "nobody chose · still
    // ingesting". See `ChannelDestination::dropping`.
    let reach = match watched {
        // The account default has ONE shape per plane — an ocean or a
        // fan-out, never both. See `LiveRouting::account_default`.
        None => match routing.account_default {
            Some(AccountDefault::MemberFanOut(r)) => Some(r),
            Some(AccountDefault::PrimaryOcean(_)) | None => None,
        },
        Some(w) if w.mode != "single" => routing.explicit_fanout,
        Some(_) => None,
    };
    let dropping = reach.is_some_and(|r| r.mapped_members == 0);
    let visible_fanout = if fanout_reach_is_visible(visibility) { reach } else { None };

    let destination = match watched {
        // No row: the account-level default, resolved by ingest's OWN
        // fallback resolver (`resolve_live_routing`). The match is on the
        // enum rather than on two options, so a personal default cannot pick
        // up a sibling row's fan-out reach.
        None => ChannelDestination {
            kind: DestinationKind::Default,
            ocean_name: match routing.account_default {
                Some(AccountDefault::PrimaryOcean(g)) => graph_names.get(&g).cloned(),
                Some(AccountDefault::MemberFanOut(_)) | None => None,
            },
            fanout: visible_fanout,
            dropping,
            chosen_at: None,
            stale_name: false,
        },
        Some(w) => {
            // Empty stored name means "nothing was ever recorded" (the
            // convention every current write site but one follows — see
            // `ChannelDestination::stale_name`'s doc comment) — never treated
            // as a stale mismatch against the live name.
            let stale_name = !w.display_name.is_empty() && w.display_name != live_name;
            match w.mode.as_str() {
                "single" => ChannelDestination {
                    kind: DestinationKind::Single,
                    ocean_name: w.graph_id.and_then(|g| graph_names.get(&g).cloned()),
                    // One named ocean — there is no fan-out to describe, and
                    // filling this in from `routing` would attach the
                    // account default's reach to a row that does not use it.
                    fanout: None,
                    // A named ocean always exists to receive the next
                    // message; `ocean_name: None` here means the graph row
                    // could not be resolved for DISPLAY, not that ingest has
                    // nowhere to write.
                    dropping: false,
                    chosen_at: Some(w.created_at),
                    stale_name,
                },
                // "fanout", and (defensively) anything else the table's own
                // CHECK constraint should already have made unreachable —
                // rendered as Fanout rather than panicking, same "loud,
                // catchable degradation over a panic on a webhook-adjacent
                // read path" choice `materialize_channel_destination` makes
                // for its own `other` arm.
                _ => ChannelDestination {
                    kind: DestinationKind::Fanout,
                    ocean_name: None,
                    fanout: visible_fanout,
                    dropping,
                    chosen_at: Some(w.created_at),
                    stale_name,
                },
            }
        }
    };

    let atoms = atom_stats.get(channel_id);
    let huddles = huddle_rows
        .get(channel_id)
        .map(|rows| huddle_health_from_rows(rows))
        .unwrap_or(HuddleChannelHealth { seen: 0, ingested: 0, skipped: 0, reasons: Vec::new() });

    // Biggest share first, then by name so the order is stable across loads
    // for the (common) ties at 1 coral each — `fetch_landed_in`'s GROUP BY
    // gives no ordering guarantee, and a table that reshuffles its own rows
    // between refreshes reads as movement that did not happen.
    let mut landed_in: Vec<LandedOcean> = landed
        .get(channel_id)
        .map(|rows| {
            rows.iter()
                .filter(|r| landed_ocean_is_visible(visibility, r.owner_user_id))
                .map(|r| LandedOcean {
                    ocean_name: r.ocean_name.clone(),
                    ocean_kind: r.ocean_kind.clone(),
                    atom_count: r.atom_count,
                })
                .collect()
        })
        .unwrap_or_default();
    landed_in.sort_by(|a, b| {
        b.atom_count.cmp(&a.atom_count).then_with(|| a.ocean_name.cmp(&b.ocean_name))
    });

    ChannelRow {
        channel_id: channel_id.to_string(),
        name: format!("#{live_name}"),
        destination,
        last_activity_at: atoms.and_then(|a| a.last_activity_at),
        atom_count: atoms.map(|a| a.atom_count).unwrap_or(0),
        landed_in,
        huddles,
    }
}

// ── Impure: DB + HTTP ────────────────────────────────────────────────────────

/// Derive the full [`SlackChannelManagement`] payload for one connected
/// workspace. Caller has already resolved the `slack_integrations` row (same
/// division of labor as `slack_onboarding::derive_status`) and already knows
/// `viewer_role` (see [`crate::routes::slack::channel_management`]'s doc
/// comment — it comes from the already-live-checked `UserContext::role`, no
/// extra query here).
///
/// `caller_user_id` is only ever consulted when `plane == Org && viewer_role
/// == "member"` (to resolve [`RowVisibility::MemberChannels`], which carries
/// it so the restricted view can tell the caller's OWN ocean from a
/// colleague's) — unused otherwise, but always required so a future
/// viewer-role branch cannot silently reach for a caller identity it was
/// never given.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn derive_channel_management(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    plane: AccountPlane,
    viewer_role: &str,
    caller_user_id: Uuid,
    workspace_id: &str,
    workspace_name: &str,
    bot_token: &str,
) -> Result<SlackChannelManagement, String> {
    // Through `slack_onboarding`'s memo, not `fetch_bot_member_channels`
    // directly: this module's header promises the checklist and this page
    // "can never disagree about whether the bot can currently see its channel
    // list", and two independent live calls made that promise good only by
    // luck. See `cached_bot_channels`' own doc comment.
    let live_result = crate::connectors::slack_onboarding::cached_bot_channels(
        http,
        control_pool,
        account_id,
        workspace_id,
        bot_token,
    )
    .await;

    // `unattributed_atom_count` is computed inside the Ok/visible arm only —
    // see its own doc comment on `SlackChannelManagement` for why every other
    // arm must leave it `None` rather than reporting a zero it never counted.
    let mut unattributed_atom_count: Option<UnattributedAtoms> = None;

    let channels = match live_result {
        // Live call failed — the row SET itself cannot be established. Never
        // fall through to the DB reads below: an `unavailable` `conversations.
        // list` must render as `state: "unavailable"` with an empty row list,
        // not as "here are the channels we happen to have watched rows for"
        // (which would be a DIFFERENT, and misleading, row set — see this
        // module's header).
        Err(e) => ChannelsSection::unavailable(e),
        Ok(live_channels) => {
            // Scope FIRST: an unlinked member has an empty page by
            // definition and skips every DB read below; a failed member
            // lookup must render "unavailable" rather than a row set we
            // cannot actually scope (unavailable-vs-zero, viewer edition).
            let scope = resolve_member_channel_scope(
                http,
                tenant_pool,
                account_id,
                plane,
                viewer_role,
                caller_user_id,
                workspace_id,
                bot_token,
                false, // read path: cached membership, ≤SLACK_LOOKUP_TTL stale
            )
            .await?;
            match scope {
                MemberChannelScope::Unlinked => {
                    ChannelsSection { state: ChannelsListState::Unlinked, error: None, rows: Vec::new() }
                }
                MemberChannelScope::Unavailable(e) => ChannelsSection::unavailable(e),
                MemberChannelScope::Unrestricted | MemberChannelScope::MemberChannels(_) => {
                    let visibility = match &scope {
                        MemberChannelScope::MemberChannels(mine) => {
                            RowVisibility::MemberChannels { channels: mine, caller_user_id }
                        }
                        _ => RowVisibility::Unrestricted,
                    };
                    // Stats are fetched only for the channels this viewer
                    // will actually see — for a member that can be a small
                    // fraction of the workspace, and the hidden rows' stats
                    // would be computed just to be filtered out.
                    let channel_ids: Vec<String> = live_channels
                        .iter()
                        .map(|(id, _)| id.clone())
                        .filter(|id| row_is_visible(visibility, id))
                        .collect();

                    let watched = fetch_watched_rows(tenant_pool, account_id, workspace_id).await?;

                    // Which account-level routing questions this page
                    // actually asks, decided from the VISIBLE rows only: a
                    // channel with no watched row ingests via the account
                    // default, and a `mode = 'fanout'` row resolves through
                    // the same fan-out computation. A page of purely
                    // `mode = 'single'` rows asks neither and resolves
                    // nothing — no `account_members` read, no per-member
                    // graph lookups, no `connector_health` warnings.
                    //
                    // **This is keyed on visible ROWS, not on viewer role,
                    // and that is deliberate.** An earlier reading of this
                    // was that an org member pays for a fan-out resolution
                    // whose numbers `build_channel_row` then masks away —
                    // true when the only consumer was
                    // `ChannelDestination::fanout`. It is not true now:
                    // `ChannelDestination::dropping` is derived from the
                    // SAME resolution and is sent to every viewer, precisely
                    // because a member whose messages are going nowhere was
                    // being shown "still ingesting". So the resolution is
                    // consumed by every viewer, and gating it on
                    // `fanout_reach_is_visible` would restore that defect
                    // rather than save work. What the fold still buys is the
                    // case that genuinely needs nothing: a fully-routed
                    // page, for anyone.
                    let (needs_account_default, needs_explicit_fanout) = live_channels
                        .iter()
                        .filter(|(id, _)| row_is_visible(visibility, id))
                        .fold((false, false), |(default, fanout), (id, _)| {
                            match watched.get(id) {
                                None => (true, fanout),
                                Some(w) if w.mode != "single" => (default, true),
                                Some(_) => (default, fanout),
                            }
                        });
                    let routing = resolve_live_routing(
                        control_pool,
                        tenant_pool,
                        account_id,
                        plane,
                        workspace_id,
                        needs_account_default,
                        needs_explicit_fanout,
                    )
                    .await?;

                    // The personal-plane account default names an ocean, so
                    // its graph joins the SAME batch name lookup the watched
                    // rows' graphs use — one query, not a second read for one
                    // id.
                    let graph_ids: Vec<Uuid> = watched
                        .values()
                        .filter_map(|w| w.graph_id)
                        .chain(match routing.account_default {
                            Some(AccountDefault::PrimaryOcean(g)) => Some(g),
                            _ => None,
                        })
                        .collect();
                    let graph_names = fetch_graph_names(tenant_pool, account_id, &graph_ids).await?;

                    let atom_stats =
                        fetch_atom_stats(tenant_pool, account_id, workspace_id, &channel_ids).await?;
                    let landed =
                        fetch_landed_in(tenant_pool, account_id, workspace_id, &channel_ids).await?;
                    let huddle_rows = fetch_huddle_stats(control_pool, account_id, &channel_ids).await?;

                    // Workspace-level, so it is deliberately NOT restricted
                    // to `channel_ids` — which is exactly why a RESTRICTED
                    // viewer must not receive it at all: it counts content
                    // outside their own row set (see
                    // `SlackChannelManagement::unattributed_atom_count`).
                    if matches!(visibility, RowVisibility::Unrestricted) {
                        unattributed_atom_count = Some(
                            fetch_unattributed_atom_count(tenant_pool, account_id, workspace_id).await?,
                        );
                    }

                    let rows = channel_rows_from_data(
                        &live_channels,
                        &watched,
                        &graph_names,
                        &atom_stats,
                        &landed,
                        &routing,
                        &huddle_rows,
                        visibility,
                    );
                    ChannelsSection { state: ChannelsListState::Ok, error: None, rows }
                }
            }
        }
    };

    Ok(SlackChannelManagement {
        workspace_id: workspace_id.to_string(),
        workspace_name: workspace_name.to_string(),
        plane: plane.into(),
        viewer_role: viewer_role.to_string(),
        unattributed_atom_count,
        channels,
    })
}

/// Resolve the account-level routing facts a `Default` or `Fanout` row needs,
/// by CALLING INGEST'S OWN RESOLVERS rather than describing what they do.
///
/// This is the whole fix for "the lands-in label lied". The label used to be
/// a client-side constant per [`DestinationKind`] — `Default` rendered as
/// "your account default" — which is true on the personal plane and false on
/// the org plane, where the account default is a live fan-out to each MAPPED
/// member's private ocean and the shared org ocean receives nothing at all. A
/// display-side re-implementation of that rule would have been a second
/// derivation of "where does content land", free to drift from the first the
/// moment the default changes; so this delegates:
///
/// * [`crate::connectors::slack::resolve_account_default_destination`] — the
///   extracted no-row branch of `resolve_channel_destinations`, i.e. exactly
///   what ingest falls back to.
/// * [`crate::connectors::slack::resolve_org_fanout_destinations`] — the same
///   membership computation an explicit `mode = 'fanout'` row resolves
///   through.
///
/// Both flags come from the VISIBLE rows (see [`derive_channel_management`]);
/// when both are `false` this issues no queries at all and returns
/// [`LiveRouting::default`].
///
/// **Errors propagate; they are never folded into an empty
/// [`LiveRouting`].** A `None` reach renders as "we are not saying how far
/// this reaches", which a failed roster read would turn into a quiet,
/// confident understatement of exactly the thing this page exists to report.
/// Same rule the rest of this module's reads follow.
async fn resolve_live_routing(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    plane: AccountPlane,
    workspace_id: &str,
    needs_account_default: bool,
    needs_explicit_fanout: bool,
) -> Result<LiveRouting, String> {
    use crate::connectors::slack::ChannelDestination as IngestDestination;

    let mut routing = LiveRouting::default();

    if needs_account_default {
        let resolved = crate::connectors::slack::resolve_account_default_destination(
            control_pool,
            tenant_pool,
            account_id,
            plane,
            workspace_id,
        )
        .await
        .map_err(|e| format!("channel management: account default destination: {e}"))?;
        routing.account_default = Some(match resolved {
            IngestDestination::Single(graph_id) => AccountDefault::PrimaryOcean(graph_id),
            IngestDestination::FanOut { graphs, unmapped_members, .. } => AccountDefault::MemberFanOut(
                FanoutReach { mapped_members: graphs.len(), unmapped_members },
            ),
        });
    }

    if needs_explicit_fanout {
        // On the ORG plane the account default IS this same
        // account-and-workspace-scoped computation, so answer from it rather
        // than paying for it twice. On the PERSONAL plane it is not (the
        // default there is one ocean), so a legacy `mode = 'fanout'` row is
        // resolved on its own — matching what
        // `materialize_channel_destination` does for that row, plane and all.
        routing.explicit_fanout = match routing.account_default {
            Some(AccountDefault::MemberFanOut(reach)) => Some(reach),
            _ => {
                let crate::connectors::slack::FanOut { graphs, unmapped_members, .. } =
                    crate::connectors::slack::resolve_org_fanout_destinations(
                        control_pool,
                        tenant_pool,
                        account_id,
                        workspace_id,
                    )
                    .await
                    .map_err(|e| format!("channel management: fan-out reach: {e}"))?;
                Some(FanoutReach { mapped_members: graphs.len(), unmapped_members })
            }
        };
    }

    Ok(routing)
}

/// Grouped read — every `slack_watched_channels` row for this workspace,
/// keyed by `channel_id`. Scoped: same RLS hazard as every other read of this
/// table (`current_setting('app.account_id')::uuid` ERRORS, not zero-rows, on
/// an unscoped connection — `slack_onboarding::fetch_routed_status`'s own
/// comment, which this mirrors exactly, including reading `tenant_pool`).
async fn fetch_watched_rows(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<HashMap<String, WatchedChannelRow>, String> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| format!("channel management: watched rows scope: {e}"))?;
    let rows = sqlx::query(
        "SELECT channel_id, mode, graph_id, display_name, created_at \
         FROM slack_watched_channels WHERE account_id = $1 AND workspace_id = $2",
    )
    .bind(account_id)
    .bind(workspace_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("channel management: watched rows query: {e}"))?;
    tx.rollback().await.ok();

    Ok(rows
        .into_iter()
        .map(|r| {
            let channel_id: String = r.get("channel_id");
            let watched = WatchedChannelRow {
                mode: r.get("mode"),
                graph_id: r.get("graph_id"),
                display_name: r.get("display_name"),
                created_at: r.get("created_at"),
            };
            (channel_id, watched)
        })
        .collect())
}

/// Conditional grouped read (only reached when at least one watched row —
/// or the personal-plane account default — names a `graph_id`): resolve
/// destination ocean names in ONE batch,
/// keyed by `graph_id`. Skipped entirely (no query issued) when `graph_ids`
/// is empty — every channel is either `Default` or `Fanout`, neither of
/// which needs an ocean name. Scoped like [`fetch_watched_rows`]: `graphs`
/// carries the same `account_isolation` policy.
async fn fetch_graph_names(
    tenant_pool: &PgPool,
    account_id: Uuid,
    graph_ids: &[Uuid],
) -> Result<HashMap<Uuid, String>, String> {
    if graph_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| format!("channel management: graph names scope: {e}"))?;
    let rows = sqlx::query("SELECT id, name FROM graphs WHERE account_id = $1 AND id = ANY($2)")
        .bind(account_id)
        .bind(graph_ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| format!("channel management: graph names query: {e}"))?;
    tx.rollback().await.ok();

    Ok(rows.into_iter().map(|r| (r.get("id"), r.get("name"))).collect())
}

/// The atoms half: one grouped aggregate over the WHOLE
/// `channel_ids` set (from the live channel list), never one query per
/// channel — see this module's header. `last_activity_at` is `MAX(timestamp)`
/// (the source event time), not `MAX(created_at)` (ingest time) — see
/// [`ChannelRow::last_activity_at`]'s doc comment for why. Scoped like
/// [`fetch_watched_rows`]: `atoms` carries a graph-mediated
/// `account_isolation` policy (migration `0083`), exactly like
/// `slack_onboarding::fetch_capture_status`, which this mirrors.
///
/// **The `graphs` join is the account boundary, not decoration.** This query
/// used to filter on `workspace_id`/`channel_id` ALONE, leaning entirely on
/// the `begin_account_scoped` wrapper for isolation. That was wrong twice
/// over. (a) Two DIFFERENT CoralSwarm accounts can each connect the SAME
/// Slack workspace — `slack_integrations` is `UNIQUE (account_id,
/// workspace_id)`, not unique on `workspace_id` — so `workspace_id` is NOT an
/// account discriminator, and each account read the other's per-channel atom
/// counts and last-activity timestamps. (b) The RLS backstop that was
/// supposed to catch (a) was INERT at the time: the application still
/// connected as its owner/`BYPASSRLS` role, so `begin_account_scoped` set
/// `app.account_id` but enforced nothing, and the explicit `g.account_id =
/// $1` predicate below was the ONLY thing keeping two accounts on one
/// workspace apart. OPS-4 HAS SINCE RUN (verified in production 2026-08-04:
/// the app connects as the non-`BYPASSRLS` `coralswarm_app` role;
/// `0083_consumer_rls`'s header predates this and is stale — see
/// `scripts/check_rls_coverage.sh`'s header), so the wrapper is now a real
/// second layer; the predicate remains the first. `atoms.graph_id` is `NOT
/// NULL REFERENCES graphs(id)` (migration `0003`), so the inner join drops no
/// rows.
///
/// **`workspace_id` is matched permissively (`= $2 OR IS NULL`), and that is
/// not a loosening of the account boundary.** `atoms.workspace_id` only
/// arrived in migration `0058`; every Slack atom written before it, plus
/// seeded history and any ingest path that never set the column, carries
/// NULL. Filtering on `= $2` alone therefore did not scope those rows — it
/// DELETED them from the page: in production on 2026-09-09, 19 of one org's
/// 35 Slack atoms had a NULL `workspace_id`, so every per-channel count and
/// last-activity timestamp on the page silently under-reported, and a channel
/// whose corals were all pre-`0058` read as "0 corals" — indistinguishable
/// from "nothing was ever captured here". A count that is wrong in the
/// direction of "nothing is working" is the one this page can least afford.
///
/// What still bounds the query is unchanged and sufficient: `g.account_id =
/// $1` (the account boundary, argued above) AND `a.channel_id = ANY($3)`,
/// whose values are Slack channel ids from THIS workspace's live
/// `conversations.list`. A NULL-`workspace_id` atom can only join a row here
/// by carrying one of those ids, and a Slack `C…` id belongs to exactly one
/// workspace. The multi-workspace case the `graphs` join was added for is
/// likewise unaffected: two accounts sharing one workspace are still
/// separated by `g.account_id`, which is the predicate that was doing that
/// work all along. What this DOES admit is an atom from a SECOND workspace
/// connected by the SAME account whose id collides — impossible for real
/// Slack ids, and bounded to that one account either way.
///
/// **Checked, and NOT the same hole `slack_onboarding`'s capture counts had.**
/// Those relax `workspace_id` with no other anchor, so their NULL arm needs
/// `source_type = 'slack'` to stop a workspace-less Gmail or Google Chat atom
/// counting as Slack. This query keeps `a.channel_id = ANY($3)`, and `$3` is
/// this workspace's live Slack channel ids — so a non-Slack atom joins only
/// by carrying a Slack `C…` id. The nearest miss is real and worth naming:
/// `connectors::google_chat` DOES write `channel_id` with `workspace_id`
/// NULL, but its ids are Google Chat space names (`spaces/…`), and Teams
/// writes `19:…@thread.tacv2`; neither can equal a `C…`. A source filter
/// here would therefore exclude nothing except huddle transcripts
/// (`source_type = 'transcript'`), which this query is REQUIRED to count —
/// see [`ChannelRow::last_activity_at`]. So the asymmetry between the two
/// modules is deliberate, not an oversight in this one.
///
/// Atoms with a NULL `channel_id` cannot be attributed to any channel by any
/// predicate — and, lacking that anchor, a NULL-`workspace_id` one cannot be
/// attributed to this WORKSPACE either. [`fetch_unattributed_atom_count`]
/// counts them separately and keeps those two cases apart rather than
/// borrowing this function's channel-anchor argument, which does not transfer.
async fn fetch_atom_stats(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_ids: &[String],
) -> Result<HashMap<String, ChannelAtomStats>, String> {
    if channel_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| format!("channel management: atom stats scope: {e}"))?;
    let rows = sqlx::query(
        "SELECT a.channel_id, count(*) AS atom_count, max(a.timestamp) AS last_activity_at \
         FROM atoms a JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND (a.workspace_id = $2 OR a.workspace_id IS NULL) \
           AND a.channel_id = ANY($3) \
         GROUP BY a.channel_id",
    )
    .bind(account_id)
    .bind(workspace_id)
    .bind(channel_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("channel management: atom stats query: {e}"))?;
    tx.rollback().await.ok();

    Ok(rows
        .into_iter()
        .map(|r| {
            let channel_id: String = r.get("channel_id");
            let atom_count: i64 = r.get("atom_count");
            let stats = ChannelAtomStats {
                atom_count: atom_count as u64,
                last_activity_at: r.get("last_activity_at"),
            };
            (channel_id, stats)
        })
        .collect())
}

/// Where this workspace's corals ACTUALLY are: one grouped aggregate over the
/// whole `channel_ids` set, by `(channel, ocean)` — the present-tense half of
/// this module's two tenses (see its header). Never one query per channel and
/// never one per ocean.
///
/// **Same predicate as [`fetch_atom_stats`], deliberately verbatim** — same
/// `graphs` join as the account boundary, same permissive `workspace_id`
/// match, same `channel_id = ANY(...)` set. That is what makes
/// `sum(landed_in[*].atom_count) == atom_count` an identity for an
/// unrestricted viewer rather than a coincidence: two different predicates
/// over the same table would let the page's own two columns disagree about
/// how much it captured, which is a worse failure than either number being
/// absent. (For an org MEMBER the two intentionally differ, because
/// `channel_rows_from_data` then drops the oceans they may not be told about
/// — a visibility filter applied AFTER this read, not a second predicate
/// inside it. See [`ChannelRow::landed_in`].)
///
/// `graphs.owner_user_id` comes back so the row filter can tell the caller's
/// own member-private ocean from a colleague's; it never reaches the wire.
/// Scoped like [`fetch_atom_stats`].
async fn fetch_landed_in(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_ids: &[String],
) -> Result<HashMap<String, Vec<LandedOceanRow>>, String> {
    if channel_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| format!("channel management: landed-in scope: {e}"))?;
    let rows = sqlx::query(
        "SELECT a.channel_id, g.name AS ocean_name, g.kind AS ocean_kind, \
                g.owner_user_id, count(*) AS atom_count \
         FROM atoms a JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND (a.workspace_id = $2 OR a.workspace_id IS NULL) \
           AND a.channel_id = ANY($3) \
         GROUP BY a.channel_id, g.id, g.name, g.kind, g.owner_user_id",
    )
    .bind(account_id)
    .bind(workspace_id)
    .bind(channel_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("channel management: landed-in query: {e}"))?;
    tx.rollback().await.ok();

    let mut by_channel: HashMap<String, Vec<LandedOceanRow>> = HashMap::new();
    for r in rows {
        let channel_id: String = r.get("channel_id");
        let atom_count: i64 = r.get("atom_count");
        by_channel.entry(channel_id).or_default().push(LandedOceanRow {
            ocean_name: r.get("ocean_name"),
            ocean_kind: r.get("ocean_kind"),
            owner_user_id: r.get("owner_user_id"),
            atom_count: atom_count as u64,
        });
    }
    Ok(by_channel)
}

/// Slack corals that carry NO `channel_id` — the residue no per-channel row
/// can ever show (see [`UnattributedAtoms`] and
/// [`SlackChannelManagement::unattributed_atom_count`]).
///
/// **`source_type = 'slack'` is required here and absent from
/// [`fetch_atom_stats`], and the asymmetry is the point.** That query is
/// bounded by `channel_id = ANY(<this workspace's live Slack channel ids>)`,
/// which is already a Slack-specific discriminator; this one asks for
/// `channel_id IS NULL`, which discriminates nothing at all — without a
/// source-type filter it would count every Gmail message, coding session and
/// meeting note in the account and report them as unattributed SLACK corals.
///
/// The cost of that filter, stated rather than hidden: huddle-transcript
/// atoms are written with `source_type = 'transcript'`
/// (`workers::huddle`), so they are outside this count. They cannot be
/// unattributed anyway — a huddle happens IN a channel and that ingest path
/// always stamps `channel_id` — and widening to `IN ('slack','transcript')`
/// would sweep in Teams and meeting transcripts, trading a count that is
/// complete for one that is wrong.
///
/// **Two counts, not one, because this query has NO channel anchor.**
/// [`fetch_atom_stats`] can afford a permissive `workspace_id` match because
/// `channel_id = ANY(<live ids>)` still ties every row it returns to THIS
/// workspace — a Slack `C…` id belongs to exactly one workspace. Nothing of
/// the sort is true here: a row with `workspace_id IS NULL` AND
/// `channel_id IS NULL` carries no evidence of which workspace it came from
/// at all. Folding those into one number let the page say "N corals from this
/// workspace" about atoms that, on an account with two connected Slack
/// workspaces, would have been claimed identically by BOTH pages. So the
/// column that IS present is reported as this workspace's
/// ([`UnattributedAtoms::this_workspace`]) and the column that is absent is
/// reported as what it is — account-wide legacy content
/// ([`UnattributedAtoms::account_legacy`]) — and the copy says so.
///
/// Scoped like [`fetch_atom_stats`].
async fn fetch_unattributed_atom_count(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<UnattributedAtoms, String> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| format!("channel management: unattributed atoms scope: {e}"))?;
    let row = sqlx::query(
        "SELECT count(*) FILTER (WHERE a.workspace_id = $2) AS this_workspace, \
                count(*) FILTER (WHERE a.workspace_id IS NULL) AS account_legacy \
         FROM atoms a JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND a.source_type = 'slack' \
           AND a.channel_id IS NULL",
    )
    .bind(account_id)
    .bind(workspace_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("channel management: unattributed atoms query: {e}"))?;
    tx.rollback().await.ok();

    let this_workspace: i64 = row.get("this_workspace");
    let account_legacy: i64 = row.get("account_legacy");
    Ok(UnattributedAtoms {
        this_workspace: this_workspace as u64,
        account_legacy: account_legacy as u64,
    })
}

/// The huddle half — one grouped read over the WHOLE `channel_ids`
/// set from the CONTROL plane's `slack_huddle_captures` ledger, ordered
/// `channel_id, created_at DESC` so [`huddle_health_from_rows`] can simply
/// `.take(3)` the most-recent reasons per channel without a second sort.
/// Scoped like `slack_onboarding::fetch_huddles_status`, which this mirrors
/// (CONTROL pool, `account_isolation`, migration `0083`).
///
/// **Known limitation, not a bug (same one `slack_onboarding::
/// fetch_huddles_status` already flags):** `slack_huddle_captures` has no
/// `workspace_id` column — only `account_id`/`channel_id`/`huddle_thread_ts`.
/// Unlike the workspace-wide onboarding rollup this affects only theoretically:
/// per-channel, `channel_id` is already Slack's own globally-scoped identifier
/// (a `C…` id is unique to the workspace it was created in), so blending
/// across an account's multiple connected Slack workspaces would require two
/// DIFFERENT workspaces to reuse the exact same channel id, which Slack does
/// not do. Flagged here anyway because the column absence is real and this
/// function's WHERE clause has no way to express a workspace filter even if
/// two workspaces ever did collide.
async fn fetch_huddle_stats(
    control_pool: &PgPool,
    account_id: Uuid,
    channel_ids: &[String],
) -> Result<HashMap<String, Vec<(bool, Option<String>)>>, String> {
    if channel_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| format!("channel management: huddle stats scope: {e}"))?;
    let rows: Vec<(String, Option<DateTime<Utc>>, Option<String>)> = sqlx::query_as(
        "SELECT channel_id, processed_at, skip_reason FROM slack_huddle_captures \
         WHERE account_id = $1 AND channel_id = ANY($2) ORDER BY channel_id, created_at DESC",
    )
    .bind(account_id)
    .bind(channel_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("channel management: huddle stats query: {e}"))?;
    tx.rollback().await.ok();

    let mut by_channel: HashMap<String, Vec<(bool, Option<String>)>> = HashMap::new();
    for (channel_id, processed_at, skip_reason) in rows {
        by_channel.entry(channel_id).or_default().push((processed_at.is_some(), skip_reason));
    }
    Ok(by_channel)
}

/// Resolve the CALLER's own primary `kind = 'member'` ocean — the exact
/// predicate `routes::slack::resolve_org_member_primary_graph` uses, but
/// returning `Option` rather than that function's `Result`/refusal: this is a
/// read-only visibility filter, not an ingest destination resolution, so "no
/// primary member ocean yet" is not an error to propagate, it is the
/// [`RowVisibility::OwnOceanOnly(None)`] case — "show nothing" — handled by
/// the caller. Scoped like every other tenant-pool read in this module.
///
/// `pub(crate)` rather than private because
/// `slack_onboarding::derive_member_status` resolves the SAME ocean for the
/// SAME viewer on the SAME request-shaped path (its `member.my_capture`
/// section is "what landed in MY ocean"). Two copies of this predicate would
/// be two answers to "which ocean is this member's", and the surfaces would
/// disagree the first time the primary/archived rules move — the same
/// one-derivation argument docs/connectors/slack-onboarding.md §3 makes for
/// the payload as a whole.
pub(crate) async fn fetch_caller_member_ocean(
    tenant_pool: &PgPool,
    account_id: Uuid,
    caller_user_id: Uuid,
) -> Result<Option<Uuid>, String> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(tenant_pool, account_id, caller_user_id)
        .await
        .map_err(|e| format!("channel management: caller member ocean scope: {e}"))?;
    let graph_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM graphs \
         WHERE account_id = $1 AND owner_user_id = $2 AND kind = 'member' \
           AND archived_at IS NULL AND is_primary = true \
         LIMIT 1",
    )
    .bind(account_id)
    .bind(caller_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| format!("channel management: caller member ocean query: {e}"))?;
    tx.rollback().await.ok();
    Ok(graph_id)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn watched(mode: &str, graph_id: Option<Uuid>, display_name: &str) -> WatchedChannelRow {
        WatchedChannelRow {
            mode: mode.to_string(),
            graph_id,
            display_name: display_name.to_string(),
            created_at: DateTime::parse_from_rfc3339("2026-08-01T00:00:00Z").unwrap().with_timezone(&Utc),
        }
    }

    // ── unavailable vs. empty ────────────────────────────────────────────────

    #[test]
    fn unavailable_section_carries_the_error_and_no_rows() {
        let section = ChannelsSection::unavailable("conversations.list: rate-limited by Slack".to_string());
        assert_eq!(section.state, ChannelsListState::Unavailable);
        assert!(section.error.is_some());
        assert!(section.rows.is_empty());
    }

    #[test]
    fn zero_watched_channels_is_a_real_empty_ok_not_unavailable() {
        // FAILS WITHOUT THE FIX: a caller that collapsed "conversations.list
        // returned zero channels" and "conversations.list failed" onto the
        // same code path would report `state: "unavailable"` here — this
        // module keeps them apart by construction: `channel_rows_from_data`
        // has no way to produce `Unavailable` at all, only
        // `ChannelsSection::unavailable` does, and only the `Err` arm of
        // `derive_channel_management`'s live-call match reaches it.
        let rows = channel_rows_from_data(
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert!(rows.is_empty());
    }

    // ── kind: default is the no-row case, not missing ───────────────────────

    #[test]
    fn unrouted_channel_reports_kind_default_with_no_chosen_at() {
        let live = vec![("C1".to_string(), "platform".to_string())];
        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(), // no watched row at all
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].destination.kind, DestinationKind::Default);
        assert_eq!(rows[0].destination.ocean_name, None);
        assert_eq!(rows[0].destination.chosen_at, None);
        assert!(!rows[0].destination.stale_name, "nothing was ever stored to compare against");
        assert_eq!(rows[0].name, "#platform");
    }

    #[test]
    fn routed_single_channel_names_its_ocean_and_carries_chosen_at() {
        let graph_id = Uuid::new_v4();
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("single", Some(graph_id), ""));
        let mut graph_names = HashMap::new();
        graph_names.insert(graph_id, "Primary ocean".to_string());

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &graph_names,
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows[0].destination.kind, DestinationKind::Single);
        assert_eq!(rows[0].destination.ocean_name.as_deref(), Some("Primary ocean"));
        assert!(rows[0].destination.chosen_at.is_some());
    }

    #[test]
    fn fanout_channel_never_names_an_ocean() {
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("fanout", None, ""));

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows[0].destination.kind, DestinationKind::Fanout);
        assert_eq!(rows[0].destination.ocean_name, None);
        assert!(rows[0].destination.chosen_at.is_some(), "fanout was still an explicit pick");
    }

    // ── stale_name ────────────────────────────────────────────────────────

    #[test]
    fn stale_name_fires_when_stored_name_diverges_from_live_name() {
        let live = vec![("C1".to_string(), "platform-v2".to_string())];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("single", None, "platform"));
        // A `mode = 'single'` row is supposed to always carry a `graph_id`
        // per the table CHECK — pass `graph_id: None` here anyway to prove
        // `stale_name` is computed independently of ocean-name resolution.

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert!(rows[0].destination.stale_name);
    }

    #[test]
    fn empty_stored_name_is_never_reported_as_stale() {
        // The write-side convention this endpoint has to live with (see
        // `ChannelDestination::stale_name`'s doc comment): most picks today
        // store `display_name: ""`. An empty string must read as "nothing
        // recorded", not as a mismatch against every live name.
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("single", None, ""));

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert!(!rows[0].destination.stale_name);
    }

    #[test]
    fn matching_stored_and_live_name_is_not_stale() {
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("single", None, "platform"));

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert!(!rows[0].destination.stale_name);
    }

    // ── org-member row restriction ───────────────────────────────────────

    #[test]
    fn member_sees_full_rows_for_exactly_the_channels_they_are_in() {
        // Read scope = write scope (2026-08-04): visibility keys on the
        // member's CHANNEL membership, never on the watched row's kind. The
        // fan-out and unrouted-default rows they're in are VISIBLE — the old
        // own-ocean-only filter hid C3/C4 even while both fed this member's
        // ocean.
        let my_graph = Uuid::new_v4();
        let other_graph = Uuid::new_v4();
        let live = vec![
            ("C1".to_string(), "mine-single".to_string()),
            ("C2".to_string(), "not-in-it".to_string()),
            ("C3".to_string(), "unrouted".to_string()),
            ("C4".to_string(), "fanned-out".to_string()),
        ];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("single", Some(my_graph), ""));
        watched_map.insert("C2".to_string(), watched("single", Some(other_graph), ""));
        // C3: no row at all (default) — the 0013 fanout-row gap means a
        // channel the member is in may have NO tenant-side row; membership
        // still makes it visible.
        watched_map.insert("C4".to_string(), watched("fanout", None, ""));

        let mine: std::collections::HashSet<String> =
            ["C1", "C3", "C4"].into_iter().map(String::from).collect();
        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::MemberChannels { channels: &mine, caller_user_id: Uuid::new_v4() },
        );
        let ids: Vec<&str> = rows.iter().map(|r| r.channel_id.as_str()).collect();
        // Rows sort by display name ("fanned-out" < "mine-single" < "unrouted").
        assert_eq!(ids, vec!["C4", "C1", "C3"], "channels they're in, name-sorted; never C2");
    }

    #[test]
    fn member_with_empty_channel_set_sees_nothing_not_everything() {
        // FAILS WITHOUT THE FIX: treating an empty membership set as
        // `RowVisibility::Unrestricted` would show the whole workspace's
        // routing table to a member who is verifiably in none of its
        // channels. (An unlinked member or a failed lookup never reaches
        // row-building at all — `MemberChannelScope::{Unlinked,Unavailable}`
        // short-circuit in `derive_channel_management`.)
        let live = vec![
            ("C1".to_string(), "general".to_string()),
            ("C2".to_string(), "random".to_string()),
        ];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("fanout", None, ""));
        // C2: unrouted (default).

        let mine = std::collections::HashSet::new();
        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::MemberChannels { channels: &mine, caller_user_id: Uuid::new_v4() },
        );
        assert!(rows.is_empty());
    }

    #[test]
    fn owner_and_admin_see_every_kind_including_fanout_and_default() {
        let live = vec![
            ("C1".to_string(), "single".to_string()),
            ("C2".to_string(), "fanout".to_string()),
            ("C3".to_string(), "default".to_string()),
        ];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("single", Some(Uuid::new_v4()), ""));
        watched_map.insert("C2".to_string(), watched("fanout", None, ""));

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows.len(), 3);
    }

    // ── atom_count / last_activity_at ────────────────────────────────────

    #[test]
    fn channel_with_no_atoms_reports_zero_count_and_null_last_activity() {
        let live = vec![("C1".to_string(), "quiet".to_string())];
        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(), // no atom_stats entry for C1
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows[0].atom_count, 0);
        assert_eq!(rows[0].last_activity_at, None);
    }

    #[test]
    fn channel_with_atoms_reports_the_aggregate() {
        let live = vec![("C1".to_string(), "busy".to_string())];
        let last = DateTime::parse_from_rfc3339("2026-08-03T14:40:00Z").unwrap().with_timezone(&Utc);
        let mut atom_stats = HashMap::new();
        atom_stats.insert("C1".to_string(), ChannelAtomStats { atom_count: 42, last_activity_at: Some(last) });

        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &HashMap::new(),
            &atom_stats,
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows[0].atom_count, 42);
        assert_eq!(rows[0].last_activity_at, Some(last));
    }

    // ── huddle_health_from_rows ──────────────────────────────────────────

    #[test]
    fn huddle_health_counts_and_caps_raw_reasons_at_three_most_recent_first() {
        // Caller (`fetch_huddle_stats`) orders `created_at DESC`, so index 0
        // here is the most recent row — this function must not re-sort.
        let rows = vec![
            (false, Some("reason A (newest)".to_string())),
            (true, None),
            (false, Some("reason B".to_string())),
            (false, Some("reason A (newest)".to_string())), // deliberate duplicate — never deduped
            (false, Some("reason C".to_string())),
            (false, Some("reason D (oldest, dropped)".to_string())),
        ];
        let health = huddle_health_from_rows(&rows);
        assert_eq!(health.seen, 6);
        assert_eq!(health.ingested, 1);
        assert_eq!(health.skipped, 5);
        assert_eq!(health.reasons.len(), 3, "capped at 3, never grouped/deduped");
        assert_eq!(health.reasons[0], "reason A (newest)");
        assert_eq!(health.reasons[1], "reason B");
        assert_eq!(health.reasons[2], "reason A (newest)");
    }

    #[test]
    fn all_ingested_huddles_report_no_reasons() {
        let rows = vec![(true, None), (true, None)];
        let health = huddle_health_from_rows(&rows);
        assert_eq!(health.seen, 2);
        assert_eq!(health.ingested, 2);
        assert_eq!(health.skipped, 0);
        assert!(health.reasons.is_empty());
    }

    #[test]
    fn empty_huddle_history_reports_real_zeros() {
        let health = huddle_health_from_rows(&[]);
        assert_eq!(health.seen, 0);
        assert_eq!(health.ingested, 0);
        assert_eq!(health.skipped, 0);
        assert!(health.reasons.is_empty());
    }

    // ── ordering ─────────────────────────────────────────────────────────

    #[test]
    fn rows_are_sorted_by_name_case_insensitively() {
        let live = vec![
            ("C3".to_string(), "zebra".to_string()),
            ("C1".to_string(), "Apple".to_string()),
            ("C2".to_string(), "banana".to_string()),
        ];
        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["#Apple", "#banana", "#zebra"]);
    }

    // ── The account default says WHERE, not just "default" ───────────────

    fn reach(mapped: usize, unmapped: usize) -> FanoutReach {
        FanoutReach { mapped_members: mapped, unmapped_members: unmapped }
    }

    /// An ORG account with no watched row: the account default IS the
    /// fan-out, and an explicit `mode = 'fanout'` row on the same page
    /// resolves to the same reach.
    fn fanout(mapped: usize, unmapped: usize) -> LiveRouting {
        LiveRouting {
            account_default: Some(AccountDefault::MemberFanOut(reach(mapped, unmapped))),
            explicit_fanout: Some(reach(mapped, unmapped)),
        }
    }

    #[test]
    fn org_default_row_carries_the_live_fanout_reach_and_names_no_ocean() {
        // FAILS WITHOUT THE FIX — this is the production defect. An org
        // account with no watched row fans out to each MAPPED member's
        // private ocean and the shared org ocean receives nothing; the row
        // used to carry `kind: Default` and NOTHING else, which every client
        // could only render as "your account default".
        let live = vec![("C1".to_string(), "platform".to_string())];
        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(), // no watched row: the account default applies
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &fanout(1, 2),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows[0].destination.kind, DestinationKind::Default);
        assert_eq!(
            rows[0].destination.fanout,
            Some(FanoutReach { mapped_members: 1, unmapped_members: 2 })
        );
        assert_eq!(rows[0].destination.ocean_name, None, "a fan-out has no single ocean to name");
    }

    #[test]
    fn personal_default_row_names_the_accounts_own_primary_ocean() {
        let graph_id = Uuid::new_v4();
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut graph_names = HashMap::new();
        graph_names.insert(graph_id, "Alex's ocean".to_string());

        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &graph_names,
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting { account_default: Some(AccountDefault::PrimaryOcean(graph_id)), explicit_fanout: None },
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows[0].destination.kind, DestinationKind::Default);
        assert_eq!(rows[0].destination.ocean_name.as_deref(), Some("Alex's ocean"));
        assert_eq!(rows[0].destination.fanout, None);
        // Still "nobody chose": naming the destination must not fabricate a
        // pick that never happened.
        assert_eq!(rows[0].destination.chosen_at, None);
    }

    #[test]
    fn an_explicit_fanout_pick_carries_the_same_reach_as_the_default() {
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("fanout", None, ""));

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &fanout(3, 0),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows[0].destination.kind, DestinationKind::Fanout);
        assert_eq!(rows[0].destination.fanout, Some(FanoutReach { mapped_members: 3, unmapped_members: 0 }));
    }

    #[test]
    fn a_single_pick_never_borrows_the_account_defaults_reach() {
        // The reach is resolved once per page and shared; a `single` row must
        // not pick it up just because it is in scope.
        let graph_id = Uuid::new_v4();
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut watched_map = HashMap::new();
        watched_map.insert("C1".to_string(), watched("single", Some(graph_id), ""));
        let mut graph_names = HashMap::new();
        graph_names.insert(graph_id, "Engineering".to_string());

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &graph_names,
            &HashMap::new(),
            &HashMap::new(),
            &fanout(3, 0),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert_eq!(rows[0].destination.ocean_name.as_deref(), Some("Engineering"));
        assert_eq!(rows[0].destination.fanout, None);
    }

    #[test]
    fn a_member_is_never_handed_the_fanout_roster_count() {
        // Rule 2 of `RowVisibility`. How many of the org's people a channel
        // reaches is seat information about OTHER people; a member gets the
        // shape of the destination and no numbers.
        let live = vec![
            ("C1".to_string(), "unrouted".to_string()),
            ("C2".to_string(), "fanned-out".to_string()),
        ];
        let mut watched_map = HashMap::new();
        watched_map.insert("C2".to_string(), watched("fanout", None, ""));
        let mine: std::collections::HashSet<String> = ["C1", "C2"].into_iter().map(String::from).collect();

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &fanout(1, 4),
            &HashMap::new(),
            RowVisibility::MemberChannels { channels: &mine, caller_user_id: Uuid::new_v4() },
        );
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.destination.fanout, None, "{} leaked a roster count", row.name);
            // …but the reach still REACHED someone, so nothing is dropping.
            assert!(!row.destination.dropping, "{} is not actually dropping", row.name);
        }
    }

    #[test]
    fn a_member_is_told_their_channel_reaches_nobody_even_with_the_counts_masked() {
        // FAILS WITHOUT THE FIX. Masking the reach from a member left their
        // row reading "nobody chose · still ingesting" while every message
        // they sent was being fanned out to an empty set and discarded — the
        // exact misreading this whole page was rebuilt to prevent, reproduced
        // one role down. `dropping` is derived BEFORE masking for this
        // reason; it names nobody and counts nobody.
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mine: std::collections::HashSet<String> = ["C1"].into_iter().map(String::from).collect();

        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(), // unrouted: the org account default applies
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &fanout(0, 3),
            &HashMap::new(),
            RowVisibility::MemberChannels { channels: &mine, caller_user_id: Uuid::new_v4() },
        );
        assert!(rows[0].destination.dropping, "a member must be told their messages go nowhere");
        assert_eq!(rows[0].destination.fanout, None, "…without being handed the roster count");
    }

    #[test]
    fn an_owner_sees_both_the_drop_flag_and_the_counts() {
        let live = vec![("C1".to_string(), "platform".to_string())];
        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &fanout(0, 3),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert!(rows[0].destination.dropping);
        assert_eq!(rows[0].destination.fanout, Some(reach(0, 3)));
    }

    #[test]
    fn a_personal_default_never_borrows_a_legacy_fanout_rows_reach() {
        // FAILS WITHOUT THE FIX. `slack_watched_channels`' CHECK permits a
        // `mode = 'fanout'` row on a personal account even though the picker
        // cannot produce one, and resolving it populated a SHARED reach that
        // every `Default` row on the page then carried alongside its correct
        // ocean name. With an unmapped org roster that rendered a perfectly
        // healthy personal default as "nobody's ocean · messages dropped".
        // `LiveRouting::account_default` is an enum precisely so this cannot
        // be expressed.
        let graph_id = Uuid::new_v4();
        let live = vec![
            ("C1".to_string(), "unrouted".to_string()),
            ("C2".to_string(), "legacy-fanout".to_string()),
        ];
        let mut watched_map = HashMap::new();
        watched_map.insert("C2".to_string(), watched("fanout", None, ""));
        let mut graph_names = HashMap::new();
        graph_names.insert(graph_id, "Alex's ocean".to_string());

        let rows = channel_rows_from_data(
            &live,
            &watched_map,
            &graph_names,
            &HashMap::new(),
            &HashMap::new(),
            // Personal plane: the default is ONE ocean, and the legacy
            // fan-out row's reach happens to be empty.
            &LiveRouting {
                account_default: Some(AccountDefault::PrimaryOcean(graph_id)),
                explicit_fanout: Some(reach(0, 1)),
            },
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        let unrouted = rows.iter().find(|r| r.channel_id == "C1").unwrap();
        assert_eq!(unrouted.destination.ocean_name.as_deref(), Some("Alex's ocean"));
        assert_eq!(unrouted.destination.fanout, None, "a personal default is not a fan-out");
        assert!(!unrouted.destination.dropping, "it routes to the primary ocean");

        // The legacy row itself still reports the truth about ITSELF.
        let legacy = rows.iter().find(|r| r.channel_id == "C2").unwrap();
        assert_eq!(legacy.destination.fanout, Some(reach(0, 1)));
        assert!(legacy.destination.dropping);
    }

    // ── landed_in ────────────────────────────────────────────────────────

    fn landed(name: &str, kind: &str, owner: Option<Uuid>, count: u64) -> LandedOceanRow {
        LandedOceanRow {
            ocean_name: name.to_string(),
            ocean_kind: kind.to_string(),
            owner_user_id: owner,
            atom_count: count,
        }
    }

    #[test]
    fn landed_in_lists_every_ocean_biggest_share_first() {
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut landed_map = HashMap::new();
        landed_map.insert(
            "C1".to_string(),
            vec![
                landed("Engineering", "team", None, 2),
                landed("My Ocean (Alex)", "member", Some(Uuid::new_v4()), 30),
            ],
        );
        let mut atom_stats = HashMap::new();
        atom_stats.insert("C1".to_string(), ChannelAtomStats { atom_count: 32, last_activity_at: None });

        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &HashMap::new(),
            &atom_stats,
            &landed_map,
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        let names: Vec<&str> = rows[0].landed_in.iter().map(|l| l.ocean_name.as_str()).collect();
        assert_eq!(names, vec!["My Ocean (Alex)", "Engineering"], "biggest share first");
        assert_eq!(rows[0].landed_in[0].ocean_kind, "member", "graphs.kind passes through verbatim");
        let total: u64 = rows[0].landed_in.iter().map(|l| l.atom_count).sum();
        assert_eq!(total, rows[0].atom_count, "an unrestricted viewer's breakdown sums to the total");
    }

    #[test]
    fn landed_in_ties_break_by_name_so_the_table_does_not_reshuffle() {
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut landed_map = HashMap::new();
        landed_map.insert(
            "C1".to_string(),
            vec![landed("Zulu", "team", None, 1), landed("Alpha", "team", None, 1)],
        );

        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &landed_map,
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        let names: Vec<&str> = rows[0].landed_in.iter().map(|l| l.ocean_name.as_str()).collect();
        assert_eq!(names, vec!["Alpha", "Zulu"]);
    }

    #[test]
    fn a_member_sees_only_their_own_ocean_in_landed_in() {
        // Rule 3 of `RowVisibility`. FAILS WITHOUT THE FILTER: the breakdown
        // is computed from every ocean the channel fed, which for an org
        // fan-out is a list of colleagues' private-ocean names — precisely
        // what the row restriction exists to withhold. The TEAM ocean is in
        // this fixture on purpose: `owner_user_id: None` must NOT read as
        // "unowned, therefore shared, therefore fine to show".
        let me = Uuid::new_v4();
        let colleague = Uuid::new_v4();
        let live = vec![("C1".to_string(), "platform".to_string())];
        let mut landed_map = HashMap::new();
        landed_map.insert(
            "C1".to_string(),
            vec![
                landed("My Ocean (Alex)", "member", Some(me), 2),
                landed("My Ocean (Rae)", "member", Some(colleague), 9),
                landed("Engineering", "team", None, 4),
            ],
        );
        let mut atom_stats = HashMap::new();
        atom_stats.insert("C1".to_string(), ChannelAtomStats { atom_count: 15, last_activity_at: None });
        let mine: std::collections::HashSet<String> = ["C1"].into_iter().map(String::from).collect();

        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &HashMap::new(),
            &atom_stats,
            &landed_map,
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::MemberChannels { channels: &mine, caller_user_id: me },
        );
        assert_eq!(rows[0].landed_in.len(), 1);
        assert_eq!(rows[0].landed_in[0].ocean_name, "My Ocean (Alex)");
        // The channel total is NOT restricted (the member is in the channel
        // and can read every message in it) — so the two deliberately do not
        // sum here. Documented on `ChannelRow::landed_in`.
        assert_eq!(rows[0].atom_count, 15);
    }

    #[test]
    fn a_channel_with_no_atoms_has_an_empty_landed_in() {
        let live = vec![("C1".to_string(), "quiet".to_string())];
        let rows = channel_rows_from_data(
            &live,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &LiveRouting::default(),
            &HashMap::new(),
            RowVisibility::Unrestricted,
        );
        assert!(rows[0].landed_in.is_empty());
        assert_eq!(rows[0].atom_count, 0);
    }
}

#[cfg(test)]
mod db_pg_tests {
    //! DB-backed tests for the four `fetch_*` helpers — isolation across
    //! account/workspace boundaries is exactly what a pure unit test cannot
    //! prove (it never touches RLS or a real WHERE clause). Same convention
    //! as `slack_onboarding::db_derivation_pg_tests`: in-crate (every
    //! function under test is private), gated on `ORGT_TEST_BASE_URL`,
    //! migrating the CONTROL tree, connecting as the database owner (proves
    //! the SQL, not the RLS boundary itself — see that module's header for
    //! the full rationale, copied here rather than re-argued).
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib slack_channel_management::db_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::{
        fetch_atom_stats, fetch_caller_member_ocean, fetch_graph_names, fetch_huddle_stats,
        fetch_landed_in, fetch_unattributed_atom_count, fetch_watched_rows,
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
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
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

    async fn seed_account(pool: &PgPool) -> Uuid {
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
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

    async fn seed_graph(pool: &PgPool, account_id: Uuid, name: &str) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(id, account_id, name) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    // ── fetch_watched_rows ───────────────────────────────────────────────

    /// FAILS WITHOUT THE FIX: a query missing the `workspace_id` filter would
    /// report this OTHER workspace's row too — same shape as
    /// `slack_onboarding`'s `routed_status_counts_only_this_workspace_pg`.
    #[tokio::test]
    async fn watched_rows_scoped_to_account_and_workspace_pg() {
        let db = "orgt_chanmgmt_watched";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping watched_rows_scoped_to_account_and_workspace_pg: set ORGT_TEST_BASE_URL");
            return;
        };

        let account_id = seed_account(&pool).await;
        let other_account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id, "primary").await;
        let user_id: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let other_user_id: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
            .bind(other_account_id)
            .fetch_one(&pool)
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id, display_name) \
             VALUES ($1, $2, 'single', $3, 'T1', 'C1', 'platform')",
        )
        .bind(account_id).bind(user_id).bind(graph_id)
        .execute(&pool).await.unwrap();
        // Same account, DIFFERENT workspace — must not appear for T1.
        sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id, display_name) \
             VALUES ($1, $2, 'single', $3, 'T2', 'C9', 'other-workspace')",
        )
        .bind(account_id).bind(user_id).bind(graph_id)
        .execute(&pool).await.unwrap();
        // DIFFERENT account, same workspace id string — must not leak across.
        sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id, display_name) \
             VALUES ($1, $2, 'fanout', NULL, 'T1', 'C1', '')",
        )
        .bind(other_account_id).bind(other_user_id)
        .execute(&pool).await.unwrap();

        let rows = fetch_watched_rows(&pool, account_id, "T1").await.unwrap();
        assert_eq!(rows.len(), 1, "must see only this account's T1 row");
        assert_eq!(rows["C1"].mode, "single");
        assert_eq!(rows["C1"].display_name, "platform");

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_graph_names ─────────────────────────────────────────────────

    #[tokio::test]
    async fn graph_names_scoped_to_account_and_empty_input_skips_the_query_pg() {
        let db = "orgt_chanmgmt_graphnames";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping graph_names_scoped_to_account_and_empty_input_skips_the_query_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let other_account_id = seed_account(&pool).await;
        let my_graph = seed_graph(&pool, account_id, "Primary ocean").await;
        let foreign_graph = seed_graph(&pool, other_account_id, "Someone else's ocean").await;

        // Ask for both this account's graph AND a foreign one — the foreign
        // one must not come back, proving the `account_id = $1` filter (not
        // merely the `id = ANY($2)` membership) is what gates this.
        let names = fetch_graph_names(&pool, account_id, &[my_graph, foreign_graph]).await.unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names[&my_graph], "Primary ocean");
        assert!(!names.contains_key(&foreign_graph));

        let empty = fetch_graph_names(&pool, account_id, &[]).await.unwrap();
        assert!(empty.is_empty());

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_atom_stats ─────────────────────────────────────────────────

    async fn seed_atom(pool: &PgPool, graph_id: Uuid, source_id: &str, workspace_id: &str, channel_id: &str) {
        sqlx::query(
            "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, \
             timestamp, workspace_id, channel_id) \
             VALUES (gen_random_uuid(), $1, 'slack', $2, 'blob-key', now(), $3, $4)",
        )
        .bind(graph_id)
        .bind(source_id)
        .bind(workspace_id)
        .bind(channel_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A Slack atom as written BEFORE migration `0058` added workspace
    /// provenance: a real `channel_id`, no `workspace_id` at all. The shape
    /// the pre-fix `workspace_id = $2` predicate made invisible.
    async fn seed_atom_null_workspace(pool: &PgPool, graph_id: Uuid, source_id: &str, channel_id: &str) {
        sqlx::query(
            "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, \
             timestamp, workspace_id, channel_id) \
             VALUES (gen_random_uuid(), $1, 'slack', $2, 'blob-key', now(), NULL, $3)",
        )
        .bind(graph_id)
        .bind(source_id)
        .bind(channel_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// An atom with NO `channel_id` — `source_type` is a parameter because
    /// the whole point of [`fetch_unattributed_atom_count`]'s Slack filter is
    /// that "no channel" describes most other sources too.
    async fn seed_channelless_atom(
        pool: &PgPool,
        graph_id: Uuid,
        source_id: &str,
        source_type: &str,
        workspace_id: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, \
             timestamp, workspace_id, channel_id) \
             VALUES (gen_random_uuid(), $1, $2, $3, 'blob-key', now(), $4, NULL)",
        )
        .bind(graph_id)
        .bind(source_type)
        .bind(source_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn atom_stats_grouped_per_channel_and_scoped_to_workspace_pg() {
        let db = "orgt_chanmgmt_atoms";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping atom_stats_grouped_per_channel_and_scoped_to_workspace_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id, "primary").await;

        seed_atom(&pool, graph_id, "m1", "T1", "C1").await;
        seed_atom(&pool, graph_id, "m2", "T1", "C1").await;
        seed_atom(&pool, graph_id, "m3", "T1", "C2").await;
        // Different workspace, same channel id string — must not blend into T1's C1.
        seed_atom(&pool, graph_id, "m4", "T2", "C1").await;

        let stats = fetch_atom_stats(&pool, account_id, "T1", &["C1".to_string(), "C2".to_string()]).await.unwrap();
        assert_eq!(stats["C1"].atom_count, 2);
        assert_eq!(stats["C2"].atom_count, 1);
        assert!(!stats.contains_key("C9"), "no atoms, no entry — caller defaults to zero");

        let empty = fetch_atom_stats(&pool, account_id, "T1", &[]).await.unwrap();
        assert!(empty.is_empty());

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX. Two SEPARATE CoralSwarm accounts each connect
    /// the SAME Slack workspace (`T1`) and each have a channel with the same
    /// Slack channel id (`C1` — Slack ids are per-workspace, so on ONE shared
    /// workspace both accounts genuinely see the same `C1`). The pre-fix
    /// query filtered on `workspace_id`/`channel_id` only, so each account's
    /// channel-management page showed the other's per-channel atom count and
    /// last-activity timestamp.
    ///
    /// **Deliberately does not rely on RLS to prove the point.** This module
    /// connects as the database owner (see this module's header), so
    /// `begin_account_scoped` inside `fetch_atom_stats` enforces nothing IN
    /// THE TEST, and a green result here means the `g.account_id = $1`
    /// predicate itself is doing the work. (Production has since cut over to
    /// the non-`BYPASSRLS` `coralswarm_app` role — OPS-4 has run — making RLS
    /// a live second layer there; the owner connection here is exactly what
    /// keeps this test an honest proof of the FIRST layer: delete the
    /// `graphs` join and it goes red, with no policy to mask the hole.)
    #[tokio::test]
    async fn atom_stats_do_not_blend_a_second_account_on_the_same_workspace_pg() {
        let db = "orgt_chanmgmt_atoms_two_accounts";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping atom_stats_do_not_blend_a_second_account_on_the_same_workspace_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let account_a = seed_account(&pool).await;
        let account_b = seed_account(&pool).await;
        let graph_a = seed_graph(&pool, account_a, "a-primary").await;
        let graph_b = seed_graph(&pool, account_b, "b-primary").await;

        // Account A: one atom in C1. Account B: three in the SAME C1 on the
        // SAME workspace — plus one in C2, which A has no row for at all.
        seed_atom(&pool, graph_a, "a1", "T1", "C1").await;
        seed_atom(&pool, graph_b, "b1", "T1", "C1").await;
        seed_atom(&pool, graph_b, "b2", "T1", "C1").await;
        seed_atom(&pool, graph_b, "b3", "T1", "C1").await;
        seed_atom(&pool, graph_b, "b4", "T1", "C2").await;

        let channels = ["C1".to_string(), "C2".to_string()];
        let a = fetch_atom_stats(&pool, account_a, "T1", &channels).await.unwrap();
        assert_eq!(a["C1"].atom_count, 1, "account A must not count account B's C1 atoms");
        assert!(
            !a.contains_key("C2"),
            "C2 exists only for account B — A must get NO entry (the caller renders that as zero), \
             not B's count"
        );

        let b = fetch_atom_stats(&pool, account_b, "T1", &channels).await.unwrap();
        assert_eq!(b["C1"].atom_count, 3, "…and the isolation is symmetric, not one-directional");
        assert_eq!(b["C2"].atom_count, 1);

        // `last_activity_at` leaks the same way a count does — "someone was
        // active in this channel at 14:02" is itself information about the
        // other tenant, so assert it too rather than only the counts.
        let a_last = a["C1"].last_activity_at.expect("A has one atom, so a timestamp");
        let b_last = b["C1"].last_activity_at.expect("B has three atoms, so a timestamp");
        let a_only: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT max(timestamp) FROM atoms WHERE graph_id = $1 AND channel_id = 'C1'",
        )
        .bind(graph_a)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(Some(a_last), a_only, "A's last-activity must come from A's own atoms only");
        assert!(b_last >= a_last, "sanity: B's atoms were inserted after A's");

        teardown(admin_conn, pool, db).await;
    }

    /// FAILS WITHOUT THE FIX. `atoms.workspace_id` only arrived in migration
    /// `0058`; every Slack atom written before it (plus seeded history, plus
    /// any path that never set the column) carries NULL. The pre-fix
    /// `a.workspace_id = $2` did not merely fail to SCOPE those rows — it
    /// deleted them from the page, so a channel whose corals all predate
    /// `0058` read as "0 corals", which is indistinguishable from "nothing
    /// was ever captured here". In production on 2026-09-09, 19 of one org's
    /// 35 Slack atoms were invisible this way.
    ///
    /// Also pins what the relaxation must NOT admit: an atom explicitly
    /// stamped with a DIFFERENT workspace still stays out (the `= $2` arm is
    /// unchanged), and the account boundary is untouched.
    #[tokio::test]
    async fn atom_stats_count_atoms_whose_workspace_id_predates_the_column_pg() {
        let db = "orgt_chanmgmt_atoms_null_ws";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping atom_stats_count_atoms_whose_workspace_id_predates_the_column_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let other_account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id, "primary").await;
        let other_graph_id = seed_graph(&pool, other_account_id, "theirs").await;

        seed_atom(&pool, graph_id, "new1", "T1", "C1").await;
        // Pre-`0058`: no workspace provenance at all, but a real channel id.
        seed_atom_null_workspace(&pool, graph_id, "old1", "C1").await;
        seed_atom_null_workspace(&pool, graph_id, "old2", "C1").await;
        // A DIFFERENT workspace, explicitly stamped — must still be excluded.
        seed_atom(&pool, graph_id, "elsewhere", "T2", "C1").await;
        // Another account's NULL-workspace atom in the same channel id — the
        // `graphs` join is still the account boundary.
        seed_atom_null_workspace(&pool, other_graph_id, "theirs1", "C1").await;

        let stats = fetch_atom_stats(&pool, account_id, "T1", &["C1".to_string()]).await.unwrap();
        assert_eq!(
            stats["C1"].atom_count, 3,
            "1 stamped + 2 pre-0058; not the other workspace's, not the other account's"
        );

        // ...and the isolation is symmetric.
        let theirs = fetch_atom_stats(&pool, other_account_id, "T1", &["C1".to_string()]).await.unwrap();
        assert_eq!(theirs["C1"].atom_count, 1);

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_landed_in ──────────────────────────────────────────────────

    /// The breakdown must group by OCEAN within a channel, carry each
    /// graph's `kind`/`owner_user_id` (the row filter's inputs), and sum to
    /// what `fetch_atom_stats` reports for the same channel — the two are
    /// deliberately the same predicate, and a drift between them would let
    /// the page's own two columns disagree about how much it captured.
    #[tokio::test]
    async fn landed_in_groups_by_ocean_and_agrees_with_the_channel_total_pg() {
        let db = "orgt_chanmgmt_landed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping landed_in_groups_by_ocean_and_agrees_with_the_channel_total_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let other_account_id = seed_account(&pool).await;
        let team_graph = seed_graph(&pool, account_id, "Engineering").await;
        let foreign_graph = seed_graph(&pool, other_account_id, "Someone else's ocean").await;

        // A member-owned ocean, so `owner_user_id` comes back non-NULL —
        // that column is what the org-member row filter keys on.
        let member_user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
                .bind(format!("{}@example.com", Uuid::new_v4()))
                .fetch_one(&pool)
                .await
                .unwrap();
        let member_graph = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO graphs(id, account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, $2, 'My Ocean (Alex)', 'member', $3, true)",
        )
        .bind(member_graph).bind(account_id).bind(member_user_id)
        .execute(&pool).await.unwrap();

        seed_atom(&pool, member_graph, "m1", "T1", "C1").await;
        seed_atom(&pool, member_graph, "m2", "T1", "C1").await;
        // Pre-`0058` atom in the same ocean — counted here for the same
        // reason `fetch_atom_stats` counts it.
        seed_atom_null_workspace(&pool, member_graph, "m3", "C1").await;
        seed_atom(&pool, team_graph, "t1", "T1", "C1").await;
        // A different channel, and a different account — neither may blend in.
        seed_atom(&pool, team_graph, "t2", "T1", "C2").await;
        seed_atom(&pool, foreign_graph, "f1", "T1", "C1").await;

        let landed = fetch_landed_in(&pool, account_id, "T1", &["C1".to_string()]).await.unwrap();
        let mut rows = landed["C1"].clone();
        rows.sort_by(|a, b| b.atom_count.cmp(&a.atom_count));
        assert_eq!(rows.len(), 2, "two oceans of this account fed C1; never the foreign one");
        assert_eq!(rows[0].ocean_name, "My Ocean (Alex)");
        assert_eq!(rows[0].ocean_kind, "member");
        assert_eq!(rows[0].owner_user_id, Some(member_user_id));
        assert_eq!(rows[0].atom_count, 3);
        assert_eq!(rows[1].ocean_name, "Engineering");
        assert_eq!(rows[1].owner_user_id, None, "an account-wide ocean has no owner");
        assert_eq!(rows[1].atom_count, 1);

        // The identity the shared predicate exists to guarantee.
        let stats = fetch_atom_stats(&pool, account_id, "T1", &["C1".to_string()]).await.unwrap();
        let breakdown_total: u64 = rows.iter().map(|r| r.atom_count).sum();
        assert_eq!(breakdown_total, stats["C1"].atom_count);

        let empty = fetch_landed_in(&pool, account_id, "T1", &[]).await.unwrap();
        assert!(empty.is_empty());

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_unattributed_atom_count ────────────────────────────────────

    /// The residue no row can show: Slack atoms with NO `channel_id`. This
    /// pins three things — that such atoms ARE counted (they are real
    /// captured content, and omitting them silently made the page's totals
    /// short); that the `source_type = 'slack'` filter is load-bearing,
    /// because `channel_id IS NULL` on its own describes every Gmail message
    /// and coding session in the account too; and that the two halves stay
    /// APART.
    ///
    /// The split is the part with teeth. On an account with two connected
    /// Slack workspaces, a row with neither `workspace_id` nor `channel_id`
    /// is claimed by both workspaces' pages if it is folded into a single
    /// "corals from this workspace" number — so this asserts that the
    /// SAME legacy atom shows up as `account_legacy` (never
    /// `this_workspace`) no matter which workspace does the asking, which no
    /// single-number version of this function could satisfy.
    #[tokio::test]
    async fn unattributed_count_is_slack_only_and_keeps_legacy_atoms_out_of_this_workspaces_total_pg() {
        let db = "orgt_chanmgmt_unattributed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping unattributed_count_is_slack_only_and_keeps_legacy_atoms_out_of_this_workspaces_total_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let other_account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id, "primary").await;
        let other_graph_id = seed_graph(&pool, other_account_id, "theirs").await;

        // This workspace's own residue: stamped T1, no channel.
        seed_channelless_atom(&pool, graph_id, "u1", "slack", Some("T1")).await;
        // Account-wide legacy: neither column. Belongs to the ACCOUNT; no row
        // says which of its workspaces produced it.
        seed_channelless_atom(&pool, graph_id, "u2", "slack", None).await;
        seed_channelless_atom(&pool, graph_id, "u3", "slack", None).await;
        // NOT counted anywhere: a different source with no channel either —
        // the trap the `source_type` filter exists for.
        seed_channelless_atom(&pool, graph_id, "g1", "gmail", None).await;
        seed_channelless_atom(&pool, graph_id, "s1", "coding_session", None).await;
        // A SECOND connected Slack workspace's own residue.
        seed_channelless_atom(&pool, graph_id, "w2", "slack", Some("T2")).await;
        // NOT counted: it HAS a channel, so a row above accounts for it.
        seed_atom(&pool, graph_id, "attributed", "T1", "C1").await;
        // NOT counted: another account's.
        seed_channelless_atom(&pool, other_graph_id, "theirs1", "slack", None).await;

        let t1 = fetch_unattributed_atom_count(&pool, account_id, "T1").await.unwrap();
        assert_eq!(t1.this_workspace, 1, "only the atom actually stamped T1");
        assert_eq!(t1.account_legacy, 2, "the two provenance-less atoms, reported as account-wide");

        // FAILS WITHOUT THE SPLIT: asking as the OTHER workspace on the SAME
        // account must not re-claim T1's stamped atom, and must report the
        // same legacy pair as account-wide rather than as its own.
        let t2 = fetch_unattributed_atom_count(&pool, account_id, "T2").await.unwrap();
        assert_eq!(t2.this_workspace, 1, "T2's own stamped atom, not T1's");
        assert_eq!(t2.account_legacy, 2, "the legacy atoms are claimed by neither workspace");

        let theirs = fetch_unattributed_atom_count(&pool, other_account_id, "T1").await.unwrap();
        assert_eq!(theirs.account_legacy, 1, "and the account isolation is symmetric");
        assert_eq!(theirs.this_workspace, 0);

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_huddle_stats ───────────────────────────────────────────────

    #[tokio::test]
    async fn huddle_stats_grouped_per_channel_ordered_most_recent_first_pg() {
        let db = "orgt_chanmgmt_huddles";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping huddle_stats_grouped_per_channel_ordered_most_recent_first_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let other_account_id = seed_account(&pool).await;

        sqlx::query(
            "INSERT INTO slack_huddle_captures(account_id, channel_id, huddle_thread_ts, processed_at, skip_reason, created_at) \
             VALUES ($1, 'C1', '1.1', NULL, 'older reason', now() - interval '2 hours')",
        )
        .bind(account_id).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO slack_huddle_captures(account_id, channel_id, huddle_thread_ts, processed_at, skip_reason, created_at) \
             VALUES ($1, 'C1', '1.2', NULL, 'newer reason', now())",
        )
        .bind(account_id).execute(&pool).await.unwrap();
        // Different account — must not leak into this account's C1.
        sqlx::query(
            "INSERT INTO slack_huddle_captures(account_id, channel_id, huddle_thread_ts, processed_at, skip_reason) \
             VALUES ($1, 'C1', '9.1', now(), NULL)",
        )
        .bind(other_account_id).execute(&pool).await.unwrap();

        let stats = fetch_huddle_stats(&pool, account_id, &["C1".to_string()]).await.unwrap();
        let rows = &stats["C1"];
        assert_eq!(rows.len(), 2, "must not include the other account's row");
        assert_eq!(rows[0].1.as_deref(), Some("newer reason"), "ordered created_at DESC");
        assert_eq!(rows[1].1.as_deref(), Some("older reason"));

        let empty = fetch_huddle_stats(&pool, account_id, &[]).await.unwrap();
        assert!(empty.is_empty());

        teardown(admin_conn, pool, db).await;
    }

    // ── fetch_caller_member_ocean ────────────────────────────────────────

    #[tokio::test]
    async fn caller_member_ocean_resolves_exactly_the_callers_own_primary_member_graph_pg() {
        let db = "orgt_chanmgmt_memberocean";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping caller_member_ocean_resolves_exactly_the_callers_own_primary_member_graph_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool).await;
        let caller_id: Uuid =
            sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
                .bind(format!("{}@example.com", Uuid::new_v4()))
                .fetch_one(&pool)
                .await
                .unwrap();
        let other_member_id: Uuid =
            sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
                .bind(format!("{}@example.com", Uuid::new_v4()))
                .fetch_one(&pool)
                .await
                .unwrap();

        // No member ocean yet — must resolve None, not error, not someone else's.
        let none_yet = fetch_caller_member_ocean(&pool, account_id, caller_id).await.unwrap();
        assert_eq!(none_yet, None);

        // `graphs_member_requires_owner` (migration 0090) requires
        // `kind = 'member'` IFF `owner_user_id IS NOT NULL` — inserted
        // directly as `'member'`, not `'personal'`-then-updated, so the
        // CHECK is satisfied on the INSERT itself.
        let my_graph_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO graphs(id, account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, $2, 'My ocean', 'member', $3, true)",
        )
        .bind(my_graph_id).bind(account_id).bind(caller_id)
        .execute(&pool).await.unwrap();
        // Another member's own primary ocean — must never come back for `caller_id`.
        sqlx::query(
            "INSERT INTO graphs(id, account_id, name, kind, owner_user_id, is_primary) \
             VALUES (gen_random_uuid(), $1, 'Someone else''s ocean', 'member', $2, true)",
        )
        .bind(account_id).bind(other_member_id)
        .execute(&pool).await.unwrap();

        let resolved = fetch_caller_member_ocean(&pool, account_id, caller_id).await.unwrap();
        assert_eq!(resolved, Some(my_graph_id));

        teardown(admin_conn, pool, db).await;
    }

    // ── The conversion itself, under a NON-BYPASSRLS role ─────────────────

    /// The other pg tests in this module connect as the database owner, which
    /// is BYPASSRLS — so they prove these queries still return the right rows,
    /// but they would pass just as happily if the scoping GUC were never set
    /// at all. This one connects as `coralswarm_app` (migration `0083`'s
    /// non-BYPASSRLS role), where `account_isolation`'s
    /// `current_setting('app.account_id')::uuid` ERRORS on an unscoped
    /// connection rather than quietly returning zero rows.
    ///
    /// So it is the test that actually exercises what this change did: the
    /// converted `begin_tenant_system_scoped` / `begin_tenant_scoped` calls
    /// still set `app.account_id` first (both are strict supersets of
    /// `begin_account_scoped`), so consumer-plane RLS must be satisfied
    /// exactly as before. FAILS WITHOUT THAT: drop the account GUC from either
    /// primitive and this test errors instead of returning rows.
    ///
    /// What it does NOT prove: that the tenant GUC does anything. Tenant
    /// policies live in the tenant tree and are enforced by
    /// `coralswarm_tenant_app`, which the DSNs are not repointed at yet
    /// (Phase 3, docs/tenant-rls-cutover-runbook.md). That proof arrives with
    /// the cutover, not here.
    #[tokio::test]
    async fn converted_scoping_still_satisfies_consumer_rls_under_the_coralswarm_app_role_pg() {
        let db = "orgt_chanmgmt_approle";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some(base) = base_url() else {
            eprintln!(
                "skipping converted_scoping_still_satisfies_consumer_rls_under_the_coralswarm_app_role_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let admin_conn = recreate(&base, db).await;
        let url = with_db(&base, db);
        let pool: PgPool = PgPoolOptions::new().connect(&url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_id = seed_account(&pool).await;
        let other_account_id = seed_account(&pool).await;
        let my_graph = seed_graph(&pool, account_id, "Primary ocean").await;
        let foreign_graph = seed_graph(&pool, other_account_id, "Someone else's ocean").await;
        let user_id: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id, display_name) \
             VALUES ($1, $2, 'single', $3, 'T1', 'C1', 'platform')",
        )
        .bind(account_id).bind(user_id).bind(my_graph)
        .execute(&pool).await.unwrap();

        let app_pool = crate::app_role_test_support::app_role_pool(&pool, &url).await;

        // begin_tenant_system_scoped path.
        let rows = fetch_watched_rows(&app_pool, account_id, "T1").await.unwrap();
        assert_eq!(rows.len(), 1, "converted watched-rows read must survive consumer RLS");
        assert_eq!(rows["C1"].display_name, "platform");

        let names = fetch_graph_names(&app_pool, account_id, &[my_graph, foreign_graph]).await.unwrap();
        assert_eq!(names.len(), 1, "a foreign graph must not come back under the app role");
        assert_eq!(names[&my_graph], "Primary ocean");

        // begin_tenant_scoped path — the caller's own member ocean. No member
        // ocean exists for this user, so the correct answer is None, reached
        // WITHOUT an RLS error (which is the thing being asserted).
        let mine = fetch_caller_member_ocean(&app_pool, account_id, user_id).await.unwrap();
        assert!(mine.is_none(), "no member ocean seeded, and the read must not error");

        app_pool.close().await;
        teardown(admin_conn, pool, db).await;
    }
}

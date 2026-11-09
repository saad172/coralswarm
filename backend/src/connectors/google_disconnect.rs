//! Shared Google disconnect/purge routine — the analog of
//! [`crate::connectors::microsoft_disconnect`] for the Google Workspace
//! connector (gsuite Train G2 PR 5, debt).
//!
//! # The gap this closes
//!
//! Before this module existed, disconnecting a Google connection
//! (`routes::oauth::delete_connection`'s `google_gmail`/`google` arms — the
//! only surfaces that remove a Google `oauth_tokens` row today) deleted
//! `oauth_tokens` and nothing else. `google_watch_channels` (the `users.watch`/
//! `events.watch` registration `workers::google_watch_renewal` keeps alive)
//! and, for Gmail, `gmail_watched_threads` (the opted-in threads
//! `connectors::google_gmail::propagate_deletions`/live-follow match inbound
//! notifications against) survived untouched. This is precisely the
//! "Disconnect didn't stop ingest" incident class `slack_disconnect`'s own
//! header documents for Slack and `microsoft_disconnect`'s own header
//! documents for Microsoft:
//!
//! * A lingering `google_watch_channels` row (`surface='gmail'`) keeps
//!   `workers::google_watch_renewal`'s tenant-loop ticks renewing a Gmail
//!   watch for a mailbox whose `oauth_tokens` row — and therefore whose only
//!   valid access token — is already gone. The NEXT push notification for
//!   that channel has nothing to authenticate a sync against
//!   (`connectors::google_gmail::resolve_watch_owners_for_push` still resolves the
//!   row, `connectors::google_gmail::fetch_pending`'s own token load then
//!   fails), so the watch spins uselessly until Google itself lets it lapse
//!   (~7 days) — silent operational noise, not silent ingest, but exactly
//!   the "renewal worker keeps renewing a watch for a token that no longer
//!   exists" gap this PR's brief names.
//! * A lingering `gmail_watched_threads` row has no live path back to an
//!   ingest (no token, no watch survives past its own natural lapse either),
//!   but leaves stale opt-in state behind that a future reconnect would
//!   silently resume live-following without the member ever having
//!   re-opted-in on the new connection.
//! * A lingering Calendar `google_watch_channels` row (`surface='calendar'`)
//!   is the closer analog of Microsoft's own gap: Calendar's token is
//!   `TokenOwner::AccountWide` (`connectors::google::Surface::Calendar`'s own
//!   doc), so ANY member reconnecting Calendar for the account would silently
//!   resume push delivery through a channel nobody asked to keep alive.
//!
//! # What this deliberately does NOT purge
//!
//! Same contract as `slack_disconnect`/`microsoft_disconnect`:
//! **`connector_identities` and every captured atom are left untouched.**
//! Disconnecting withdraws consent to keep CAPTURING new content — it is not
//! an erasure request. A caller that also wants already-ingested content gone
//! needs the separately-reviewed `services::deletion` path, not this one.
//!
//! `oauth_tokens` itself is also not touched here — every caller already owns
//! tearing that down its own way (same reasoning `microsoft_disconnect`'s own
//! header gives).
//!
//! # No best-effort upstream `users.stop`/`channels.stop` (deliberate,
//! matches precedent rather than inventing one)
//!
//! `microsoft_disconnect::purge_microsoft_connection` DOES make a best-effort
//! upstream `DELETE /subscriptions/{id}` call — but only for Teams
//! RESOURCE-SPECIFIC-CONSENT (app-mode) subscriptions, which are kept alive
//! by `MICROSOFT_CLIENT_ID`/`MICROSOFT_CLIENT_SECRET` CLIENT-CREDENTIALS
//! tokens (`connectors::microsoft::app_token`) with no dependency on any one
//! member's own OAuth grant. Its own header is explicit that DELEGATED (mail)
//! subscriptions are the opposite case — "NOT attempted upstream... this
//! disconnect no longer holds a live delegated access token to attempt one
//! with anyway" — and lets Graph expire them on its own short TTL instead.
//!
//! Every Google watch this codebase creates — Gmail's `users.watch` AND
//! Calendar's `events.watch` — is the DELEGATED case, never the app-only one:
//! the gsuite plan's decision 3 explicitly rules out domain-wide delegation
//! ("No domain-wide delegation — that is the all-mailboxes application grant
//! M365 explicitly rejected"), so there is no Google equivalent of Graph's
//! tenant-wide client-credentials app token this module could mint a
//! `users.stop`/`channels.stop` call with. Following
//! `microsoft_disconnect`'s own precedent for its delegated case exactly
//! (rather than inventing a new "decrypt the just-deleted access token and
//! hope it's not already expired" path with no established pattern to
//! follow), this purge is LOCAL-ONLY: it removes the rows that make CoralSwarm
//! act on a notification, and lets the upstream watch itself lapse
//! naturally (Gmail: ~7 days; Calendar: the channel's own `expires_at`) —
//! the same "the load-bearing half already stopped" posture
//! `microsoft_disconnect`'s own header takes for its delegated subscriptions.
//!
//! # `purge_not_after` — the reconnect race (CodeRabbit round 1 on PR #690,
//! Major, confirmed real; fixed here)
//!
//! `routes::oauth::delete_connection` runs this purge AFTER the `oauth_tokens`
//! DELETE has already committed, synchronously within the same request but on
//! a separate `tenant_pool` connection. `google_watch_channels`/
//! `gmail_watched_threads` are NOT `ON DELETE CASCADE` from `oauth_tokens` (by
//! design — see this module's own header), so between that commit and this
//! purge's own DELETE statements, the row this purge is about to remove is
//! provably still present, unmodified, under the exact same
//! `(account_id, surface, resource_id)` / `(account_id, thread_id)` identity a
//! RECONNECT would reuse.
//!
//! That matters because — unlike `microsoft_disconnect`'s four tables, none of
//! which are written synchronously from `routes::oauth::callback` — Google's
//! `ConnectorProvider::post_store` (`connectors::google::GoogleConnector::
//! post_store`) calls `google_gmail::ensure_watch`/`google_calendar::
//! ensure_watch` SYNCHRONOUSLY, in the SAME request that just committed the
//! new `oauth_tokens` row, before redirecting. `ensure_watch`'s own upsert
//! (`google_gmail::upsert_watch` / `google_calendar::upsert_channel`) is keyed
//! on that identical `(account_id, surface, resource_id)` unique constraint
//! and unconditionally sets `updated_at = now()` on every insert AND every
//! `ON CONFLICT DO UPDATE` — so a reconnect for the SAME mailbox landing in
//! this window does not insert a distinguishably-new row; it refreshes the
//! SAME row in place. A disconnect's purge running after that refresh, with no
//! guard, deletes state the reconnect just re-established — Microsoft has no
//! equivalent exposure because nothing writes its four tables synchronously
//! from `callback`; Outlook's `ensure_mail_subscriptions` and Teams' watch
//! creation are both reached only from `sync()`/explicit opt-in, never from
//! connect itself, so there is no "reconnect commits, then a stale disconnect
//! purges the fresh row" window there to begin with (a narrower, worker/
//! sync-tick-driven version of the same missing-guard SHAPE still exists for
//! `microsoft_disconnect` — e.g. a concurrent "sync now" landing between its
//! token-delete commit and its own purge — recorded as debt for that module,
//! out of scope for this PR).
//!
//! Fix: [`purge_google_connection`] takes `purge_not_after`, a timestamp the
//! caller captures BEFORE running the purge (see `routes::oauth::
//! delete_connection`'s own call site) and both DELETEs are additionally
//! bounded by `updated_at <= purge_not_after` (`google_watch_channels`) /
//! `last_ingested_at <= purge_not_after` (`gmail_watched_threads` — its own
//! upsert, `google_gmail::upsert_watched_thread`, bumps that column
//! identically on every insert/update). A row this disconnect legitimately
//! owns was last touched strictly before the disconnect request began, so its
//! timestamp is always `<= purge_not_after`; a row a concurrent reconnect just
//! refreshed carries a strictly later timestamp and survives. No new advisory
//! lock, no schema change, no behavior change to `ensure_watch` itself.
//! CodeRabbit's own "lifecycle generation" suggestion would require plumbing
//! a generation/connection-identity column through every writer of these two
//! tables for a race this narrow; a timestamp cutoff closes it with no schema
//! or writer-side change at all.
//!
//! # `purge_not_after` must come from the SAME clock as `updated_at`/
//! `last_ingested_at` (CodeRabbit round 2 on PR #690, Major, confirmed real;
//! fixed here)
//!
//! Round 1's fix originally sourced `purge_not_after` as `Utc::now()` in the
//! API process, compared against `updated_at`/`last_ingested_at` — both
//! stamped by Postgres's OWN `now()` inside `upsert_watch`/`upsert_channel`/
//! `upsert_watched_thread`. Two different clocks producing the two sides of
//! one comparison is only safe if they agree: if the API host's clock runs
//! behind the database host's, a row this disconnect genuinely owns (last
//! touched before the request even began, by the database's clock) could
//! still read as `updated_at > purge_not_after` by the API host's clock,
//! matching 0 rows and logging "purge complete" while the row survives — the
//! false-negative CodeRabbit's round 1 fix was supposed to eliminate, now
//! reintroduced from the other direction. The round 1 pg tests didn't catch
//! this because they sourced their own cutoff via a direct `SELECT now()`
//! against the SAME tenant pool the assertions read back from — correct for
//! what they were proving (the guard's DIRECTION), but blind to a
//! cross-process clock disagreeing with itself.
//!
//! Fixed at the call site (`routes::oauth::delete_connection`): `purge_not_after`
//! is now sourced via `SELECT now()` against `tenant_pool` itself — the SAME
//! connection pool `delete_watch_channels`/`delete_watched_threads` run
//! their own comparisons against — immediately before calling
//! [`purge_google_connection`], rather than `Utc::now()` in this process.
//! One clock produces both sides of every comparison this module makes, by
//! construction: [`purge_google_connection`] never touches any pool other
//! than the single `tenant_pool` its caller passes in (both DELETEs run
//! against it), so there is no second plane's clock to reconcile here —
//! unlike, say, comparing a control-plane timestamp against a tenant-plane
//! column (which this module never does; Google connections are
//! Personal-plane-only today per gsuite plan Q1, so `tenant_pool` and the
//! control pool the `oauth_tokens` DELETE runs against happen to be the same
//! physical server anyway, but this fix does not rely on that — it would
//! stay correct even once/if that changes). A `SELECT now()` failure skips
//! the ENTIRE purge with a `warn!`, the same best-effort posture the missing-
//! `tenant_pool` branch already had (see `routes::oauth::delete_connection`'s
//! own call site) — matches this module's own "No durable retry on purge
//! failure" disposition below rather than failing the whole disconnect over
//! a purge precondition. [`purge_google_connection`]'s own signature is
//! unchanged by this fix (still takes an explicit `purge_not_after` — only
//! WHERE the caller sources its value from changed), so this module's own pg
//! tests (which already build their cutoff via a `SELECT now()` against the
//! tenant pool, `db_now`) needed no restructuring; they were already
//! exercising the corrected, single-clock shape from round 1 — round 2's fix
//! makes the REAL call site match what those tests were already proving,
//! not the other way around.
//!
//! # No durable retry on purge failure (CodeRabbit round 1 on PR #690, Major,
//! evidence-backed rejection — matches precedent, disposition below)
//!
//! `routes::oauth::delete_connection` logs and returns `200` on a purge
//! failure or a missing `tenant_pool`, with nothing that ever retries — same
//! as `slack_disconnect`/`microsoft_disconnect`'s own hooks, both already
//! best-effort-and-`200` with no retry. Traced rather than assumed: a
//! surviving row from a failed purge is NOT silent forever.
//!
//! * `workers::google_watch_renewal::renew_one` loads the token via
//!   `google_token`/`load_creds_bg`; with the `oauth_tokens` row already
//!   gone (deleted unconditionally, regardless of purge success — see this
//!   module's own header on why that ordering is fixed and out of this
//!   module's control), `load_creds_bg` returns `None`, `renew_one` returns
//!   `Err(ConnectorError::NotConfigured(...))`, and `renew_due` catches it,
//!   `warn!`s, and calls `mark_failed` — the channel's `expires_at` is never
//!   advanced. Once it genuinely lapses, `tick_backstop`'s `alert_lapsed`
//!   fires at `error!` with `connector_health = true` (module doc: "the
//!   loud-failure signal the plan's... obligation requires") — bounded by
//!   the channel's own remaining TTL (Gmail ~7 days, Calendar per its own
//!   `expires_at`) plus at most `BACKSTOP_INTERVAL` (6h). Not silent; loud,
//!   on a bounded delay.
//! * `routes::google_push::run_gmail_sync_for_email` hits the identical
//!   `load_creds_bg(...).ok_or_else(NotConfigured)?` — a surviving
//!   `gmail_watched_threads` row cannot "silently resume live-follow
//!   ingestion for a disconnected member" (the specific worry CodeRabbit
//!   raised) because there is structurally no token left to fetch with by
//!   the time any push notification could arrive; the sync fails closed,
//!   the same as renewal.
//! * A reconnect self-heals a surviving row for free: `ensure_watch`'s
//!   upsert (see above) refreshes it on the very next connect, no manual
//!   cleanup needed.
//!
//! Given all three existing disconnect purges in this codebase (Slack,
//! Microsoft, Google) share this exact best-effort-and-`200`-with-no-retry
//! shape, and Google's own failure mode traces to loud (bounded-delay,
//! `error!`-level, `connector_health = true`) rather than silent, this PR
//! matches precedent rather than building a durable retry queue (CodeRabbit's
//! suggested fix) with no established pattern anywhere in this codebase to
//! follow for one.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google::Surface;
use crate::connectors::TokenOwner;
use crate::tenancy::scope::begin_tenant_system_scoped;

/// What [`purge_google_connection`] actually did — logged by its caller, and
/// asserted on directly by this module's own pg tests. Mirrors
/// `microsoft_disconnect::PurgeOutcome`'s "zero is ambiguous, a `_failed`
/// flag breaks the tie" shape.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PurgeOutcome {
    /// `google_watch_channels` rows deleted (scoped to `surface`, and to
    /// `owner` when it names a specific member) — the row whose ABSENCE is
    /// what stops `workers::google_watch_renewal` from renewing a watch for
    /// a connection that no longer has a token.
    pub(crate) watch_channels_deleted: usize,
    /// `true` when that DELETE itself errored (not the same as "zero rows").
    pub(crate) watch_channels_delete_failed: bool,
    /// `gmail_watched_threads` rows deleted — only ever non-zero for a Gmail
    /// purge (`surface == Surface::Gmail`); a Calendar disconnect has no
    /// opt-in-thread table to clear. See [`purge_google_connection`]'s own
    /// doc comment.
    pub(crate) watched_threads_deleted: usize,
    pub(crate) watched_threads_delete_failed: bool,
    /// Watched SPACES whose designated reader was this member and which have
    /// been re-elected onto another connected member (gsuite Train G4 PR 4,
    /// design doc §3.5 — *failover is a re-election, not a row deletion*).
    pub(crate) chat_spaces_reader_reelected: usize,
    /// Watched spaces that kept their destination and cursors but now have
    /// NOBODY left who can read them — §3.5's *"nobody left"* end state.
    /// Deliberately NOT an error and deliberately NOT a terminal ledger row
    /// (§1.10 lesson 4): the condition is ours to fix and fully reversible
    /// the moment any member of the space reconnects Chat.
    pub(crate) chat_spaces_orphaned: usize,
    pub(crate) chat_reader_reelect_failed: bool,
    /// `google_meet_sync_state` rows deleted (gsuite Train G3 PR 2) — the
    /// row whose ABSENCE is what stops `workers::google_meet_sweep`'s
    /// discovery query (`resolve_google_meet_sync_due()`) from ever claiming
    /// this member again. Only ever non-zero for a Meet purge
    /// (`surface == Surface::Meet`); every other surface leaves this at 0.
    pub(crate) meet_sync_state_deleted: usize,
    pub(crate) meet_sync_state_delete_failed: bool,
    pub(crate) meet_captures_revoked: usize,
    pub(crate) meet_captures_revoke_failed: bool,
}

impl PurgeOutcome {
    /// `true` when any local DB delete failed. Unlike
    /// `microsoft_disconnect::PurgeOutcome::purge_had_a_failure`, there is no
    /// upstream-only failure category to exclude here — this module makes no
    /// upstream calls at all (see this module's own doc comment).
    pub(crate) fn purge_had_a_failure(&self) -> bool {
        self.watch_channels_delete_failed
            || self.watched_threads_delete_failed
            || self.chat_reader_reelect_failed
            || self.meet_sync_state_delete_failed
            || self.meet_captures_revoke_failed
    }
}

/// The one shared Google disconnect purge.
///
/// `tenant_pool` is the CONTENT plane for this account — the consumer pool
/// for Personal, the org's own tenant database for Org (same plane
/// `microsoft_disconnect::purge_microsoft_connection`'s own doc documents for
/// its four tables). `control_pool` is the CONTROL plane (gsuite Train G2
/// PR 7): `google_watch_channels` (surface `'gmail'`) now lives there ONLY —
/// see `connectors::google_gmail`'s own module doc, "Watch rows are
/// CONTROL-plane, always" — while a Calendar (`surface='calendar'`) row and
/// every `gmail_watched_threads` row keep living on `tenant_pool`, unchanged
/// (Calendar stays account-wide, Personal-plane-only today — see
/// `connectors::google::Surface`'s own doc — so its `tenant_pool` and
/// `control_pool` are the same physical database regardless of this split).
/// [`purge_google_connection`] picks which of the two pools a given
/// `google_watch_channels` delete runs against, per `surface`; the caller
/// (`routes::oauth::delete_connection`) supplies a `purge_not_after` cutoff
/// sourced from EACH pool's own clock (see the `purge_not_after` section
/// below — the single-clock requirement now applies per-pool, not once).
///
/// `surface` picks which `google_watch_channels` rows this disconnect owns —
/// `Surface::Gmail` or `Surface::Calendar` are the only two reachable from
/// `routes::oauth::delete_connection` today (Chat/Meet are inert, gsuite plan
/// Trains G3/G4). Also decides whether `gmail_watched_threads` participates
/// at all: that table has no per-surface column of its own (it is
/// structurally Gmail-only, see its own migration header), so a Calendar
/// purge never touches it.
///
/// `owner` selects which rows within that surface this disconnect is
/// responsible for, same contract as `microsoft_disconnect::
/// purge_microsoft_connection`'s own `owner` parameter:
///
/// * [`TokenOwner::AccountWide`] — Calendar's shape
///   (`Surface::Calendar::token_owner_for`, `connectors::google::Surface`'s
///   own doc comment: Calendar OAuth is account-wide, no per-member storage
///   built yet). Purges every `surface='calendar'` row for `account_id`.
/// * [`TokenOwner::Member`] — Gmail's shape (the only surface with real
///   per-member OAuth today, gsuite Train G2 PR 1). Scopes
///   `google_watch_channels`/`gmail_watched_threads` to the rows THAT
///   member owns (`owner_user_id`/`ingested_by`), never another member's —
///   a second connected Gmail member's watch/opt-in state must survive one
///   member's disconnect untouched.
///
/// `control_purge_not_after`/`tenant_purge_not_after` close the reconnect
/// race this module's own doc comment describes ("`purge_not_after` — the
/// reconnect race" section, above) — the caller captures EACH one BEFORE
/// doing anything (see `routes::oauth::delete_connection`'s own call site),
/// sourced from that SAME pool's own clock (round 2's fix — see the
/// `purge_not_after` clock section above), and every delete additionally
/// requires the row's own last-touched timestamp to be `<= ` the cutoff
/// sourced from the pool that row actually lives on, so a row a concurrent
/// reconnect refreshed after the disconnect began survives instead of being
/// deleted out from under it.
pub(crate) async fn purge_google_connection(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    surface: Surface,
    owner: TokenOwner,
    control_purge_not_after: DateTime<Utc>,
    tenant_purge_not_after: DateTime<Utc>,
) -> PurgeOutcome {
    let mut outcome = PurgeOutcome::default();
    let owner_uid = owner.owner_user_id();

    // ── 1. google_watch_channels — the row whose ABSENCE is what actually
    // stops `workers::google_watch_renewal` from renewing a watch for a
    // connection whose token is already gone. gsuite Train G2 PR 7: a Gmail
    // row lives on `control_pool`; every other surface (Calendar today)
    // stays on `tenant_pool`, unchanged. ────────────────────────────────────
    // gsuite Train G4 PR 4: `Surface::Chat` joins Gmail on the CONTROL pool,
    // because that is where its rows are actually WRITTEN — both
    // `GoogleConnector::post_store` (which receives `routes::oauth::
    // callback`'s own `state.db`, control by construction) and
    // `workers::google_chat_sweep`'s self-heal call
    // `google_chat::ensure_subscription(control_pool, ..)`. Deleting from
    // `tenant_pool` instead would leave the real row behind on an org
    // tenant, and the renewal worker would keep spinning on a subscription
    // whose token is gone. **This does NOT settle #695's org-plane watch-row
    // plane split** — design doc §G4.6 assigns that settlement to the org
    // lift explicitly, and on the personal plane (the only plane Chat can
    // reach until then) both pools are the same physical database, so this
    // choice has no observable consequence yet. It matches the writer, which
    // is the property that matters today.
    // Gmail, Chat and Meet watches are always on control.
    {
        // Google Calendar org lift: an org member's Calendar row lives on the
        // CONTROL plane (like Gmail's — `connectors::google_calendar`'s module
        // doc, "Org lift"), the personal account-wide row on `tenant_pool`
        // (the same physical database as `control_pool` on the Personal
        // shape). So Calendar purges BOTH, exactly like Meet's cursor purge
        // below: on Personal the second delete finds nothing; on Org the
        // tenant delete is a harmless sweep for any pre-lift stray. The
        // `owner_uid` scoping is identical on both, so one member's
        // disconnect can never remove another member's channel.
        let watch_targets: Vec<(&PgPool, DateTime<Utc>)> = match surface {
            Surface::Gmail | Surface::Chat | Surface::Meet => vec![(control_pool, control_purge_not_after)],
            Surface::Calendar => vec![
                (control_pool, control_purge_not_after),
                (tenant_pool, tenant_purge_not_after),
            ],
        };
        for (watch_pool, watch_purge_not_after) in watch_targets {
            match delete_watch_channels(watch_pool, account_id, surface, owner_uid, watch_purge_not_after).await {
                Ok(n) => outcome.watch_channels_deleted += n,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        %account_id,
                        surface = surface.as_str(),
                        connector_health = true,
                        "google disconnect purge: failed to delete google_watch_channels rows — \
                         continuing with the rest of the purge"
                    );
                    outcome.watch_channels_delete_failed = true;
                }
            }
        }
    }

    // ── 2. gmail_watched_threads — Gmail-only; see this function's own doc
    // comment for why a Calendar purge never reaches this branch. ALWAYS
    // `tenant_pool` — this decision never moved (module doc, "Watch rows are
    // CONTROL-plane, always" is scoped to `google_watch_channels` only). ────
    if surface == Surface::Gmail {
        match delete_watched_threads(tenant_pool, account_id, owner_uid, tenant_purge_not_after).await {
            Ok(n) => outcome.watched_threads_deleted = n,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    %account_id,
                    connector_health = true,
                    "google disconnect purge: failed to delete gmail_watched_threads rows — \
                     continuing"
                );
                outcome.watched_threads_delete_failed = true;
            }
        }
    }

    // ── 3. google_chat_watched_spaces — participation, NOT deletion ─────────
    // gsuite Train G4 PR 4 (design doc §1.8, §3.5). A departing member's
    // Chat disconnect must stop THEM being read from, and must not disturb
    // anything else: a space's destination is a HUMAN'S I2 authorisation
    // (contract: fixed at authorisation time), its cursors are the only
    // record of what has already been ingested, and both belong to the SPACE,
    // not to whoever happened to be reading it. So the row is neither
    // deleted nor deactivated — the reader is RE-ELECTED onto another
    // connected member, and when there is nobody left the row simply loses
    // its reader and waits. Deleting it here would silently discard a
    // destination pick and, on the next `ADDED_TO_SPACE`, restart the picker
    // from scratch.
    //
    // Container rows only (`owner_user_id IS NULL`, enforced inside
    // `reelect_reader_for_departing_member`'s own query). G4.5's mailbox
    // rows are per-owner and get their own disconnect handling in that PR.
    if surface == Surface::Chat {
        if let Some(departing_user_id) = owner_uid {
            // S6: route through `on_backer_lost` so directory + space-reader
            // share one entry. Disconnect-while-staying does NOT open a
            // domain_directory row (CS-448 derived read); Chat re-election
            // still runs and may open `no_eligible_backer` if orphaned.
            match crate::services::connector_health::on_backer_lost(
                control_pool,
                Some(tenant_pool),
                account_id,
                Some(departing_user_id),
                "google_chat",
                crate::services::connector_health::BackerLostCause::Disconnect,
                None,
                None,
            )
            .await
            {
                Ok(report) => {
                    outcome.chat_spaces_reader_reelected = report.chat_spaces_reelected;
                    outcome.chat_spaces_orphaned = report.chat_spaces_orphaned;
                    if report.chat_spaces_orphaned > 0 {
                        tracing::warn!(
                            %account_id, orphaned = report.chat_spaces_orphaned, connector_health = true,
                            "google chat disconnect: watched spaces have no connected member left \
                             who can read them — ingest is paused for them. Every destination and \
                             cursor is preserved; ingest resumes the moment any member of those \
                             spaces reconnects Google Chat."
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e, %account_id, connector_health = true,
                        "google chat disconnect: on_backer_lost failed — a watched space may \
                         keep naming a disconnected member as its reader until the next sweep \
                         tick re-elects around it"
                    );
                    outcome.chat_reader_reelect_failed = true;
                }
            }
        }
    }

    // ── 4. google_meet_sync_state — Meet's own pull cursor (gsuite Train G3
    // PR 2). Deleting this row is what "disconnecting Meet stops Meet" means
    // here: `workers::google_meet_sweep`'s discovery query
    // (`resolve_google_meet_sync_due()`) can only ever claim a row that
    // exists, so removing it is equivalent in effect to the watch-channel
    // deletion above for the push-based surfaces, even though Meet has no
    // upstream subscription to leave behind. Always `control_pool` — the
    // ONLY plane this table lives on (see migration `0204`'s own header) —
    // and always owner-scoped (`Surface::Meet` is unconditionally
    // `TokenOwner::Member`, never `AccountWide`, so `owner_uid` is always
    // `Some` here in practice; the query is written defensively the same
    // `owner_user_id IS NULL OR ...`-free way `delete_watch_channels` is).
    //
    // Meet org-plane lift: an ORG member's cursor lives on the org's TENANT
    // database (tenant twin `0077`), seeded by `workers::google_meet_sweep::
    // tick_org`. It is deleted here too, on `tenant_pool`. On the Personal
    // shape `control_pool` and `tenant_pool` are the same database, so the
    // second delete simply finds nothing (idempotent, counts add up to the
    // one real row).
    if surface == Surface::Meet {
        let revoked = async {
            let mut tx = begin_tenant_system_scoped(tenant_pool, account_id).await.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            let result = sqlx::query("UPDATE google_meet_captures SET status='revoked',revoked_at=now(),updated_at=now() WHERE account_id=$1 AND ($2::uuid IS NULL OR owner_user_id=$2) AND status='allowed' AND approved_at <= $3")
                .bind(account_id).bind(owner_uid).bind(tenant_purge_not_after).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok::<usize,sqlx::Error>(result.rows_affected() as usize)
        }.await;
        match revoked {
            Ok(n) => outcome.meet_captures_revoked = n,
            Err(e) => { outcome.meet_captures_revoke_failed = true; tracing::warn!(error=%e,%account_id,"Meet disconnect approval revocation failed"); }
        }
        for (plane_name, result) in [
            ("control", delete_meet_sync_state(control_pool, account_id, owner_uid, control_purge_not_after).await),
            ("tenant", delete_meet_sync_state(tenant_pool, account_id, owner_uid, tenant_purge_not_after).await),
        ] {
            match result {
                Ok(n) => outcome.meet_sync_state_deleted += n,
                Err(e) => {
                    tracing::warn!(
                        error = %e, %account_id, plane = plane_name, connector_health = true,
                        "google disconnect purge: failed to delete google_meet_sync_state row — \
                         continuing"
                    );
                    outcome.meet_sync_state_delete_failed = true;
                }
            }
        }
    }

    outcome
}

/// Best-effort upstream `subscriptions.delete` for a disconnecting Chat
/// member (gsuite Train G4 PR 4, design doc §1.8).
///
/// **This module's own doc says it *"makes no upstream calls at all"*, and
/// Chat is the deliberate exception** — that claim rests on there being no
/// app-level Google credential in the delegated case, which is true, but a
/// member's OWN token can revoke a member's OWN subscription, and leaving a
/// live Workspace Events subscription behind after a disconnect means Google
/// keeps publishing that member's Chat activity to our topic for up to the
/// full 7-day TTL. We discard those pushes as unwatched, so nothing is
/// ingested either way — but "we throw it away on receipt" is a weaker
/// promise than "we asked them to stop", and the difference is exactly what
/// a disconnect is for. Called out in the PR body rather than left to make
/// the module doc quietly false.
///
/// Runs AFTER the `oauth_tokens` row is already gone, on the access token
/// the delete's own `RETURNING` clause captured — the same post-commit,
/// decrypt-after-the-transaction-closes discipline
/// `routes::oauth::delete_connection`'s Slack `conversations.leave` step
/// already uses. Entirely non-fatal: an expired token, a revoked grant, or
/// a network failure all leave the subscription to expire on its own.
///
/// **It is also BOUNDED, which "best-effort" alone would not guarantee.**
/// This runs post-commit on the `DELETE /v1/oauth/connections/:provider`
/// request task, so an unbounded call would hold the user's disconnect
/// response open on a stalled Google endpoint — "best-effort" covers a
/// failure, not a hang. The bound is not `state.http_client`'s (that carries
/// the LLM-sized `LLM_TIMEOUT_SECS` total deadline, 60s by default): the
/// upstream call goes through [`crate::connectors::google::google_send`],
/// which applies its own per-request `.timeout(30s)` and overrides the
/// client-wide one, with at most one 429/503 retry. `routes::oauth`'s caller
/// additionally makes exactly ONE such call per disconnect — see the
/// `break` there and its comment.
pub(crate) async fn revoke_chat_subscription_best_effort(
    http: &reqwest::Client,
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    access_token: &str,
    surface: &str,
    disconnect_before: Option<DateTime<Utc>>,
) -> bool {
    let Some(disconnect_before) = disconnect_before else { return false; };
    let subscription_name: Option<String> = {
        let tx = begin_tenant_system_scoped(control_pool, account_id).await;
        match tx {
            Ok(mut tx) => {
                let row: Result<Option<(String,)>, sqlx::Error> = sqlx::query_as(
                    "SELECT channel_id FROM google_watch_channels \
                     WHERE account_id = $1 AND surface = $3 AND owner_user_id = $2 \
                       AND channel_id <> '' AND updated_at <= $4",
                )
                .bind(account_id)
                .bind(owner_user_id)
                .bind(surface)
                .bind(disconnect_before)
                .fetch_optional(&mut *tx)
                .await;
                tx.rollback().await.ok();
                match row {
                    Ok(r) => r.map(|(c,)| c),
                    Err(e) => {
                        tracing::info!(error = %e, %account_id, "google chat disconnect: could not read the subscription name (non-fatal)");
                        None
                    }
                }
            }
            Err(e) => {
                tracing::info!(error = %e, %account_id, "google chat disconnect: subscription-name scope failed (non-fatal)");
                None
            }
        }
    };
    let Some(subscription_name) = subscription_name else {
        return false;
    };
    if surface == "meet" {
        match crate::connectors::google_meet_events::referenced_by_another_watch(control_pool, &subscription_name, account_id, owner_user_id).await {
            Ok(false) => {}
            Ok(true) => {
                tracing::info!(%account_id, "google meet disconnect: subscription is still referenced by another account; leaving it");
                return false;
            }
            Err(e) => {
                tracing::info!(error = %e, %account_id, "google meet disconnect: could not check for other references; leaving the subscription (non-fatal)");
                return false;
            }
        }
    }
    crate::connectors::google_chat::delete_subscription_best_effort(http, access_token, &subscription_name).await
}

/// Delete `google_watch_channels` rows for `account_id` and `surface`, scoped
/// to `owner_uid` when `Some` (a per-member purge, Gmail's only case today)
/// or every row of that surface when `None` (account-wide — Calendar's only
/// case today). No `owner_user_id IS NULL OR owner_user_id = $n` fallback
/// hazard: same "management/deletion surface, not a credential read"
/// distinction `microsoft_disconnect::delete_watched_conversations`'s own doc
/// comment draws, and `owner_uid` is never client input here either.
///
/// `updated_at <= $4` (`purge_not_after`) is the reconnect-race guard — see
/// this module's own doc comment. `ensure_watch`'s upsert
/// (`google_gmail::upsert_watch`/`google_calendar::upsert_channel`) sets
/// `updated_at = now()` on every insert AND every `ON CONFLICT DO UPDATE`, so
/// a row a concurrent reconnect just refreshed always fails this predicate.
///
/// `pool` (gsuite Train G2 PR 7, CodeRabbit round 1): NOT necessarily the
/// tenant pool — the caller ([`purge_google_connection`]) resolves it to
/// `control_pool` for a Gmail-surface delete, `tenant_pool` for every other
/// surface. This function itself has no opinion on which; it just runs the
/// scoped delete against whichever pool it's handed.
async fn delete_watch_channels(
    pool: &PgPool,
    account_id: Uuid,
    surface: Surface,
    owner_uid: Option<Uuid>,
    purge_not_after: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_watch_channels scoped tx: {e}")))?;
    let result = sqlx::query(
        "DELETE FROM google_watch_channels \
         WHERE account_id = $1 AND surface = $2 AND ($3::uuid IS NULL OR owner_user_id = $3) \
           AND updated_at <= $4",
    )
    .bind(account_id)
    .bind(surface.as_str())
    .bind(owner_uid)
    .bind(purge_not_after)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

/// Delete this owner's `google_meet_sync_state` row (gsuite Train G3 PR 2) —
/// same owner-scoping/`purge_not_after` reconnect-race-guard shape as
/// [`delete_watch_channels`], bounded on `updated_at`
/// (`connectors::google_meet::ensure_sync_state`'s own upsert leaves it at
/// its insert time; a re-run within the reconnect race window would need to
/// have refreshed the row for this guard to matter, which today's
/// `ON CONFLICT DO NOTHING` upsert does not do — the guard is still applied
/// for consistency with every other table in this module, and would start
/// mattering the moment that upsert ever gains an `ON CONFLICT DO UPDATE`
/// arm). Always `control_pool` — see this table's own migration header,
/// "Watch rows are CONTROL-plane, always"'s identical reasoning applied to
/// Meet's cursor instead of a push channel.
async fn delete_meet_sync_state(
    control_pool: &PgPool,
    account_id: Uuid,
    owner_uid: Option<Uuid>,
    purge_not_after: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(control_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_meet_sync_state scoped tx: {e}")))?;
    let result = sqlx::query(
        "DELETE FROM google_meet_sync_state \
         WHERE account_id = $1 AND ($2::uuid IS NULL OR owner_user_id = $2) \
           AND updated_at <= $3",
    )
    .bind(account_id)
    .bind(owner_uid)
    .bind(purge_not_after)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

/// Delete `gmail_watched_threads` rows, same owner-scoping rule as
/// [`delete_watch_channels`] — and the same `purge_not_after` reconnect-race
/// guard, bounded on `last_ingested_at` (`google_gmail::
/// upsert_watched_thread` bumps it on every insert/update, identically to
/// `delete_watch_channels`'s own `updated_at` reasoning).
async fn delete_watched_threads(
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner_uid: Option<Uuid>,
    purge_not_after: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_watched_threads scoped tx: {e}")))?;
    let result = sqlx::query(
        "DELETE FROM gmail_watched_threads \
         WHERE account_id = $1 AND ($2::uuid IS NULL OR ingested_by = $2) \
           AND last_ingested_at <= $3",
    )
    .bind(account_id)
    .bind(owner_uid)
    .bind(purge_not_after)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_wide_owner_uid_is_none() {
        assert_eq!(TokenOwner::AccountWide.owner_user_id(), None);
    }

    #[test]
    fn member_owner_uid_is_the_wrapped_id() {
        let id = Uuid::new_v4();
        assert_eq!(TokenOwner::Member(id).owner_user_id(), Some(id));
    }

    #[test]
    fn purge_had_a_failure_is_false_for_a_clean_outcome() {
        assert!(!PurgeOutcome::default().purge_had_a_failure());
    }

    #[test]
    fn purge_had_a_failure_is_true_for_either_local_delete_failure() {
        for outcome in [
            PurgeOutcome { watch_channels_delete_failed: true, ..PurgeOutcome::default() },
            PurgeOutcome { watched_threads_delete_failed: true, ..PurgeOutcome::default() },
        ] {
            assert!(outcome.purge_had_a_failure());
        }
    }
}

/// pg-gated integration tests: proves the purge against a REAL Postgres
/// database. These three tests run against a single `migrations_tenant`
/// database, passed as BOTH `control_pool` and `tenant_pool` — the Personal
/// tenancy shape (`tenancy::extractor::Tenant`'s own doc: the two planes are
/// the same physical database there), which is also the only shape reachable
/// in production today (`ORG_GOOGLE_GMAIL_ENABLED` is still default-off). A
/// genuinely two-database proof (control and tenant as separate physical
/// databases, under the real least-privilege `coralswarm_app` role) lives
/// alongside the module doc's org-plane decision, in
/// `connectors::google_gmail::tests` (gsuite Train G2 PR 7) — see that
/// module's own `org_plane_self_heal_lands_on_control_push_resolver_finds_it_
/// disconnect_purges_it_pg` test, which drives THIS function
/// (`purge_google_connection`) as its final step.
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib google_disconnect_pg_tests
#[cfg(test)]
mod google_disconnect_pg_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

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
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }
    async fn drop_db(admin_conn: &mut PgConnection, db: &str) {
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    async fn build_tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }
    async fn seed_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) \
             VALUES ($1, 'test ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    #[allow(clippy::too_many_arguments)]
    async fn seed_watch_channel(
        pool: &PgPool,
        account_id: Uuid,
        surface: &str,
        resource_id: &str,
        owner_user_id: Option<Uuid>,
    ) {
        sqlx::query(
            "INSERT INTO google_watch_channels \
                 (account_id, surface, resource_id, owner_user_id, expires_at) \
             VALUES ($1, $2, $3, $4, now() + interval '1 day')",
        )
        .bind(account_id)
        .bind(surface)
        .bind(resource_id)
        .bind(owner_user_id)
        .execute(pool)
        .await
        .unwrap();
    }
    async fn seed_watched_thread(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        ingested_by: Uuid,
        thread_id: &str,
    ) {
        sqlx::query(
            "INSERT INTO gmail_watched_threads (account_id, graph_id, ingested_by, thread_id) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(ingested_by)
        .bind(thread_id)
        .execute(pool)
        .await
        .unwrap();
    }
    async fn count(pool: &PgPool, table: &str, account_id: Uuid) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE account_id = $1"))
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    /// The tenant DB's own clock, read back directly — used as
    /// `purge_not_after` in every test below instead of an app-side
    /// `Utc::now()`, so a test's cutoff is never at the mercy of clock skew
    /// between the test process and Postgres (the real
    /// `routes::oauth::delete_connection` call site uses `Utc::now()`; see
    /// this module's own doc comment on why that's fine for a real
    /// deployment's NTP-synced clocks).
    async fn db_now(pool: &PgPool) -> DateTime<Utc> {
        sqlx::query_scalar("SELECT now()").fetch_one(pool).await.unwrap()
    }
    /// Simulate a reconnect's `ensure_watch` upsert refreshing an EXISTING
    /// `google_watch_channels` row in place — the exact mechanism this
    /// module's own doc comment describes (`upsert_watch`/`upsert_channel`
    /// both `ON CONFLICT DO UPDATE ... updated_at = now()`), pinned to an
    /// explicit, deterministic future timestamp rather than a real sleep.
    async fn bump_watch_channel_updated_at(pool: &PgPool, account_id: Uuid, surface: &str, resource_id: &str) {
        sqlx::query(
            "UPDATE google_watch_channels SET updated_at = now() + interval '1 hour' \
             WHERE account_id = $1 AND surface = $2 AND resource_id = $3",
        )
        .bind(account_id)
        .bind(surface)
        .bind(resource_id)
        .execute(pool)
        .await
        .unwrap();
    }
    /// Simulate a reconnect's opt-in re-touching an EXISTING
    /// `gmail_watched_threads` row — `google_gmail::upsert_watched_thread`'s
    /// `ON CONFLICT DO UPDATE SET last_ingested_at = now()`.
    async fn bump_watched_thread_last_ingested_at(pool: &PgPool, account_id: Uuid, thread_id: &str) {
        sqlx::query(
            "UPDATE gmail_watched_threads SET last_ingested_at = now() + interval '1 hour' \
             WHERE account_id = $1 AND thread_id = $2",
        )
        .bind(account_id)
        .bind(thread_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A `Surface::Gmail` + `TokenOwner::Member` purge must remove ONLY the
    /// disconnecting member's own `google_watch_channels` (`surface='gmail'`)
    /// row and `gmail_watched_threads` rows — leaving another member's Gmail
    /// watch/threads AND the account's unrelated Calendar watch channel
    /// completely untouched. This is also the "renewal worker keeps renewing
    /// a watch for a token that no longer exists" assertion this PR exists to
    /// pre-empt: a purged `google_watch_channels` row is what
    /// `workers::google_watch_renewal`'s tenant-loop sweep stops seeing.
    #[tokio::test]
    async fn gmail_member_purge_touches_only_that_members_rows_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping gmail_member_purge_touches_only_that_members_rows_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_g25_gmail_member";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let tenant = build_tenant_pool(&base, db, "org_g25_gmail_member").await;

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();
        let other_member = Uuid::new_v4();
        let graph_id = seed_graph(&tenant, account_id).await;

        seed_watch_channel(&tenant, account_id, "gmail", "member@example.com", Some(member)).await;
        seed_watched_thread(&tenant, account_id, graph_id, member, "thread-member").await;

        seed_watch_channel(&tenant, account_id, "gmail", "other@example.com", Some(other_member)).await;
        seed_watched_thread(&tenant, account_id, graph_id, other_member, "thread-other").await;

        // Unrelated Calendar watch channel for the SAME account — must
        // survive a Gmail-surface purge untouched.
        seed_watch_channel(&tenant, account_id, "calendar", "primary", None).await;

        let cutoff = db_now(&tenant).await;
        // Single-DB test — `control_pool` and `tenant_pool` are the SAME
        // pool/cutoff, matching Personal tenancy (gsuite Train G2 PR 7's
        // module doc: the two planes are the same physical database there).
        let outcome = purge_google_connection(
            &tenant,
            &tenant,
            account_id,
            Surface::Gmail,
            TokenOwner::Member(member),
            cutoff,
            cutoff,
        )
        .await;

        assert_eq!(outcome.watch_channels_deleted, 1);
        assert_eq!(outcome.watched_threads_deleted, 1);
        assert!(!outcome.purge_had_a_failure());

        let remaining_gmail: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND surface = 'gmail'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(remaining_gmail, 1, "the other member's Gmail watch channel must survive");

        assert_eq!(
            count(&tenant, "gmail_watched_threads", account_id).await,
            1,
            "the other member's watched thread must survive"
        );

        let calendar_survives: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND surface = 'calendar'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(calendar_survives, 1, "the unrelated Calendar watch channel must never be touched");

        tenant.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// A `Surface::Calendar` + `TokenOwner::AccountWide` purge must remove
    /// every `surface='calendar'` `google_watch_channels` row for the
    /// account, and must NOT touch `gmail_watched_threads` at all — a
    /// Calendar disconnect has no opt-in-thread table to clear (see this
    /// module's own doc comment).
    #[tokio::test]
    async fn calendar_account_wide_purge_removes_the_calendar_channel_and_leaves_gmail_threads_alone_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping calendar_account_wide_purge_removes_the_calendar_channel_and_leaves_gmail_threads_alone_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g25_calendar_accountwide";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let tenant = build_tenant_pool(&base, db, "org_g25_calendar_accountwide").await;

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();
        let graph_id = seed_graph(&tenant, account_id).await;

        seed_watch_channel(&tenant, account_id, "calendar", "primary", None).await;
        // Unrelated Gmail watch + thread for the same account — must survive
        // a Calendar-surface purge untouched.
        seed_watch_channel(&tenant, account_id, "gmail", "member@example.com", Some(member)).await;
        seed_watched_thread(&tenant, account_id, graph_id, member, "thread-member").await;

        let cutoff = db_now(&tenant).await;
        let outcome = purge_google_connection(
            &tenant,
            &tenant,
            account_id,
            Surface::Calendar,
            TokenOwner::AccountWide,
            cutoff,
            cutoff,
        )
        .await;

        assert_eq!(outcome.watch_channels_deleted, 1);
        assert_eq!(
            outcome.watched_threads_deleted, 0,
            "a Calendar purge must never touch gmail_watched_threads"
        );
        assert!(!outcome.purge_had_a_failure());

        let calendar_remaining: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND surface = 'calendar'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(calendar_remaining, 0);

        let gmail_channel_survives: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND surface = 'gmail'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(gmail_channel_survives, 1, "the unrelated Gmail watch channel must survive");
        assert_eq!(
            count(&tenant, "gmail_watched_threads", account_id).await,
            1,
            "gmail_watched_threads must be completely untouched by a Calendar purge"
        );

        tenant.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// The reconnect race this module's own doc comment describes
    /// (CodeRabbit round 1 on PR #690, Major, confirmed real): a disconnect's
    /// `purge_not_after` is captured BEFORE the purge runs; a row a
    /// concurrent reconnect's `ensure_watch`/`upsert_watched_thread` upsert
    /// refreshes AFTER that capture (simulated here by bumping
    /// `updated_at`/`last_ingested_at` to a deterministic future timestamp,
    /// exactly what those two upserts' own `ON CONFLICT DO UPDATE ...
    /// updated_at = now()` / `last_ingested_at = now()` clauses would
    /// produce for real) must survive the purge, while a genuinely stale
    /// sibling row (never refreshed, timestamp still `<= purge_not_after`)
    /// in the SAME purge call is still removed normally — proving the guard
    /// is surgical, not a blanket "purge does nothing near the cutoff"
    /// regression.
    #[tokio::test]
    async fn a_row_refreshed_by_a_concurrent_reconnect_after_purge_not_after_survives_the_purge_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_row_refreshed_by_a_concurrent_reconnect_after_purge_not_after_survives_the_purge_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g25_reconnect_race";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let tenant = build_tenant_pool(&base, db, "org_g25_reconnect_race").await;

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();
        let graph_id = seed_graph(&tenant, account_id).await;

        // The row a normal (no race) disconnect is responsible for — never
        // refreshed after the cutoff.
        seed_watch_channel(&tenant, account_id, "gmail", "stale@example.com", Some(member)).await;
        seed_watched_thread(&tenant, account_id, graph_id, member, "thread-stale").await;

        // The row that WOULD be raced — same shape, different resource id/
        // thread so both can be asserted on independently.
        seed_watch_channel(&tenant, account_id, "gmail", "reconnected@example.com", Some(member)).await;
        seed_watched_thread(&tenant, account_id, graph_id, member, "thread-reconnected").await;

        // The disconnect captures its cutoff here — before the reconnect's
        // refresh below happens (mirrors `routes::oauth::delete_connection`
        // capturing `purge_not_after` before running the purge).
        let purge_not_after = db_now(&tenant).await;

        // Simulate the concurrent reconnect's `ensure_watch`/
        // `upsert_watched_thread` upsert landing AFTER the cutoff was
        // captured but BEFORE the purge's own DELETE runs.
        bump_watch_channel_updated_at(&tenant, account_id, "gmail", "reconnected@example.com").await;
        bump_watched_thread_last_ingested_at(&tenant, account_id, "thread-reconnected").await;

        let outcome = purge_google_connection(
            &tenant,
            &tenant,
            account_id,
            Surface::Gmail,
            TokenOwner::Member(member),
            purge_not_after,
            purge_not_after,
        )
        .await;

        assert_eq!(outcome.watch_channels_deleted, 1, "only the stale channel, not the reconnected one");
        assert_eq!(outcome.watched_threads_deleted, 1, "only the stale thread, not the reconnected one");
        assert!(!outcome.purge_had_a_failure());

        let stale_channel_gone: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_watch_channels WHERE account_id = $1 AND resource_id = 'stale@example.com'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(stale_channel_gone, 0, "the genuinely stale channel must still be purged normally");

        let reconnected_channel_survives: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_watch_channels \
             WHERE account_id = $1 AND resource_id = 'reconnected@example.com'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(
            reconnected_channel_survives, 1,
            "a channel a concurrent reconnect just refreshed must survive the disconnect's purge"
        );

        let stale_thread_gone: i64 =
            sqlx::query_scalar("SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1 AND thread_id = 'thread-stale'")
                .bind(account_id)
                .fetch_one(&tenant)
                .await
                .unwrap();
        assert_eq!(stale_thread_gone, 0, "the genuinely stale thread must still be purged normally");

        let reconnected_thread_survives: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM gmail_watched_threads WHERE account_id = $1 AND thread_id = 'thread-reconnected'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert_eq!(
            reconnected_thread_survives, 1,
            "a thread a concurrent reconnect just re-touched must survive the disconnect's purge"
        );

        tenant.close().await;
        drop_db(&mut admin_conn, db).await;
    }
}

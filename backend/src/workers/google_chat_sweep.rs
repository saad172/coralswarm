//! Google Chat spaces sweep — backfill, catch-up, and roster reconcile
//! (gsuite Train G4 PR 4, `.scratch/gsuite-enterprise/g4-chat-design.md`
//! §4.4/§3.4/§3.5, §7's "G4.4" item 5).
//!
//! # Why this worker exists at all, and why it is on the TENANT loop
//!
//! Push (`routes::google_push`'s Chat arm) is the fast path, but it is only
//! a trigger and it can lapse: a Workspace Events subscription expires after
//! 7 days (§4.2's D5), a deploy window can drop deliveries, and a space
//! routed today has history from before it that no push will ever announce.
//! Three pull jobs close those gaps, and they share one pass:
//!
//! * **Backfill** — `spaces.messages.list` from the space's start,
//!   `orderBy=createTime ASC`, resumable on
//!   `google_chat_watched_spaces.backfill_cursor`, no page cap
//!   (`plan.md:86`), budgeted per tick. Armed by the destination pick
//!   (`connectors::google_chat::arm_backfill_for_space`).
//! * **Catch-up** — `spaces.spaceEvents.list` from the per-space watermark,
//!   floored at the API's 28-day retention (§2.7). This is what repairs a
//!   lapsed subscription, and it is why a lapsed-subscription alert is
//!   correctness rather than polish.
//! * **Roster reconcile** — re-walks `spaces.members.list` for spaces routed
//!   to a TEAM ocean and re-asserts seats (§3.4/D7). Grants generously,
//!   revokes narrowly: this pass only ever GRANTS, because *"an `Err` here
//!   means 'we do not know', and the only safe response to not knowing is to
//!   change nothing"* (`workers::slack_seat_reconcile`'s rule). Revocation
//!   happens exclusively on an explicit `membership.v1.deleted` push, where
//!   we actually know somebody left.
//!
//! **Tenant loop, not the consumer loop.** `CONNECTOR_CONTRACT.md` §5.5 is
//! explicit that a pull sweep registered on the single consumer loop
//! *silently never runs for org tenants* — the failure mode `workers::
//! graph_subscriptions` had to be converted away from in m365 Train 2 PR
//! 2.2. Chat starts tenant-native, like `workers::google_watch_renewal` and
//! `workers::gmail_everything_mode` before it. D1 keeps the org GATE closed
//! until G4.6, so `tick_org` finds nothing in production today — but the
//! registration and the code path are real, so G4.6 is a gate flip rather
//! than a worker rewrite (§3.8).
//!
//! # Consumer-plane discovery uses `accounts`, NOT a new SECURITY DEFINER
//!
//! `google_chat_watched_spaces` carries `account_isolation` RLS, so an
//! unscoped cross-account read under the least-privilege `coralswarm_app`
//! role returns zero rows. The sibling everything-mode workers solve this
//! with a `SECURITY DEFINER` bootstrap function (`resolve_gmail_everything_
//! mode_due()`, migration `0197`) because they additionally need to CLAIM
//! rows atomically across accounts. This sweep does not claim anything — it
//! only discovers — so it uses the tree's other established consumer-plane
//! enumeration instead: `SELECT id FROM accounts ORDER BY id` (no RLS on
//! `accounts`; the same read `workers::atom_title_backfill`,
//! `workers::content_tsv_backfill`, `workers::embed_retry`,
//! `workers::journal_sweep` and `workers::billing_derivation` all already
//! do), followed by one properly-scoped per-account probe. **That is why
//! this PR needs no migration** — see the PR body.
//!
//! # Serialisation with the push path
//!
//! Both paths take the SAME per-space `pg_try_advisory_xact_lock`
//! ([`space_lock_key`]), try-and-skip: two concurrent passes over one space
//! would run two `spaceEvents.list` diffs from the same watermark and then
//! race to commit it (duplicate ingest at best, a REGRESSED watermark at
//! worst). The second caller returns immediately rather than blocking —
//! there is nothing to wait FOR, since the next push or tick re-triggers.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::{google_chat, ConnectorError, GoogleConnector, TokenOwner};
use crate::routes::connectors::load_creds_bg;
use crate::routes::oauth::store_token_bundle;
use crate::routes::slack::AccountPlane;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// Sweep cadence. Design §4.4: *"Cadence hourly is ample"* for catch-up;
/// 30 minutes matches every sibling Google/Microsoft sweep in this tree
/// (`workers::gmail_everything_mode::POLL_INTERVAL`,
/// `workers::google_watch_renewal::POLL_INTERVAL`) and there is no reason
/// for these clocks to differ.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// Bounded worst case per tick, across every account this tenant covers —
/// same reasoning as `workers::gmail_everything_mode::OWNERS_PER_TICK`.
///
/// **Corrected in review round 1 (H3).** This doc used to claim the ordering
/// in `google_chat::due_spaces_for_account` made the budget "self-rotating
/// with no cursor state to keep". That was true only of SUCCESSFUL passes:
/// the key was `last_ingested_at`, which advances only on success, so a
/// permanently-failing space sorted first forever and pinned a slot. Three
/// things make the claim true now — `google_chat::mark_space_attempted`
/// stamps the rotation key on every attempt, [`SPACES_PER_ACCOUNT`] is 25
/// rather than 5, and [`CONSUMER_ROTATION`] rotates which accounts get
/// probed when this budget runs out.
const SPACES_PER_TICK: usize = 10;

/// Per-account cap fed to `due_spaces_for_account`.
///
/// Raised 5 → 25 in review round 1 (H3), matching the identical raise #722
/// made to `workers::gmail_everything_mode` for the identical reason: with a
/// cap of 5, five permanently-failing spaces filled every slot an account
/// ever got and no healthy sibling was reached. The rotation fix
/// (`google_chat::mark_space_attempted`) is the primary defence; a larger
/// window is the belt to its braces.
const SPACES_PER_ACCOUNT: i64 = 25;

/// How many distinct readers one pass will try before giving up on a space
/// (security review, L2). Bounded so a large account cannot turn one
/// unreadable space into a full membership walk every tick; the next tick
/// resumes from a fresh election, so it still converges.
const MAX_READER_CANDIDATES_PER_PASS: usize = 5;

/// Whether this pass should also re-walk the space's roster.
///
/// **Push passes must not** (review round 1, M4): `sync_space_once` is
/// invoked for EVERY `message.v1.created`, and an unconditional reconcile
/// meant every single Chat message triggered a full `spaces.members.list`
/// walk (up to `MEMBERS_MAX_PAGES` × `LIST_PAGE_SIZE`) plus identity
/// resolution and a seat write, on the push-ack path. Design §4.4 puts the
/// reconcile on the 30-minute sweep, and a genuine membership change already
/// triggers one through `routes::google_push::handle_chat_membership_event`
/// — so the amplification bought nothing and ran straight into design risk
/// 13 (Chat quotas are per-project AND per-user).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RosterReconcile {
    Yes,
    No,
}

/// Round-robin start offset for consumer-plane account discovery (review
/// round 1, H3).
///
/// `tick_consumer` walks `SELECT id FROM accounts ORDER BY id` and `break`s
/// when the per-tick budget is spent. With a FIXED start that is a
/// starvation cliff: ten stuck spaces on the two lowest-sorting accounts
/// exhausted the budget every tick and no higher-sorting account was ever
/// probed again — the whole consumer fleet silenced by ten rows. Advancing a
/// start offset each tick guarantees every account is reached within
/// ceil(N / accounts-consumed-per-tick) ticks regardless of what the
/// unlucky-first accounts are doing. Process-local and best-effort by
/// design: it is a fairness nudge, not a durable cursor, and losing it on
/// restart costs nothing.
static CONSUMER_ROTATION: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The account order for one consumer tick: `accounts` rotated left by
/// `start`. Pure, so the fairness property is testable without a database.
fn rotated_accounts(accounts: &[Uuid], start: usize) -> Vec<Uuid> {
    if accounts.is_empty() {
        return Vec::new();
    }
    let n = accounts.len();
    let start = start % n;
    accounts[start..].iter().chain(accounts[..start].iter()).copied().collect()
}

/// The advisory-lock key for one space. Shared verbatim with
/// `routes::google_push`'s Chat arm — a different literal in the two places
/// would silently mean no serialisation at all.
pub(crate) fn space_lock_key(account_id: Uuid, space_id: &str) -> String {
    format!("google_chat_sync:{account_id}:{space_id}")
}

/// Identical resolution to `workers::gmail_everything_mode::
/// resolve_tenant_account` — kept as its own copy per those modules' own doc
/// comments (duplicating one two-line query is cheaper than a shared
/// dependency between independent connector workers).
async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

/// The reader's live access token for one space, with §3.5's failover
/// re-election folded in: if the row's designated `reader_user_id` has no
/// usable `google_chat` credential any more, elect another connected member
/// of the same account, PERSIST that election, and use them.
///
/// Returns `None` when nobody is left — the *"nobody left"* end state
/// (§3.5): not an error, and deliberately NOT a terminal ledger row (§1.10
/// lesson 4 — the condition is ours to fix and fully reversible the moment
/// anybody reconnects).
async fn reader_credentials(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    space: &google_chat::IngestableSpace,
) -> Result<Option<(Uuid, String)>, ConnectorError> {
    // Every reader tried in THIS pass, so a failover walks the candidate
    // list instead of re-electing the same broken one (security review, L2).
    // The election orders by `updated_at DESC`, and `store_token_bundle`
    // only runs on SUCCESS — so a failing token's sort position never
    // changes, and a single-exclusion failover re-picked the same two
    // most-recent-but-broken members forever while logging
    // `connector_health = true` "nobody left", with a working third member
    // sitting right behind them.
    let mut tried: Vec<Uuid> = Vec::new();
    // The STORED reader is held to the same membership standard as a newly
    // elected one (security review round 2). `elect_space_reader` gates on
    // `account_members`, but this path reached it only when the row had no
    // reader OR the stored reader's refresh FAILED — so an offboarded member
    // whose refresh token still worked was used indefinitely, and the
    // `store_token_bundle` call below re-minted and re-persisted an access
    // token in an ex-member's name on every tick. Offboarding deletes the
    // `account_members` row and deliberately never touches `oauth_tokens`
    // (`clerk_provision::remove_org_membership`), which is precisely the
    // state this check detects. It fails CLOSED: an `Err` propagates and
    // abandons the pass rather than assuming membership.
    let mut candidate = match space.reader_user_id {
        Some(uid) if google_chat::is_current_account_member(control_pool, account_id, uid).await? => Some(uid),
        Some(uid) => {
            tracing::info!(
                %account_id, ex_member_user_id = %uid, space_id = %space.space_id,
                "google_chat sweep: the designated reader is no longer a member of this account \
                 — their credential will NOT be used and a current member is elected instead"
            );
            // Excluded explicitly as well as by the election's own gate, so
            // the invariant does not depend on a second query's WHERE clause.
            tried.push(uid);
            google_chat::elect_space_reader(control_pool, account_id, &tried).await?
        }
        None => google_chat::elect_space_reader(control_pool, account_id, &tried).await?,
    };
    // The `oauth_tokens` ROW is on `control_pool`; the KEY is on whichever
    // plane THE decision table (`crypto::DekPlane::resolve_for_org`) names for
    // this account — control for a personal account, the org's own tenant
    // `account_keys` for an org one. Resolved once per space, outside the
    // candidate walk below, so a re-election never re-pays for it.
    //
    // Deliberately placed AFTER the membership gate above and BEFORE the walk
    // (the #743/#775 merge point). The two are orthogonal and both are
    // required: `is_current_account_member` decides WHETHER a credential may
    // be used, this decides WHICH DATABASE its key comes from. Ordering them
    // this way means the fail-closed `?` on membership abandons the pass
    // before any key is resolved, so an ex-member's row never reaches a
    // decrypt; it also keeps the resolution off the per-candidate path.
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;

    for _ in 0..MAX_READER_CANDIDATES_PER_PASS {
        let Some(user_id) = candidate else {
            break;
        };
        tried.push(user_id);
        // NOT `.unwrap_or(None)`. `load_creds_bg` maps a scoped-transaction
        // failure, a query failure, a missing DEK and a decrypt failure ALL
        // into `ConnectorError::Internal` (`routes::connectors`), so
        // swallowing the `Err` would make one control-plane blip
        // indistinguishable from "this member has no `google_chat`
        // credential" — and this loop would then RE-ELECT a reader and
        // PERSIST that election for a space whose reader was fine, ending on
        // the next pass at `set_space_reader(.., None)` plus a false
        // "nobody is left" `connector_health` alert. This module's own rule
        // at the top applies: an `Err` means *we do not know*, and the only
        // safe response to not knowing is to change nothing.
        let creds = load_creds_bg(control_pool, &dek_plane, account_id, "google_chat", TokenOwner::Member(user_id))
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "reader_credentials: could not load the google_chat credential for member \
                     {user_id} of account {account_id} (space {space_id}) — abandoning the pass \
                     with the reader election and every cursor unchanged: {e}",
                    space_id = space.space_id
                ))
            })?;
        if let Some(mut creds) = creds {
            let google = GoogleConnector;
            match google.refresh_if_needed(&state.http_client, &mut creds).await {
                Ok(()) => {
                    if let Some(refreshed) = creds.refreshed.as_ref() {
                        if let Err(e) =
                            store_token_bundle(control_pool, &dek_plane, account_id, "google_chat", TokenOwner::Member(user_id), refreshed)
                                .await
                        {
                            tracing::warn!(
                                %account_id, %user_id, status = ?e.status(),
                                "google_chat sweep: failed to persist a refreshed token (non-fatal)"
                            );
                        }
                    }
                    if space.reader_user_id != Some(user_id) {
                        google_chat::set_space_reader(content_pool, account_id, &space.space_id, Some(user_id)).await?;
                    }
                    return Ok(Some((user_id, creds.access_token)));
                }
                Err(e) => tracing::info!(
                    error = %e, %account_id, %user_id, space_id = %space.space_id,
                    "google_chat sweep: designated reader's token could not be refreshed — re-electing"
                ),
            }
        }

        candidate = google_chat::elect_space_reader(control_pool, account_id, &tried).await?;
    }

    match classify_reader_walk(candidate) {
        ReaderWalkOutcome::BudgetSpent => {
            // We ran out of BUDGET, not out of members: the walk stopped with
            // an untried candidate still queued, so nothing has been
            // established about whether anybody can read this space — and
            // this module's rule for "we do not know" is to change nothing.
            // The stored reader is left alone (clearing it would discard a
            // real election on no evidence) and NO `connector_health` alert
            // fires, because the "nobody left" condition has not been
            // demonstrated.
            //
            // Honest about the residual: `elect_space_reader` orders by
            // `updated_at DESC` and `tried` resets every pass, so an account
            // whose first `MAX_READER_CANDIDATES_PER_PASS` google_chat tokens
            // are all broken re-walks that same prefix every tick and never
            // reaches candidate N+1. That is a pre-existing bound of a
            // stateless election — closing it needs a durable rotation
            // cursor, the shape `CONSUMER_ROTATION` uses for accounts — and
            // it is NOT what this arm claims to fix. What this arm fixes is
            // the reporting: the condition is logged as what it is instead of
            // paging an operator about an end state nobody reached, and the
            // reader/destination/cursors survive it untouched.
            tracing::info!(
                %account_id, space_id = %space.space_id, candidates_tried = tried.len(),
                "google_chat: the per-pass reader-candidate budget was spent before every \
                 eligible member had been tried — the designated reader, the destination pick \
                 and every cursor are left exactly as they were, and the next tick tries again"
            );
            Ok(None)
        }
        ReaderWalkOutcome::NobodyLeft => {
            google_chat::set_space_reader(content_pool, account_id, &space.space_id, None).await?;
            tracing::warn!(
                %account_id, space_id = %space.space_id, candidates_tried = tried.len(),
                connector_health = true,
                "google_chat: no connected member is left who can read this space — ingest is \
                 paused for it. The destination pick and every cursor are preserved; ingest \
                 resumes the moment any member of the space reconnects Google Chat."
            );
            Ok(None)
        }
    }
}

/// What a finished reader-candidate walk actually ESTABLISHED.
///
/// Conflating these two is review round 2's finding 3: both exits of
/// [`reader_credentials`]' loop used to fall through to
/// `set_space_reader(.., None)` plus a `connector_health = true` *"no
/// connected member is left"* alert. An account whose first
/// `MAX_READER_CANDIDATES_PER_PASS` candidates happened to have stale tokens
/// therefore had its designated reader wiped and paged an operator about an
/// end state that had never been reached — while a working member sat
/// untried at position N+1. The old code even said so in a comment and
/// shipped anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderWalkOutcome {
    /// The election itself returned nobody, so every eligible member has been
    /// tried. "Nobody left" is ESTABLISHED and may be acted on.
    NobodyLeft,
    /// The per-pass budget ran out with a candidate still queued. Nothing is
    /// established: change nothing, alert nobody.
    BudgetSpent,
}

/// Classify the walk from the candidate left over at loop exit.
///
/// `None` is the conclusive case and it covers BOTH conclusive shapes — the
/// `break` (the election returned nobody mid-walk) and the last iteration's
/// re-election also returning nobody. That second shape is why this reads
/// the leftover candidate rather than a "did we break?" flag: a flag would
/// misclassify a walk that spent its budget AND exhausted the roster on the
/// same iteration as inconclusive, and suppress a real alert.
fn classify_reader_walk(candidate_at_exit: Option<Uuid>) -> ReaderWalkOutcome {
    match candidate_at_exit {
        None => ReaderWalkOutcome::NobodyLeft,
        Some(_) => ReaderWalkOutcome::BudgetSpent,
    }
}

/// Run one bounded pass over ONE space: backfill-or-catch-up, ingest,
/// commit, then (for a team-destined space) reconcile its roster.
///
/// Returns the number of atoms written. `Ok(0)` covers every "nothing to do"
/// case — the advisory lock was already held, nobody can read the space, the
/// pass found nothing.
///
/// **The commit-after-ingest split is the whole point.** `fetch_pending`
/// deliberately does not persist a cursor; this function commits one ONLY
/// when every destination and every item landed
/// ([`google_chat::WriteOutcome::fully_succeeded`]). A failure therefore
/// leaves the cursor exactly where it was and the next pass re-walks the
/// same range — idempotent by construction, because both the atom write
/// (`ON CONFLICT (graph_id, source_type, source_id) DO NOTHING`) and the
/// deletion (`delete_source`) are.
///
/// `Ok(None)` means the pass DID NOT RUN — the per-space lock was already
/// held, nobody can read the space, or it resolved to no destination. That is
/// distinct from `Ok(Some(0))`, a pass that really ran and had nothing to
/// write (a pure-deletion or fully-filtered range, whose cursor DID advance).
/// `sweep_account` counts only the latter, so a tenant whose every space is
/// locked or unreadable reports `Tick::idle` and keeps its idle backoff
/// instead of being rescheduled at `base_poll` forever.
pub(crate) async fn sync_space_once(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    tenant: &TenantId,
    account_id: Uuid,
    space: &google_chat::IngestableSpace,
    reconcile_roster: RosterReconcile,
    directory_cache: &mut google_chat::DomainDirectoryCache,
) -> Result<Option<usize>, ConnectorError> {
    let plane = AccountPlane::for_tenant(tenant);

    // Per-space serialisation, try-and-skip. Transaction-scoped so it
    // auto-releases on commit/rollback/early-return/panic alike — no manual
    // unlock to forget (`routes::google_push::run_gmail_sync_for_email`'s
    // own reasoning for the per-mailbox twin).
    //
    // DELIBERATELY still `_xact_` rather than a session-level
    // `pg_try_advisory_lock` on a dedicated connection, even though this
    // transaction stays open across the pass's Google calls:
    //
    // * It holds NO snapshot, so it does not hold back `VACUUM`. The
    //   transaction is READ COMMITTED and performs no write, so between
    //   statements it reports `backend_xmin = NULL` in `pg_stat_activity` —
    //   measured, not assumed, against this tree's own test container.
    // * A session-level lock on a POOLED connection is the genuinely unsafe
    //   option: sqlx returns the connection to the pool without resetting
    //   session state, so a task that dies between lock and unlock leaks the
    //   lock into the pool and blocks that space forever. Transaction scope
    //   makes that untypeable.
    //
    // What it does cost is one pooled connection for the pass's duration,
    // which is why the pass is bounded (`SYNC_PASS_BUDGET`,
    // `SPACES_PER_TICK`) rather than run to completion.
    let mut lock_tx = content_pool
        .begin()
        .await
        .map_err(|e| ConnectorError::Internal(format!("chat sync lock: begin failed: {e}")))?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1))")
        .bind(space_lock_key(account_id, &space.space_id))
        .fetch_one(&mut *lock_tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("chat sync lock: acquire failed: {e}")))?;
    if !acquired {
        tracing::info!(
            %account_id, space_id = %space.space_id,
            "google_chat: a sync is already in flight for this space — skipping (the next push \
             or sweep tick re-triggers)"
        );
        lock_tx.rollback().await.ok();
        return Ok(None);
    }

    // The rotation key advances on EVERY attempt, not only on success
    // (review round 1, H3) — stamped here, inside the lock and before any
    // fallible work, so a space that fails every pass still rotates to the
    // back of its account's queue instead of pinning a slot forever.
    if let Err(e) = google_chat::mark_space_attempted(content_pool, account_id, &space.space_id, None).await {
        tracing::warn!(
            error = %e, %account_id, space_id = %space.space_id,
            "google_chat: could not stamp the sweep-rotation key for this space — continuing, but              a repeatedly-failing space may be re-picked ahead of its siblings"
        );
    }

    let result =
        sync_space_locked(
            state,
            control_pool,
            content_pool,
            tenant,
            plane,
            account_id,
            space,
            reconcile_roster,
            directory_cache,
        )
        .await;
    lock_tx.rollback().await.ok();
    result
}

#[allow(clippy::too_many_arguments)]
async fn sync_space_locked(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    tenant: &TenantId,
    plane: AccountPlane,
    account_id: Uuid,
    space: &google_chat::IngestableSpace,
    reconcile_roster: RosterReconcile,
    directory_cache: &mut google_chat::DomainDirectoryCache,
) -> Result<Option<usize>, ConnectorError> {
    if let TenantId::Org(org_id) = tenant {
        if !crate::routes::oauth::org_google_chat_enabled_for(state, org_id).await { return Ok(None); }
    }
    let Some((reader_user_id, access_token)) =
        reader_credentials(state, control_pool, content_pool, account_id, space).await?
    else {
        return Ok(None);
    };

    // ONE rate-limit scope per logical operation (`graph_rate_limit`'s own
    // rule at `:338`-`:343`) — Chat quotas are per-project AND per-user, so
    // the key is per account, matching `renew_one`'s own
    // `google_chat:{account_id}`.
    let key = format!("google_chat:{account_id}");
    let http = state.http_client.clone();

    // The space's route, as an ingest grant: `single` names its ocean,
    // `fanout` resolves each mapped member's own ocean (Org plane only). A
    // fan-out that resolves to ZERO oceans is *retryable, nothing committed*
    // below — §1.10 lesson 4 (BLOCKER 4): an infrastructure gap must never
    // write a terminal row, because `ON CONFLICT DO NOTHING` would make it
    // permanent and *"silently suppress every org [space] seen between today
    // and that PR, forever."* The warn text is deliberately CAUSE-NEUTRAL with
    // `unmapped_members` as the discriminator (`slack_events.rs:5059`-
    // `:5075`), because the real incident it comes from was *"an org could
    // capture nothing for days while every dashboard stayed green."*
    let grant = crate::ingest::OrgSharedGrant::google_chat_space(
        control_pool, content_pool, tenant, account_id, plane, &space.space_id, None,
    )
    .await
    .map_err(ConnectorError::Internal)?;
    if let Some((grant, unmapped_members)) = &grant {
        if grant.oceans().is_empty() {
            tracing::warn!(
                %account_id, space_id = %space.space_id, unmapped_members,
                connector_health = true,
                "google_chat: a fan-out space resolved to no destination oceans this pass — \
                 nothing ingested, nothing committed, the next tick retries"
            );
        }
    }
    let graphs: &[Uuid] = grant.as_ref().map_or(&[], |(grant, _)| grant.oceans());

    // Self-heal the member's subscription on every pass — `ensure_watch`'s
    // dual-call-site shape (`g2-design.md:30`), so a member who connected
    // before this PR, or whose subscription lapsed, is repaired by ordinary
    // traffic rather than by a backfill script. Best-effort: a failure here
    // must not stop the pull half, which is precisely the half that exists
    // to cover a missing subscription.
    if let Some(sub) = member_sub(control_pool, account_id, reader_user_id).await? {
        if let Err(e) = crate::graph_rate_limit::scoped(
            key.clone(),
            google_chat::ensure_subscription(&http, control_pool, account_id, reader_user_id, &sub, &access_token),
        )
        .await
        {
            tracing::warn!(
                error = %e, %account_id, %reader_user_id,
                "google_chat: ensure_subscription failed during a sweep pass (non-fatal — the \
                 pull half below still runs, and the next tick retries)"
            );
        }
    }

    // The directory for `<users/{id}>` mention substitution (§3.7 rule 2).
    //
    // A FAILED read is not the same thing as an absent identity row, and the
    // difference is PERMANENT. `fetch_pending` substitutes against whatever
    // map it is handed, and a pass that then succeeds COMMITS the cursor
    // below — so an empty map produced by a one-tick DB error writes
    // `@unknown` into that range's atom text forever, because the range is
    // never re-fetched. `unwrap_or_default()` erases exactly that
    // distinction. On `Err` do what the `graphs.is_empty()` branch below
    // does: change nothing, commit nothing, let the next tick re-walk.
    let directory = match mention_directory(control_pool, content_pool, account_id).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id, space_id = %space.space_id, connector_health = true,
                "google_chat: the mention directory could not be read this pass — nothing was \
                 ingested and the space's cursor was NOT advanced, so the next tick re-fetches \
                 the same range. Ingesting now would bake `@unknown` into this range's atom \
                 text permanently, because Chat never re-reports a range we have already read."
            );
            return Ok(None);
        }
    };

    let own_bot = std::env::var("GOOGLE_CHAT_APP_USER_ID").unwrap_or_default();
    if own_bot.is_empty() {
        // Once per process, not once per pass, and deliberately NOT a
        // `connector_health` warn: `.env.example` documents unset as the
        // safe current default, because the app posts nothing today (G4.5+
        // is what changes that). Raising it to an alert now would be a
        // standing false alarm on every deployment. What this line buys is
        // that the day the app DOES start posting, the reason its own
        // messages are being ingested back into the ocean they came from is
        // already in the log rather than something to rediscover.
        static NOTED_NO_APP_USER_ID: std::sync::Once = std::sync::Once::new();
        NOTED_NO_APP_USER_ID.call_once(|| {
            tracing::info!(
                "google_chat: GOOGLE_CHAT_APP_USER_ID is unset, so the ingest funnel cannot \
                 recognise our own Chat app as a sender. Harmless while the app posts nothing; \
                 set it (see .env.example) before the app ever posts, or its own messages will \
                 be ingested as ordinary space content."
            );
        });
    }

    let pending = crate::graph_rate_limit::scoped(
        key,
        google_chat::fetch_pending(
            &http,
            content_pool,
            account_id,
            &space.space_id,
            None,
            &access_token,
            &own_bot,
            directory.as_map(),
            graphs.first().copied(),
        ),
    )
    .await?;

    // §2.2/§3.4b/risk 6: a `HISTORY_OFF` space keeps only 24 hours of
    // history UPSTREAM, by Google. Nothing breaks — the C6 promise is
    // simply vacuous for that content — but contract §4's C6 row requires
    // saying so *"so nobody debugs an empty backfill"*, and Chat is the
    // first surface where that is a PER-CONTAINER fact rather than a
    // per-connector one. Logged at the pass that would otherwise look
    // broken, not only in the docs.
    if space.history_state == "HISTORY_OFF" && !space.backfill_complete {
        tracing::info!(
            %account_id, space_id = %space.space_id,
            "google_chat: backfilling a HISTORY_OFF space — Google retains only 24 hours of its \
             messages upstream, so a near-empty backfill here is expected and correct, not a \
             failure"
        );
    }

    if pending.filtered > 0 {
        tracing::info!(
            %account_id, space_id = %space.space_id, filtered = pending.filtered,
            "google_chat: messages filtered at the ingest funnel (poison pills — empty body, \
             system message, our own bot, or an unparseable createTime)"
        );
    }

    if pending.items.is_empty() {
        // Nothing to write. A pure-deletion or empty pass still banks its
        // cursor — the deletions were already applied durably inside
        // `fetch_pending`, and a backfill page that yielded only filtered
        // messages must still advance or it would re-walk forever.
        google_chat::commit_pending(content_pool, account_id, &space.space_id, None, pending.commit).await?;
        maybe_reconcile_space_roster(
            state,
            control_pool,
            content_pool,
            account_id,
            reader_user_id,
            space,
            &access_token,
            reconcile_roster,
            directory_cache,
        )
            .await;
        // `Some(0)`, not `None`: this pass genuinely ran and banked a cursor.
        return Ok(Some(0));
    }

    let Some((grant, _)) = grant.as_ref().filter(|_| !graphs.is_empty()) else {
        // Retryable, and deliberately nothing committed (§1.10 lesson 4 /
        // BLOCKER 4): an infrastructure gap must never write a terminal row,
        // because `ON CONFLICT DO NOTHING` would make it permanent and
        // silently suppress every space seen between today and the fix.
        //
        // And it must be ALERTABLE. This exact shape — a `graphs.is_empty()`
        // fan-out branch that drops user content with no
        // `connector_health = true` field — cost testorg several hours of
        // completely silent data loss: job `done`, no error, 200s
        // everywhere, zero atoms. The warn text is deliberately
        // CAUSE-NEUTRAL with `items_dropped` as the discriminator
        // (`slack_events.rs:5059`-`:5075`), because the real incident was
        // "an org could capture nothing for days while every dashboard
        // stayed green."
        tracing::warn!(
            %account_id,
            space_id = %space.space_id,
            items_dropped = pending.items.len(),
            mode = space.mode.as_deref().unwrap_or("unrouted"),
            connector_health = true,
            "google_chat: this space resolved to NO destination ocean this pass — its messages \
             were not ingested and its cursor was NOT advanced, so the next tick re-fetches the \
             same range. Nothing is lost yet, but nothing is landing either."
        );
        return Ok(None);
    };

    let write = google_chat::ingest_space_items(
        state,
        tenant,
        content_pool,
        grant,
        &space.display_name,
        Some(reader_user_id),
        &pending.items,
    )
    .await;

    if write.fully_succeeded() {
        google_chat::commit_pending(content_pool, account_id, &space.space_id, None, pending.commit).await?;
    } else {
        tracing::warn!(
            %account_id, space_id = %space.space_id,
            atoms = write.atoms_written,
            destinations_failed = write.destinations_failed,
            items_failed = write.items_failed,
            "google_chat: one or more writes failed — the space's cursor is left unchanged and \
             the next pass re-fetches this range"
        );
    }

    maybe_reconcile_space_roster(
        state,
        control_pool,
        content_pool,
        account_id,
        reader_user_id,
        space,
        &access_token,
        reconcile_roster,
        directory_cache,
    )
        .await;
    Ok(Some(write.atoms_written))
}

// ---------------------------------------------------------------------------
// G4.5 — the MAILBOX archetype (`GROUP_CHAT`/`DIRECT_MESSAGE`) sweep pass.
// See `connectors::google_chat`'s "G4.5 — the MAILBOX archetype" section for
// the storage/write side; this is the pull side, reusing the SAME
// `fetch_pending`/`commit_pending`/`ingest_space_items` funnel G4.4 built for
// the container archetype above. Three archetype differences, each called
// out at its own divergence point below:
//   * the reader is ALWAYS the owner — no election, no failover, because
//     borrowing a colleague's token to read someone else's DM/group would
//     defeat the "no fan-out ever" guarantee this archetype exists to
//     provide (contract §7's Mailbox archetype);
//   * there is exactly one destination — the owner's own private ocean —
//     never a fan-out resolution;
//   * no roster reconcile / auto-seating — a private ocean has no seats to
//     grant.
// ---------------------------------------------------------------------------

/// The advisory-lock key for one mailbox opt-in. Deliberately OWNER-scoped
/// (unlike [`space_lock_key`]'s account+space scope): two different members
/// may independently opt the SAME physical space into their own oceans
/// (`uq_google_chat_watched_spaces_mailbox`, migration `0198`), and their
/// passes must not serialise against each other — each owns a wholly
/// separate cursor and a wholly separate destination.
pub(crate) fn mailbox_space_lock_key(account_id: Uuid, space_id: &str, owner_user_id: Uuid) -> String {
    format!("google_chat_mailbox_sync:{account_id}:{space_id}:{owner_user_id}")
}

/// Run one bounded pass over ONE mailbox opt-in. Same lock-then-mark-then-run
/// shape as [`sync_space_once`], same `Ok(None)` (did not run) vs
/// `Ok(Some(n))` (ran, wrote `n` atoms) contract.
pub(crate) async fn sync_mailbox_space_once(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    tenant: &TenantId,
    account_id: Uuid,
    owner_user_id: Uuid,
    space: &google_chat::IngestableSpace,
) -> Result<Option<usize>, ConnectorError> {
    let mut lock_tx = content_pool
        .begin()
        .await
        .map_err(|e| ConnectorError::Internal(format!("chat mailbox sync lock: begin failed: {e}")))?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1))")
        .bind(mailbox_space_lock_key(account_id, &space.space_id, owner_user_id))
        .fetch_one(&mut *lock_tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("chat mailbox sync lock: acquire failed: {e}")))?;
    if !acquired {
        tracing::info!(
            %account_id, %owner_user_id, space_id = %space.space_id,
            "google_chat: a mailbox sync is already in flight for this opt-in — skipping (the \
             next push or sweep tick re-triggers)"
        );
        lock_tx.rollback().await.ok();
        return Ok(None);
    }

    if let Err(e) = google_chat::mark_space_attempted(content_pool, account_id, &space.space_id, Some(owner_user_id)).await {
        tracing::warn!(
            error = %e, %account_id, %owner_user_id, space_id = %space.space_id,
            "google_chat: could not stamp the sweep-rotation key for this mailbox opt-in — \
             continuing, but a repeatedly-failing opt-in may be re-picked ahead of its siblings"
        );
    }

    let result = sync_mailbox_space_locked(state, control_pool, content_pool, tenant, account_id, owner_user_id, space).await;
    lock_tx.rollback().await.ok();
    result
}

pub(crate) async fn mailbox_history_allowed(control_pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Result<bool, ConnectorError> {
    let mut gate = crate::tenancy::scope::begin_account_scoped(control_pool, account_id).await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    let allowed: bool = sqlx::query_scalar("SELECT COALESCE((SELECT enabled AND paused_at IS NULL FROM connector_sources WHERE account_id = $1 AND owner_user_id = $2 AND provider = 'google' AND source = 'chat'), true) AND NOT EXISTS (SELECT 1 FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2 AND provider = 'google_chat' AND credential_invalid_at IS NOT NULL)")
        .bind(account_id).bind(owner_user_id).fetch_one(&mut *gate).await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    gate.rollback().await.ok();
    Ok(allowed)
}

pub(crate) async fn mailbox_capture_gate(
    control_pool: &PgPool, account_id: Uuid, owner_user_id: Uuid,
) -> Result<Option<sqlx::Transaction<'_, sqlx::Postgres>>, ConnectorError> {
    let mut capture_gate = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
    // Lock credentials first, matching disconnect's deletion order.
    let credential: Option<bool> = sqlx::query_scalar("SELECT credential_invalid_at IS NULL FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2 AND provider = 'google_chat' FOR SHARE")
        .bind(account_id).bind(owner_user_id).fetch_optional(&mut *capture_gate).await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    if credential != Some(true) { return Ok(None); }
    // Legacy opt-ins may predate catalog source rows. Materialize their existing
    // enabled default so a concurrent pause upsert has a row to synchronize on.
    sqlx::query("INSERT INTO connector_sources(account_id, owner_user_id, provider, source, enabled) VALUES ($1,$2,'google','chat',true) ON CONFLICT DO NOTHING")
        .bind(account_id).bind(owner_user_id).execute(&mut *capture_gate).await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    let source: Option<bool> = sqlx::query_scalar("SELECT enabled AND paused_at IS NULL FROM connector_sources WHERE account_id = $1 AND owner_user_id = $2 AND provider = 'google' AND source = 'chat' FOR SHARE")
        .bind(account_id).bind(owner_user_id).fetch_optional(&mut *capture_gate).await
        .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    if source != Some(true) { return Ok(None); }
    Ok(Some(capture_gate))
}

async fn sync_mailbox_space_locked(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    tenant: &TenantId,
    account_id: Uuid,
    owner_user_id: Uuid,
    space: &google_chat::IngestableSpace,
) -> Result<Option<usize>, ConnectorError> {
    if !mailbox_history_allowed(control_pool, account_id, owner_user_id).await? { return Ok(None); }
    if let TenantId::Org(org_id) = tenant {
        if !crate::routes::oauth::org_google_chat_enabled_for(state, org_id).await { return Ok(None); }
    }
    if !google_chat::is_current_account_member(control_pool, account_id, owner_user_id).await? { return Ok(None); }
    // The member's pause or off switch for Chat, before any Google call or write.
    // The space's watermark stays, so a resumed source catches up from it.
    if crate::connectors::catalog::pause::is_paused(
        control_pool, account_id, owner_user_id, crate::connectors::catalog::pause::GOOGLE_CHAT,
    ).await {
        return Ok(None);
    }
    // The opt-in's route, as an ingest grant: exactly one destination, the
    // owner's own private ocean. The table's archetype CHECK (0198) makes a
    // routeless or fan-out mailbox row unwritable; this defends against that
    // invariant somehow not holding rather than trusting it silently.
    let plane = AccountPlane::for_tenant(tenant);
    let route = crate::ingest::OrgSharedGrant::google_chat_space(
        control_pool, content_pool, tenant, account_id, plane, &space.space_id, Some(owner_user_id),
    )
    .await
    .map_err(ConnectorError::Internal)?;
    let Some((grant, _)) = route.filter(|(grant, _)| grant.oceans().len() == 1) else {
        tracing::warn!(
            %account_id, %owner_user_id, space_id = %space.space_id,
            "google_chat: a mailbox opt-in with no single destination — should be unreachable \
             under the table's own archetype CHECK constraint; skipping rather than guessing one"
        );
        return Ok(None);
    };
    let graph_id = grant.oceans()[0];

    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;

    // NOT `.unwrap_or(None)` / `.unwrap_or(false)` — an `Err` here means *we
    // do not know*, and the only safe response is to change nothing this
    // pass, same rule `reader_credentials` documents for the container path.
    let Some(mut creds) = load_creds_bg(control_pool, &dek_plane, account_id, "google_chat", TokenOwner::Member(owner_user_id))
        .await
        .map_err(|e| {
            ConnectorError::Internal(format!(
                "sync_mailbox_space_locked: could not load the google_chat credential for owner \
                 {owner_user_id} of account {account_id} (space {space_id}) — abandoning the \
                 pass with every cursor unchanged: {e}",
                space_id = space.space_id
            ))
        })?
    else {
        // The owner disconnected Google Chat entirely. Unlike the container
        // archetype there is NO failover candidate — this mailbox is that
        // one person's own opt-in — so ingest simply pauses until they
        // reconnect. The destination and every cursor are left untouched.
        tracing::info!(
            %account_id, %owner_user_id, space_id = %space.space_id,
            "google_chat: the mailbox opt-in's owner has no google_chat credential — ingest is \
             paused for it (never failed over to another member) until they reconnect"
        );
        return Ok(None);
    };

    let google = GoogleConnector;
    if let Err(e) = google.refresh_if_needed(&state.http_client, &mut creds).await {
        // Same "pause, never fail over" posture as the disconnected case
        // above — a refresh failure here is not this pass's job to recover
        // from by borrowing someone else's token.
        tracing::info!(
            error = %e, %account_id, %owner_user_id, space_id = %space.space_id,
            "google_chat: the mailbox opt-in owner's token could not be refreshed — ingest is \
             paused for it until they reconnect"
        );
        return Ok(None);
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = store_token_bundle(control_pool, &dek_plane, account_id, "google_chat", TokenOwner::Member(owner_user_id), refreshed).await {
            tracing::warn!(
                %account_id, %owner_user_id, status = ?e.status(),
                "google_chat sweep: failed to persist a refreshed mailbox-owner token (non-fatal)"
            );
        }
    }

    let key = format!("google_chat:{account_id}");
    let http = state.http_client.clone();

    // Self-heal the owner's own subscription on every pass — reuse of the
    // SAME call the container path makes (§4.1's dual-call-site shape).
    if let Some(sub) = member_sub(control_pool, account_id, owner_user_id).await? {
        if let Err(e) = crate::graph_rate_limit::scoped(
            key.clone(),
            google_chat::ensure_subscription(&http, control_pool, account_id, owner_user_id, &sub, &creds.access_token),
        )
        .await
        {
            tracing::warn!(
                error = %e, %account_id, %owner_user_id,
                "google_chat: ensure_subscription failed during a mailbox sweep pass (non-fatal \
                 — the pull half below still runs, and the next tick retries)"
            );
        }
    }

    let directory = match mention_directory(control_pool, content_pool, account_id).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id, %owner_user_id, space_id = %space.space_id, connector_health = true,
                "google_chat: the mention directory could not be read this mailbox pass — nothing \
                 was ingested and the cursor was NOT advanced"
            );
            return Ok(None);
        }
    };

    let own_bot = std::env::var("GOOGLE_CHAT_APP_USER_ID").unwrap_or_default();

    let pending = crate::graph_rate_limit::scoped(
        key,
        google_chat::fetch_pending(
            &http,
            content_pool,
            account_id,
            &space.space_id,
            Some(owner_user_id),
            &creds.access_token,
            &own_bot,
            directory.as_map(),
            Some(graph_id),
        ),
    )
    .await?;

    apply_mailbox_pending(state, control_pool, content_pool, tenant, account_id, owner_user_id, space, graph_id, pending).await
}

pub(crate) async fn apply_mailbox_pending(
    state: &Arc<AppState>, control_pool: &PgPool, content_pool: &PgPool,
    tenant: &TenantId, account_id: Uuid, owner_user_id: Uuid,
    space: &google_chat::IngestableSpace, graph_id: Uuid, pending: google_chat::PendingSync,
) -> Result<Option<usize>, ConnectorError> {
    // Hold authoritative control rows through ingestion AND cursor commit.
    // Pause updates and credential deletion must wait for this transaction;
    // when they win first, the freshly fetched page is discarded.
    let Some(_capture_gate) = mailbox_capture_gate(control_pool, account_id, owner_user_id).await? else {
        return Ok(None);
    };

    if pending.filtered > 0 {
        tracing::info!(
            %account_id, %owner_user_id, space_id = %space.space_id, filtered = pending.filtered,
            "google_chat: mailbox messages filtered at the ingest funnel (poison pills — empty \
             body, system message, our own bot, or an unparseable createTime)"
        );
    }

    if pending.items.is_empty() {
        google_chat::commit_pending(content_pool, account_id, &space.space_id, Some(owner_user_id), pending.commit).await?;
        // `Some(0)`, not `None`: this pass genuinely ran and banked a cursor.
        return Ok(Some(0));
    }

    // The integrated ingest funnel requires a validated routing grant. Resolve
    // it again after fetch/admission, and reject a destination changed in flight.
    let route = crate::ingest::OrgSharedGrant::google_chat_space(
        control_pool, content_pool, tenant, account_id, AccountPlane::for_tenant(tenant),
        &space.space_id, Some(owner_user_id),
    ).await.map_err(ConnectorError::Internal)?;
    let Some((grant, _)) = route.filter(|(grant, _)| grant.oceans() == [graph_id]) else {
        return Ok(None);
    };

    // Exactly one destination — the owner's own private ocean — never a
    // fan-out resolution: the mailbox archetype has no "everyone" to fan out
    // to (contract §7), and the table's own archetype CHECK makes a
    // `mode='fanout'` mailbox row unwritable in the first place.
    let write = google_chat::ingest_space_items(
        state,
        tenant,
        content_pool,
        &grant,
        &space.display_name,
        Some(owner_user_id),
        &pending.items,
    )
    .await;

    if write.fully_succeeded() {
        google_chat::commit_pending(content_pool, account_id, &space.space_id, Some(owner_user_id), pending.commit).await?;
    } else {
        tracing::warn!(
            %account_id, %owner_user_id, space_id = %space.space_id,
            atoms = write.atoms_written,
            destinations_failed = write.destinations_failed,
            items_failed = write.items_failed,
            "google_chat: one or more mailbox writes failed — the cursor is left unchanged and \
             the next pass re-fetches this range"
        );
    }

    Ok(Some(write.atoms_written))
}

/// The member's Google `sub`, for the subscription's stable upsert key
/// (§1.2). Read from the CONTROL plane's `oauth_tokens` — identification
/// only, never a credential read.
async fn member_sub(
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Option<String>, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("member_sub scoped tx: {e}")))?;
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT provider_user_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'google_chat' AND owner_user_id = $2 \
           AND provider_user_id <> '' \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("member_sub: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(s,)| s))
}

/// `external_user_id -> canonical_name` for Chat, from the shared
/// `connector_identities` directory (§3.3). A MISS renders `@unknown` rather
/// than leaking a raw id; a failed READ is a different thing entirely and is
/// handled at the call site (it abandons the pass rather than committing
/// degraded text).
///
/// **Scoping, stated because it looks under-scoped and cannot be fixed
/// here.** There is no `account_id` predicate on this query because
/// `connector_identities` has no `account_id` column: its primary key is
/// `(provider, workspace_id, external_user_id)` (control `0049`), and it
/// carries no RLS policy on either plane (control `0083`, tenant `0009`
/// both list it as deliberately un-policied). The scoped transaction above
/// therefore buys nothing for this table, and the only available anchor is
/// `workspace_id` — whose meaning for Chat is settled by the row WRITER,
/// which does not exist yet. No code path writes a `google_chat` row into
/// this table at all today (it needs a migration to widen `0104`'s
/// `link_method` CHECK), so this read returns zero rows in production and
/// every Chat mention currently renders `@unknown`. Both halves — the
/// writer and the anchor this query should then predicate on — are issue
/// #740. Do not add a predicate here before that issue picks the anchor.
async fn mention_directory(
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
) -> Result<MentionDirectory, ConnectorError> {
    // The workspace anchor (security review, L1). `connector_identities` has
    // NO RLS on either plane and no `account_id` column — its PK is
    // `(provider, workspace_id, external_user_id)` — so an unanchored
    // `provider = 'google_chat'` read on the shared control database is a
    // cross-ACCOUNT read of the entire Chat directory, collapsed into a map
    // keyed on `external_user_id` with the last row winning
    // nondeterministically. Every sibling reader in the tree scopes by
    // `workspace_id` (`connectors::slack`, `seats::
    // resolve_linked_user_directory_only`, `store`); this one did not.
    let workspace_ids = google_chat::account_chat_workspace_ids(control_pool, account_id).await?;
    if workspace_ids.is_empty() {
        // A SUCCESSFUL read that legitimately has nothing to return: this
        // account has connected no Chat workspace, so no directory row could
        // belong to it. Constructed in-module rather than through a public
        // constructor precisely because this type's invariant is "every
        // value came from a read that succeeded" — which this one did.
        return Ok(MentionDirectory(std::collections::HashMap::new()));
    }
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(content_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("mention_directory scoped tx: {e}")))?;
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT external_user_id, canonical_name FROM connector_identities \
         WHERE provider = 'google_chat' AND workspace_id = ANY($1) AND canonical_name <> ''",
    )
    .bind(&workspace_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("mention_directory: {e}")))?;
    tx.rollback().await.ok();
    Ok(MentionDirectory(rows.into_iter().collect()))
}

/// `external_user_id -> canonical_name`, obtainable ONLY from a successful
/// [`mention_directory`] read.
///
/// A newtype with NO `Default` impl, and that is the whole point. The defect
/// this closes was a call site written as
/// `mention_directory(..).await.unwrap_or_default()`: it turned a FAILED read
/// into an empty map, substituted `@unknown` for every mention in the range,
/// and then committed the cursor — writing degraded text that can never be
/// repaired, because Chat never re-reports a range we have already read. A
/// comment saying "do not do that" would have been a review note. Without a
/// `Default` impl, `unwrap_or_default()` does not compile, so the erasing
/// shape is untypeable rather than merely discouraged.
pub(crate) struct MentionDirectory(std::collections::HashMap<String, String>);

impl MentionDirectory {
    /// Borrow the underlying map for the substitution pass. Deliberately not
    /// `From`/`Into`, and there is deliberately no constructor from a bare
    /// map: every value of this type came from a read that SUCCEEDED.
    pub(crate) fn as_map(&self) -> &std::collections::HashMap<String, String> {
        &self.0
    }
}

/// [`reconcile_space_roster`], gated on the caller (review round 1, M4).
#[allow(clippy::too_many_arguments)]
async fn maybe_reconcile_space_roster(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    reader_user_id: Uuid,
    space: &google_chat::IngestableSpace,
    access_token: &str,
    reconcile: RosterReconcile,
    directory_cache: &mut google_chat::DomainDirectoryCache,
) {
    if reconcile == RosterReconcile::No {
        return;
    }
    reconcile_space_roster(
        state,
        control_pool,
        content_pool,
        account_id,
        reader_user_id,
        space,
        access_token,
        directory_cache,
    )
    .await;
}

/// Reconcile the seats a space justifies against its live roster (§3.4/D7).
///
/// # Grants generously, revokes narrowly — and it DOES revoke
/// # (security review, M2)
///
/// This used to be grant-only, on the reasoning that revocation belongs on
/// the explicit `membership.v1.deleted` push. That left departure-driven
/// revocation with **no backstop at all**: `space_events_filter` names only
/// `message.v1.created`/`.deleted`, so a membership deletion lost to a 7-day
/// subscription lapse, a deploy window, or the 28-day `spaceEvents`
/// retention floor was unrecoverable — a permanently stale seat on a team
/// ocean, with `history_from = NULL`, i.e. unbounded history. Combined with
/// the `active`-predicate bug (M1) it meant a member could leave a space and
/// keep reading its ocean forever.
///
/// So this now diffs, exactly as `workers::slack_seat_reconcile` does:
/// `seats::users_granted_by_source` gives everyone this space currently
/// justifies, the live roster gives who is still in it, and anyone in the
/// former but not the latter has their grant dropped through
/// `revoke_seat_grant`'s DELETE-first, count-the-dropped-rows discipline —
/// so a `preexisting` (hand-granted) seat is still never ours to revoke and
/// a member seated by a second routed space still keeps their seat.
///
/// **A roster read that FAILS revokes nothing and grants nothing.** *"An
/// `Err` here means 'we do not know', and the only safe response to not
/// knowing is to change nothing"* — treating a rate-limited
/// `spaces.members.list` as an empty roster would un-seat an entire space.
/// That is why the revoke half reads the roster through
/// `auto_seat_space_members`'s own `Ok` outcome rather than independently.
///
/// `reader_user_id` (chat-per-user-setup PR, 2026-09-20): threaded straight
/// through to `auto_seat_space_members`, which needs it to resolve its own
/// `connector_identities` `workspace_id` anchor — see that function's own
/// doc comment.
#[allow(clippy::too_many_arguments)]
async fn reconcile_space_roster(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    reader_user_id: Uuid,
    space: &google_chat::IngestableSpace,
    access_token: &str,
    directory_cache: &mut google_chat::DomainDirectoryCache,
) {
    // GRANT side: `active` required — never seat people into a space the app
    // is no longer in.
    let team_id = match google_chat::resolve_space_team_for_grant(content_pool, account_id, &space.space_id).await {
        Ok(Some(t)) => t,
        Ok(None) => return, // fan-out or a non-team ocean seats nobody.
        Err(e) => {
            tracing::warn!(error = %e, %account_id, space_id = %space.space_id, "google_chat: team resolution failed — skipping roster reconcile this pass");
            return;
        }
    };
    let key = format!("google_chat:{account_id}");
    let outcome = crate::graph_rate_limit::scoped(
        key,
        google_chat::auto_seat_space_members(
            control_pool,
            content_pool,
            &state.http_client,
            account_id,
            reader_user_id,
            &space.space_id,
            team_id,
            access_token,
            directory_cache,
        ),
    )
    .await;
    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id, space_id = %space.space_id, connector_health = true,
                "google_chat: roster could not be established — skipping seating for this space \
                 entirely rather than seating a partial roster (an unknown roster is never treated \
                 as an empty one), and revoking nothing"
            );
            return;
        }
    };

    // REVOKE side: everyone this space still justifies, minus everyone still
    // in it. Only reached on a SUCCESSFUL roster read.
    let source = crate::seats::google_chat_space_source(&space.space_id);
    let granted = match crate::seats::users_granted_by_source(content_pool, account_id, team_id, &source).await {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, space_id = %space.space_id, "google_chat: outstanding-grant read failed — granted this pass, revoked nothing");
            return;
        }
    };
    let still_present: std::collections::HashSet<Uuid> = outcome.matched_user_ids.iter().copied().collect();
    let mut revoked = 0usize;
    for user_id in granted {
        if still_present.contains(&user_id) {
            continue;
        }
        match google_chat::revoke_space_seat(content_pool, account_id, team_id, user_id, &space.space_id).await {
            Ok(o) => {
                revoked += 1;
                tracing::info!(
                    %account_id, space_id = %space.space_id, %team_id, %user_id, ?o,
                    "google_chat: roster reconcile revoked a grant for a member who is no longer \
                     in the space (the membership.v1.deleted backstop)"
                );
            }
            // Per-member isolation: one failed revoke must not stop the rest.
            Err(e) => tracing::warn!(
                error = %e, %account_id, %team_id, %user_id, space_id = %space.space_id,
                "google_chat: roster reconcile could not revoke one grant — continuing"
            ),
        }
    }

    tracing::info!(
        %account_id, space_id = %space.space_id, %team_id,
        total_humans = outcome.total_humans, matched = outcome.matched, seated = outcome.seated,
        unmapped = outcome.unmapped, google_groups = outcome.group_memberships, revoked,
        "google_chat: roster reconcile complete"
    );
}

// ── Ticks ───────────────────────────────────────────────────────────────────

/// How much of `available` (the per-tick budget remaining when THIS
/// account's `sweep_account` turn starts) the CONTAINER loop may spend,
/// reserving the rest for the mailbox archetype's loop that runs after it.
/// Pure and directly testable, same discipline as [`decide_after_page`]/
/// [`rotated_accounts`] — a starvation-fix rounding rule is exactly the kind
/// of thing worth pinning with boundary tests rather than trusting by
/// inspection.
///
/// Container gets the CEILING half, mailbox the FLOOR half: at
/// `available == 1` the container loop still gets to run (matching this
/// worker's behaviour before the mailbox archetype existed at all, for the
/// smallest possible budget), and at any `available >= 2` the mailbox loop
/// is guaranteed at least one real slot rather than however-many-are-left-
/// after-container spent everything (CodeRabbit round 1, PR #811 — an
/// account with `>= SPACES_PER_TICK` continuously-selected container spaces
/// could previously spend the WHOLE tick budget before
/// `due_mailbox_spaces_for_account` was ever even queried, starving that
/// SAME account's own mailbox opt-ins indefinitely; `mark_space_attempted`'s
/// per-space rotation prevents ANY ONE SPACE from pinning a slot forever,
/// but does nothing for a whole ARCHETYPE being out-prioritised by the
/// other every single tick).
fn container_budget_cap(available: usize) -> usize {
    available - available / 2
}

async fn sweep_account(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    tenant: &TenantId,
    account_id: Uuid,
    budget: &mut usize,
) -> usize {
    if *budget == 0 {
        return 0;
    }
    // Computed BEFORE the container loop runs, from the budget available at
    // this account's turn — not a fixed fraction of `SPACES_PER_TICK`, which
    // would over-reserve (and could underflow) once earlier accounts in the
    // same tick have already spent most of it. Only a CAP on how much the
    // container loop may spend, never a pre-subtraction: budget the
    // container loop does not actually use (fewer due spaces than its cap,
    // or every one already locked by a concurrent push) still reaches the
    // mailbox loop below via `*budget`'s ordinary value, so a quiet
    // container account never wastes reserved-but-unused mailbox headroom.
    let container_cap = container_budget_cap(*budget);
    let spaces = match google_chat::due_spaces_for_account(content_pool, account_id, SPACES_PER_ACCOUNT).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "google_chat sweep: due-space discovery failed — skipping this account this tick");
            return 0;
        }
    };
    let mut processed = 0usize;
    let mut container_spent = 0usize;
    // The sweep-pass boundary for [`google_chat::DomainDirectoryCache`]
    // (CodeRabbit round 1, #1005, Thread 23): one cache per call to
    // `sweep_account` — i.e. per account per tick — so a reader whose
    // access token backs several of this account's routed spaces has its
    // domain directory fetched at most once across the whole loop below,
    // not once per space.
    let mut directory_cache = google_chat::DomainDirectoryCache::new();
    for space in spaces {
        if *budget == 0 || container_spent >= container_cap {
            break;
        }
        *budget -= 1;
        container_spent += 1;
        // Per-space fault isolation: one unreadable space must never stop
        // the rest of the account's sweep.
        match sync_space_once(
            state,
            control_pool,
            content_pool,
            tenant,
            account_id,
            &space,
            // The sweep is the ONLY caller that reconciles rosters (M4).
            RosterReconcile::Yes,
            &mut directory_cache,
        )
        .await
        {
            // `None` = the pass did not run (lock held, no reader, no
            // destination). Counting it would emit `Tick::worked` without
            // having worked, which resets the org idle backoff and reschedules
            // at `base_poll` forever on a tenant that can never make progress.
            Ok(Some(_)) => processed += 1,
            Ok(None) => {}
            Err(e) => tracing::warn!(
                error = %e, %account_id, space_id = %space.space_id,
                "google_chat sweep: space pass failed — continuing with the account's other spaces"
            ),
        }
    }

    // G4.5 — the mailbox archetype's opt-ins, sharing the SAME per-tick
    // `*budget` as the container loop above but no longer able to be
    // entirely CROWDED OUT by it: `container_budget_cap` reserved roughly
    // half of what was available at this account's turn before that loop
    // ran, so this query — and this loop — are reached even when the
    // container list alone would have exhausted the whole remaining budget.
    if *budget > 0 {
        let mailbox_spaces = match google_chat::due_mailbox_spaces_for_account(content_pool, account_id, SPACES_PER_ACCOUNT).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "google_chat sweep: due-mailbox discovery failed — skipping this account's mailbox opt-ins this tick");
                Vec::new()
            }
        };
        for (owner_user_id, space) in mailbox_spaces {
            if *budget == 0 {
                break;
            }
            *budget -= 1;
            match sync_mailbox_space_once(state, control_pool, content_pool, tenant, account_id, owner_user_id, &space).await {
                Ok(Some(_)) => processed += 1,
                Ok(None) => {}
                Err(e) => tracing::warn!(
                    error = %e, %account_id, %owner_user_id, space_id = %space.space_id,
                    "google_chat sweep: mailbox opt-in pass failed — continuing with the \
                     account's other opt-ins"
                ),
            }
        }
    }

    processed
}

async fn tick_org(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    org_id: &str,
) -> Result<Tick, String> {
    let account_id = resolve_tenant_account(control_pool, org_id).await?;
    let mut budget = SPACES_PER_TICK;
    let processed = sweep_account(state, control_pool, &t.pool, &t.tenant, account_id, &mut budget).await;
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

async fn tick_consumer(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    // Cross-account discovery WITHOUT a `SECURITY DEFINER` bootstrap — see
    // this module's own doc, "Consumer-plane discovery uses `accounts`".
    //
    // KNOWN COST, and why it is not pre-filtered here: this probes EVERY
    // account once per tick, including the overwhelming majority with no
    // Chat connection at all, so the work grows with signups rather than
    // with Chat-connected accounts. The obvious pre-filter — restrict the
    // roster to accounts that actually hold a `google_chat_watched_spaces`
    // row — is NOT reachable from this pool under the least-privilege
    // `coralswarm_app` role: that table carries `account_isolation` RLS, so
    // an unscoped cross-account read returns zero rows, which is the exact
    // reason this enumeration goes through `accounts` in the first place.
    // Making it cheap therefore needs a `SECURITY DEFINER` bootstrap (a
    // MIGRATION), which is out of scope for a PR that deliberately claims no
    // migration numbers. Bounded meanwhile: one indexed
    // `due_spaces_for_account` query per account per POLL_INTERVAL, and the
    // whole loop short-circuits once `SPACES_PER_TICK` is spent.
    let accounts: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
        .fetch_all(control_pool)
        .await
        .map_err(|e| e.to_string())?;
    // Rotate the start each tick so budget exhaustion cannot permanently
    // starve the higher-sorting tail of the fleet — see
    // [`CONSUMER_ROTATION`]'s own doc (review round 1, H3).
    let start = CONSUMER_ROTATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let accounts = rotated_accounts(&accounts, start);
    let mut budget = SPACES_PER_TICK;
    let mut processed = 0usize;
    for account_id in accounts {
        if budget == 0 {
            break;
        }
        processed += sweep_account(state, control_pool, &t.pool, &t.tenant, account_id, &mut budget).await;
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

/// One sweep pass. Driven by `workers::tenant_harness::run_multi_tenant` as
/// a sweep worker (`HarnessConfig::sweep_worker(_, POLL_INTERVAL)`); BOTH
/// arms do real work.
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    crate::connectors::catalog::everything::refresh(t, control_pool, state, "google").await?;
    match &t.tenant {
        TenantId::Consumer => tick_consumer(t, control_pool, state).await,
        TenantId::Org(org_id) => tick_org(t, control_pool, state, org_id).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **Finding 3, review round 2.** The two ways the candidate walk can
    /// end are not the same claim, and only one of them licenses clearing the
    /// designated reader and raising a `connector_health` alert.
    ///
    /// Honest about the limit of this test: it pins the CLASSIFIER, not the
    /// two `reader_credentials` arms it drives. Reaching `BudgetSpent` for
    /// real needs `MAX_READER_CANDIDATES_PER_PASS` consecutive token
    /// refreshes to fail, and `GoogleConnector::refresh_if_needed` only fails
    /// by making a live call to Google's token endpoint — there is no HTTP
    /// mock in this crate and no injection seam, so that arm has no hermetic
    /// test. What IS enforced here: the distinction exists as a type, the
    /// compiler forces both arms to be handled, and the `None` case is
    /// classified conclusively (the subtle one — see the function's doc).
    #[test]
    fn a_spent_budget_is_not_the_same_claim_as_nobody_left() {
        // The election returned nobody: conclusive, act on it.
        assert_eq!(classify_reader_walk(None), ReaderWalkOutcome::NobodyLeft);

        // A candidate is still queued: the budget ran out first and NOTHING
        // has been established. This is the case that used to wipe a live
        // reader and page an operator.
        assert_eq!(classify_reader_walk(Some(Uuid::new_v4())), ReaderWalkOutcome::BudgetSpent);

        // The two are genuinely distinct values, not a renamed bool that a
        // future edit could collapse back into one branch.
        assert_ne!(ReaderWalkOutcome::NobodyLeft, ReaderWalkOutcome::BudgetSpent);
    }

    // -- The offboarded-reader authorization test ------------------------
    //
    // Everything below the `use` line is a local fixture, deliberately NOT
    // shared with `connectors::google_chat`'s test module: these are the
    // three tables the bug spans (`account_members`, `oauth_tokens`,
    // `google_chat_watched_spaces`) and #677's lesson is that a fixture
    // copied between two tests is a fixture bug shipped twice.
    use crate::app_role_test_support::app_role_pool;
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

    async fn fresh_control_db(base: &str, db: &str) -> (PgPool, PgPool) {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn.close().await.ok();
        let url = with_db(base, db);
        let owner: PgPool = PgPoolOptions::new().connect(&url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
        let app = app_role_pool(&owner, &url).await;
        (owner, app)
    }

    async fn drop_db(base: &str, db: &str) {
        let mut conn = PgConnection::connect(base).await.unwrap();
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Routing pin (ADR-0016 layer 2): a space's route decides its oceans.
    /// `single` names one; a `fanout` route on the Personal plane is refused;
    /// an Org fan-out with no mapped member resolves to none, so nothing can
    /// be written.
    #[tokio::test]
    async fn a_space_route_decides_its_destinations_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_space_route_decides_its_destinations_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_ingest_door_routes";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app) = fresh_control_db(&base, db).await;
        let alice = sqlx::query_scalar::<_, Uuid>("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("alice-{}@test.local", Uuid::new_v4()))
            .fetch_one(&owner)
            .await
            .unwrap();
        let account_id = sqlx::query_scalar::<_, Uuid>("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(alice)
            .fetch_one(&owner)
            .await
            .unwrap();
        let graph_id = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'My Ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&owner)
        .await
        .unwrap();
        for (space, mode, graph) in [("spaces/single", "single", Some(graph_id)), ("spaces/fanout", "fanout", None)] {
            sqlx::query(
                "INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, mode, graph_id, active) \
                 VALUES ($1, $2, 'SPACE', $3, $4, true)",
            )
            .bind(account_id)
            .bind(space)
            .bind(mode)
            .bind(graph)
            .execute(&owner)
            .await
            .unwrap();
        }
        let grant = |space_id: &'static str, plane| {
            crate::ingest::OrgSharedGrant::google_chat_space(&app, &app, &TenantId::Consumer, account_id, plane, space_id, None)
        };
        let (single, _) = grant("spaces/single", AccountPlane::Personal).await.unwrap().expect("routed");
        assert_eq!(single.oceans(), [graph_id]);
        assert!(grant("spaces/fanout", AccountPlane::Personal).await.is_err());
        let (fanout, _) = grant("spaces/fanout", AccountPlane::Org).await.unwrap().expect("routed");
        assert!(fanout.oceans().is_empty());
        assert!(grant("spaces/unrouted", AccountPlane::Personal).await.unwrap().is_none());

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    /// **The authorization regression test (security review round 2,
    /// finding 1).**
    ///
    /// Offboarding (`clerk_provision::remove_org_membership`) deletes the
    /// `account_members` row and deliberately leaves `oauth_tokens` alone.
    /// `elect_space_reader` refuses to ELECT such a member -- but this
    /// function reached the election only when the row had no stored reader
    /// or the stored one's refresh FAILED, so an ex-member whose refresh
    /// token still worked was used forever: their credential read the space
    /// every tick and `store_token_bundle` re-minted and re-persisted an
    /// access token in their name.
    ///
    /// The fixture is that exact state -- the account's ONLY `google_chat`
    /// credential belongs to an ex-member who is still NAMED as the space's
    /// reader -- and the assertion is that we come back with nobody.
    ///
    /// **Negative control**: with the membership gate removed this test
    /// fails, and it fails by reaching `load_creds_bg` for the ex-member --
    /// the error text names her user id. That is the bug, observed.
    #[tokio::test]
    async fn an_offboarded_stored_reader_is_never_used_and_never_re_persisted_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping an_offboarded_stored_reader_is_never_used_and_never_re_persisted_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_offboarded_reader";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app) = fresh_control_db(&base, db).await;

        let alice = sqlx::query_scalar::<_, Uuid>("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("alice-{}@test.local", Uuid::new_v4()))
            .fetch_one(&owner)
            .await
            .unwrap();
        // Mallory: connected Google Chat, then left the org.
        let mallory = sqlx::query_scalar::<_, Uuid>("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("mallory-{}@test.local", Uuid::new_v4()))
            .fetch_one(&owner)
            .await
            .unwrap();
        let account_id = sqlx::query_scalar::<_, Uuid>("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(alice)
            .fetch_one(&owner)
            .await
            .unwrap();
        let graph_id = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'My Ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&owner)
        .await
        .unwrap();

        // Alice is a current member but has NEVER connected Chat -- so the
        // account's only google_chat credential is the ex-member's, which is
        // what makes "nobody left" the CORRECT answer here.
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(alice)
            .execute(&owner)
            .await
            .unwrap();
        // Mallory's surviving credential. NO `account_members` row: offboarded.
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, owner_user_id, provider_user_id, access_token, refresh_token) \
             VALUES ($1, 'google_chat', 'test-scope', $2, 'sub-mallory', 'enc-access', 'enc-refresh')",
        )
        .bind(account_id)
        .bind(mallory)
        .execute(&owner)
        .await
        .unwrap();
        assert!(
            !google_chat::is_current_account_member(&app, account_id, mallory).await.unwrap(),
            "fixture: mallory must be offboarded"
        );
        assert!(
            google_chat::is_current_account_member(&app, account_id, alice).await.unwrap(),
            "fixture: alice must still be a member -- otherwise this test could pass for the \
             wrong reason (a gate that rejects everybody)"
        );

        // A routed space that still NAMES mallory as its reader, with a
        // watermark to prove nothing is disturbed.
        google_chat::record_added_to_space(
            &app,
            &app,
            AccountPlane::Personal,
            account_id,
            Some(mallory),
            &crate::connectors::chat_body::ChatSpace {
                name: "spaces/OFFB".to_string(),
                space_type: "SPACE".to_string(),
                display_name: "eng-platform".to_string(),
                space_threading_state: "THREADED_MESSAGES".to_string(),
                history_state: "HISTORY_ON".to_string(),
                single_user_bot_dm: false,
                membership_count: None,
            },
        )
        .await
        .unwrap();
        google_chat::pick_space_destination(
            &app,
            &app,
            AccountPlane::Personal,
            account_id,
            "spaces/OFFB",
            google_chat::PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        google_chat::commit_pending(
            &app,
            account_id,
            "spaces/OFFB",
            None,
            google_chat::PendingCommit::CatchUpComplete("2026-08-01T00:00:00+00:00".into()),
        )
        .await
        .unwrap();
        let space = google_chat::ingestable_space(&app, account_id, "spaces/OFFB").await.unwrap().unwrap();
        assert_eq!(space.reader_user_id, Some(mallory), "fixture: the ex-member is the stored reader");

        // `access_token` is BYTEA (encrypted at rest), so `Vec<u8>` — a `String`
        // here decodes-errors rather than asserting anything.
        let token_before: (Vec<u8>, chrono::DateTime<chrono::Utc>) =
            sqlx::query_as("SELECT access_token, updated_at FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2")
                .bind(account_id)
                .bind(mallory)
                .fetch_one(&owner)
                .await
                .unwrap();

        // THE ASSERTION. Pre-fix this same call loads mallory's credential
        // (and, with a live refresh token, refreshes and RE-PERSISTS it).
        let state = crate::test_app_state();
        let got = reader_credentials(&state, &app, &app, account_id, &space).await.unwrap();
        assert!(
            got.is_none(),
            "an offboarded member's surviving credential must NOT be used to read a space -- \
             the only google_chat token on this account belongs to an ex-member, so the correct \
             answer is nobody-left, not use-the-ex-member. Got: {got:?}"
        );

        // Her token was neither refreshed nor re-persisted.
        let token_after: (Vec<u8>, chrono::DateTime<chrono::Utc>) =
            sqlx::query_as("SELECT access_token, updated_at FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2")
                .bind(account_id)
                .bind(mallory)
                .fetch_one(&owner)
                .await
                .unwrap();
        assert_eq!(
            token_before, token_after,
            "the ex-member's token row must be untouched -- `store_token_bundle` re-persisting a \
             freshly minted access token for a non-member is half of this finding"
        );

        // The row degrades to the honest end state: no reader, destination
        // and cursor intact.
        let after = google_chat::ingestable_space(&app, account_id, "spaces/OFFB").await.unwrap().unwrap();
        assert_eq!(after.reader_user_id, None, "the stale ex-member name must be cleared, not kept");
        let route_graph: Option<Uuid> = sqlx::query_scalar(
            "SELECT graph_id FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/OFFB'",
        )
        .bind(account_id)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(route_graph, Some(graph_id), "nobody-left must never discard the destination pick");
        assert_eq!(
            google_chat::load_space_sync_state(&app, account_id, "spaces/OFFB", None)
                .await
                .unwrap()
                .unwrap()
                .sync_token
                .as_deref(),
            Some("2026-08-01T00:00:00+00:00"),
            "...nor the cursor: ingest resumes the moment a current member connects Chat"
        );

        // And the gate is not a blanket refusal -- re-adding mallory to the
        // org makes her eligible again, so an operator's fix is one INSERT.
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(account_id)
            .bind(mallory)
            .execute(&owner)
            .await
            .unwrap();
        assert_eq!(
            google_chat::elect_space_reader(&app, account_id, &[]).await.unwrap(),
            Some(mallory),
            "the gate is membership, not a permanent ban -- a re-added member is electable again"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[test]
    fn space_lock_key_is_per_account_and_per_space() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        assert_ne!(space_lock_key(a, "spaces/X"), space_lock_key(a, "spaces/Y"));
        assert_ne!(space_lock_key(a, "spaces/X"), space_lock_key(b, "spaces/X"));
        // Stable across calls — the push path and this worker build the key
        // from the same function precisely so they cannot drift.
        assert_eq!(space_lock_key(a, "spaces/X"), space_lock_key(a, "spaces/X"));
    }

    /// **The mailbox-starvation regression test** (CodeRabbit round 1, PR
    /// #811). Before `container_budget_cap`, `sweep_account`'s container
    /// loop ran to completion (or budget exhaustion) BEFORE
    /// `due_mailbox_spaces_for_account` was even queried — an account with
    /// `>= *budget` continuously-due container spaces spent the whole
    /// budget on itself and `due_mailbox_spaces_for_account` was never
    /// reached AT ALL, starving that SAME account's own mailbox opt-ins on
    /// every tick, indefinitely (not merely "less often" — the mailbox query
    /// itself never ran).
    ///
    /// Network-free by construction, same trick `an_offboarded_stored_
    /// reader_is_never_used_and_never_re_persisted_pg` above relies on: no
    /// `oauth_tokens` row exists for this account for EITHER archetype, so
    /// each pass's own credential lookup (`reader_credentials`/
    /// `sync_mailbox_space_locked`'s `load_creds_bg`) returns cleanly with
    /// nobody found — no live Google API call is reachable from this
    /// harness, and none is needed to prove which spaces were even
    /// ATTEMPTED. "Attempted" is observed via `last_resolved_at`
    /// (`mark_space_attempted`), which both archetypes' `sync_*_once`
    /// stamp unconditionally, before any credential/network work, on every
    /// single pass regardless of outcome.
    #[tokio::test]
    async fn sweep_account_reserves_mailbox_budget_so_a_busy_container_account_never_starves_its_own_mailbox_optins_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping sweep_account_reserves_mailbox_budget_so_a_busy_container_account_never_starves_its_own_mailbox_optins_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g45_budget_reserve";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app) = fresh_control_db(&base, db).await;

        let alice = sqlx::query_scalar::<_, Uuid>("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("alice-{}@test.local", Uuid::new_v4()))
            .fetch_one(&owner)
            .await
            .unwrap();
        let account_id = sqlx::query_scalar::<_, Uuid>("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(alice)
            .fetch_one(&owner)
            .await
            .unwrap();
        let graph_id = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'My Ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&owner)
        .await
        .unwrap();

        // Ten routed, active CONTAINER spaces — more than the test's own
        // small budget below, so this account alone can (and, before the
        // fix, DID) exhaust the whole tick budget before the mailbox
        // archetype's query is ever reached.
        for i in 0..10 {
            sqlx::query(
                "INSERT INTO google_chat_watched_spaces \
                    (account_id, space_id, space_type, mode, graph_id, active) \
                 VALUES ($1, $2, 'SPACE', 'single', $3, true)",
            )
            .bind(account_id)
            .bind(format!("spaces/container-{i}"))
            .bind(graph_id)
            .execute(&owner)
            .await
            .unwrap();
        }

        // One active MAILBOX opt-in on the SAME account (G4.5).
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id, active) \
             VALUES ($1, 'spaces/mailbox-1', 'DIRECT_MESSAGE', 'single', $2, $3, $3, true)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(alice)
        .execute(&owner)
        .await
        .unwrap();

        // See `space_cursor_round_trip_and_backfill_arming_pg`'s own
        // precedent (`connectors::google_chat`'s test module) for why this
        // sleep is load-bearing, not decoration: without it, a `now()` stamp
        // from the sweep call below could land inside the same timestamp
        // resolution window as the fixture's own INSERT defaults, making the
        // "was it bumped" comparison pass vacuously.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let before: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT last_resolved_at FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-1'",
        )
        .fetch_one(&owner)
        .await
        .unwrap();

        let state = crate::test_app_state();
        // `container_budget_cap(4) == 2` — the container loop may spend AT
        // MOST 2 of these 4 units despite 10 spaces being due, leaving >= 2
        // for the mailbox loop that runs after it.
        let mut budget = 4usize;
        sweep_account(&state, &app, &app, &TenantId::Consumer, account_id, &mut budget).await;

        let container_attempted: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces \
             WHERE account_id = $1 AND owner_user_id IS NULL AND last_resolved_at > $2",
        )
        .bind(account_id)
        .bind(before)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(
            container_attempted,
            container_budget_cap(4) as i64,
            "the container loop must spend exactly its reserved cap, no more (and no fewer, \
             since 10 due container spaces comfortably exceed the cap)"
        );

        let mailbox_after: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT last_resolved_at FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-1'",
        )
        .fetch_one(&owner)
        .await
        .unwrap();
        assert!(
            mailbox_after > before,
            "the mailbox opt-in must have been ATTEMPTED (its sweep-rotation stamp bumped) even \
             though this account alone has more due container spaces than the whole tick budget \
             — before this fix, the container loop spent the ENTIRE budget first and \
             due_mailbox_spaces_for_account was never even queried this tick"
        );

        drop_db(&base, db).await;
    }

    /// **The starvation regression test** (review round 1, H3). The test this
    /// replaces asserted `SPACES_PER_ACCOUNT < SPACES_PER_TICK` — a constant
    /// inequality that proved nothing and would have stayed green through
    /// the entire bug.
    ///
    /// The real property: when the per-tick budget is exhausted by the
    /// lowest-sorting accounts every single tick, EVERY account must still be
    /// reached within a bounded number of ticks. Before the rotation cursor,
    /// `tick_consumer` started at the same account every time and `break`d on
    /// budget exhaustion, so ten permanently-stuck spaces on the two
    /// lowest-sorting accounts silenced the entire consumer fleet forever.
    #[test]
    fn consumer_rotation_reaches_every_account_even_when_the_budget_is_always_exhausted() {
        let accounts: Vec<Uuid> = (0..7).map(|_| Uuid::new_v4()).collect();
        // The pathological case: the first two accounts consume the whole
        // per-tick budget, so only two accounts are ever PROBED per tick.
        const PROBED_PER_TICK: usize = 2;

        let mut ever_probed: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
        for tick in 0..accounts.len() {
            let order = rotated_accounts(&accounts, tick);
            assert_eq!(order.len(), accounts.len(), "rotation must not drop or duplicate accounts");
            for a in order.iter().take(PROBED_PER_TICK) {
                ever_probed.insert(*a);
            }
        }
        assert_eq!(
            ever_probed.len(),
            accounts.len(),
            "every account must be reached within N ticks even when each tick only ever gets to \
             {PROBED_PER_TICK} of them — this is the fleet-starvation property, and a FIXED start \
             offset fails it"
        );

        // A fixed start (the pre-fix behaviour) provably does NOT hold the
        // property — pinned so nobody "simplifies" the rotation away.
        let mut fixed_start_probed: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
        for _ in 0..accounts.len() {
            for a in rotated_accounts(&accounts, 0).iter().take(PROBED_PER_TICK) {
                fixed_start_probed.insert(*a);
            }
        }
        assert_eq!(
            fixed_start_probed.len(),
            PROBED_PER_TICK,
            "a fixed start reaches only the first {PROBED_PER_TICK} accounts, ever — that is the \
             bug this rotation exists to prevent"
        );
    }

    #[test]
    fn rotation_is_total_order_preserving_and_handles_the_degenerate_cases() {
        assert!(rotated_accounts(&[], 3).is_empty());
        let one = vec![Uuid::new_v4()];
        assert_eq!(rotated_accounts(&one, 99), one);
        let three: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        assert_eq!(rotated_accounts(&three, 0), three);
        assert_eq!(rotated_accounts(&three, 1), vec![three[1], three[2], three[0]]);
        // Wraps rather than panicking or truncating on a counter far past the
        // fleet size — `CONSUMER_ROTATION` is a monotonically increasing
        // process-lifetime counter, so this is the normal case, not an edge.
        assert_eq!(rotated_accounts(&three, 3), three);
        assert_eq!(rotated_accounts(&three, usize::MAX).len(), 3);
    }

    // ── container_budget_cap (CodeRabbit round 1, PR #811) ──────────────────

    #[test]
    fn container_budget_cap_reserves_the_floor_half_leaving_the_ceiling_half_for_container() {
        // The concrete starvation scenario: a full SPACES_PER_TICK budget
        // must leave the mailbox loop a REAL, non-zero share rather than
        // whatever happens to survive the container loop.
        assert_eq!(container_budget_cap(SPACES_PER_TICK), SPACES_PER_TICK - SPACES_PER_TICK / 2);
        assert!(
            container_budget_cap(SPACES_PER_TICK) < SPACES_PER_TICK,
            "the container loop must NOT be able to spend the entire tick budget by itself \
             once a real reservation exists — that is precisely the starvation bug this \
             function exists to fix"
        );
    }

    #[test]
    fn container_budget_cap_boundary_values() {
        assert_eq!(container_budget_cap(0), 0, "no budget at all — nothing to cap");
        assert_eq!(
            container_budget_cap(1), 1,
            "a single remaining unit must still let the container loop run — matches this \
             worker's own pre-mailbox-archetype behaviour at the smallest possible budget, \
             rather than reserving the one unit away and running NEITHER loop"
        );
        assert_eq!(container_budget_cap(2), 1, "an even budget splits exactly in half");
        assert_eq!(container_budget_cap(3), 2, "an odd budget rounds the CONTAINER share up, the mailbox share down");
        assert_eq!(container_budget_cap(10), 5);
        assert_eq!(container_budget_cap(11), 6);
    }

    #[test]
    fn container_budget_cap_never_exceeds_the_available_budget() {
        // A cap greater than what is actually available would let the
        // container loop's own `container_spent >= container_cap` guard
        // never trip before `*budget == 0` does anyway — harmless, but
        // asserted so a future edit to the rounding rule cannot silently
        // widen the cap past what it is capping.
        for available in 0..64usize {
            let cap = container_budget_cap(available);
            assert!(cap <= available, "container_budget_cap({available}) = {cap} exceeds the available budget");
        }
    }

    #[test]
    fn per_tick_budget_is_bounded_and_the_reader_candidate_walk_is_bounded_too() {
        assert!(SPACES_PER_TICK > 0);
        assert!(SPACES_PER_ACCOUNT > 0);
        // A bounded candidate walk is what stops one unreadable space from
        // turning into a full membership enumeration on every tick (security
        // review, L2) — while still being >1, which is what stops the
        // livelock the single-exclusion version had.
        assert!(
            MAX_READER_CANDIDATES_PER_PASS > 1,
            "a failover that can only try ONE alternative re-elects the same broken pair forever"
        );
        assert!(MAX_READER_CANDIDATES_PER_PASS < 50);
    }

    #[test]
    fn roster_reconcile_is_opt_in_per_caller() {
        // Review round 1, M4: the distinction has to be a TYPE, not a
        // convention, because the push path calls the same function once per
        // message.
        assert_ne!(RosterReconcile::Yes, RosterReconcile::No);
    }
}

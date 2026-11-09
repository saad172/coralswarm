//! Slack ID-shaped name backfill worker (repair for PR #176).
//!
//! PR #176 made the frontend mask ID-shaped values ("U57C3A9EF09",
//! "C0ABC123XY") as "Unknown author" / "Unknown channel" / "Unknown
//! participant" — but the raw id is still what's persisted forever on
//! `atoms.author_name` / `atoms.channel_name` / `atoms.participants` whenever
//! ingest-time resolution (`routes::slack::resolve_user_name` /
//! `resolve_channel_name`) fell back to it. This worker re-resolves those
//! stored values once a real name becomes available, preferring the
//! `connector_identities` directory (migration 0049) over a fresh Slack API
//! call, and updates the atom in place.
//!
//! # Triggers
//! - **Natural repair** (the common path): `routes::slack::ingest_slack_message`
//!   enqueues a `SlackNameBackfill` job scoped to `(graph_id, workspace_id,
//!   author_id)` immediately after the identity write commits — see
//!   `routes::slack::persist_identity_and_enqueue_backfill`'s doc comment for
//!   why that's a single atomic transaction (a failed enqueue after a
//!   committed identity write would otherwise silently drop the repair
//!   forever) — i.e. exactly the moment the directory "gains or updates a
//!   resolved name", per this worker's design brief.
//! - **Standalone one-shot backfill**: every field is optional, so a bare
//!   `SlackNameBackfill { graph_id: None, workspace_id: None, author_id: None,
//!   after_atom_id: None }` job sweeps every graph/author with an ID-shaped
//!   name still on record. Safe to hand-insert in prod:
//!
//!   ```sql
//!   INSERT INTO jobs (kind, payload)
//!   VALUES ('SlackNameBackfill', '{"graph_id": null, "workspace_id": null, "author_id": null, "after_atom_id": null}'::jsonb);
//!   ```
//!
//!   (Mirrors the `{"atom_ids": [...]}` shape the team has hand-inserted for
//!   `EmbedAtoms` before — same `jobs` table, same "paste JSON, INSERT,
//!   done" workflow.) A scoped one-shot for a single ocean or author works
//!   the same way, e.g.
//!   `'{"graph_id": "<uuid>", "workspace_id": null, "author_id": null, "after_atom_id": null}'`.
//!
//! # Batching and safety
//! [`ATOM_BATCH_SIZE`] atoms are read and (at most) rewritten per job
//! execution — deliberately tiny. This worker holds no ML tensors (unlike
//! `workers::embed`, which OOM-killed production at a 64-item batch — see
//! that module's `EMBED_BATCH_SIZE` doc for the incident), but a backfill
//! sweep can still touch an unbounded number of historical atoms, and each
//! resolution may cost a live Slack API round-trip; a small batch bounds
//! both the query result set held in memory and the worst-case Slack API
//! burst per job, and lets [`process_job`] make an idempotent, incremental
//! amount of progress rather than one huge transaction.
//!
//! ## Continuation and retry (cursor-based sweep)
//! Every batch carries an `after_atom_id` keyset cursor. Whenever a batch is
//! full (more rows likely remain beyond it), a continuation job is enqueued
//! with the cursor advanced past the LAST atom examined this pass —
//! unconditionally, regardless of whether this window itself made any repair
//! progress. This is deliberate: without it, a "stuck" window (e.g. every
//! atom in it belongs to a workspace whose Slack API/token is currently
//! unreachable) would silently block every later candidate in the sweep
//! forever, since the un-cursored query always re-selects the same lowest
//! atom ids. Advancing the cursor on every full batch guarantees the sweep
//! as a whole visits every matching atom exactly once per pass and always
//! terminates.
//!
//! If enqueueing that continuation itself fails, the CURRENT job is failed
//! (`queue::fail`, not `queue::complete`) and processing returns immediately
//! — otherwise a full batch that also repaired something would complete
//! successfully with the continuation silently lost, permanently stranding
//! every atom beyond this window. Retrying the current job gives the
//! enqueue another chance (and is cheap: every atom this pass already fixed
//! simply drops out of the next attempt's WHERE filter).
//!
//! Separately (checked only once the continuation side is handled), if a
//! window leaves ANY atom with at least one field still ID-shaped after this
//! pass — whether zero atoms were touched, or an atom was PARTIALLY repaired
//! (e.g. its author resolved but its channel didn't) — the CURRENT job is
//! also sent through `queue::fail` instead of `queue::complete`, so it gets a
//! few bounded retries via the queue's own exponential backoff. This gives a
//! transient failure (Slack API blip, directory read hiccup) a chance to
//! self-heal without touching the sweep's forward progress; once
//! `max_attempts` is exhausted the job dies rather than retrying forever, so
//! this never becomes an unbounded loop — including for atoms that are
//! genuinely, permanently unresolvable right now (e.g. a deactivated Slack
//! user with no directory entry): they get a bounded number of extra tries,
//! then are left alone for a future sweep rather than retried forever.
//!
//! Never guesses a workspace: an atom with no recorded workspace of its own
//! (a pre-migration-0058 legacy row) is resolved only when there is EXACTLY
//! ONE possible Slack workspace it could belong to — either because a
//! specific workspace was requested and matched, or because the atom's
//! account has only one Slack integration in total. When more than one
//! candidate workspace is possible (an ambiguous legacy atom on a
//! multi-workspace account, only reachable via a fully-unscoped sweep), the
//! atom is left untouched rather than guessed — attributing it to the wrong
//! workspace's directory/API could write an actively WRONG name, which is
//! worse than leaving the raw id in place.
//!
//! Idempotent: re-running against atoms that are already resolved is a
//! no-op (the ID-shape `WHERE` filter simply matches nothing).
//!
//! # Two pools, tenant loop (org-slack step 4)
//!
//! This worker now joins `workers::tenant_harness::run_multi_tenant`: the
//! natural-repair enqueue in `routes::slack_events::persist_identity_and_
//! enqueue_backfill` moved planes in the SAME commit (that function's own doc
//! comment has the sequencing detail), so `SlackNameBackfill` jobs land on
//! WHICHEVER plane the identity write resolved — the consumer `jobs` table
//! for a personal (or not-yet-tenant-migrated org) account, or a specific
//! org's own tenant `jobs` table — and this worker's `dequeue` must run on
//! that SAME plane's queue.
//!
//! Everything this worker repairs (`atoms`, `graphs`, `slack_integrations`)
//! and its directory (`connector_identities`) are content-plane tables —
//! present, under an org, on the TENANT database — so `run_batch` and every
//! helper it calls take only the one content pool (`t.pool` from the
//! caller's `WorkerTenant`) and need no control-plane pool at all. The one
//! exception is the account-less **seed** job (see below): resolving "every
//! account this sweep should fan out to" is a control-plane operation on the
//! consumer plane (`accounts`) and a control-plane LOOKUP on an org tenant
//! (`tenant_databases`, to find that tenant's own singleton account id) —
//! [`fan_out_per_account`] is the only function in this file that touches
//! `control_pool`.
//!
//! # Consumer-plane RLS scoping (org-tenancy Phase W.2) / tenant-plane RLS
//! # scoping (org-slack step 4): seed → per-account
//!
//! `atoms` and `graphs` are on `0009_tenant_rls`'s roster too (`slack_
//! integrations` is not, on either plane), so `begin_account_scoped` alone
//! — sufficient on the consumer plane's `0083` policies — is not sufficient
//! on an org tenant: `begin_account_scoped` sets only `app.account_id`, which
//! `0009`'s `tenant_rls_visible_graph_ids()` never treats as enough for a
//! tenant-wide background sweep with no acting end user. Every RLS-covered
//! statement below therefore runs inside `tenancy::scope::
//! begin_tenant_system_scoped` (not the plain `begin_account_scoped` this
//! file used before it had an org-tenant plane to serve) — harmless on the
//! consumer plane, since `0083`'s policies never read the extra
//! `app.tenant_actor` GUC it sets. Per-statement, not per-job, because the
//! repair loop interleaves DB statements with Slack API round-trips. Same
//! queue-native loop-per-account conversion as `workers::content_tsv_backfill`
//! (see that module's RLS section for the full flagged-decision rationale):
//!
//!   * An account-less job (`jobs.account_id IS NULL`) with `graph_id: None`
//!     — the operator-inserted all-accounts sweep — becomes a **seed**: on
//!     the consumer plane it enumerates every account id from `accounts`
//!     (control table, outside the RLS roster) and enqueues one child job PER
//!     ACCOUNT; on an org tenant there is exactly ONE account (that tenant's
//!     own), resolved from `tenant_databases` on the control plane, so the
//!     "fan-out" is a single stamped child. Either way the seed enqueues onto
//!     `t.pool` — the SAME plane it was dequeued from — carrying the same
//!     payload filters with `jobs.account_id` stamped, and completes.
//!   * An account-less job with `graph_id: Some(_)` is DEFENSIVELY FAILED:
//!     post-W.1, `queue::dequeue` backfills or dead-letters every NULL
//!     legacy row whose kind carries a graph reference, so reaching this
//!     worker unscoped means an upstream bug — never process it.
//!   * A stamped job (every natural-repair enqueue, every seed child, every
//!     continuation) runs each RLS-covered statement inside its own short
//!     `begin_tenant_system_scoped` transaction — per-statement, not
//!     per-job, because the repair loop interleaves DB statements with Slack
//!     API round-trips. The candidate query additionally carries an explicit
//!     `graph_id IN (SELECT id FROM graphs WHERE account_id = $n)` predicate
//!     (app-layer scoping primary even pre-OPS-4/pre-tenant-RLS; RLS the
//!     backstop). `connector_identities` reads/writes are deliberately NOT on
//!     either roster (shared directory) and stay on plain `Store` calls.

use std::collections::HashMap;
use std::time::Duration;

use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::jobs::JobKind;
use crate::routes::slack::{resolve_channel_name, resolve_user_name};
use crate::store::StoreError;
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::tenancy::TenantId;
use crate::workers::embed::UNSCOPED_JOB_ERR;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How many `SlackNameBackfill` jobs to dequeue per poll cycle.
const JOB_BATCH_SIZE: usize = 4;
/// How many candidate atoms a single job execution reads (and at most
/// rewrites). Deliberately small — see module docs.
const ATOM_BATCH_SIZE: i64 = 20;
/// Delay before a continuation job (more likely remains) becomes eligible to
/// run — spaces out Slack API calls across job cycles.
const CONTINUATION_DELAY_SECS: i64 = 2;
/// How long to sleep between polls when the queue is empty — the tenant
/// harness's `base_poll` / idle-backoff floor for this worker.
pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

// ── Errors ─────────────────────────────────────────────────────────────────────

/// Typed errors for this worker's fallible internal paths (replacing
/// stringly-typed `String` errors so callers can distinguish failure classes
/// instead of matching on formatted text). `queue::fail`'s `error_message`
/// column is `TEXT`, so a final `.to_string()` at that one boundary is
/// unavoidable — but every `Result` inside this module now carries a real
/// source error via `#[source]`/`#[from]`.
#[derive(Debug, thiserror::Error)]
enum SlackNameBackfillError {
    #[error("cannot deserialise JobKind from job payload: {0}")]
    InvalidPayload(#[source] serde_json::Error),
    #[error("expected SlackNameBackfill job kind, got {0:?}")]
    WrongJobKind(JobKind),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

// ── ID-shape detection (mirrors the frontend guard exactly) ───────────────────

/// True when `v` has the shape of a raw (unresolved) Slack user id: a leading
/// `U`/`W` followed by 8+ uppercase-alnum characters, nothing else — the
/// backend twin of `web/lib/coral.ts::isRawSlackUserId`
/// (`/^[UW][A-Z0-9]{8,}$/`). Deliberately conservative (uppercase-only, no
/// separators) so a real display name is never mistaken for a raw id.
pub(crate) fn is_id_shaped_user(v: &str) -> bool {
    is_id_shaped(v, &['U', 'W'])
}

/// True when `v` has the shape of a raw (unresolved) Slack channel id: a
/// leading `C`/`G`/`D` followed by 8+ uppercase-alnum characters — the
/// backend twin of `web/lib/coral.ts::isRawSlackChannelId`
/// (`/^[CGD][A-Z0-9]{8,}$/`).
pub(crate) fn is_id_shaped_channel(v: &str) -> bool {
    is_id_shaped(v, &['C', 'G', 'D'])
}

/// Shared shape check: exactly one leading char from `leading`, followed by
/// 8+ chars that are each ASCII uppercase or a digit (i.e. `[A-Z0-9]{8,}`),
/// anchored over the WHOLE string (no prefix/suffix slop) — equivalent to the
/// frontend's `^...$`-anchored regex.
fn is_id_shaped(v: &str, leading: &[char]) -> bool {
    let mut chars = v.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !leading.contains(&first) {
        return false;
    }
    let rest: Vec<char> = chars.collect();
    rest.len() >= 8
        && rest
            .iter()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

// ── Resolution-preference logic (pure, unit-testable) ─────────────────────────

/// Choose the repaired value for one ID-shaped field, preferring the
/// persisted `connector_identities` directory over a fresh connector API
/// lookup. Callers must pre-filter both inputs to `None` unless they hold a
/// genuine (non-ID-shaped) resolution — mirrors
/// `routes::slack::canonical_author_name`'s "never persist/use the raw-id
/// fallback" contract, generalized to repair. Returns `None` when neither
/// source produced anything usable, meaning the atom is left untouched for a
/// future pass to retry.
pub(crate) fn preferred_repair(
    directory_hit: Option<&str>,
    api_derived: Option<&str>,
) -> Option<String> {
    directory_hit.or(api_derived).map(str::to_string)
}

// ── Worker entry point ────────────────────────────────────────────────────────

/// One `SlackNameBackfill` pass on one tenant's `(pool, store)` pair:
/// dequeue up to [`JOB_BATCH_SIZE`] jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a queue worker
/// (`HarnessConfig::queue_worker`), which owns the loop, the empty-poll
/// sleep, per-tenant error backoff, and shutdown (see that module's doc for
/// the full contract — this worker no longer owns its own loop or shutdown
/// check, exactly like `workers::slack_ingest`/`workers::huddle`).
///
/// `control_pool` is the shared control/consumer pool — needed ONLY by the
/// account-less **seed** job's fan-out (see this module's doc comment);
/// every other read/write in this worker runs on `t.pool`/`t.store`.
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, http: &reqwest::Client) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["SlackNameBackfill"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(&t.pool, &t.tenant, control_pool, t.store.as_ref(), http, job).await;
    }
    Ok(Tick::worked(n))
}

// ── Job processing ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct BackfillFilter {
    graph_id: Option<Uuid>,
    workspace_id: Option<String>,
    author_id: Option<String>,
    after_atom_id: Option<Uuid>,
}

fn parse_filter(job: &JobRow) -> Result<BackfillFilter, SlackNameBackfillError> {
    let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
    let kind: JobKind =
        serde_json::from_value(tagged).map_err(SlackNameBackfillError::InvalidPayload)?;

    match kind {
        JobKind::SlackNameBackfill {
            graph_id,
            workspace_id,
            author_id,
            after_atom_id,
        } => Ok(BackfillFilter {
            graph_id,
            workspace_id,
            author_id,
            after_atom_id,
        }),
        other => Err(SlackNameBackfillError::WrongJobKind(other)),
    }
}

/// `pub` (like `workers::infer_expertise::run_expertise`) so integration
/// tests can drive one seed/child job against a real Postgres without going
/// through the worker loop.
pub async fn process_job(
    pool: &PgPool,
    tenant: &TenantId,
    control_pool: &PgPool,
    store: &dyn crate::store::Store,
    http: &reqwest::Client,
    job: JobRow,
) {
    let filter = match parse_filter(&job) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "slack-name-backfill: malformed job payload");
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // ── Per-job RLS scope dispatch (org-tenancy Phase W.2 / org-slack step 4
    // — module doc) ──────────────────────────────────────────────────────
    let account_id = match (job.account_id, filter.graph_id) {
        // Stamped job (natural repair, seed child, or continuation).
        (Some(account_id), _) => account_id,
        // Account-less all-accounts sweep ⇒ SEED: fan out per-account children.
        (None, None) => {
            fan_out_per_account(pool, tenant, control_pool, job.id, &filter).await;
            return;
        }
        // Account-less but graph-scoped: post-W.1 unreachable through dequeue
        // (backfilled or dead-lettered there) — refuse to process unscoped.
        (None, Some(_)) => {
            tracing::error!(job_id = %job.id, kind = %job.kind, error = UNSCOPED_JOB_ERR, "slack-name-backfill: refusing unscoped graph-scoped job");
            if let Err(qe) = queue::fail(pool, job.id, UNSCOPED_JOB_ERR).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark unscoped job as failed");
            }
            return;
        }
    };

    // `tenant` is threaded down to the bot-token read (org-plane DEK fix):
    // the token's ROW is on `control_pool`, but for an org account its KEY is
    // in that org's tenant `account_keys` — exactly the database `pool`
    // already points at here. See `workspace_candidates`.
    match run_batch(
        pool,
        control_pool,
        tenant,
        store,
        http,
        account_id,
        &filter,
    )
    .await
    {
        Ok(outcome) => {
            tracing::debug!(
                job_id = %job.id,
                examined = outcome.examined,
                resolved = outcome.resolved,
                unresolved = outcome.unresolved,
                batch_full = outcome.batch_full,
                "slack-name-backfill: batch complete"
            );

            // ALWAYS advance the cursor and continue the sweep when more rows
            // are likely to remain (batch was full) — regardless of whether
            // THIS window made any repair progress. See module docs: this is
            // what stops a stuck head-of-line window from permanently
            // blocking every later candidate.
            if outcome.batch_full {
                if let Some(cursor) = outcome.last_atom_id {
                    // Key includes the account so two accounts' independent
                    // sweeps can never dedup each other (org-tenancy Phase
                    // W.2 — every continuation is per-account now).
                    let idempotency = format!(
                        "slack_name_backfill:{account_id}:{:?}:{:?}:{:?}:{cursor}",
                        filter.graph_id, filter.workspace_id, filter.author_id
                    );
                    if let Err(e) = queue::enqueue(
                        pool,
                        &JobKind::SlackNameBackfill {
                            graph_id: filter.graph_id,
                            workspace_id: filter.workspace_id.clone(),
                            author_id: filter.author_id.clone(),
                            after_atom_id: Some(cursor),
                        },
                        // Propagate this job's own account scope (org-tenancy
                        // Phase W.2 — see the module doc's RLS section).
                        Some(account_id),
                        Some(&idempotency),
                        CONTINUATION_DELAY_SECS,
                    )
                    .await
                    {
                        // A silently-dropped continuation here would
                        // permanently strand every atom beyond this window
                        // even though this batch itself may have repaired
                        // plenty — fail THIS job instead of completing it, so
                        // its own bounded retry gets another chance to
                        // enqueue the continuation (cheap to retry: every
                        // atom this pass already fixed drops out of the next
                        // attempt's WHERE filter). Return immediately — the
                        // unresolved-fields check below is skipped on this
                        // path since the job is already going through
                        // `queue::fail`.
                        tracing::error!(job_id = %job.id, error = %e, "slack-name-backfill: failed to enqueue continuation job — failing this job to retry");
                        if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                            tracing::error!(error = %qe, job_id = %job.id, "failed to mark job failed after continuation enqueue failure");
                        }
                        return;
                    }
                }
            }

            if outcome.unresolved > 0 {
                // At least one atom in this window still has an ID-shaped
                // field after this pass — whether NO atom was touched, or an
                // atom was only PARTIALLY repaired (e.g. its author resolved
                // but its channel didn't). Route THIS job through the
                // queue's normal bounded backoff instead of silently
                // completing it — a few retries give a transient failure
                // (Slack API blip, directory read hiccup) a chance to
                // self-heal; once `max_attempts` is exhausted the job dies
                // rather than looping forever, which also bounds atoms that
                // are genuinely, permanently unresolvable right now. The
                // sweep's forward progress does not depend on this outcome —
                // the continuation above (if any) was already enqueued.
                if let Err(e) = queue::fail(
                    pool,
                    job.id,
                    "batch left one or more ID-shaped fields unresolved — retrying with backoff",
                )
                .await
                {
                    tracing::error!(error = %e, job_id = %job.id, "slack-name-backfill: failed to mark incomplete-progress job for retry");
                }
            } else {
                // `unresolved == 0`: `resolved == 0` means this window's
                // candidates came back empty — a legitimate sweep no-op.
                let job_outcome = if outcome.resolved > 0 {
                    queue::JobOutcome::DidWork
                } else {
                    queue::JobOutcome::NothingToDo
                };
                if let Err(e) = queue::complete(pool, job.id, job_outcome).await {
                    tracing::error!(error = %e, job_id = %job.id, "slack-name-backfill: failed to complete job");
                }
            }
        }
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "slack-name-backfill: hard error — failing job for retry");
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark slack-name-backfill job failed");
            }
        }
    }
}

/// SEED handling for the account-less all-accounts sweep: resolve which
/// account(s) this sweep should fan out to and enqueue one per-account child
/// (same payload filters, `jobs.account_id` stamped) onto `pool` — the SAME
/// plane the seed was dequeued from. Any failure fails the seed for a
/// bounded queue retry; per-account idempotency keys make a retried seed
/// re-enqueue only the children that didn't make it. Same shape as
/// `workers::content_tsv_backfill::fan_out_per_account`.
///
/// The account roster itself is plane-dependent (org-slack step 4): on the
/// consumer plane it is every row in `accounts` (control table, needs no
/// scope — see module doc); on an org tenant there is exactly ONE account
/// (that tenant's own), resolved from the control plane's `tenant_databases`
/// routing row rather than assumed — `t.tenant`'s Clerk org id is the only
/// thing this worker is handed, not the account uuid.
async fn fan_out_per_account(
    pool: &PgPool,
    tenant: &TenantId,
    control_pool: &PgPool,
    job_id: Uuid,
    filter: &BackfillFilter,
) {
    let fail_seed = |msg: String| async move {
        tracing::error!(job_id = %job_id, error = %msg, "slack-name-backfill: seed fan-out failed — failing seed job to retry");
        if let Err(qe) = queue::fail(pool, job_id, &msg).await {
            tracing::error!(error = %qe, job_id = %job_id, "failed to mark seed job as failed");
        }
    };

    let account_ids: Vec<Uuid> = match tenant {
        // Control-table read, needs no scope (see module doc).
        TenantId::Consumer => {
            match sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
                .fetch_all(control_pool)
                .await
            {
                Ok(ids) => ids,
                Err(e) => {
                    fail_seed(format!("seed account enumeration failed: {e}")).await;
                    return;
                }
            }
        }
        // An org tenant serves exactly one account — its own. Resolved from
        // the control-plane routing row (`tenant_databases`), the same
        // "resolve the org's account_id from control before working on
        // tenant" pattern `workers::member_ocean_reconciler::tick` uses.
        TenantId::Org(org_id) => {
            match sqlx::query_scalar::<_, Uuid>(
                "SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1",
            )
            .bind(org_id.as_str())
            .fetch_optional(control_pool)
            .await
            {
                Ok(Some(id)) => vec![id],
                Ok(None) => {
                    // Defensive only: the harness never schedules a tenant
                    // without an active routing row.
                    fail_seed(format!(
                        "seed account resolution failed: no tenant_databases row for org {org_id}"
                    ))
                    .await;
                    return;
                }
                Err(e) => {
                    fail_seed(format!("seed account resolution failed: {e}")).await;
                    return;
                }
            }
        }
    };

    for account_id in &account_ids {
        let cursor_label = filter
            .after_atom_id
            .map(|c| c.to_string())
            .unwrap_or_else(|| "start".to_string());
        let idempotency = format!(
            "slack_name_backfill:{account_id}:{:?}:{:?}:{:?}:{cursor_label}",
            filter.graph_id, filter.workspace_id, filter.author_id
        );
        if let Err(e) = queue::enqueue(
            pool,
            &JobKind::SlackNameBackfill {
                graph_id: filter.graph_id,
                workspace_id: filter.workspace_id.clone(),
                author_id: filter.author_id.clone(),
                after_atom_id: filter.after_atom_id,
            },
            Some(*account_id),
            Some(&idempotency),
            CONTINUATION_DELAY_SECS,
        )
        .await
        {
            fail_seed(format!("seed child enqueue failed for account {account_id}: {e}")).await;
            return;
        }
    }

    tracing::info!(
        job_id = %job_id,
        accounts = account_ids.len(),
        "slack-name-backfill: seed fanned out one per-account child job per account"
    );
    if let Err(e) = queue::complete(pool, job_id, queue::JobOutcome::DidWork).await {
        tracing::error!(error = %e, job_id = %job_id, "slack-name-backfill: failed to complete seed job");
    }
}

struct BatchOutcome {
    examined: usize,
    /// Atoms where AT LEAST ONE field was successfully rewritten this pass
    /// (may still have OTHER fields left ID-shaped — see `unresolved`).
    resolved: usize,
    /// Atoms that, after this pass, STILL have at least one ID-shaped field
    /// (whether nothing about them changed, or they were only partially
    /// repaired). Drives the retry decision in [`process_job`] — see module
    /// docs.
    unresolved: usize,
    batch_full: bool,
    /// `atom_id` of the LAST row examined this pass (by `ORDER BY atom_id`),
    /// used as the next continuation's `after_atom_id` cursor. `None` when
    /// the batch was empty.
    last_atom_id: Option<Uuid>,
}

/// Candidate Slack row still carrying an ID-shaped value somewhere.
struct AtomRow {
    atom_id: Uuid,
    graph_id: Uuid,
    /// Originating Slack workspace (migration 0058). `None` on atoms ingested
    /// before that column existed.
    workspace_id: Option<String>,
    channel_id: Option<String>,
    channel_name: Option<String>,
    author_id: Option<String>,
    author_name: Option<String>,
    participants: Vec<String>,
}

/// Which workspace(s) to try resolving a graph's Slack ids against — a
/// tenant/graph-scoped join through `graphs -> slack_integrations`, never a
/// bare table scan.
#[derive(Clone)]
struct WorkspaceCtx {
    account_id: Uuid,
    workspace_id: String,
    /// Foundation PR 1.5: resolved from the encrypted `oauth_tokens` copy
    /// (CONTROL-plane), not `slack_integrations.bot_token` directly. `None`
    /// when no account-wide `oauth_tokens` row exists yet for this workspace
    /// (not backfilled, DEK/decrypt failure) — soft: [`resolve_user`]/
    /// [`resolve_channel`] simply skip the Slack-API fallback for that
    /// candidate and fall through to the next one (or to "unresolved"),
    /// exactly like any other Slack API failure this module already treats
    /// as soft.
    bot_token: Option<String>,
}

/// Process one bounded batch of candidate atoms for `filter`. Returns `Err`
/// only for a hard DB/store error (which fails the whole job for retry, and
/// is safe to retry since every atom already written this pass is no longer
/// ID-shaped and drops out of the next attempt's candidate set); individual
/// Slack API failures are soft — logged and skipped so one bad token never
/// blocks the rest of the batch.
#[allow(clippy::too_many_arguments)]
async fn run_batch(
    pool: &PgPool,
    control_pool: &PgPool,
    tenant: &TenantId,
    store: &dyn crate::store::Store,
    http: &reqwest::Client,
    account_id: Uuid,
    filter: &BackfillFilter,
) -> Result<BatchOutcome, SlackNameBackfillError> {
    let rows = load_candidate_atoms(pool, account_id, filter, ATOM_BATCH_SIZE).await?;
    let examined = rows.len();
    let batch_full = rows.len() as i64 == ATOM_BATCH_SIZE;
    let last_atom_id = rows.last().map(|r| r.atom_id);

    // Per-job memoization so repeated raw ids (the overwhelmingly common
    // case — the same author across many atoms) cost one directory lookup
    // + at most one Slack API call each, not one per atom. Keyed by
    // (workspace_id, raw_id); `None` means "tried and still unresolved".
    let mut resolved_names: HashMap<(String, String), Option<String>> = HashMap::new();
    // Workspace candidates depend on BOTH the graph and the effective
    // workspace scope for a given atom (tenancy — see the per-row `scope`
    // computation below), so the cache key includes both.
    let mut workspace_cache: HashMap<(Uuid, Option<String>), Vec<WorkspaceCtx>> = HashMap::new();

    let mut resolved = 0usize;
    let mut unresolved = 0usize;

    for row in &rows {
        // ── Workspace scoping (tenancy) ───────────────────────────────────
        // Prefer the atom's OWN recorded workspace (migration 0058) — that's
        // ground truth, and lets even a fully-unscoped sweep resolve each
        // atom against exactly the workspace it actually came from instead
        // of guessing across every Slack integration the account has. Only
        // pre-migration atoms with no recorded workspace fall back to the
        // job's own `workspace_id` filter (if any), then to every one of the
        // account's integrations — but see the `contexts.len() > 1` check
        // just below: if that leaves MORE than one candidate, the atom is
        // ambiguous and is skipped rather than guessed.
        let scope = row
            .workspace_id
            .clone()
            .or_else(|| filter.workspace_id.clone());
        let cache_key = (row.graph_id, scope.clone());
        let contexts = match workspace_cache.get(&cache_key) {
            Some(c) => c.clone(),
            None => {
                let ctxs = workspace_candidates(
                    pool,
                    control_pool,
                    tenant,
                    account_id,
                    row.graph_id,
                    scope.as_deref(),
                )
                .await?;
                workspace_cache.insert(cache_key, ctxs.clone());
                ctxs
            }
        };

        if contexts.is_empty() {
            tracing::debug!(atom_id = %row.atom_id, graph_id = %row.graph_id, "slack-name-backfill: no Slack workspace context available — skipping atom this pass");
            unresolved += 1;
            continue;
        }

        // Never guess a workspace (CodeRabbit review, PR #181 round 2): this
        // is only reachable when `scope` is `None` (fully-unscoped sweep)
        // AND the atom has no recorded workspace of its own AND its account
        // has more than one Slack integration — a scoped sweep or a
        // single-integration account always yields exactly one candidate
        // (`workspace_candidates`' `si.workspace_id = $2` filter, or there
        // simply being only one row). Picking any one of several candidates
        // here would risk writing a name from the WRONG workspace's
        // directory/API — actively incorrect, not just still-unresolved — so
        // this atom is left untouched for a future pass that can either
        // scope explicitly or recover its workspace provenance.
        if contexts.len() > 1 {
            tracing::debug!(atom_id = %row.atom_id, graph_id = %row.graph_id, candidates = contexts.len(), "slack-name-backfill: ambiguous legacy atom (multiple candidate workspaces) — skipping rather than guessing");
            unresolved += 1;
            continue;
        }

        let mut new_author_name: Option<String> = None;
        let mut new_channel_name: Option<String> = None;
        let mut new_participants: Option<Vec<String>> = None;

        // ── Author repair ─────────────────────────────────────────────────
        let author_still_bad = if let Some(author_name) = row.author_name.as_deref() {
            if is_id_shaped_user(author_name) {
                // `author_id` is the stable structured column (migration
                // 0034); some pre-0034 atoms never got it backfilled, in
                // which case the field value itself IS the raw id (that's
                // exactly why it's ID-shaped).
                let raw_id = row
                    .author_id
                    .clone()
                    .unwrap_or_else(|| author_name.to_string());
                match resolve_user(store, http, &contexts, &raw_id, &mut resolved_names).await? {
                    Some(name) => {
                        new_author_name = Some(name);
                        false
                    }
                    None => true,
                }
            } else {
                false
            }
        } else {
            false
        };

        // ── Channel repair ─────────────────────────────────────────────────
        let channel_still_bad = if let Some(channel_name) = row.channel_name.as_deref() {
            if is_id_shaped_channel(channel_name) {
                let raw_id = row
                    .channel_id
                    .clone()
                    .unwrap_or_else(|| channel_name.to_string());
                match resolve_channel(http, &contexts, &raw_id, &mut resolved_names).await {
                    Some(name) => {
                        new_channel_name = Some(name);
                        false
                    }
                    None => true,
                }
            } else {
                false
            }
        } else {
            false
        };

        // ── Participants repair ────────────────────────────────────────────
        let mut participants_still_bad = false;
        if row.participants.iter().any(|p| is_id_shaped_user(p)) {
            let mut repaired = row.participants.clone();
            let mut any_changed = false;
            for p in repaired.iter_mut() {
                if is_id_shaped_user(p) {
                    match resolve_user(store, http, &contexts, p, &mut resolved_names).await? {
                        Some(name) => {
                            *p = name;
                            any_changed = true;
                        }
                        None => participants_still_bad = true,
                    }
                }
            }
            if any_changed {
                new_participants = Some(repaired);
            }
        }

        if new_author_name.is_some() || new_channel_name.is_some() || new_participants.is_some() {
            // Propagate a write failure instead of silently logging past it:
            // a transient UPDATE error would otherwise leave this atom
            // "examined but never resolved" with no retry. Failing the whole
            // batch here is safe to retry — every atom already written this
            // pass is no longer ID-shaped and simply drops out of the next
            // attempt's candidate set (idempotent).
            apply_repair(
                pool,
                account_id,
                row.atom_id,
                new_author_name,
                new_channel_name,
                new_participants,
            )
            .await?;
            resolved += 1;
        }

        // Track whether this atom STILL has any ID-shaped field after this
        // pass — drives the retry decision in `process_job` (CodeRabbit
        // review, PR #181 round 2: a PARTIAL repair, e.g. author fixed but
        // channel not, must not silently look like full success).
        if author_still_bad || channel_still_bad || participants_still_bad {
            unresolved += 1;
        }
    }

    Ok(BatchOutcome {
        examined,
        resolved,
        unresolved,
        batch_full,
        last_atom_id,
    })
}

/// Resolve a raw Slack user id to a real display name, preferring the
/// `connector_identities` directory over a fresh `users.info` call, trying
/// each candidate workspace in order (memoized per job run).
///
/// A directory READ failure is propagated (`Err`), never silently treated as
/// "identity absent": doing so would let the Slack API fallback below
/// overwrite the atom with a possibly-DIFFERENT name than whatever canonical
/// value is already on file (which this call simply failed to read this
/// round) — a real risk of diverging from the "one canonical name per human"
/// invariant CS-ER Part B established. Propagating instead fails the whole
/// batch through the caller's normal DB-error retry path.
async fn resolve_user(
    store: &dyn crate::store::Store,
    http: &reqwest::Client,
    contexts: &[WorkspaceCtx],
    raw_id: &str,
    cache: &mut HashMap<(String, String), Option<String>>,
) -> Result<Option<String>, SlackNameBackfillError> {
    for ctx in contexts {
        let key = (ctx.workspace_id.clone(), raw_id.to_string());
        if let Some(cached) = cache.get(&key) {
            if let Some(name) = cached {
                return Ok(Some(name.clone()));
            }
            continue; // memoized miss for this (workspace, id) — try the next candidate workspace
        }

        let directory_hit = store
            .connector_identity_name("slack", &ctx.workspace_id, raw_id)
            .await?
            .filter(|n| !is_id_shaped_user(n));

        let api_derived = if directory_hit.is_none() {
            match ctx.bot_token.as_deref() {
                None => None, // no resolvable bot token this pass — API fallback unavailable
                Some(bot_token) => {
                    let derived = resolve_user_name(http, ctx.account_id, raw_id, bot_token).await;
                    if is_id_shaped_user(&derived) {
                        None // still the raw id — API resolution failed or has no data
                    } else {
                        // Persist so future ingest/backfill passes are stable (mirrors
                        // `routes::slack::canonical_author_name`'s persist-on-genuine-name rule).
                        if let Err(e) = store
                            .upsert_connector_identity("slack", &ctx.workspace_id, raw_id, &derived)
                            .await
                        {
                            tracing::warn!(error = %e, raw_id = %raw_id, "slack-name-backfill: failed to persist connector identity (non-fatal)");
                        }
                        Some(derived)
                    }
                }
            }
        } else {
            None
        };

        let result = preferred_repair(directory_hit.as_deref(), api_derived.as_deref());
        cache.insert(key, result.clone());
        if result.is_some() {
            return Ok(result);
        }
    }
    Ok(None)
}

/// Resolve a raw Slack channel id to a real channel name. There is no
/// identity-directory table for channels (only `connector_identities`, which
/// is keyed for people — see migration 0049), so this always falls back to
/// the Slack API, trying each candidate workspace in order.
async fn resolve_channel(
    http: &reqwest::Client,
    contexts: &[WorkspaceCtx],
    raw_id: &str,
    cache: &mut HashMap<(String, String), Option<String>>,
) -> Option<String> {
    for ctx in contexts {
        let key = (format!("channel:{}", ctx.workspace_id), raw_id.to_string());
        if let Some(cached) = cache.get(&key) {
            if let Some(name) = cached {
                return Some(name.clone());
            }
            continue;
        }

        let Some(bot_token) = ctx.bot_token.as_deref() else {
            continue; // no resolvable bot token this pass — try the next candidate workspace
        };
        let derived = resolve_channel_name(http, ctx.account_id, raw_id, bot_token).await;
        let result = if is_id_shaped_channel(&derived) {
            None
        } else {
            Some(derived)
        };
        cache.insert(key, result.clone());
        if result.is_some() {
            return result;
        }
    }
    None
}

// ── DB helpers ─────────────────────────────────────────────────────────────────

/// Fetch up to `limit` Slack atoms whose `author_name` / `channel_name` /
/// `participants` still contain an ID-shaped value, scoped by `filter`.
/// Tenant/graph-scoped: `source_type = 'slack'` always, plus the optional
/// `graph_id` / `author_id` / `after_atom_id` (keyset cursor) filters —
/// never an unscoped table-wide query unless the caller explicitly asked for
/// a full sweep (`graph_id`/`author_id` both `None`).
///
/// The author filter also matches legacy rows where `author_id` is NULL
/// (atoms ingested before migration 0034/0052) whose `author_name` equals
/// the requested id directly — those rows never got the structured
/// `author_id` column backfilled, but the raw id is still exactly what's
/// sitting in `author_name` (see the `author_name`-as-fallback-raw-id
/// handling in [`run_batch`]). Without this an author-scoped natural-repair
/// job (which always supplies `author_id`) would silently skip every one of
/// that author's pre-migration atoms.
async fn load_candidate_atoms(
    pool: &PgPool,
    account_id: Uuid,
    filter: &BackfillFilter,
    limit: i64,
) -> Result<Vec<AtomRow>, StoreError> {
    // RLS scope (org-tenancy Phase W.2 / org-slack step 4): `atoms`/`graphs`
    // are RLS-covered on BOTH planes, so this runs inside a
    // `begin_tenant_system_scoped` transaction — the explicit `$6` account
    // predicate is the primary scoping mechanism even pre-OPS-4/pre-tenant-
    // RLS (see module doc).
    let mut tx = begin_tenant_system_scoped(pool, account_id).await?;
    let rows = sqlx::query(
        r#"
        SELECT atom_id, graph_id, workspace_id, channel_id, channel_name,
               author_id, author_name, participants
        FROM   atoms
        WHERE  source_type = 'slack'
          AND  graph_id IN (SELECT id FROM graphs WHERE account_id = $6)
          AND  ($1::uuid IS NULL OR graph_id = $1)
          AND  (
                 $2::text IS NULL
              OR author_id = $2
              OR (author_id IS NULL AND author_name = $2)
               )
          AND  ($4::uuid IS NULL OR atom_id > $4)
          AND  (
                 -- Tenancy (finding #7): a workspace-scoped sweep must never
                 -- touch an atom from a DIFFERENT workspace on the same
                 -- account. Exact match is the common case; the one carve-out
                 -- is a pre-0058 atom with no recorded workspace_id AND whose
                 -- account has exactly ONE Slack integration total — there is
                 -- only one workspace it could possibly belong to, so
                 -- treating it as "matching" is safe. A multi-workspace
                 -- account's legacy atoms are excluded here (ambiguous) and
                 -- only reachable via a fully-unscoped sweep ($5 IS NULL)
                 -- below, which resolves them via the atom's-own-workspace
                 -- preference / best-effort fallback in `run_batch`.
                 $5::text IS NULL
              OR workspace_id = $5
              OR (
                   workspace_id IS NULL
                   AND (
                     SELECT COUNT(*) FROM slack_integrations si
                     JOIN   graphs g ON g.account_id = si.account_id
                     WHERE  g.id = atoms.graph_id
                   ) = 1
                 )
               )
          AND  (
                 (author_name IS NOT NULL AND author_name ~ '^[UW][A-Z0-9]{8,}$')
              OR (channel_name IS NOT NULL AND channel_name ~ '^[CGD][A-Z0-9]{8,}$')
              OR EXISTS (
                   SELECT 1 FROM unnest(participants) AS p
                   WHERE p ~ '^[UW][A-Z0-9]{8,}$'
                 )
               )
        ORDER BY atom_id
        LIMIT  $3
        "#,
    )
    .bind(filter.graph_id)
    .bind(filter.author_id.as_deref())
    .bind(limit)
    .bind(filter.after_atom_id)
    .bind(filter.workspace_id.as_deref())
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(rows
        .into_iter()
        .map(|r| AtomRow {
            atom_id: r.get("atom_id"),
            graph_id: r.get("graph_id"),
            workspace_id: r.get("workspace_id"),
            channel_id: r.get("channel_id"),
            channel_name: r.get("channel_name"),
            author_id: r.get("author_id"),
            author_name: r.get("author_name"),
            participants: r.get("participants"),
        })
        .collect())
}

/// Which Slack workspace(s) to try for a given graph's atoms: join through
/// `graphs -> slack_integrations` on the graph's owning account (tenant-scoped
/// — never a bare `slack_integrations` scan). When `scope` is set (either the
/// atom's own recorded `workspace_id`, migration 0058, or the job's
/// `workspace_id` filter as a fallback for legacy atoms with no recorded
/// workspace), scope to exactly that one row — atoms without matching
/// workspace provenance never reach this function with a mismatched `scope`
/// in the first place, since `load_candidate_atoms` only returns rows whose
/// `workspace_id` already equals the requested filter when one is given.
/// When `scope` is `None` (fully-unscoped sweep over a legacy atom with no
/// recorded workspace), returns every workspace the account has connected,
/// ordered oldest-first, so the caller can try each in turn (best-effort for
/// the rare multi-workspace account on that legacy-only path).
///
/// `control_pool` (Foundation PR 1.5): the bot token itself is now resolved
/// from the encrypted `oauth_tokens` copy — a CONTROL-plane table, like
/// `slack_integrations` — via `workers::huddle::get_slack_bot_token_for_workspace`,
/// not read out of this function's own `slack_integrations` join. `pool`
/// stays as the plane `graphs`/`slack_integrations` are queried against
/// (unchanged from before this PR); `control_pool` is passed down from
/// `process_job` purely for the token decrypt, mirroring the `control_pool`
/// split `routes::slack_events::ingest_slack_message` already threads for
/// the identical reason (see that function's own doc comment).
async fn workspace_candidates(
    pool: &PgPool,
    control_pool: &PgPool,
    tenant: &TenantId,
    account_id: Uuid,
    graph_id: Uuid,
    scope: Option<&str>,
) -> Result<Vec<WorkspaceCtx>, StoreError> {
    // RLS scope (org-tenancy Phase W.2 / org-slack step 4): `graphs` is
    // policy-covered on BOTH planes (`slack_integrations` on neither); the
    // join through the job account's own graph already tenant-scopes the
    // read app-side, RLS is the backstop — but on an org tenant that
    // backstop requires `begin_tenant_system_scoped`, not the plain
    // `begin_account_scoped` that was sufficient when this only ran on the
    // consumer plane (see module doc).
    let mut tx = begin_tenant_system_scoped(pool, account_id).await?;
    let rows = sqlx::query(
        r#"
        SELECT si.workspace_id, g.account_id
        FROM   slack_integrations si
        JOIN   graphs g ON g.account_id = si.account_id
        WHERE  g.id = $1
          AND  ($2::text IS NULL OR si.workspace_id = $2)
        ORDER BY si.created_at ASC
        "#,
    )
    .bind(graph_id)
    .bind(scope)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;

    let mut contexts = Vec::with_capacity(rows.len());
    for r in rows {
        let ctx_account_id: Uuid = r.get("account_id");
        let workspace_id: String = r.get("workspace_id");
        // ROW on control, KEY on this tick's own plane (org-plane DEK fix).
        // Built per row from `ctx_account_id` — the account the query itself
        // returned — rather than from the job's `account_id`, so this makes no
        // assumption about the two being equal; `tenant`/`pool` are the
        // `WorkerTenant` pair, i.e. the org's tenant DB where `account_keys`
        // lives, or the control pool on Consumer.
        let row_plane = crate::crypto::DekPlane::from_tenant(tenant, pool.clone(), ctx_account_id);
        let bot_token = crate::workers::huddle::get_slack_bot_token_for_workspace(
            control_pool,
            &row_plane,
            ctx_account_id,
            &workspace_id,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(
                error = %e,
                account_id = %ctx_account_id,
                workspace_id = %workspace_id,
                "slack-name-backfill: failed to resolve bot token from oauth_tokens (non-fatal — \
                 API-fallback name resolution unavailable for this workspace this pass)"
            );
            None
        });
        contexts.push(WorkspaceCtx { account_id: ctx_account_id, workspace_id, bot_token });
    }
    Ok(contexts)
}

/// Rewrite the repaired fields on one atom in place. `None` for a field means
/// "leave it untouched" (`COALESCE` against the current value) — callers only
/// pass `Some` for fields they actually resolved this pass.
async fn apply_repair(
    pool: &PgPool,
    account_id: Uuid,
    atom_id: Uuid,
    author_name: Option<String>,
    channel_name: Option<String>,
    participants: Option<Vec<String>>,
) -> Result<(), StoreError> {
    // RLS scope (org-tenancy Phase W.2 / org-slack step 4): the atom id came
    // out of this job's own scoped candidate read, and the write re-runs
    // under the same tenant-system scope (`atoms` is RLS-covered on both
    // planes — see module doc).
    let mut tx = begin_tenant_system_scoped(pool, account_id).await?;
    sqlx::query(
        r#"
        UPDATE atoms
        SET    author_name  = COALESCE($2::text, author_name),
               channel_name = COALESCE($3::text, channel_name),
               participants = COALESCE($4::text[], participants)
        WHERE  atom_id = $1
        "#,
    )
    .bind(atom_id)
    .bind(author_name)
    .bind(channel_name)
    .bind(participants)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── ID-shape detection ─────────────────────────────────────────────────────

    #[test]
    fn detects_raw_slack_user_ids() {
        assert!(is_id_shaped_user("U57C3A9EF09"));
        assert!(is_id_shaped_user("W0123456789"));
    }

    #[test]
    fn detects_raw_slack_channel_ids() {
        assert!(is_id_shaped_channel("C0ABC123XY"));
        assert!(is_id_shaped_channel("G012345678"));
        assert!(is_id_shaped_channel("D012345678"));
    }

    #[test]
    fn real_names_are_never_flagged() {
        assert!(!is_id_shaped_user("Alex Example"));
        assert!(!is_id_shaped_user("sam"));
        assert!(!is_id_shaped_channel("general"));
        assert!(!is_id_shaped_channel("eng-backend"));
        // A short all-caps acronym-y name must not false-positive (< 9 chars total).
        assert!(!is_id_shaped_user("USA"));
    }

    #[test]
    fn wrong_leading_char_is_not_flagged() {
        // Users use U/W; channels use C/G/D — cross-checking the wrong set
        // must not match (e.g. a channel id fed to the user check).
        assert!(!is_id_shaped_user("C0ABC123XY"));
        assert!(!is_id_shaped_channel("U57C3A9EF09"));
    }

    #[test]
    fn lowercase_or_mixed_case_is_not_flagged() {
        // Conservative on purpose (same doctrine as the frontend guard):
        // lowercase anywhere in the tail means it's a real name, not a raw id.
        assert!(!is_id_shaped_user("U0b9skc77s8"));
        assert!(!is_id_shaped_channel("C0abc123xy"));
    }

    #[test]
    fn too_short_is_not_flagged() {
        // Exactly the boundary: 8 trailing chars required ({8,}), 7 must fail.
        assert!(!is_id_shaped_user("U1234567")); // 7 trailing chars
        assert!(is_id_shaped_user("U12345678")); // 8 trailing chars — boundary passes
    }

    #[test]
    fn anchored_over_the_whole_string() {
        // A real name that merely CONTAINS an id-shaped substring must not
        // match — the check is anchored start-to-end, like the frontend's
        // `^...$` regex.
        assert!(!is_id_shaped_user("re: U57C3A9EF09 thread"));
        assert!(!is_id_shaped_user("U57C3A9EF09 "));
    }

    // ── Resolution-preference logic (directory over API) ──────────────────────

    #[test]
    fn directory_hit_wins_over_api_result() {
        let result = preferred_repair(Some("Alex Example"), Some("Alex S."));
        assert_eq!(result, Some("Alex Example".to_string()));
    }

    #[test]
    fn api_result_used_on_directory_miss() {
        let result = preferred_repair(None, Some("Alex Example"));
        assert_eq!(result, Some("Alex Example".to_string()));
    }

    #[test]
    fn neither_source_resolved_leaves_atom_untouched() {
        let result = preferred_repair(None, None);
        assert_eq!(result, None);
    }

    // ── JobKind payload round-trip ──────────────────────────────────────────────

    #[test]
    fn slack_name_backfill_roundtrip_fully_scoped() {
        let graph_id = Uuid::new_v4();
        let cursor = Uuid::new_v4();
        let kind = JobKind::SlackNameBackfill {
            graph_id: Some(graph_id),
            workspace_id: Some("T12345678".to_string()),
            author_id: Some("U57C3A9EF09".to_string()),
            after_atom_id: Some(cursor),
        };
        let json = serde_json::to_value(&kind).unwrap();
        assert_eq!(json["kind"], "SlackNameBackfill");
        assert_eq!(json["payload"]["graph_id"], graph_id.to_string());
        assert_eq!(json["payload"]["workspace_id"], "T12345678");
        assert_eq!(json["payload"]["author_id"], "U57C3A9EF09");
        assert_eq!(json["payload"]["after_atom_id"], cursor.to_string());

        let decoded: JobKind = serde_json::from_value(json).unwrap();
        match decoded {
            JobKind::SlackNameBackfill {
                graph_id: g,
                workspace_id: w,
                author_id: a,
                after_atom_id: c,
            } => {
                assert_eq!(g, Some(graph_id));
                assert_eq!(w.as_deref(), Some("T12345678"));
                assert_eq!(a.as_deref(), Some("U57C3A9EF09"));
                assert_eq!(c, Some(cursor));
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn slack_name_backfill_roundtrip_fully_unscoped() {
        // The one-shot standalone shape: all filters absent — a full sweep.
        let kind = JobKind::SlackNameBackfill {
            graph_id: None,
            workspace_id: None,
            author_id: None,
            after_atom_id: None,
        };
        let json = serde_json::to_value(&kind).unwrap();
        assert_eq!(json["kind"], "SlackNameBackfill");
        assert!(json["payload"]["graph_id"].is_null());
        assert!(json["payload"]["workspace_id"].is_null());
        assert!(json["payload"]["author_id"].is_null());
        assert!(json["payload"]["after_atom_id"].is_null());

        let decoded: JobKind = serde_json::from_value(json).unwrap();
        match decoded {
            JobKind::SlackNameBackfill {
                graph_id,
                workspace_id,
                author_id,
                after_atom_id,
            } => {
                assert!(graph_id.is_none());
                assert!(workspace_id.is_none());
                assert!(author_id.is_none());
                assert!(after_atom_id.is_none());
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn parse_filter_wrong_kind_errors() {
        let kind = JobKind::RouteCopresence {
            session_id: Uuid::new_v4(),
        };
        let tagged = serde_json::to_value(&kind).unwrap();
        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };
        assert!(parse_filter(&job).is_err());
    }

    #[test]
    fn parse_filter_roundtrip() {
        let graph_id = Uuid::new_v4();
        let kind = JobKind::SlackNameBackfill {
            graph_id: Some(graph_id),
            workspace_id: Some("T1".to_string()),
            author_id: None,
            after_atom_id: None,
        };
        let tagged = serde_json::to_value(&kind).unwrap();
        let job = JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id: None,
        };
        let filter = parse_filter(&job).unwrap();
        assert_eq!(filter.graph_id, Some(graph_id));
        assert_eq!(filter.workspace_id.as_deref(), Some("T1"));
        assert_eq!(filter.author_id, None);
        assert_eq!(filter.after_atom_id, None);
    }

    // ── Live Postgres: run_batch repairs via the directory and is idempotent ──
    //
    // Gated on RELINFER_TEST_DATABASE_URL, same convention as `jobs::queue`'s
    // live debounce/reaper tests. This worker's core logic (`run_batch` and
    // friends) is private to the crate — there is nothing `pub` to call from
    // a separate `backend/tests/*_pg.rs` integration test, so the live test
    // lives here instead, gated the same way.
    //
    // Deliberately exercises only the DIRECTORY-hit path (no Slack API call)
    // so the test stays hermetic — no real network request to slack.com from
    // CI. The API-fallback branch (`resolve_user_name` / `resolve_channel_name`)
    // already has its own coverage in `routes::slack`'s tests; here it's a
    // thin, already-proven pass-through.
    #[tokio::test]
    async fn run_batch_resolves_via_directory_and_is_idempotent_against_real_postgres() {
        use crate::store::{PostgresStore, Store};

        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live slack-name-backfill test");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        // ── Seed tenancy chain: user → account → graph ────────────────────────
        let email = format!("{}@test.local", Uuid::new_v4());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
                .bind(&email)
                .fetch_one(&pool)
                .await
                .expect("insert user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("insert account");

        let store = PostgresStore::new(pool.clone());
        let graph = store
            .create_graph(account_id, "slack-backfill-e2e", "personal")
            .await
            .expect("create graph");
        let graph_id = graph.id;

        let workspace_id = format!("T{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, $2, 'Test WS', 'xoxb-test-token')",
        )
        .bind(account_id)
        .bind(&workspace_id)
        .execute(&pool)
        .await
        .expect("insert slack_integrations");

        let raw_user_id = "U0TESTUSER1";
        store
            .upsert_connector_identity("slack", &workspace_id, raw_user_id, "Test Person")
            .await
            .expect("seed connector identity");

        // Atom carrying the raw id in both author_name and participants (channel
        // left NULL so the channel-repair/API path is never reached — see doc
        // comment above on why this test stays hermetic). Also carries the new
        // workspace_id provenance column (migration 0058).
        let atom_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO atoms
                (atom_id, graph_id, source_type, source_id, content_blob_key,
                 participants, timestamp, scope, author_id, author_name, workspace_id)
            VALUES ($1, $2, 'slack', $3, 'blob/key', $4, now(), 'account', $5, $5, $6)
            "#,
        )
        .bind(atom_id)
        .bind(graph_id)
        .bind(format!("C_TEST/{}", Uuid::new_v4()))
        .bind(vec![raw_user_id.to_string()])
        .bind(raw_user_id)
        .bind(&workspace_id)
        .execute(&pool)
        .await
        .expect("insert test atom");

        let http = reqwest::Client::new();
        let filter = BackfillFilter {
            graph_id: Some(graph_id),
            workspace_id: Some(workspace_id.clone()),
            author_id: None,
            after_atom_id: None,
        };

        // ── First pass: resolves via the directory, no network call needed ─────
        let outcome = run_batch(
            &pool,
            &pool,
            &TenantId::Consumer,
            &store,
            &http,
            account_id,
            &filter,
        )
            .await
            .expect("run_batch");
        assert_eq!(
            outcome.examined, 1,
            "exactly the one seeded atom should match"
        );
        assert_eq!(outcome.resolved, 1, "directory hit must repair the atom");
        assert!(!outcome.batch_full, "well under ATOM_BATCH_SIZE");
        assert_eq!(outcome.last_atom_id, Some(atom_id));

        let row = sqlx::query("SELECT author_name, participants FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&pool)
            .await
            .expect("reload atom");
        let author_name: String = row.get("author_name");
        let participants: Vec<String> = row.get("participants");
        assert_eq!(
            author_name, "Test Person",
            "author_name repaired from the directory"
        );
        assert_eq!(
            participants,
            vec!["Test Person".to_string()],
            "participants repaired too"
        );

        // ── Second pass: idempotent — nothing left to match, zero progress ─────
        let outcome2 = run_batch(
            &pool,
            &pool,
            &TenantId::Consumer,
            &store,
            &http,
            account_id,
            &filter,
        )
            .await
            .expect("run_batch (rerun)");
        assert_eq!(
            outcome2.examined, 0,
            "already-resolved atom no longer matches the ID-shape filter"
        );
        assert_eq!(outcome2.resolved, 0);

        // Cleanup.
        sqlx::query("DELETE FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM connector_identities WHERE workspace_id = $1")
            .bind(&workspace_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM slack_integrations WHERE workspace_id = $1")
            .bind(&workspace_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM graphs WHERE id = $1")
            .bind(graph_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        eprintln!("PG slack-name-backfill e2e passed: directory-hit repair + idempotent rerun");
    }

    // ── Live Postgres: legacy atom inclusion when unambiguous (single ws) ─────
    //
    // A legacy atom (pre-0034/0052: `author_id IS NULL`; pre-0058:
    // `workspace_id IS NULL`) IS still picked up by an author-scoped AND
    // workspace-scoped job when the account has exactly one Slack
    // integration — there's only one workspace it could possibly belong to,
    // so treating a NULL `workspace_id` as "matches" is safe. Matched via
    // `author_name` (the legacy fallback for rows with no `author_id`).
    #[tokio::test]
    async fn run_batch_includes_legacy_atom_when_workspace_unambiguous_against_real_postgres() {
        use crate::store::{PostgresStore, Store};

        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live slack-name-backfill test");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        let email = format!("{}@test.local", Uuid::new_v4());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
                .bind(&email)
                .fetch_one(&pool)
                .await
                .expect("insert user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("insert account");

        let store = PostgresStore::new(pool.clone());
        let graph = store
            .create_graph(account_id, "slack-backfill-legacy-e2e", "personal")
            .await
            .expect("create graph");
        let graph_id = graph.id;

        let workspace_id = format!("T{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, $2, 'Test WS', 'xoxb-test-token')",
        )
        .bind(account_id)
        .bind(&workspace_id)
        .execute(&pool)
        .await
        .expect("insert slack_integrations");

        let raw_user_id = "U0TESTLEGACY";
        store
            .upsert_connector_identity("slack", &workspace_id, raw_user_id, "Legacy Person")
            .await
            .expect("seed connector identity");

        // Legacy atom: no author_id, no workspace_id — only author_name (the
        // ingest-time raw-id fallback) carries the id.
        let atom_legacy = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO atoms
                (atom_id, graph_id, source_type, source_id, content_blob_key,
                 participants, timestamp, scope, author_name)
            VALUES ($1, $2, 'slack', $3, 'blob/key', $4, now(), 'account', $5)
            "#,
        )
        .bind(atom_legacy)
        .bind(graph_id)
        .bind(format!("C_TEST/{}", Uuid::new_v4()))
        .bind(vec![raw_user_id.to_string()])
        .bind(raw_user_id)
        .execute(&pool)
        .await
        .expect("insert legacy atom (author_id + workspace_id NULL)");

        let http = reqwest::Client::new();
        let filter = BackfillFilter {
            graph_id: Some(graph_id),
            workspace_id: Some(workspace_id.clone()),
            author_id: Some(raw_user_id.to_string()),
            after_atom_id: None,
        };

        let outcome = run_batch(
            &pool,
            &pool,
            &TenantId::Consumer,
            &store,
            &http,
            account_id,
            &filter,
        )
            .await
            .expect("run_batch");
        assert_eq!(
            outcome.examined, 1,
            "the legacy atom must match an author+workspace-scoped sweep when the account has only one workspace"
        );
        assert_eq!(outcome.resolved, 1);

        let row = sqlx::query("SELECT author_name FROM atoms WHERE atom_id = $1")
            .bind(atom_legacy)
            .fetch_one(&pool)
            .await
            .expect("reload legacy atom");
        let author_name: String = row.get("author_name");
        assert_eq!(author_name, "Legacy Person");

        // Cleanup.
        sqlx::query("DELETE FROM atoms WHERE atom_id = $1")
            .bind(atom_legacy)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM connector_identities WHERE workspace_id = $1")
            .bind(&workspace_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM slack_integrations WHERE workspace_id = $1")
            .bind(&workspace_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM graphs WHERE id = $1")
            .bind(graph_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        eprintln!(
            "PG slack-name-backfill e2e passed: legacy atom included when workspace unambiguous"
        );
    }

    // ── Live Postgres: multi-workspace tenancy isolation ──────────────────────
    //
    // An account with TWO Slack integrations (allowed by
    // `slack_integrations.UNIQUE (account_id, workspace_id)`): a
    // workspace-scoped sweep must
    //   1. NEVER touch an atom recorded as belonging to a DIFFERENT workspace
    //      (even though that other workspace has its own valid
    //      integration/directory entry for the exact same raw id — the
    //      cross-tenant-credential-misuse bug),
    //   2. NEVER guess at an AMBIGUOUS legacy atom (no recorded workspace_id)
    //      when the account has more than one integration — that atom is
    //      only reachable via a fully-unscoped sweep, which resolves it
    //      using the atom's-own-workspace-preference / best-effort fallback.
    #[tokio::test]
    async fn run_batch_isolates_multi_workspace_tenancy_against_real_postgres() {
        use crate::store::{PostgresStore, Store};

        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live slack-name-backfill test");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        let email = format!("{}@test.local", Uuid::new_v4());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
                .bind(&email)
                .fetch_one(&pool)
                .await
                .expect("insert user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("insert account");

        let store = PostgresStore::new(pool.clone());
        let graph = store
            .create_graph(account_id, "slack-backfill-tenancy-e2e", "personal")
            .await
            .expect("create graph");
        let graph_id = graph.id;

        // TWO Slack workspaces on the SAME account. Inserted in this order so
        // `workspace_candidates`' `ORDER BY si.created_at ASC` deterministically
        // tries A before B in the fully-unscoped step below.
        let workspace_a = format!("TA{}", Uuid::new_v4().simple());
        let workspace_b = format!("TB{}", Uuid::new_v4().simple());
        for ws in [&workspace_a, &workspace_b] {
            sqlx::query(
                "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
                 VALUES ($1, $2, 'Test WS', 'xoxb-test-token')",
            )
            .bind(account_id)
            .bind(ws)
            .execute(&pool)
            .await
            .expect("insert slack_integrations");
        }

        // The SAME raw id happens to resolve to DIFFERENT names in each
        // workspace's directory (distinct Slack orgs, same-shaped user id).
        let raw_user_id = "U0TESTUSER2";
        store
            .upsert_connector_identity("slack", &workspace_a, raw_user_id, "Workspace A Person")
            .await
            .expect("seed workspace A identity");
        store
            .upsert_connector_identity("slack", &workspace_b, raw_user_id, "Workspace B Person")
            .await
            .expect("seed workspace B identity");

        // Atom #1: belongs to workspace B, has workspace provenance recorded.
        let atom_b = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO atoms
                (atom_id, graph_id, source_type, source_id, content_blob_key,
                 participants, timestamp, scope, author_id, author_name, workspace_id)
            VALUES ($1, $2, 'slack', $3, 'blob/key', $4, now(), 'account', $5, $5, $6)
            "#,
        )
        .bind(atom_b)
        .bind(graph_id)
        .bind(format!("C_TEST/{}", Uuid::new_v4()))
        .bind(vec![raw_user_id.to_string()])
        .bind(raw_user_id)
        .bind(&workspace_b)
        .execute(&pool)
        .await
        .expect("insert workspace-B atom");

        // Atom #2: a LEGACY atom, ambiguous — no author_id, no workspace_id —
        // on an account with TWO integrations, so neither is a safe guess.
        let atom_legacy = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO atoms
                (atom_id, graph_id, source_type, source_id, content_blob_key,
                 participants, timestamp, scope, author_name)
            VALUES ($1, $2, 'slack', $3, 'blob/key', $4, now(), 'account', $5)
            "#,
        )
        .bind(atom_legacy)
        .bind(graph_id)
        .bind(format!("C_TEST/{}", Uuid::new_v4()))
        .bind(vec![raw_user_id.to_string()])
        .bind(raw_user_id)
        .execute(&pool)
        .await
        .expect("insert ambiguous legacy atom (author_id + workspace_id NULL)");

        let http = reqwest::Client::new();

        // ── Workspace-A-scoped sweep: must touch NEITHER atom ─────────────────
        // (workspace-B atom doesn't match A; the ambiguous legacy atom can't be
        // safely guessed with 2 integrations on the account).
        let filter_a = BackfillFilter {
            graph_id: Some(graph_id),
            workspace_id: Some(workspace_a.clone()),
            author_id: Some(raw_user_id.to_string()),
            after_atom_id: None,
        };
        let outcome_a = run_batch(
            &pool,
            &pool,
            &TenantId::Consumer,
            &store,
            &http,
            account_id,
            &filter_a,
        )
            .await
            .expect("run_batch (workspace A)");
        assert_eq!(
            outcome_a.examined, 0,
            "a workspace-A-scoped sweep must exclude both the workspace-B atom AND the ambiguous legacy atom"
        );

        // ── Workspace-B-scoped sweep: must resolve ONLY atom_b, via B's ───────
        // OWN directory entry (never A's).
        let filter_b = BackfillFilter {
            graph_id: Some(graph_id),
            workspace_id: Some(workspace_b.clone()),
            author_id: Some(raw_user_id.to_string()),
            after_atom_id: None,
        };
        let outcome_b = run_batch(
            &pool,
            &pool,
            &TenantId::Consumer,
            &store,
            &http,
            account_id,
            &filter_b,
        )
            .await
            .expect("run_batch (workspace B)");
        assert_eq!(
            outcome_b.examined, 1,
            "only the workspace-B atom matches a workspace-B-scoped sweep"
        );
        assert_eq!(outcome_b.resolved, 1);

        let workspace_b_row = sqlx::query("SELECT author_name FROM atoms WHERE atom_id = $1")
            .bind(atom_b)
            .fetch_one(&pool)
            .await
            .expect("reload workspace-B atom");
        let workspace_b_author_name: String = workspace_b_row.get("author_name");
        assert_eq!(
            workspace_b_author_name, "Workspace B Person",
            "workspace-B atom resolved via workspace B's OWN directory entry, not A's"
        );

        // ── Fully-unscoped sweep: the ambiguous legacy atom (no recorded ─────
        // workspace, account has TWO integrations) must be left UNTOUCHED —
        // never guessed (CodeRabbit review, PR #181 round 2). It's still
        // "examined" (the SQL has no workspace restriction when the filter is
        // unscoped) but must NOT be "resolved", and its author_name must
        // remain exactly the raw id it started as.
        let filter_unscoped = BackfillFilter {
            graph_id: Some(graph_id),
            workspace_id: None,
            author_id: Some(raw_user_id.to_string()),
            after_atom_id: None,
        };
        let outcome_unscoped = run_batch(
            &pool,
            &pool,
            &TenantId::Consumer,
            &store,
            &http,
            account_id,
            &filter_unscoped,
        )
            .await
            .expect("run_batch (unscoped)");
        assert_eq!(outcome_unscoped.examined, 1, "only the legacy atom remains");
        assert_eq!(
            outcome_unscoped.resolved, 0,
            "an ambiguous legacy atom on a multi-workspace account must never be guessed"
        );
        assert_eq!(outcome_unscoped.unresolved, 1);

        let legacy_row = sqlx::query("SELECT author_name FROM atoms WHERE atom_id = $1")
            .bind(atom_legacy)
            .fetch_one(&pool)
            .await
            .expect("reload legacy atom");
        let legacy_author_name: String = legacy_row.get("author_name");
        assert_eq!(
            legacy_author_name, raw_user_id,
            "ambiguous legacy atom must be left exactly as it was — never attributed to an arbitrary workspace's directory entry"
        );

        // Cleanup.
        for atom in [atom_b, atom_legacy] {
            sqlx::query("DELETE FROM atoms WHERE atom_id = $1")
                .bind(atom)
                .execute(&pool)
                .await
                .ok();
        }
        sqlx::query("DELETE FROM connector_identities WHERE workspace_id = ANY($1)")
            .bind(vec![workspace_a.clone(), workspace_b.clone()])
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM slack_integrations WHERE workspace_id = ANY($1)")
            .bind(vec![workspace_a, workspace_b])
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM graphs WHERE id = $1")
            .bind(graph_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        eprintln!(
            "PG slack-name-backfill e2e passed: multi-workspace tenancy isolation + ambiguous-legacy exclusion"
        );
    }
}

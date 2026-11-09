//! The credit-ledger derivation worker (usage-metering PR9 of 13,
//! `.scratch/usage-metering/plan.md` §1 PR9) — **the highest-risk PR in the
//! usage-metering train**. This is the code that turns raw `llm_usage` rows
//! into money owed: a bug here either double-charges (the failure that turns
//! into a wrong invoice) or silently under-charges (the failure that never
//! shows up as an error at all).
//!
//! # Shape: `tenant_harness` sweep worker, `platform_rollup`'s two-pool split
//!
//! Registered with [`crate::workers::tenant_harness::HarnessConfig::sweep_worker`]
//! exactly like [`super::platform_rollup`] and every other tenant-loop
//! worker — this module never enumerates tenant DBs itself;
//! `tenant_harness::run_multi_tenant` (backed by `tenancy::registry`'s
//! `routing::list_active`) does that every ~30s behind a schema-version
//! gate, and calls [`tick`] once per due [`WorkerTenant`].
//!
//! `credit_ledger`/`credit_balances` are CONTROL-ONLY tables (migration
//! `0150_credit_ledger.sql`), so — same two-pool split as
//! `platform_rollup::tick(t: &WorkerTenant, control: &PgPool)` — usage is
//! COLLECTED from `t.pool` (that tenant's own content plane: the org's
//! tenant DB, or the shared control DB doubling as the consumer-content
//! plane) and the ledger is WRITTEN to `control`, always the control-plane
//! pool, passed as a second argument exactly like `platform_rollup`'s.
//!
//! # Per-account loop on BOTH branches — not just Consumer
//!
//! `platform_rollup` branches sharply: Org tenants get ONE whole-database
//! pass (one account per tenant DB, ambient/system-scoped reads); Consumer
//! loops per account because `llm_usage`'s consumer-plane RLS
//! (`account_isolation`, migration `0083_consumer_rls`) is strict per
//! `account_id` and a single unscoped read across many accounts is not
//! possible at all (an unset `app.account_id` GUC hard-errors the `::uuid`
//! cast, it does not merely filter).
//!
//! This worker's Org branch is *shaped* the same as Consumer's — a loop over
//! a list of account ids — but the list has exactly one element (see
//! [`resolve_org_account_id`]). The reason is the cursor design below, not a
//! copy-paste: this worker needed ONE per-account processing function
//! regardless, so giving Org a one-element account list reuses it rather
//! than forking a second implementation. The read SCOPE still differs
//! per-plane inside that shared function (see [`open_read_scope`]):
//! Consumer opens [`crate::tenancy::scope::begin_account_scoped`] (the
//! `account_id` GUC is load-bearing there); Org opens
//! [`crate::tenancy::scope::begin_tenant_system_scoped`] (the tenant-plane
//! `llm_usage` policy is anchored on `graph_id` via
//! `tenant_rls_visible_graph_ids()`, not `account_id` — the system-actor GUC
//! is what actually unlocks visibility there).
//!
//! # Where the three cursors live — a documented deviation from the literal
//! # brief
//!
//! The task brief says: store the cursor as columns on an "owning row" (its
//! own precedent: `graphs.er_sweep_cursor_*`, migration
//! `0136_er_sweep_cursor.sql`), never a cursor table. The entity-resolution
//! sweep that precedent comes from is genuinely PER OCEAN, so `graphs` is
//! its correct owning row. This worker's three cursors (job / query /
//! journal) are genuinely PER ACCOUNT instead — see the per-account-loop
//! design above — so the row that actually owns this state is
//! `credit_balances` (`account_id` PRIMARY KEY, migration `0150`), which
//! this worker already reads and writes every tick to materialize the
//! running balance. Migration `0151_billing_derivation_cursor.sql` adds six
//! nullable columns there instead of inventing a fourth table. This keeps
//! the letter of the instruction (an owning row, not a bespoke cursor table)
//! while fitting the grain this worker's own RLS constraints actually force
//! (per-account, not per-tenant-DB) — see that migration's header for the
//! full reasoning, spelled out there because it is the one place in this PR
//! where the implementation's shape differs from the brief's literal
//! example.
//!
//! # The three axes
//!
//! * **Job axis** — for each of this account's completed `ExtractEntities`
//!   `jobs` rows (keyset cursor `(finished_at, job_id)`, index
//!   `idx_jobs_extract_entities_done` — PR7), sum the `llm_usage` rows
//!   carrying that `source_job_id` and emit ONE ledger row keyed on
//!   `jobs.id` — never `jobs.idempotency_key` (see "The double-charge trap"
//!   below). `jobs.account_id` (the org-tenancy W.1 stamp,
//!   `jobs::queue::dequeue`'s doc comment) is trusted directly as the
//!   billing account; this needs no `graphs.account_id` resolution the way
//!   the journal axis does, because every job row already carries its own
//!   owning account.
//! * **Query axis** — same shape, keyed on `source_query_id`, keyset cursor
//!   `(latest created_at, source_query_id)` over query usage. Each newly
//!   observed cost increment appends a ledger revision under a query lock.
//!   Conversion stays pinned; prior charges and attribution stay immutable.
//!   Request-path callers (`routes::queries`, PR4) write a real
//!   `llm_usage.account_id` on BOTH planes (unlike worker call sites), so
//!   this axis's `account_id` filter is uniform across Consumer and Org —
//!   the one place this worker does NOT need the Org-branch's
//!   filter-nothing special case the journal axis needs (see below).
//! * **Journal axis** — no `jobs` row exists for a journal compile at all
//!   (`workers::journal_sweep` is sweep-driven, not queue-driven), so this
//!   axis gets its OWN cursor, `(source_journal_period, llm_usage.id)`
//!   (index `idx_llm_usage_journal_period_cursor`, PR7). Grouped by
//!   `(graph_id, source_journal_granularity, source_journal_period)` —
//!   `credit_ledger`'s own idempotency index (`0150`) is keyed on exactly
//!   that triple plus `account_id`. **`journal_sweep.rs` passes
//!   `account_id: None` to `UsageContext` on the Org arm** (by design — see
//!   that file's own `usage_account_id` comment), so `llm_usage.account_id`
//!   is NOT usable to attribute this axis's Org-plane rows the way it is
//!   for job/query. This worker resolves the account for those rows the
//!   simple way instead: on the Org branch it is ALREADY processing the
//!   one account a whole tenant DB belongs to (see the per-account-loop
//!   note above), so the journal-axis query on Org simply omits the
//!   `account_id` filter entirely (there is nothing to filter — every row
//!   the system-scoped transaction can see belongs to this one account by
//!   construction) rather than resolving `graphs.account_id` row-by-row.
//!
//! # The double-charge trap — keyed on `jobs.id`, never `idempotency_key`
//!
//! `jobs.idempotency_key` (`extract:<atom_id>`) is unique only among
//! `status='pending'` rows (`idx_jobs_idem_pending` is itself partial) — an
//! atom re-enqueued after its first completion (promotion is one such path)
//! produces a SECOND `status='done'` row sharing that same key. This worker
//! never reads or groups by `idempotency_key` anywhere; every read, cursor
//! comparison, and `credit_ledger` idempotency index in this file is keyed
//! on `jobs.id` (`credit_ledger.source_job_id`), the table's real primary
//! key, which is globally unique regardless of how many times an atom gets
//! re-processed. See `pg_tests::job_axis_keys_on_jobs_id_not_idempotency_key`
//! for the negative control this trap requires: two DISTINCT `jobs` rows
//! sharing one `idempotency_key`, each with its own real `llm_usage` spend,
//! must both be charged (proving `jobs.id` correctly keeps them distinct —
//! collapsing them via `idempotency_key` would silently under-charge one),
//! and re-running this worker over the same window must not re-charge
//! either one (proving the `source_job_id` unique index on `credit_ledger`
//! — not this worker's own bookkeeping — is what makes a re-run idempotent).
//!
//! # Partial writes — refuse to write anything if any account failed
//!
//! [`tick`] follows `platform_rollup::consumer_tick`'s discipline exactly:
//! collect every account's usage WITHOUT aborting on the first failure (a
//! timeout or a DB error for one account must not block every other
//! account's contribution this cycle from even being computed), but before
//! writing anything, check whether ANY account failed. If so, [`tick`]
//! writes NOTHING — no ledger rows, no cursor advance, for ANY account this
//! cycle, even the ones that read cleanly — and returns `Err`, which the
//! harness surfaces as a backed-off tick retry. A ledger that silently
//! dropped one failed account's jobs while still advancing every other
//! account's cursor would look complete and would not be.
//!
//! This is why `read_account_usage` below does ALL the reading (jobs +
//! llm_usage, all three axes) for one account before this worker commits to
//! writing anything for ANY account, and why the write phase
//! ([`write_account_ledger`]) only ever runs after every account in this
//! tick's account list has been read successfully.
//!
//! Unlike `consumer_tick` (one giant cross-account row, written or not as a
//! single unit), this worker's unit of "did it succeed" is the WHOLE TICK's
//! READ phase, but the WRITE phase is still necessarily one transaction PER
//! ACCOUNT — `credit_ledger`/`credit_balances` RLS is
//! `account_id`-anchored, and a single Postgres transaction can only carry
//! one `app.account_id` GUC value, so N accounts' ledger rows cannot land in
//! one atomic transaction. Each per-account write is individually
//! idempotent (`ON CONFLICT ... DO NOTHING` on every insert, keyed on
//! exactly the unique index that axis's idempotency story rests on), so a
//! write-phase failure partway through the account list is safe to retry
//! whole-tick: already-written accounts' inserts no-op harmlessly on retry,
//! and the accounts after the failure point simply get retried next tick
//! (their cursors never advanced, because the failure prevented their
//! transaction from committing).
//!
//! # The cursor — `(field, id)` composite, PLUS a persisted overlap margin
//!
//! `jobs.finished_at` and `llm_usage.created_at` are both `now()` AT COMMIT,
//! not at some earlier logical event, so concurrent transactions can commit
//! out of that order (transaction A starts first but is still in flight
//! when transaction B, which started later, commits with an EARLIER
//! `finished_at`/`created_at` value). A naive `WHERE finished_at > $cursor`
//! would then PERMANENTLY skip A's row once the cursor has already advanced
//! past B's later-but-earlier-stamped value — the cursor never goes
//! backward, so a skipped row is lost forever, not merely delayed.
//! [`CURSOR_LAG`] alone is NOT sufficient to prevent this — it only bounds
//! which rows THIS READ considers, it does not undo a cursor that has
//! ALREADY advanced past a row from an earlier read.
//!
//! **Why the cursor lags a SECOND time.** The read predicate excludes
//! anything newer than `cutoff = now() - CURSOR_LAG` (rows still "too
//! recent to trust"), but a naive design that then advances the PERSISTED
//! cursor to the last row actually seen reproduces the exact failure the
//! plan's §4 item 3 describes: tick N reads job B (`finished_at = T`,
//! already past `cutoff`) and advances the cursor to `(T, B.id)`. Job A
//! (`finished_at = T - 5s`, i.e. chronologically EARLIER) was still
//! mid-commit at tick N and simply did not exist yet from this worker's
//! point of view — it commits moments later, and by tick N+1 it is fully
//! visible and well past `cutoff` too. But `(T - 5s, A.id) > (T, B.id)` is
//! FALSE, so a cursor that had already advanced to B's position would skip
//! A forever. [`OVERLAP_WINDOW`] closes this: the PERSISTED cursor only
//! ever advances to `cutoff - OVERLAP_WINDOW` (never to the last row's own
//! position) whenever a read comes back UNDER [`READ_BATCH_SIZE`] — i.e.
//! whenever this worker has confirmed there is nothing else to find between
//! the old cursor and `cutoff` in THIS pass. Every tick therefore RE-READS
//! a rolling ~7-minute window of rows it has already charged — which is
//! free, not merely cheap, because every insert in that window is
//! `ON CONFLICT ... DO NOTHING` against the exact same idempotency index
//! `source_job_id`/`source_query_id`/the journal triple already provide
//! (see "The double-charge trap" above). A late-arriving row anywhere
//! inside that rolling window is picked up on whichever tick first sees it,
//! never silently skipped. [`advance_cursor`] is the one place this policy
//! is implemented; all three axes call it identically. When a read comes
//! back AT [`READ_BATCH_SIZE`] (truncated — more backlog may exist), the
//! cursor advances only to the last row actually seen instead, exactly like
//! ordinary keyset pagination, so a large backlog is drained in bounded
//! steps before the overlap policy ever applies.
//!
//! This is a probabilistic guarantee, not a mathematical one, and that
//! bound is stated explicitly rather than implied: it requires every
//! transaction that stamps `finished_at`/`created_at` to COMMIT within
//! `OVERLAP_WINDOW - CURSOR_LAG` (here, ~7 minutes) of that timestamp. That
//! is true by a wide margin for this codebase's actual write pattern — the
//! job-completion `UPDATE` and PR4's `finalize` insert are both short,
//! uncontended transactions with no LLM/R2 call in between (the whole
//! reason the reserve/finalize split exists) — but it is not a
//! Postgres-enforced invariant, and a future change that holds either
//! transaction open for minutes would silently widen the actual race window
//! past what this worker tolerates. `(field, id)` stays a composite (not a
//! bare timestamp) for the ordinary keyset-pagination reason: many rows can
//! share the exact same timestamp (every graph's hourly journal compile at
//! one wall-clock hour, in particular).
//!
//! `source_journal_period` is a genuine exception in KIND (it is the
//! compile's own tier boundary, not a commit-time stamp) but not in RISK:
//! many different graphs' compiles for the same period can still commit at
//! different real times, so the SAME lag-window protection is applied via
//! `llm_usage.created_at <= cutoff`, not by ordering on `created_at` — the
//! ORDER key stays `(source_journal_period, id)` (matching the index this
//! axis actually has), the LAG check is a plain filter alongside it. The
//! overlap-margin cursor policy above applies identically on top of that.
//!
//! # Classifying source-id-less `llm_usage` rows — count, don't alert-per-row
//!
//! After PR4, two writers populate `llm_usage`: the metered choke point
//! (threads `source_job_id`/`source_query_id`/`source_journal_*`) and the
//! surviving legacy `llm::usage::record` call sites, which thread none.
//! Three classes carry no source id and are NOT anomalies:
//!
//! 1. **Cache-hit rows** — `task` ending in `_cached`
//!    (`extract_entities_cached`, `extract_relationships_cached`) — a
//!    deliberately zero-token row written purely so cache-hit rate is
//!    queryable. Bills to zero by construction; there are MANY of these.
//! 2. **Batch-path rows** — `llm/batch.rs`'s Anthropic Batch API calls stay
//!    on legacy `record()` until sibling PR6 gives batch its own metered
//!    path. **Known limitation, stated plainly**: these share the EXACT
//!    SAME `task` strings (`extract_entities`, `extract_relationships`) as
//!    the now-metered non-batch calls in `extract.rs`/
//!    `extract_relationships.rs`, so this worker cannot distinguish a
//!    batch-path row from class 3 by `task` alone — nothing in
//!    `llm_usage`'s current columns can. The count logged for class 3 below
//!    is therefore an OVER-count that includes every still-unmetered batch
//!    row; it is explicitly documented as such in the log line so an
//!    operator does not read the raw number as "genuine anomalies". Tighter
//!    separation needs either a `kind` column on `llm_usage` or waiting for
//!    PR6 to close the gap outright.
//! 3. **Genuinely pre-epoch / unthreaded rows** — the only class the alert
//!    is actually for.
//!
//! Managed Ask rows (`jev_ask*`, the `interactive_ask` purpose) carry no
//! source key on purpose (their receipts are the bill, see
//! `llm::usage::managed_usage_context`) and are left out of both counts.
//!
//! [`classify_unthreaded_usage`] counts classes 1 and 3 SEPARATELY (class 1
//! logged at `info`, never `warn`) and logs class 3 at
//! `tracing::warn!(alert = true, ...)` — once per tick, as an aggregate
//! count, never per row — matching this codebase's established
//! `pool_failure_severity` lesson that warn-per-attempt is what buried a
//! prior incident as noise.
//!
//! # `Tick::idle()` vs `Tick::worked(n)`
//!
//! Matches `platform_rollup::write_rollup`'s own documented choice exactly:
//! [`Tick::idle`] when zero `credit_ledger` rows are newly inserted this
//! tick (across every account processed), [`Tick::worked`] with the actual
//! inserted-row count otherwise. A dormant tenant then backs off toward the
//! harness's idle ceiling — for a credit ledger that means credits can land
//! up to ~20 minutes late on a quiet tenant and nothing else; forcing
//! `worked` unconditionally would defeat the backoff the harness exists to
//! provide and burn a tenant-pool connection on tenants doing nothing.
//!
//! # Decimal, never float, on anything that reaches the ledger
//!
//! `credit_ledger.credits`/`.cost_usd` and `credit_balances.balance` are
//! Postgres `NUMERIC`, read and written here exclusively as
//! [`sqlx::types::Decimal`] (`rust_decimal`, already an enabled backend
//! feature — see `Cargo.toml`'s comment on the `sqlx` dependency). The ONE
//! place an `f64` intermediate is unavoidable is
//! [`crate::billing::rates::PriceSheet::rate`] itself, which is `f64`
//! end-to-end by its own documented design (a "rough, internally-consistent
//! estimate", never round-tripped through the ledger's NUMERIC columns
//! without first going through [`decimal_from_f64`], which formats to a
//! fixed number of decimal places BEFORE parsing — never
//! `Decimal::from_f64_retain`, which would carry binary-float noise digits
//! into an exact-decimal column). The division that turns a dollar cost
//! into a credit amount (`cost / credit_conversion.reference_cost_per_
//! credit`) is exact `Decimal / Decimal` arithmetic throughout — see
//! [`credits_for_cost`].
//!
//! `cost_usd` is `None` on every `llm_usage` row in this tree today (no
//! OpenRouter/cost-reporting provider is wired into production — see
//! `llm::usage`'s own doc and plan §2 item 7), so the "prefer
//! provider-reported cost" branch below is real code with no test that
//! claims it is production-reachable; every `pg_tests` fixture that wants a
//! populated `cost_usd` sets it directly via a raw `INSERT`, which is the
//! only way this tree can currently produce one.
//!
//! # Enforcement observation (usage-metering PR10)
//!
//! [`tick`]'s write phase is "the debit path" [`crate::billing::enforcement`]
//! hangs its one production call site off: right after each account's debit
//! commits, [`enforcement::check_and_record`] checks whether that account's
//! materialized balance is now `<= 0` and, if so, records an
//! `enforcement_events` row — purely observational, best-effort, and never a
//! reason to fail, retry or roll back this tick. See that module's own doc
//! comment for the full design (default-OFF `account_prefs` flag, a
//! process-wide kill switch layered over it, and the load-bearing property
//! that nothing in this whole 13-PR train ever turns a `would_have_blocked:
//! true` result into an actually-blocked write).
//!
//! # Per-member attribution (Billing Phase 2a) — carrying `attributed_user_id`
//! # from `llm_usage` onto `credit_ledger`
//!
//! Migration `0159_credit_ledger_attributed_user_id.sql` adds `credit_ledger.
//! attributed_user_id UUID NULL`, the ledger-side counterpart of Phase 1's
//! `llm_usage.attributed_user_id` (migration `0157`/`0057`). Same posture as
//! everywhere else in this file: **STORED-ONLY, never a scope selector** — it
//! is never read by [`open_read_scope`] or [`write_account_ledger`]'s
//! `begin_account_scoped` call, exactly the separation `llm::usage`'s own
//! module doc draws between `user_id` and `attributed_user_id`. Nothing here
//! changes which transaction-scope function any call site opens.
//!
//! **Where it comes from**: each of the three read-phase axes now also
//! selects `attributed_user_id` alongside the token/cost columns it already
//! aggregates, and each `LedgerDebit` carries the resolved member forward to
//! the write phase (see [`insert_ledger_row`]).
//!
//! **Disagreement policy — read this before "simplifying" any of the three
//! axes' attribution resolution.** A single ledger debit aggregates
//! POTENTIALLY MANY `llm_usage` rows (a job's primary + escalation call, a
//! query's several sub-calls, a journal period's every compile call for one
//! ocean) — see [`resolve_debit_attribution`]. Those rows do not always agree
//! on `attributed_user_id`. Query disagreement affects only the new delta;
//! it does not rewrite earlier debits or their historical allowance periods:
//!
//! * **All NULL** — the ordinary, expected case for every debit whose
//!   underlying calls came from a job worker with no member in scope, or from
//!   before this PR. Resolves to `None`. Not an error, not logged — this is
//!   the majority case today and logging it would be exactly the
//!   per-row-noise `classify_unthreaded_usage`'s own doc already warns
//!   against.
//! * **Exactly one distinct non-NULL value** — every attributed row agrees
//!   (the overwhelmingly common non-NULL case: a job's primary and
//!   escalation calls both derive from the SAME atom, so both threads the
//!   SAME `ingested_by`). Resolves to that member.
//! * **Two or more distinct non-NULL values** — a genuine disagreement.
//!   Resolved to `None` (the debit is recorded as unattributed, same as the
//!   "all NULL" case) rather than silently picking the first-seen, a
//!   majority vote, or any other arbitrary tiebreak — any of those would
//!   misattribute real spend to a member who did not cause all of it, and a
//!   ledger error at INSERT time would be worse (this worker's whole design
//!   is "never refuse to write a debit because of an enrichment field" — see
//!   the module's "Partial writes" framing throughout). Logged at `warn!`
//!   (not per-row — once per conflicted debit, which is rare enough that
//!   this does NOT reduce to the same noise `classify_unthreaded_usage`
//!   guards against) so a conflicted debit is visible to an operator rather
//!   than silently indistinguishable from the "all NULL" case. This is
//!   expected to be rare in practice today (see each axis's own doc for why)
//!   but is a real, reachable state the code must not crash or miscount on.
//!
//! **Per axis, concretely:**
//!
//! * **Job axis** — a job's `llm_usage` rows are its own primary extraction
//!   call plus, rarely, one escalation retry (`extract_with_escalation`),
//!   both threaded from the SAME `UsageContext` (`extract.rs`'s
//!   `usage_ctx.with_source_job(job.id)`, built once and reused). Both calls'
//!   `attributed_user_id` therefore come from the SAME atom's `ingested_by`,
//!   so disagreement is not expected to occur in production, but the code
//!   does not assume it away.
//! * **Query axis** — a request-path query's several sub-calls all thread
//!   the SAME acting user (`routes::queries`/`services::ask`'s
//!   `.with_attributed_user(user_ctx.user_id)`, one call per query,
//!   documented in `llm::usage`'s own module doc as "the SAME id passed to
//!   both `with_user` and `with_attributed_user`"). Disagreement here would
//!   indicate a genuine bug in a caller, not an expected data shape.
//! * **Journal axis** — the one axis where disagreement is a REAL, expected
//!   possibility, not just a defensive branch: a journal period's compile
//!   covers every atom touched in that hour/day/week/month for one ocean
//!   (`journal_sweep.rs`'s `compile_hour`/`compile_tier_group`), which can
//!   easily span atoms ingested by several different members of a shared
//!   team ocean. The resolution above applies identically — a journal debit
//!   whose underlying atoms disagree resolves to `None`, not a fabricated
//!   "whoever ingested the most" or "the ocean owner" pick.

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::types::Decimal;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::billing::credit_conversion::CreditConversionTable;
use crate::billing::enforcement;
use crate::billing::funding_policy::db::{
    debit_key, write_extra_debit, ExtraDebit, PeriodSel, SettlementPlanner, Split,
};
use crate::billing::rates::PriceSheet;
use crate::store::StoreError;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

// ── Tunables ──────────────────────────────────────────────────────────────

/// Default tick cadence — matches the plan's "~5-min cadence,
/// `platform_rollup` shape". Env-overridable via
/// `BILLING_DERIVATION_INTERVAL_SECS`, same accessor shape as
/// `platform_rollup::rollup_interval`.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5 * 60);

pub fn derivation_interval() -> Duration {
    std::env::var("BILLING_DERIVATION_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_INTERVAL)
}

/// See the module doc's "The cursor" section. 3 minutes is generous relative
/// to how quickly `jobs.finished_at`/`llm_usage.created_at` actually commit
/// after being stamped — PR4's reserve/finalize split deliberately holds no
/// connection across the LLM round-trip, so the write that stamps either
/// column is a short, uncontended transaction, not one held open for 60s+.
const CURSOR_LAG: chrono::Duration = chrono::Duration::seconds(180);

/// How far behind `cutoff` the PERSISTED cursor is allowed to sit once a
/// read has drained everything currently available — see the module doc's
/// "The cursor" section, "Why the cursor lags a SECOND time" subsection.
/// Strictly larger than [`CURSOR_LAG`] so the persisted cursor never
/// approaches the same boundary [`CURSOR_LAG`] itself protects; the gap
/// between the two (here, ~7 minutes) is the safety margin against a
/// straggler transaction whose commit is delayed longer than [`CURSOR_LAG`]
/// alone would tolerate.
const OVERLAP_WINDOW: chrono::Duration = chrono::Duration::seconds(600);

/// Bound on how many candidate rows one account's one axis reads per tick —
/// keeps a single very-active account from starving every other account's
/// turn in one tick, mirroring `embed::JOB_BATCH_SIZE`-style batch caps used
/// throughout `workers/`.
const READ_BATCH_SIZE: i64 = 500;

/// Window `classify_unthreaded_usage` looks back over — see the module
/// doc's "Classifying source-id-less rows" section. Wider than one tick
/// interval so a slow tick cycle doesn't create a blind gap between passes.
const CLASSIFICATION_WINDOW: chrono::Duration = chrono::Duration::minutes(15);

/// Lower bound on the unattributed-`ExtractEntities` alert's side-query
/// below (see its call site) — the alert fires only for rows whose
/// `finished_at` falls in `[as_of - UNATTRIBUTED_ALERT_WINDOW, cutoff]`.
///
/// Without a lower bound, ANY historical residue trips `alert = true` on
/// EVERY tick, forever — exactly what happened in production: 3,493 rows,
/// `finished_at` spanning 2026-05-23..2026-07-23, ZERO in the trailing 14
/// days (the bug that produced them is already fixed; this is pure
/// history), firing this alert on every single pass with nothing an
/// operator could do about it. A permanently-firing alert is worse than
/// none — it trains everyone to ignore it, which is exactly when a genuine
/// NEW occurrence would go unnoticed. `jobs_account_backfill` closes out
/// the resolvable residue, but a handful of rows are unresolvable by
/// construction (their atom was deleted) and would otherwise keep tripping
/// this alert forever at a smaller, still-meaningless count — bounding the
/// query, not just running the backfill once, is what makes the alert
/// self-limiting for good.
///
/// 24 hours, chosen against [`CURSOR_LAG`] and [`derivation_interval`],
/// with a large margin in both directions:
///   * **Never misses a real new occurrence.** [`CURSOR_LAG`] (180s) is
///     already why `cutoff` excludes anything that hasn't had time to
///     settle; this window's LOWER edge sits a full day further back than
///     that, so a row that just became eligible is nowhere near the edge
///     that could exclude it. And because [`DEFAULT_INTERVAL`] runs this
///     query roughly every 5 minutes, a genuinely new unattributable job
///     does not get ONE chance to be noticed — it re-trips this alert on
///     roughly 288 consecutive ticks before aging out of the window, which
///     is margin enough to survive hours of derivation-worker downtime (a
///     bad deploy, a stuck tenant loop) between whoever is on call actually
///     seeing it.
///   * **Never re-fires on residue.** The measured historical rows are
///     weeks old; 24 hours is orders of magnitude narrower than that gap,
///     so once `jobs_account_backfill` reduces `account_id IS NULL` down to
///     its unresolvable remainder, those rows fall out of this window
///     within a day of the backfill running and never re-enter it — they
///     cannot get MORE recent with time.
const UNATTRIBUTED_ALERT_WINDOW: chrono::Duration = chrono::Duration::hours(24);

/// `credit_ledger.entry_kind` values this worker writes — the authoritative
/// vocabulary for the debit axes (migration `0150`'s header explicitly left
/// this open for PR9/PR10 to define).
pub const ENTRY_KIND_JOB: &str = "usage_job";
pub const ENTRY_KIND_QUERY: &str = "usage_query";
pub const ENTRY_KIND_JOURNAL: &str = "usage_journal";
/// COR-88: one debit per charged managed attempt receipt.
pub const ENTRY_KIND_MANAGED: &str = "usage_managed";

/// Job kinds the job axis bills from `llm_usage`. Managed kinds
/// ([`crate::managed::MANAGED_JOB_KINDS`] and
/// [`crate::managed::MANAGED_ENRICHMENT_JOB_KINDS`]) are charged from
/// receipts on the receipt axis instead and must never appear here. Their
/// usage rows carry no query or journal key either
/// (`llm::usage::metered_complete_managed`), so no other axis selects them. `ReevaluateKind` is retired (no producer since
/// tenant 0160 / control 0327) but stays so any done-but-unbilled historical
/// row is still charged once; it can no longer mint new rows.
pub const JOB_AXIS_KINDS: &[&str] = &["ExtractEntities", "ReevaluateKind"];

// ── Entry point ───────────────────────────────────────────────────────────

/// One derivation pass for one tenant. `control` is always the control-plane
/// pool — see the module doc's "Shape" section for the full two-pool
/// rationale (identical to `platform_rollup::tick`'s).
pub async fn tick(t: &WorkerTenant, control: &PgPool) -> Result<Tick, String> {
    tick_with_runtime(t,control,None).await
}

pub(crate) async fn tick_with_runtime(
    t: &WorkerTenant, control: &PgPool, runtime: Option<&crate::tenancy::runtime::TenantRuntime>,
) -> Result<Tick,String> {
    let ordinary = ordinary_tick(t, control).await;
    let recovery = recover_managed(t, control, runtime).await;
    match (ordinary, recovery) {
        (Ok(mut ordinary), Ok(recovered)) => {
            ordinary.processed += recovered;
            Ok(ordinary)
        }
        (ordinary, recovery) => {
            tracing::error!(?ordinary, ?recovery, tenant=?t.tenant,
                "billing ordinary derivation and independent managed recovery outcomes");
            Err(format!("ordinary derivation: {ordinary:?}; separate managed recovery: {recovery:?}"))
        }
    }
}

async fn ordinary_tick(t: &WorkerTenant, control: &PgPool) -> Result<Tick, String> {
    let sheet = PriceSheet::load(control)
        .await
        .map_err(|e| format!("billing derivation: failed to load PriceSheet: {e}"))?;
    let conversion_table = CreditConversionTable::load(control)
        .await
        .map_err(|e| format!("billing derivation: failed to load CreditConversionTable: {e}"))?;
    let as_of = Utc::now();
    let reference_cost_per_credit = conversion_table
        .active(as_of)
        .map(|row| row.reference_cost_per_credit)
        .ok_or_else(|| "billing derivation: no active credit_conversion row".to_string())?;
    let cutoff = as_of - CURSOR_LAG;

    // Every per-account job-axis read below filters `jobs WHERE account_id =
    // $1`, so a completed `ExtractEntities` job with `account_id IS NULL`
    // (a legacy pre-W.1 row `queue::dequeue`'s backfill resolver could not
    // resolve, dead-lettered instead of completed in the normal case, but
    // theoretically still reachable) is invisible to every account's loop —
    // no cursor ever covers it, and it would otherwise be silently
    // unbillable forever with no signal at all. `jobs` carries no RLS on
    // either plane (0083's own header, "flagged, not covered"), so this is
    // a single unscoped count against `t.pool`, best-effort: a failure here
    // must never fail the tick over an alerting side-query.
    //
    // BOUNDED below by `UNATTRIBUTED_ALERT_WINDOW` (see that constant's own
    // doc for the full CURSOR_LAG-relative justification) as well as above
    // by `cutoff` — without the lower bound this alert cannot distinguish a
    // brand-new occurrence from weeks-old historical residue and fires on
    // every tick forever once any residue exists, which is exactly what
    // happened in production (3,493 rows, zero in the trailing 14 days).
    // `jobs_account_backfill` (the one-shot `backfill_unattributed_job_
    // accounts`) is what actually clears resolvable residue; this bound is
    // what keeps the small unresolvable remainder (a deleted atom) from
    // re-tripping this alert forever at a lower count.
    let alert_floor = as_of - UNATTRIBUTED_ALERT_WINDOW;
    match sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM jobs \
         WHERE status = 'done' AND kind = 'ExtractEntities' AND account_id IS NULL \
           AND finished_at <= $1 AND finished_at >= $2",
    )
    .bind(cutoff)
    .bind(alert_floor)
    .fetch_one(&t.pool)
    .await
    {
        Ok(n) if n > 0 => tracing::warn!(
            alert = true,
            count = n,
            tenant = ?t.tenant,
            "billing derivation: completed ExtractEntities jobs with no account_id — unattributable, \
             never billed, and no per-account cursor will ever cover them"
        ),
        Ok(_) => {}
        Err(e) => tracing::warn!(
            tenant = ?t.tenant, error = %e,
            "billing derivation: failed to count unattributed jobs this pass (non-fatal)"
        ),
    }

    let is_org = matches!(t.tenant, TenantId::Org(_));
    let account_ids: Vec<Uuid> = match &t.tenant {
        TenantId::Consumer => enumerate_consumer_accounts(control).await?,
        TenantId::Org(org) => vec![resolve_org_account_id(control, org).await?],
    };

    // ── Read phase: never abort on one account's failure, but track it ────
    let mut ready: Vec<(Uuid, AccountRead)> = Vec::with_capacity(account_ids.len());
    let mut failed: Vec<(Uuid, String)> = Vec::new();
    let mut cache_hit_total: i64 = 0;
    let mut unthreaded_total: i64 = 0;

    for account_id in account_ids.iter().copied() {
        match read_account_usage(t, control, account_id, is_org, cutoff, &sheet, as_of).await {
            Ok(read) => {
                cache_hit_total += read.cache_hit_rows;
                unthreaded_total += read.unthreaded_rows;
                ready.push((account_id, read));
            }
            Err(e) => {
                tracing::warn!(
                    %account_id, tenant = ?t.tenant, error = %e,
                    "billing derivation: failed to read usage for account this pass"
                );
                failed.push((account_id, e));
            }
        }
    }

    if unthreaded_total > 0 {
        tracing::warn!(
            alert = true,
            count = unthreaded_total,
            cache_hit_rows = cache_hit_total,
            tenant = ?t.tenant,
            "billing derivation: llm_usage rows with no source id and a non-cache-hit task \
             label — this count OVER-includes extract_batch.rs's still-unmetered legacy rows \
             (same task labels as the now-metered non-batch path; not distinguishable from this \
             table alone until PR6 lands), so treat it as an upper bound, not a precise anomaly \
             count"
        );
    } else if cache_hit_total > 0 {
        tracing::info!(
            cache_hit_rows = cache_hit_total,
            tenant = ?t.tenant,
            "billing derivation: source-id-less llm_usage rows this pass were all cache-hit \
             markers (zero-cost by construction)"
        );
    }

    if !failed.is_empty() {
        tracing::error!(
            failed = failed.len(),
            total = account_ids.len(),
            ?failed,
            tenant = ?t.tenant,
            "billing derivation: pass incomplete — refusing to write anything this cycle"
        );
        return Err(format!(
            "billing derivation: {}/{} accounts failed to report usage for tenant {:?} — \
             nothing written this pass",
            failed.len(),
            account_ids.len(),
            t.tenant
        ));
    }

    // ── Write phase: every account read cleanly, now commit ───────────────
    let mut inserted_rows: usize = 0;
    // Accounts whose write gave up on the financial lock this tick (#1497).
    // One contended account must not abort the writes of every account
    // after it in `ready` (on the consumer tenant: every personal account),
    // so a skip moves on; the tick is reported partial at the end and the
    // next tick retries the account from its unadvanced cursor. The skip
    // itself is already logged under `billing.lock_skip`.
    let mut skipped: Vec<Uuid> = Vec::new();
    for (account_id, read) in ready {
        if read.debits.is_empty() && !read.cursor_advanced() {
            continue;
        }
        let had_debits = !read.debits.is_empty();
        let n = match write_account_ledger(control, account_id, &read, reference_cost_per_credit).await {
            Ok(n) => n,
            Err(e) if crate::billing::entry_queue::skip_kind(&e).is_some() => {
                skipped.push(account_id);
                continue;
            }
            Err(e) => {
                return Err(format!(
                    "billing derivation: failed to write ledger for account {account_id} \
                     (tenant {:?}): {e}",
                    t.tenant
                ))
            }
        };
        inserted_rows += n as usize;

        // ── Enforcement observation — usage-metering PR10 ───────────────────
        //
        // "The debit path" (plan §1 PR10's own phrase): right after this
        // account's debit(s) for this tick have COMMITTED (write_account_ledger
        // returned Ok above), check whether its materialized balance is now
        // <= 0 and, if so, record one `enforcement_events` row via
        // `billing::enforcement::check_and_record`. This NEVER affects
        // anything above — the debit already committed before this runs, and
        // a failure here is logged and swallowed, never propagated as a tick
        // error, never a reason to roll back or retry an already-successful
        // debit. That is the whole point: this is an OBSERVATION about a
        // write that already happened, not a gate on whether it happens (see
        // that module's doc comment, and the plan's C2 invariant: writes are
        // never rejected).
        //
        // Only checked when THIS tick actually had debits for the account —
        // a tick that only advanced a cursor with zero new debits has
        // nothing new to observe, and re-observing a balance that hasn't
        // moved on every idle tick would be pure noise on an
        // already-recorded state.
        if had_debits {
            if let Err(e) = enforcement::check_and_record(
                control,
                account_id,
                "usage_debit_zero_or_negative_balance",
            )
            .await
            {
                tracing::warn!(
                    %account_id, tenant = ?t.tenant, error = %e,
                    "billing derivation: enforcement observation failed this pass (non-fatal — \
                     the debit itself already committed)"
                );
            }
        }
    }

    if !skipped.is_empty() {
        // Partial, the same way `recover_managed` reports an unavailable
        // account: everything else committed, the tick is still an error so
        // the harness and the logs see it, and nothing was marked.
        return Err(format!(
            "billing derivation: {} account(s) skipped on the financial lock this tick \
             (tenant {:?}; retried next tick, nothing marked): {skipped:?}; {inserted_rows} ledger rows written",
            skipped.len(),
            t.tenant
        ));
    }
    if inserted_rows > 0 {
        Ok(Tick::worked(inserted_rows))
    } else {
        Ok(Tick::idle())
    }
}

// ── Account roster ────────────────────────────────────────────────────────

/// Control-table read, needs no scope — same as
/// `platform_rollup::consumer_tick`'s identical enumeration.
///
/// `pub(crate)`: reused verbatim by `workers::usage_reconciliation` (PR11),
/// which needs the identical Consumer/Org account-roster shape this worker
/// already built — reusing it here rather than re-implementing is the
/// literal "do not duplicate or contradict PR9" instruction from that PR's
/// brief.
pub(crate) async fn enumerate_consumer_accounts(control: &PgPool) -> Result<Vec<Uuid>, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts ORDER BY id")
        .fetch_all(control)
        .await
        .map_err(|e| format!("billing derivation: consumer account enumeration failed: {e}"))
}

/// The one billing account an org tenant DB belongs to (`tenant_databases`
/// maps `clerk_org_id -> account_id`; platform_rollup's own module doc: "a
/// tenant DB holds exactly one org").
///
/// `pub(crate)`: see [`enumerate_consumer_accounts`]'s doc — reused verbatim
/// by `workers::usage_reconciliation` (PR11).
pub(crate) async fn resolve_org_account_id(control: &PgPool, clerk_org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(clerk_org_id)
        .fetch_optional(control)
        .await
        .map_err(|e| format!("billing derivation: failed to resolve account for org {clerk_org_id}: {e}"))?
        .ok_or_else(|| format!("billing derivation: no tenant_databases row for org {clerk_org_id}"))
}

/// Which scoped-transaction primitive to open on `t.pool` for reading
/// `llm_usage` — see the module doc's "Per-account loop on BOTH branches"
/// section for why these differ per plane.
///
/// Kept `private` (unlike [`enumerate_consumer_accounts`]/
/// [`resolve_org_account_id`] above, both `pub(crate)` and reused verbatim by
/// `workers::usage_reconciliation`, PR11): `scripts/check_rls_coverage.sh`'s
/// `converted` check greps each file for a LITERAL `begin_account_scoped`/
/// `begin_tenant_system_scoped` call, not a transitive one through an
/// imported helper, so `usage_reconciliation.rs` calls those two primitives
/// directly (its own two-line match, same shape as this one) rather than
/// importing this function — see that file's own doc for why.
async fn open_read_scope(
    pool: &PgPool,
    tenant: &TenantId,
    account_id: Uuid,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    match tenant {
        TenantId::Consumer => begin_account_scoped(pool, account_id).await,
        TenantId::Org(_) => begin_tenant_system_scoped(pool, account_id).await,
    }
}

// ── Cursor state ──────────────────────────────────────────────────────────

/// One account's three axis cursors, loaded from `credit_balances` (NULL =
/// "no cursor yet for this account / this axis" — see migration `0151`'s
/// header).
#[derive(Debug, Clone, Copy, Default)]
struct AccountCursor {
    job: Option<(DateTime<Utc>, Uuid)>,
    query: Option<(DateTime<Utc>, Uuid)>,
    journal: Option<(DateTime<Utc>, Uuid)>,
    receipt: Option<(DateTime<Utc>, Uuid)>,
}

/// # Why this opens a transaction to run one `SELECT`
///
/// `credit_balances` is RLS-covered (`account_isolation`, migration `0150`)
/// with `USING (account_id = current_setting('app.account_id')::uuid)`. On a
/// connection that has NOT been account-scoped, `app.account_id` is the empty
/// string and `''::uuid` raises `invalid input syntax for type uuid: ""` —
/// which Postgres evaluates at executor init for the primary-key index
/// condition, so **it throws even when the table is empty**. A bare
/// `.fetch_optional(control)` here therefore does not read zero rows; it
/// hard-errors under `coralswarm_app`.
///
/// That is not hypothetical. This function ran unscoped from the day the
/// worker shipped, and every pass failed with exactly that error for every
/// account. [`tick`]'s refuse-to-write-on-partial-failure discipline then
/// correctly declined to write anything, so `credit_ledger` and
/// `credit_balances` stayed EMPTY in production while `llm_usage` accumulated
/// 71,687 rows. The failure was loud in the logs and invisible in the data.
///
/// The tests never caught it because they connect as an owner role with
/// `rolbypassrls = true`; only `coralswarm_app` (`rolbypassrls = false`) —
/// i.e. only production — evaluates the policy at all.
async fn load_cursor(control: &PgPool, account_id: Uuid) -> Result<AccountCursor, sqlx::Error> {
    let mut tx = begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    #[allow(clippy::type_complexity)]
    let row: Option<(
        Option<DateTime<Utc>>,
        Option<Uuid>,
        Option<DateTime<Utc>>,
        Option<Uuid>,
        Option<DateTime<Utc>>,
        Option<Uuid>,
        Option<DateTime<Utc>>,
        Option<Uuid>,
    )> = sqlx::query_as(
        "SELECT job_axis_cursor_finished_at, job_axis_cursor_job_id, \
                query_axis_cursor_created_at, query_axis_cursor_usage_id, \
                journal_axis_cursor_period, journal_axis_cursor_usage_id, \
                receipt_cursor_at, receipt_cursor_id \
         FROM credit_balances WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;

    // Read-only, but committed rather than dropped — same discipline as
    // `routes::usage`'s scoped read, so the scope ends explicitly.
    tx.commit().await?;

    Ok(match row {
        None => AccountCursor::default(),
        Some((jf, ji, qc, qi, jp, jui, rc, ri)) => AccountCursor {
            job: jf.zip(ji),
            query: qc.zip(qi),
            journal: jp.zip(jui),
            receipt: rc.zip(ri),
        },
    })
}

/// The one cursor-advance policy shared by all three axes — see the module
/// doc's "The cursor" section for the full reasoning. `batch_len` /
/// `batch_limit` distinguish "this read drained everything up to `cutoff`"
/// (`batch_len < batch_limit`, safe to jump to the overlap-protected
/// boundary) from "this read was truncated" (`batch_len == batch_limit`,
/// standard keyset pagination continues from `last_row` instead — jumping
/// straight to the overlap boundary here could skip whatever backlog didn't
/// fit in this batch).
///
/// Never moves the cursor BACKWARD relative to `old` — `old` is itself
/// always a valid, already-safe position (either `None`, or the result of a
/// previous call to this same function), so a computed target at or behind
/// it represents no forward progress and is simply skipped (returns `old`
/// unchanged) rather than regressing.
fn advance_cursor(
    old: Option<(DateTime<Utc>, Uuid)>,
    cutoff: DateTime<Utc>,
    batch_len: usize,
    batch_limit: usize,
    last_row: Option<(DateTime<Utc>, Uuid)>,
) -> Option<(DateTime<Utc>, Uuid)> {
    if batch_len >= batch_limit {
        // Truncated — more backlog may remain before `cutoff`. Advance only
        // to the last row actually seen (ordinary keyset pagination); never
        // jump ahead to the overlap boundary while a backlog might still be
        // sitting between here and there.
        return last_row.or(old);
    }
    // Drained: nothing else exists between `old` and `cutoff` as of this
    // read. Safe to advance all the way to the overlap-protected boundary,
    // clamped so it never moves backward relative to `old`.
    let target = cutoff - OVERLAP_WINDOW;
    match old {
        Some((old_ts, _)) if old_ts >= target => old,
        _ => Some((target, Uuid::nil())),
    }
}

// ── Ledger debit rows (in-memory, pre-write) ─────────────────────────────

#[derive(Debug, Clone)]
struct LedgerDebit {
    funded: Option<crate::billing::managed_reservation::TerminalFunding>,
    entry_kind: &'static str,
    /// The final, ledger-ready NEGATIVE credit amount is only computable once
    /// `credit_conversion.reference_cost_per_credit` is in scope (loaded once
    /// in [`tick`], not re-loaded per debit) — see [`credits_for_cost`], called
    /// from [`write_account_ledger`]. This struct carries only `cost_usd`
    /// through the read phase; there is deliberately no `credits` field here
    /// to go stale against.
    cost_usd: Decimal,
    source_job_id: Option<Uuid>,
    source_query_id: Option<Uuid>,
    graph_id: Option<Uuid>,
    journal_period: Option<DateTime<Utc>>,
    journal_granularity: Option<String>,
    /// COR-88: the managed attempt receipt this debit charges.
    source_receipt_id: Option<Uuid>,
    /// Billing Phase 2a — see the module doc's "Per-member attribution"
    /// section. Resolved by [`resolve_debit_attribution`] from every
    /// `llm_usage` row this debit aggregates; `None` for "no member in
    /// scope on any row" AND for "rows disagree" alike (see that function's
    /// own doc for why those two cases are deliberately not distinguished on
    /// the ledger row itself).
    attributed_user_id: Option<Uuid>,
}

/// Billing Phase 2a's attribution-resolution rule — see the module doc's
/// "Disagreement policy" section for the full reasoning. `seen` is every
/// DISTINCT non-NULL `attributed_user_id` observed among the `llm_usage`
/// rows folded into one debit. Returns `(resolved, conflicted)`:
/// `conflicted` is `true` only when 2+ distinct members were seen, so a
/// caller can log a `warn!` for that case specifically without also
/// warning on the far more common "nobody attributed" case (`seen` empty).
///
/// `pub(crate)`: this axis-agnostic rule is also what
/// `workers::summary::resolve_meeting_attribution` uses to resolve a
/// meeting's chunk-atom `ingested_by` values onto its `summary_map`/
/// `summary_reduce` calls — one rule, one place it is decided, not a second
/// copy that could silently drift from this one.
pub(crate) fn resolve_debit_attribution(seen: &HashSet<Uuid>) -> (Option<Uuid>, bool) {
    let mut it = seen.iter();
    match (it.next(), it.next()) {
        (None, _) => (None, false),
        (Some(only), None) => (Some(*only), false),
        (Some(_), Some(_)) => (None, true),
    }
}

/// The full read-phase result for one account — everything [`tick`] needs
/// before it can decide whether to write.
#[derive(Debug, Default)]
struct AccountRead {
    debits: Vec<LedgerDebit>,
    old_cursor: AccountCursor,
    new_cursor: AccountCursor,
    cache_hit_rows: i64,
    unthreaded_rows: i64,
}

impl AccountRead {
    fn cursor_advanced(&self) -> bool {
        self.new_cursor.job != self.old_cursor.job
            || self.new_cursor.query != self.old_cursor.query
            || self.new_cursor.journal != self.old_cursor.journal
            || self.new_cursor.receipt != self.old_cursor.receipt
    }
}

// ── Decimal helpers — see module doc's "Decimal, never float" section ────

/// `f64` -> exact `Decimal`, via a FIXED-PRECISION string round-trip rather
/// than `Decimal::from_f64_retain` — the latter would carry binary-float
/// representation noise (e.g. `0.1` is not exactly representable in `f64`)
/// into a column whose whole purpose is exact decimal accounting. 8 decimal
/// places comfortably exceeds any real per-token USD rate's precision
/// (`model_rates` seeds have at most 2 decimal digits per-million, so a
/// per-call cost at realistic token counts never needs more than a handful
/// of significant digits past the decimal point).
fn decimal_from_f64(v: f64) -> Decimal {
    let s = format!("{v:.8}");
    s.parse::<Decimal>().unwrap_or(Decimal::ZERO)
}

/// `cost / reference_cost_per_credit`, exact `Decimal` division throughout
/// (rust_decimal's `Div` is exact within its internal scale, never routed
/// through `f64`). Returns a NEGATIVE value — see [`LedgerDebit::credits`].
fn credits_for_cost(cost_usd: Decimal, reference_cost_per_credit: Decimal) -> Decimal {
    if reference_cost_per_credit.is_zero() {
        return Decimal::ZERO;
    }
    -(cost_usd / reference_cost_per_credit)
}

/// One (task, model)-agnostic token bundle priced against [`PriceSheet`],
/// returned as an exact [`Decimal`] via [`decimal_from_f64`].
///
/// `provider` (OpenRouter provider port, PR1) is the REAL per-bucket
/// provider, threaded all the way from each axis's own `llm_usage.provider`
/// column (via `ModelBucketRow`/`UsageRow`, see `read_job_axis`/
/// `read_query_axis`/`read_journal_axis`) — not hardcoded, unlike
/// `routes::admin::model_rate`/`routes::usage::estimate_cost_usd`, whose
/// own aggregation queries don't carry it per bucket (see their own doc
/// comments for why those two stay on `PriceSheet::rate`'s provider-
/// agnostic pattern-match fallback instead). This path only runs when
/// [`ModelBucket::cost_usd_sum`] is `None` — i.e. NOT every row in the
/// bucket carried a provider-reported cost — which OpenRouter (this PR
/// wires `usage.cost` unconditionally on every response) makes the
/// UNCOMMON case; this is a fallback + reconciliation cross-check, not the
/// primary pricing path (see [`price_bucket`]).
fn estimate_cost_usd(
    sheet: &PriceSheet,
    as_of: DateTime<Utc>,
    provider: &str,
    model: &str,
    input_tokens: i64,
    output_tokens: i64,
    cache_creation_tokens: i64,
    cache_read_tokens: i64,
) -> Decimal {
    let r = sheet.rate(provider, model, as_of);
    let cost = (input_tokens as f64 / 1_000_000.0) * r.input_per_million
        + (output_tokens as f64 / 1_000_000.0) * r.output_per_million
        + (cache_creation_tokens as f64 / 1_000_000.0) * r.input_per_million * r.cache_write_multiplier
        + (cache_read_tokens as f64 / 1_000_000.0) * r.input_per_million * r.cache_read_multiplier;
    decimal_from_f64(cost)
}

// ── Read phase ────────────────────────────────────────────────────────────

/// One raw `llm_usage` model-bucket row, shared shape across all three axes'
/// aggregation queries.
struct ModelBucket {
    input_tokens: i64,
    output_tokens: i64,
    cache_creation_tokens: i64,
    cache_read_tokens: i64,
    /// `Some` only when EVERY row folded into this bucket carried a
    /// provider-reported `cost_usd` — `None` the moment even ONE row in the
    /// bucket is missing it (an errored/estimated finalize, a
    /// `StreamUsageGuard::finalize_err`/`Drop` row, a `usage_pending_sweep`
    /// orphan — none of these carry a real cost). OpenRouter (this PR wires
    /// `usage.cost` unconditionally on every response) is the first
    /// provider in this tree to populate this at all; the JOURNAL axis
    /// implements the all-or-nothing contract via a Rust-side
    /// fold (`(Some(acc), Some(v)) => Some(acc + v), _ => None`), the JOB
    /// axis via the SQL-level `CASE WHEN bool_and(cost_usd IS NOT NULL)
    /// THEN SUM(cost_usd) END` in `read_job_axis` — a bare `SUM(cost_usd)`
    /// silently ignores NULLs in Postgres, which would have summed only the
    /// non-NULL rows in a mixed bucket instead of correctly falling back to
    /// [`estimate_cost_usd`] for the whole bucket.
    cost_usd_sum: Option<Decimal>,
}

/// Prefer the provider-reported sum when present, else the token-based
/// estimate — the "prefer provider-reported cost_usd, fall back to
/// model_rates" branch the plan requires.
fn price_bucket(
    sheet: &PriceSheet,
    as_of: DateTime<Utc>,
    provider: &str,
    model: &str,
    bucket: &ModelBucket,
) -> Decimal {
    match bucket.cost_usd_sum {
        Some(cost) => cost,
        None => estimate_cost_usd(
            sheet,
            as_of,
            provider,
            model,
            bucket.input_tokens,
            bucket.output_tokens,
            bucket.cache_creation_tokens,
            bucket.cache_read_tokens,
        ),
    }
}

/// Read every axis for one account inside ONE scoped transaction on
/// `t.pool` — see the module doc's "Partial writes" section for why this
/// must complete (or fail) as a unit before [`tick`] decides whether to
/// write anything. `control` is needed only to load this account's cursor
/// (`credit_balances` is control-plane) before the `t.pool` transaction
/// opens.
#[allow(clippy::too_many_arguments)]
async fn read_account_usage(
    t: &WorkerTenant,
    control: &PgPool,
    account_id: Uuid,
    is_org: bool,
    cutoff: DateTime<Utc>,
    sheet: &PriceSheet,
    as_of: DateTime<Utc>,
) -> Result<AccountRead, String> {
    let old_cursor = load_cursor(control, account_id)
        .await
        .map_err(|e| format!("failed to load cursor: {e}"))?;

    let mut tx = open_read_scope(&t.pool, &t.tenant, account_id)
        .await
        .map_err(|e| format!("failed to open read scope: {e}"))?;

    let (job_debits, new_job_cursor) =
        read_job_axis(&mut tx, account_id, old_cursor.job, cutoff, sheet, as_of).await?;
    let (query_debits, new_query_cursor) =
        read_query_axis(&mut tx, account_id, old_cursor.query, cutoff, sheet).await?;
    let (journal_debits, new_journal_cursor, journal_unattributed) =
        read_journal_axis(&mut tx, account_id, is_org, old_cursor.journal, cutoff, sheet, as_of).await?;
    let (receipt_debits, new_receipt_cursor) =
        read_receipt_axis(&mut tx, account_id, old_cursor.receipt, cutoff).await?;
    let (cache_hit_rows, unthreaded_rows) =
        classify_unthreaded_usage(&mut tx, account_id, is_org, as_of).await?;

    tx.commit()
        .await
        .map_err(|e| format!("failed to commit read scope: {e}"))?;

    let mut debits = job_debits;
    debits.extend(query_debits);
    debits.extend(journal_debits);
    debits.extend(receipt_debits);

    Ok(AccountRead {
        debits,
        old_cursor,
        new_cursor: AccountCursor {
            job: new_job_cursor.or(old_cursor.job),
            query: new_query_cursor.or(old_cursor.query),
            journal: new_journal_cursor.or(old_cursor.journal),
            receipt: new_receipt_cursor.or(old_cursor.receipt),
        },
        cache_hit_rows,
        unthreaded_rows: unthreaded_rows + journal_unattributed,
    })
}

/// Job axis — see the module doc's "The three axes" section.
async fn read_job_axis(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    cutoff: DateTime<Utc>,
    sheet: &PriceSheet,
    as_of: DateTime<Utc>,
) -> Result<(Vec<LedgerDebit>, Option<(DateTime<Utc>, Uuid)>), String> {
    let (cursor_ts, cursor_id) = cursor.unzip();

    #[derive(sqlx::FromRow)]
    struct JobRow {
        id: Uuid,
        finished_at: DateTime<Utc>,
    }

    // Job-axis billing: extraction plus Library kind re-eval. `source_job_id`
    // on `llm_usage` is not enough — this query is what selects which `jobs`
    // rows produce a debit.
    let jobs: Vec<JobRow> = sqlx::query_as(
        "SELECT id, finished_at FROM jobs \
         WHERE account_id = $1 AND status = 'done' AND kind = ANY($6) \
           AND finished_at <= $2 \
           AND (finished_at, id) > (COALESCE($3, '-infinity'::timestamptz), \
                                     COALESCE($4, '00000000-0000-0000-0000-000000000000'::uuid)) \
         ORDER BY finished_at, id \
         LIMIT $5",
    )
    .bind(account_id)
    .bind(cutoff)
    .bind(cursor_ts)
    .bind(cursor_id)
    .bind(READ_BATCH_SIZE)
    .bind(JOB_AXIS_KINDS)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| format!("job axis: failed to read candidate jobs: {e}"))?;

    let new_cursor = advance_cursor(
        cursor,
        cutoff,
        jobs.len(),
        READ_BATCH_SIZE as usize,
        jobs.last().map(|j| (j.finished_at, j.id)),
    );

    if jobs.is_empty() {
        return Ok((Vec::new(), new_cursor));
    }

    let job_ids: Vec<Uuid> = jobs.iter().map(|j| j.id).collect();

    #[derive(sqlx::FromRow)]
    struct ModelBucketRow {
        source_job_id: Uuid,
        model: String,
        // OpenRouter provider port, PR1 — IN the GROUP BY for the identical
        // reason `attributed_user_id` below is: without it, a job whose
        // `(model)` bucket somehow spans two DIFFERENT providers (should not
        // happen in practice — model ids are provider-namespaced — but the
        // schema doesn't forbid it) would collapse into one row and get
        // priced under a SINGLE, possibly-wrong provider string. Passed
        // straight through to `sheet.rate(provider, model, as_of)` via
        // `price_bucket` below.
        provider: String,
        // Billing Phase 2a — IN the GROUP BY (see the module doc's
        // "Disagreement policy" section), not just SELECTed: this makes a
        // job whose rows carry different `attributed_user_id` values (or a
        // mix of NULL and a real id) surface as multiple bucket rows for
        // the SAME `(source_job_id, model)` rather than being silently
        // collapsed by an ungrouped aggregate. `by_job` below still sums
        // COST across every bucket for a job regardless of this column —
        // the finer grouping changes nothing about the priced total, only
        // what [`resolve_debit_attribution`] gets to see per job.
        attributed_user_id: Option<Uuid>,
        input_tokens: i64,
        output_tokens: i64,
        cache_creation_tokens: i64,
        cache_read_tokens: i64,
        cost_usd_sum: Option<Decimal>,
    }

    // `SUM(bigint)` -> NUMERIC decode pitfall (the codebase's own repeated
    // gotcha, see `platform_rollup::collect_account_stats`) — explicit
    // `::bigint` casts below.
    //
    // `CASE WHEN bool_and(cost_usd IS NOT NULL) THEN SUM(cost_usd) END`
    // (OpenRouter provider port, PR1 fix): bare `SUM(cost_usd)` silently
    // IGNORES NULL rows in Postgres, returning the sum of only the non-NULL
    // ones for a bucket that mixes billed and unbilled (errored/estimated)
    // rows — `ModelBucket::cost_usd_sum`'s own contract ("Some only when
    // EVERY row carried cost") was violated here even though the QUERY and
    // JOURNAL axes below already implement it correctly in Rust (their
    // row-level fold does `(Some(acc), Some(v)) => Some(acc + v), _ =>
    // None`). This `CASE`/`bool_and` expression reproduces that SAME
    // all-or-nothing contract at the SQL layer: `None` (NULL) unless every
    // row in the group has a non-NULL `cost_usd`.
    let buckets: Vec<ModelBucketRow> = sqlx::query_as(
        "SELECT source_job_id, model, provider, attributed_user_id, \
                COALESCE(SUM(input_tokens), 0)::bigint AS input_tokens, \
                COALESCE(SUM(output_tokens), 0)::bigint AS output_tokens, \
                COALESCE(SUM(cache_creation_input_tokens), 0)::bigint AS cache_creation_tokens, \
                COALESCE(SUM(cache_read_input_tokens), 0)::bigint AS cache_read_tokens, \
                CASE WHEN bool_and(cost_usd IS NOT NULL) THEN SUM(cost_usd) END AS cost_usd_sum \
         FROM llm_usage \
         WHERE source_job_id = ANY($1) \
         GROUP BY source_job_id, model, provider, attributed_user_id",
    )
    .bind(&job_ids)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| format!("job axis: failed to sum llm_usage for jobs: {e}"))?;

    let mut by_job: HashMap<Uuid, Decimal> = HashMap::new();
    let mut by_job_attrs: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    for row in &buckets {
        if let Some(u) = row.attributed_user_id {
            by_job_attrs.entry(row.source_job_id).or_default().insert(u);
        }
        let bucket = ModelBucket {
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            cache_creation_tokens: row.cache_creation_tokens,
            cache_read_tokens: row.cache_read_tokens,
            cost_usd_sum: row.cost_usd_sum,
        };
        let cost = price_bucket(sheet, as_of, &row.provider, &row.model, &bucket);
        *by_job.entry(row.source_job_id).or_insert(Decimal::ZERO) += cost;
    }

    // Every job in the window gets exactly one ledger row, even a job whose
    // extraction made no billable call at all (a bare `NothingToDo`
    // completion, or one whose only llm_usage row is a zero-token
    // `*_cached` marker) — a complete audit trail, and a stable idempotency
    // anchor so this job's id never gets "reconsidered" ambiguously later.
    let debits = job_ids
        .into_iter()
        .map(|job_id| {
            let cost_usd = by_job.get(&job_id).copied().unwrap_or(Decimal::ZERO);
            let empty = HashSet::new();
            let seen = by_job_attrs.get(&job_id).unwrap_or(&empty);
            let (attributed_user_id, conflicted) = resolve_debit_attribution(seen);
            if conflicted {
                tracing::warn!(
                    %job_id, seen = seen.len(),
                    "billing-derivation: job axis debit's llm_usage rows disagree on \
                     attributed_user_id — recording the debit unattributed rather than \
                     guessing (see the module doc's Disagreement policy)"
                );
            }
            LedgerDebit {
                funded: None,
                entry_kind: ENTRY_KIND_JOB,
                cost_usd,
                source_job_id: Some(job_id),
                source_query_id: None,
                graph_id: None,
                journal_period: None,
                journal_granularity: None,
                source_receipt_id: None,
                attributed_user_id,
            }
        })
        .collect();

    Ok((debits, new_cursor))
}

/// Query axis — see the module doc's "The three axes" section.
async fn read_query_axis(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    cutoff: DateTime<Utc>,
    sheet: &PriceSheet,
) -> Result<(Vec<LedgerDebit>, Option<(DateTime<Utc>, Uuid)>), String> {
    let (cursor_ts, cursor_id) = cursor.unzip();

    #[derive(sqlx::FromRow)]
    struct UsageRow {
        last_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
        source_query_id: Uuid,
        model: String,
        provider: String,
        input_tokens: i32,
        output_tokens: i32,
        cache_creation_input_tokens: i64,
        cache_read_input_tokens: i64,
        cost_usd: Option<Decimal>,
        attributed_user_id: Option<Uuid>,
    }

    // Request-path callers (`routes::queries`) write a real `account_id` on
    // BOTH planes (PR4's `UsageContext::new(pool, Some(user_ctx.account_id),
    // ...)`), so this axis's `account_id` filter is uniform — unlike the
    // journal axis below, no Org special case is needed here.
    let rows: Vec<UsageRow> = sqlx::query_as(
        "WITH touched AS ( \
             SELECT DISTINCT source_query_id FROM llm_usage \
              WHERE account_id = $1 AND source_query_id IS NOT NULL \
                AND created_at >= COALESCE($3, '-infinity'::timestamptz)), \
         ready AS ( \
             SELECT u.source_query_id, max(u.created_at) AS last_at \
               FROM llm_usage u JOIN touched t USING (source_query_id) \
              WHERE u.account_id = $1 \
              GROUP BY u.source_query_id \
             HAVING max(u.created_at) <= $2 \
                AND (max(u.created_at), u.source_query_id) > \
                    (COALESCE($3, '-infinity'::timestamptz), \
                     COALESCE($4, '00000000-0000-0000-0000-000000000000'::uuid)) \
              ORDER BY last_at, u.source_query_id \
              LIMIT $5) \
         SELECT r.last_at, u.created_at, u.source_query_id, u.model, u.provider, u.input_tokens, \
                u.output_tokens, u.cache_creation_input_tokens, u.cache_read_input_tokens, \
                u.cost_usd, u.attributed_user_id \
           FROM llm_usage u JOIN ready r USING (source_query_id) \
          WHERE u.account_id = $1",
    )
    .bind(account_id)
    .bind(cutoff)
    .bind(cursor_ts)
    .bind(cursor_id)
    .bind(READ_BATCH_SIZE)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| format!("query axis: failed to read candidate rows: {e}"))?;

    let queries: HashSet<Uuid> = rows.iter().map(|r| r.source_query_id).collect();
    let new_cursor = advance_cursor(
        cursor,
        cutoff,
        queries.len(),
        READ_BATCH_SIZE as usize,
        rows.iter().map(|r| (r.last_at, r.source_query_id)).max(),
    );

    if rows.is_empty() {
        return Ok((Vec::new(), new_cursor));
    }

    let mut by_query: HashMap<Uuid, Decimal> = HashMap::new();
    let mut by_query_attrs: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    for row in &rows {
        let cost = row.cost_usd.unwrap_or_else(|| estimate_cost_usd(
            sheet, row.created_at, &row.provider, &row.model,
            row.input_tokens as i64, row.output_tokens as i64,
            row.cache_creation_input_tokens, row.cache_read_input_tokens,
        ));
        *by_query.entry(row.source_query_id).or_default() += cost;
        if let Some(u) = row.attributed_user_id {
            by_query_attrs.entry(row.source_query_id).or_default().insert(u);
        }
    }

    let mut debits = Vec::with_capacity(by_query.len());
    for (query_id, cost_usd) in by_query {
        let empty = HashSet::new();
        let seen = by_query_attrs.get(&query_id).unwrap_or(&empty);
        let (attributed_user_id, conflicted) = resolve_debit_attribution(seen);
        if conflicted {
            tracing::warn!(
                %query_id, seen = seen.len(),
                "billing-derivation: query axis debit's llm_usage rows disagree on \
                 attributed_user_id — recording the debit unattributed rather than \
                 guessing (see the module doc's Disagreement policy)"
            );
        }
        debits.push(LedgerDebit {
            funded: None,
            entry_kind: ENTRY_KIND_QUERY,
            cost_usd,
            source_job_id: None,
            source_query_id: Some(query_id),
            graph_id: None,
            journal_period: None,
            journal_granularity: None,
            source_receipt_id: None,
            attributed_user_id,
        });
    }

    debits.sort_unstable_by_key(|debit| debit.source_query_id);
    Ok((debits, new_cursor))
}

/// Journal axis — see the module doc's "The three axes" section for the
/// Org-plane account-filter special case.
#[allow(clippy::too_many_arguments)]
async fn read_journal_axis(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    is_org: bool,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    cutoff: DateTime<Utc>,
    sheet: &PriceSheet,
    as_of: DateTime<Utc>,
) -> Result<(Vec<LedgerDebit>, Option<(DateTime<Utc>, Uuid)>, i64), String> {
    let (cursor_ts, cursor_id) = cursor.unzip();

    #[derive(sqlx::FromRow)]
    struct UsageRow {
        id: Uuid,
        // `created_at` is filtered in SQL (the lag-window cutoff) but never
        // read back in Rust for this axis — the cursor orders on
        // `source_journal_period`, not `created_at` (see the module doc's
        // "The cursor" section) — so it is deliberately NOT selected here.
        graph_id: Option<Uuid>,
        source_journal_period: DateTime<Utc>,
        source_journal_granularity: Option<String>,
        model: String,
        // OpenRouter provider port, PR1 — see `read_query_axis`'s
        // `UsageRow::provider` doc; same reasoning, same fix.
        provider: String,
        input_tokens: i32,
        output_tokens: i32,
        cache_creation_input_tokens: i64,
        cache_read_input_tokens: i64,
        cost_usd: Option<Decimal>,
        // Billing Phase 2a — see the module doc's "Disagreement policy"
        // section, journal-axis bullet: this is the axis where genuine
        // multi-member disagreement is EXPECTED, not just defensively
        // handled.
        attributed_user_id: Option<Uuid>,
    }

    // `journal_sweep.rs` passes `account_id: None` to `UsageContext` on the
    // Org arm (by design — see that file's own `usage_account_id` doc), so
    // `llm_usage.account_id` cannot filter Org-plane journal rows the way it
    // filters job/query rows. The Org branch is ALREADY scoped to this one
    // account's whole tenant DB (see the module doc's per-account-loop
    // section), so it omits the account_id predicate entirely instead of
    // resolving `graphs.account_id` row by row.
    let rows: Vec<UsageRow> = if is_org {
        sqlx::query_as(
            "SELECT id, graph_id, source_journal_period, source_journal_granularity, \
                    model, provider, input_tokens, output_tokens, cache_creation_input_tokens, \
                    cache_read_input_tokens, cost_usd, attributed_user_id \
             FROM llm_usage \
             WHERE source_journal_period IS NOT NULL AND created_at <= $1 \
               AND (source_journal_period, id) > (COALESCE($2, '-infinity'::timestamptz), \
                                                   COALESCE($3, '00000000-0000-0000-0000-000000000000'::uuid)) \
             ORDER BY source_journal_period, id \
             LIMIT $4",
        )
        .bind(cutoff)
        .bind(cursor_ts)
        .bind(cursor_id)
        .bind(READ_BATCH_SIZE)
        .fetch_all(&mut **tx)
        .await
    } else {
        sqlx::query_as(
            "SELECT id, graph_id, source_journal_period, source_journal_granularity, \
                    model, provider, input_tokens, output_tokens, cache_creation_input_tokens, \
                    cache_read_input_tokens, cost_usd, attributed_user_id \
             FROM llm_usage \
             WHERE account_id = $1 AND source_journal_period IS NOT NULL AND created_at <= $2 \
               AND (source_journal_period, id) > (COALESCE($3, '-infinity'::timestamptz), \
                                                   COALESCE($4, '00000000-0000-0000-0000-000000000000'::uuid)) \
             ORDER BY source_journal_period, id \
             LIMIT $5",
        )
        .bind(account_id)
        .bind(cutoff)
        .bind(cursor_ts)
        .bind(cursor_id)
        .bind(READ_BATCH_SIZE)
        .fetch_all(&mut **tx)
        .await
    }
    .map_err(|e| format!("journal axis: failed to read candidate rows: {e}"))?;

    // The advanced cursor covers EVERY row in the batch, including ones with
    // `graph_id IS NULL` (unattributable — `credit_ledger`'s own CHECK
    // constraint `journal_period IS NULL OR graph_id IS NOT NULL` would
    // reject them as ledger rows anyway) — a permanently-unbillable row must
    // not get re-read forever. See the module doc's "The cursor" section for
    // the overlap-window policy `advance_cursor` implements.
    let new_cursor = advance_cursor(
        cursor,
        cutoff,
        rows.len(),
        READ_BATCH_SIZE as usize,
        rows.last().map(|r| (r.source_journal_period, r.id)),
    );

    if rows.is_empty() {
        return Ok((Vec::new(), new_cursor, 0));
    }

    let mut unattributed = 0i64;
    // Keyed on `(provider, model)`, not `model` alone (OpenRouter provider
    // port, PR1) — see `UsageRow::provider`'s own doc.
    let mut by_group: HashMap<(Uuid, String, DateTime<Utc>), HashMap<(String, String), ModelBucket>> =
        HashMap::new();
    let mut by_group_attrs: HashMap<(Uuid, String, DateTime<Utc>), HashSet<Uuid>> = HashMap::new();
    for row in &rows {
        let Some(graph_id) = row.graph_id else {
            unattributed += 1;
            continue;
        };
        let Some(granularity) = row.source_journal_granularity.clone() else {
            // Paired-by-CHECK on the write side (`llm_usage`'s own CHECK
            // constraint requires both or neither) — defensive only.
            unattributed += 1;
            continue;
        };
        let key = (graph_id, granularity, row.source_journal_period);
        let per_model = by_group.entry(key.clone()).or_default();
        let entry = per_model
            .entry((row.provider.clone(), row.model.clone()))
            .or_insert(ModelBucket {
                input_tokens: 0,
                output_tokens: 0,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
                cost_usd_sum: Some(Decimal::ZERO),
            });
        entry.input_tokens += row.input_tokens as i64;
        entry.output_tokens += row.output_tokens as i64;
        entry.cache_creation_tokens += row.cache_creation_input_tokens;
        entry.cache_read_tokens += row.cache_read_input_tokens;
        entry.cost_usd_sum = match (entry.cost_usd_sum, row.cost_usd) {
            (Some(acc), Some(v)) => Some(acc + v),
            _ => None,
        };
        if let Some(u) = row.attributed_user_id {
            by_group_attrs.entry(key).or_default().insert(u);
        }
    }

    let mut debits = Vec::with_capacity(by_group.len());
    for ((graph_id, granularity, period), per_model) in by_group {
        let mut cost_usd = Decimal::ZERO;
        for ((provider, model), bucket) in &per_model {
            cost_usd += price_bucket(sheet, as_of, provider, model, bucket);
        }
        let key = (graph_id, granularity.clone(), period);
        let empty = HashSet::new();
        let seen = by_group_attrs.get(&key).unwrap_or(&empty);
        let (attributed_user_id, conflicted) = resolve_debit_attribution(seen);
        if conflicted {
            // Expected to fire in practice — see the module doc's
            // journal-axis bullet — so this stays at `warn!` (visible, not
            // silent) without being treated as an anomaly to chase down.
            tracing::warn!(
                %graph_id, %granularity, %period, seen = seen.len(),
                "billing-derivation: journal axis debit's llm_usage rows disagree on \
                 attributed_user_id (multiple members touched this ocean in this period) \
                 — recording the debit unattributed rather than guessing"
            );
        }
        debits.push(LedgerDebit {
            funded: None,
            entry_kind: ENTRY_KIND_JOURNAL,
            cost_usd,
            source_job_id: None,
            source_query_id: None,
            graph_id: Some(graph_id),
            journal_period: Some(period),
            journal_granularity: Some(granularity),
            source_receipt_id: None,
            attributed_user_id,
        });
    }

    Ok((debits, new_cursor, unattributed))
}

/// COR-88 receipt axis: one debit per managed attempt receipt that charged
/// money. Receipts are the only debit source for managed spend; a managed
/// job's `llm_usage` rows sit on no axis. The credit is what the ledger
/// charged, so an ambiguous attempt (timeout) is debited at its reservation.
#[derive(sqlx::FromRow)]
struct ManagedReceipt {
        receipt_id: Uuid,
        settled_at: DateTime<Utc>,
        settled_micro_usd: i64,
        graph_id: Uuid,
        attributed_user_id: Option<Uuid>,
        protocol_version: i16,
        content_plane: Option<String>,
        request_hash: Option<String>,
        reserved_micro_usd: i64,
        logical_call: Uuid,
        dispatch_phase: Option<String>,
    }

impl ManagedReceipt {
    fn into_debit(self, account_id: Uuid) -> Result<LedgerDebit, String> {
        let funded = if self.protocol_version == 1 {
            Some(crate::billing::managed_reservation::TerminalFunding {
                request: crate::billing::managed_reservation::FundingRequest {
                    plane: self.content_plane.ok_or("funded receipt has no content plane")?,
                    receipt: self.receipt_id,
                    account: account_id,
                    payer: self.attributed_user_id.ok_or("funded receipt has no payer")?,
                    graph: self.graph_id,
                    logical_call: self.logical_call,
                    request_hash: self.request_hash.ok_or("funded receipt has no request hash")?,
                    bound_micro_usd: self.reserved_micro_usd,
                },
                charged_micro_usd: if self.dispatch_phase.as_deref() == Some("cancelled_unsent") {
                    None
                } else { Some(self.settled_micro_usd) },
            })
        } else { None };
        Ok(LedgerDebit {
            funded,
            entry_kind: ENTRY_KIND_MANAGED,
            cost_usd: Decimal::new(self.settled_micro_usd,6),
            source_job_id: None,
            source_query_id: None,
            graph_id: Some(self.graph_id),
            journal_period: None,
            journal_granularity: None,
            source_receipt_id: Some(self.receipt_id),
            attributed_user_id: self.attributed_user_id,
        })
    }
}

pub(crate) enum LegacyContent<'a> {
    Control,
    Org { content: &'a mut PgConnection, org: &'a str },
}

pub(crate) struct ManagedRecovery {
    pub pending_legacy: bool,
    pub applied: u64,
    /// Tenant-plane `managed_receipt_recovery_queue` rows this transaction
    /// confirmed. The caller removes them only after the control transaction
    /// commits; control-plane rows are removed inside it.
    pub confirmed_tenant: Vec<Uuid>,
    /// Queued receipts remain past [`RECOVERY_QUEUE_PAGES_PER_TX`]; the caller
    /// runs another transaction so the account lock is released in between.
    pub backlog: bool,
}

/// Receipts read per recovery page.
const RECOVERY_PAGE: i64 = 500;
/// Queued funded pages one financial transaction reconciles, so a backlog
/// (the first pass after the queue's backfill, or a long outage) never holds
/// the account lock for more than a bounded stretch at a time.
pub(crate) const RECOVERY_QUEUE_PAGES_PER_TX: usize = 4;
/// Financial transactions one pass may run for one account before it leaves
/// the rest of a backlog to the next pass.
pub(crate) const RECOVERY_TX_PER_PASS: usize = 32;

/// The receipt seal, and for the funded pass also the recovery queue's
/// trigger (control 0345 / tenant 0177), must be installed and enabled before
/// recovery trusts what it reads. Without the trigger a receipt could turn
/// terminal without being queued, and the queue would skip it.
async fn verify_source_seal(conn: &mut PgConnection, funded_queue: bool) -> Result<(), sqlx::Error> {
    let (sealed, queued): (bool, bool) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.managed_attempt_receipts'::regclass \
         AND tgname='managed_source_receipt_history' AND tgenabled IN ('O','A') \
         AND tgfoid='public.managed_source_receipt_history()'::regprocedure), \
         NOT $1 OR EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.managed_attempt_receipts'::regclass \
         AND tgname='managed_receipt_recovery_queue' AND tgenabled IN ('O','A') \
         AND tgfoid=to_regprocedure('public.managed_receipt_recovery_enqueue()'))",
    ).bind(funded_queue).fetch_one(conn).await?;
    if !sealed { return Err(sqlx::Error::Protocol("legacy source receipt seal is not installed".into())); }
    if !queued { return Err(sqlx::Error::Protocol("managed receipt recovery queue trigger is not installed".into())); }
    Ok(())
}

async fn legacy_reserved(conn: &mut PgConnection, account: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM managed_attempt_receipts WHERE account_id=$1 \
       AND protocol_version=0 AND purpose='source_extraction' AND state='reserved')")
       .bind(account).fetch_one(conn).await
}

pub(crate) async fn reconcile_legacy_managed_tx(
    control: &mut PgConnection, account: Uuid, content: LegacyContent<'_>,
) -> Result<ManagedRecovery, sqlx::Error> {
    reconcile_managed_tx(control, account, content, false).await
}

const RECEIPT_COLUMNS: &str = "r.receipt_id,r.settled_at,r.settled_micro_usd,r.graph_id,r.attributed_user_id, \
     r.protocol_version,r.content_plane,r.request_hash,r.reserved_micro_usd,r.logical_call,r.dispatch_phase";

/// Legacy (protocol 0) charged receipts are sealed history and replayed in
/// full. Funded (protocol 1) receipts come only from the recovery queue: the
/// ones whose terminal fact control has not yet confirmed (see control
/// migration 0345 for the invariant and why no receipt can be missed).
async fn reconcile_managed_tx(
    control: &mut PgConnection, account: Uuid, mut content: LegacyContent<'_>, include_funded: bool,
) -> Result<ManagedRecovery, sqlx::Error> {
    crate::billing::renewal::lock_period_rotation(control, account).await?;
    verify_source_seal(control, include_funded).await?;
    // The same resolver managed admission reads (COR-114), so admission and
    // billing cannot disagree about a plane. A personal org is Control.
    use crate::managed::plane::{resolve_liability_plane, LiabilityPlane, PlaneError};
    let plane = match resolve_liability_plane(control, account, false).await {
        Ok(plane) => Some(plane),
        Err(PlaneError::Db(error)) => return Err(error),
        Err(PlaneError::UnknownAccount | PlaneError::Unresolvable(_)) => None,
    };
    match (&mut content, plane) {
        (LegacyContent::Control, Some(LiabilityPlane::Consumer)) => {},
        (LegacyContent::Org { content, org }, Some(LiabilityPlane::Org(actual))) if actual == *org => {
            let actual_org: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id")
                .fetch_optional(&mut **content).await?;
            let system: bool = sqlx::query_scalar("SELECT current_setting('app.tenant_actor',true)='system'")
                .fetch_one(&mut **content).await?;
            if !system || actual_org.as_deref()!=Some(*org) {
                return Err(sqlx::Error::Protocol("managed legacy tenant scope/fingerprint mismatch".into()));
            }
            verify_source_seal(content, include_funded).await?;
        },
        _ => return Err(sqlx::Error::Protocol("managed legacy liability planes unavailable or ambiguous".into())),
    }
    let mut pending = legacy_reserved(control,account).await?;
    if let LegacyContent::Org { content, .. } = &mut content {
        pending |= legacy_reserved(content,account).await?;
    }
    let mut applied = 0;
    let mut confirmed_tenant = Vec::new();
    let mut backlog = false;
    for tenant_plane in [false,true] {
        if tenant_plane && matches!(content,LegacyContent::Control) { break; }
        let expected_plane = match (&content,tenant_plane) {
            (LegacyContent::Org { org,.. },true) => format!("org:{org}"),
            _ => "consumer".to_owned(),
        };
        for funded in [false,true] {
            if funded && !include_funded { break; }
            let mut cursor: Option<Uuid> = None;
            let mut confirmed: Vec<Uuid> = Vec::new();
            let mut pages = 0usize;
            loop {
                if funded && pages == RECOVERY_QUEUE_PAGES_PER_TX { backlog = true; break; }
                let conn: &mut PgConnection = if tenant_plane {
                    match &mut content { LegacyContent::Org { content,.. } => content, LegacyContent::Control => unreachable!() }
                } else { &mut *control };
                let rows: Vec<ManagedReceipt> = if funded {
                    sqlx::query_as(&format!(
                        "SELECT {RECEIPT_COLUMNS} FROM managed_receipt_recovery_queue q \
                         JOIN managed_attempt_receipts r ON r.receipt_id=q.receipt_id \
                         WHERE q.account_id=$1 AND r.account_id=$1 AND r.purpose='source_extraction' \
                         AND r.state<>'reserved' AND r.protocol_version=1 \
                         AND ($2::uuid IS NULL OR q.receipt_id>$2) ORDER BY q.receipt_id LIMIT $3",
                    )).bind(account).bind(cursor).bind(RECOVERY_PAGE).fetch_all(conn).await?
                } else {
                    sqlx::query_as(&format!(
                        "SELECT {RECEIPT_COLUMNS} FROM managed_attempt_receipts r \
                         WHERE r.account_id=$1 AND r.purpose='source_extraction' \
                         AND r.state<>'reserved' AND r.protocol_version=0 AND r.settled_micro_usd>0 \
                         AND ($2::uuid IS NULL OR r.receipt_id>$2) ORDER BY r.receipt_id LIMIT $3",
                    )).bind(account).bind(cursor).bind(RECOVERY_PAGE).fetch_all(conn).await?
                };
                if rows.is_empty() { break; }
                pages += 1;
                let full = rows.len() as i64 == RECOVERY_PAGE;
                cursor = Some(rows.last().unwrap().receipt_id);
                if funded { confirmed.extend(rows.iter().map(|r| r.receipt_id)); }
                applied += reconcile_receipt_page(control, account, tenant_plane, &expected_plane, rows).await?;
                if !full { break; }
            }
            if !funded || confirmed.is_empty() { continue; }
            if tenant_plane {
                confirmed_tenant = confirmed;
            } else {
                // Same transaction as the confirmation above, and every
                // deleter holds this account's financial lock, so exactly the
                // confirmed rows go.
                let removed = sqlx::query(
                    "DELETE FROM managed_receipt_recovery_queue WHERE account_id=$1 AND receipt_id=ANY($2)",
                ).bind(account).bind(&confirmed).execute(&mut *control).await?.rows_affected();
                if removed != confirmed.len() as u64 {
                    return Err(sqlx::Error::Protocol("managed recovery queue changed under the financial lock".into()));
                }
            }
        }
    }
    if include_funded && !pending {
        crate::corpus_enrichment::resume_legacy_financial_holds_tx(control,account).await?;
        if let LegacyContent::Org { content,.. } = &mut content {
            crate::corpus_enrichment::resume_legacy_financial_holds_tx(content,account).await?;
        }
    }
    Ok(ManagedRecovery { pending_legacy: pending, applied, confirmed_tenant, backlog })
}

/// Applies one page of terminal receipts read from one content plane, then
/// verifies every charged one has exactly its ledger debit.
async fn reconcile_receipt_page(
    control: &mut PgConnection, account: Uuid, tenant_plane: bool, expected_plane: &str,
    rows: Vec<ManagedReceipt>,
) -> Result<u64, sqlx::Error> {
    let ids: Vec<Uuid> = rows.iter().map(|r|r.receipt_id).collect();
    if tenant_plane {
        let duplicate: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM managed_attempt_receipts WHERE account_id=$1 \
             AND purpose='source_extraction' AND receipt_id=ANY($2))",
        ).bind(account).bind(&ids).fetch_one(&mut *control).await?;
        if duplicate { return Err(sqlx::Error::Protocol("duplicate managed receipt provenance across content planes".into())); }
    }
    if rows.iter().any(|r|r.protocol_version==1 && r.content_plane.as_deref()!=Some(expected_plane)) {
        return Err(sqlx::Error::Protocol("managed recovery receipt plane mismatch".into()));
    }
    let debits: Vec<LedgerDebit> = rows.into_iter().map(|r|r.into_debit(account))
        .collect::<Result<_,_>>().map_err(sqlx::Error::Protocol)?;
    let ledger = receipt_ledger_page(control,&ids).await?;
    let mut missing = Vec::new();
    for debit in &debits {
        if debit.funded.is_some() { missing.push(debit.clone()); continue; }
        match ledger.get(&debit.source_receipt_id.unwrap()) {
            Some(existing) => verify_receipt_debit(account,debit,existing)?,
            None => missing.push(debit.clone()),
        }
    }
    let needs_conversion = missing.iter().any(|d|d.funded.is_none());
    let rate: Option<Decimal> = if needs_conversion {
        sqlx::query_scalar("SELECT reference_cost_per_credit FROM credit_conversion \
          WHERE effective_from<=clock_timestamp() ORDER BY effective_from DESC LIMIT 1")
          .fetch_optional(&mut *control).await?
    } else { Some(Decimal::ONE) };
    let rate = rate.filter(|r|*r>Decimal::ZERO)
        .ok_or_else(||sqlx::Error::Protocol("legacy recovery needs positive conversion".into()))?;
    let (n,_) = apply_account_debits_tx(control,account,&missing,rate,CursorWrite::Preserve).await?;
    let ledger = receipt_ledger_page(control,&ids).await?;
    for debit in &debits {
        if debit.cost_usd>Decimal::ZERO {
            verify_receipt_debit(account,debit,ledger.get(&debit.source_receipt_id.unwrap())
              .ok_or_else(||sqlx::Error::Protocol("managed recovery debit missing after apply".into()))?)?;
        }
    }
    Ok(n)
}

#[derive(sqlx::FromRow)]
struct ReceiptLedger {
    source_receipt_id: Uuid, account_id: Uuid, entry_kind: String, credits: Decimal,
    cost_usd: Option<Decimal>, graph_id: Option<Uuid>, attributed_user_id: Option<Uuid>,
}
async fn receipt_ledger_page(conn: &mut PgConnection, ids: &[Uuid]) -> Result<HashMap<Uuid,ReceiptLedger>,sqlx::Error> {
    let rows: Vec<ReceiptLedger> = sqlx::query_as(
        "SELECT source_receipt_id,account_id,entry_kind,credits,cost_usd,graph_id,attributed_user_id \
         FROM credit_ledger WHERE source_receipt_id=ANY($1)",
    ).bind(ids).fetch_all(conn).await?;
    Ok(rows.into_iter().map(|r|(r.source_receipt_id,r)).collect())
}
fn verify_receipt_debit(account: Uuid, debit: &LedgerDebit, row: &ReceiptLedger) -> Result<(),sqlx::Error> {
    if row.account_id!=account || row.entry_kind!=ENTRY_KIND_MANAGED || row.cost_usd!=Some(debit.cost_usd)
       || row.graph_id!=debit.graph_id || row.attributed_user_id!=debit.attributed_user_id
       || row.credits>Decimal::ZERO {
        return Err(sqlx::Error::Protocol("managed receipt conflicts with existing financial facts".into()));
    }
    Ok(())
}

/// One financial transaction of managed recovery for one account. Tenant
/// queue rows it confirmed are removed only after control commits; if that
/// removal fails they stay queued and the next pass re-confirms them, which
/// changes nothing.
async fn recover_account_once(
    t: &WorkerTenant, control: &PgPool, runtime: Option<&crate::tenancy::runtime::TenantRuntime>, account: Uuid,
) -> Result<ManagedRecovery,String> {
    // The in-process turn (#1497) before the control connection, then the
    // bounded lock: while this pass holds the account's financial lock,
    // same-process admissions for the account wait in memory instead of on
    // control connections, and a holder in another process makes this pass
    // skip the account (logged under `billing.lock_skip`) for the next tick
    // to retry. Nothing is written or marked on a skip: the transaction has
    // not begun its work, and the caller only logs and moves on.
    let (turn,mut financial)=crate::billing::entry_queue::begin_entered(account,"billing_recovery",
        || begin_account_scoped(control,account)).await.map_err(|e|e.to_string())?;
    let (outcome,tenant_pool)=match &t.tenant {
        TenantId::Consumer => {
            use crate::managed::plane::{resolve_liability_plane, LiabilityPlane, PlaneError};
            // The resolver admission reads (COR-114): a personal org's
            // receipts are on control and reconcile there.
            match resolve_liability_plane(&mut financial, account, false).await {
                Ok(LiabilityPlane::Consumer) => {
                    (reconcile_managed_tx(&mut financial,account,LegacyContent::Control,true).await,None)
                }
                Ok(LiabilityPlane::Org(org)) => {
                    let secret: String=sqlx::query_scalar("SELECT db_secret_ref FROM tenant_databases WHERE account_id=$1 AND clerk_org_id=$2")
                        .bind(account).bind(&org).fetch_one(&mut *financial).await.map_err(|e|e.to_string())?;
                    let runtime=runtime.filter(|r|r.enabled).ok_or("Org legacy recovery requires its real tenant connection")?;
                    let pool=runtime.registry.get_or_build_worker_pool(&org,account,&secret).await.map_err(|e|e.to_string())?;
                    let mut content=begin_tenant_system_scoped(&pool,account).await.map_err(|e|e.to_string())?;
                    let result=reconcile_managed_tx(&mut financial,account,LegacyContent::Org {content:&mut content,org:&org},true).await;
                    content.commit().await.map_err(|e|e.to_string())?;
                    (result,Some(pool))
                }
                Err(PlaneError::Db(error)) => return Err(error.to_string()),
                Err(why) => {
                    // No plane this pass could read. With no receipt on
                    // control there is nothing to recover; any receipt
                    // still refuses.
                    let receipts: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM managed_attempt_receipts WHERE account_id=$1 AND purpose='source_extraction')")
                        .bind(account).fetch_one(&mut *financial).await.map_err(|e|e.to_string())?;
                    if receipts { return Err(format!("managed recovery routing unavailable: account has managed receipts on control but {why}")); }
                    financial.commit().await.map_err(|e|e.to_string())?;
                    return Ok(ManagedRecovery { pending_legacy:false, applied:0, confirmed_tenant:Vec::new(), backlog:false });
                }
            }
        },
        TenantId::Org(org) => {
            let mut content=begin_tenant_system_scoped(&t.pool,account).await.map_err(|e|e.to_string())?;
            let result=reconcile_managed_tx(&mut financial,account,LegacyContent::Org {content:&mut content,org},true).await;
            content.commit().await.map_err(|e|e.to_string())?;
            (result,Some(t.pool.clone()))
        },
    };
    let outcome=outcome.map_err(|e|e.to_string())?;
    financial.commit().await.map_err(|e|e.to_string())?;
    drop(turn);
    if !outcome.confirmed_tenant.is_empty() {
        let drained = async {
            let pool=tenant_pool.as_ref().ok_or("tenant recovery queue rows without a tenant plane")?;
            let mut content=begin_tenant_system_scoped(pool,account).await.map_err(|e|e.to_string())?;
            sqlx::query("DELETE FROM managed_receipt_recovery_queue WHERE account_id=$1 AND receipt_id=ANY($2)")
                .bind(account).bind(&outcome.confirmed_tenant).execute(&mut *content).await.map_err(|e|e.to_string())?;
            content.commit().await.map_err(|e|e.to_string())
        }.await;
        drained.map_err(|e| format!(
            "managed recovery committed on control but its tenant queue was not drained \
             (rows stay queued and are re-confirmed next pass): {e}"))?;
    }
    Ok(outcome)
}

/// The managed recovery half of a tick alone, so tests can count its round
/// trips apart from the ordinary derivation's.
#[cfg(test)]
pub(crate) async fn recover_managed_only(t: &WorkerTenant, control: &PgPool) -> Result<usize,String> {
    recover_managed(t, control, None).await
}

async fn recover_managed(
    t: &WorkerTenant, control: &PgPool, runtime: Option<&crate::tenancy::runtime::TenantRuntime>,
) -> Result<usize,String> {
    let accounts = match &t.tenant {
        TenantId::Consumer => enumerate_consumer_accounts(control).await?,
        TenantId::Org(org) => vec![resolve_org_account_id(control,org).await?],
    };
    let mut committed=0usize;
    let mut failures=Vec::new();
    for account in accounts {
        let mut transactions=0usize;
        let result = loop {
            transactions += 1;
            match recover_account_once(t,control,runtime,account).await {
                Ok(outcome) if outcome.backlog && transactions < RECOVERY_TX_PER_PASS => {
                    committed += outcome.applied as usize;
                }
                other => break other,
            }
        };
        match result {
            Ok(outcome) => {
                committed += outcome.applied as usize;
                if outcome.backlog {
                    tracing::warn!(%account,transactions,
                        "managed recovery queue backlog remains after this pass; the next pass continues it");
                }
                tracing::info!(%account,applied=outcome.applied,pending_legacy=outcome.pending_legacy,transactions,
                    "separate managed recovery account committed");
            },
            Err(error) => { tracing::warn!(%account,%error,"managed recovery account unavailable; continuing other accounts"); failures.push((account,error)); },
        }
    }
    if failures.is_empty() { Ok(committed) }
    else { Err(format!("managed recovery committed {committed} changes; unavailable accounts: {failures:?}")) }
}

async fn read_receipt_axis(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    cursor: Option<(DateTime<Utc>, Uuid)>,
    cutoff: DateTime<Utc>,
) -> Result<(Vec<LedgerDebit>, Option<(DateTime<Utc>, Uuid)>), String> {
    let (cursor_ts, cursor_id) = cursor.unzip();
    let rows: Vec<ManagedReceipt> = sqlx::query_as(
        "SELECT receipt_id,settled_at,settled_micro_usd,graph_id,attributed_user_id, \
                protocol_version,content_plane,request_hash,reserved_micro_usd,logical_call,dispatch_phase \
           FROM managed_attempt_receipts \
          WHERE account_id=$1 AND settled_at<=$2 \
            AND (settled_micro_usd>0 OR protocol_version=1) \
            AND (settled_at,receipt_id)>(COALESCE($3,'-infinity'::timestamptz), \
                COALESCE($4,'00000000-0000-0000-0000-000000000000'::uuid)) \
          ORDER BY settled_at,receipt_id LIMIT $5",
    )
    .bind(account_id).bind(cutoff).bind(cursor_ts).bind(cursor_id).bind(READ_BATCH_SIZE)
    .fetch_all(&mut **tx).await
    .map_err(|e| format!("receipt axis: failed to read settled receipts: {e}"))?;
    let new_cursor = advance_cursor(
        cursor, cutoff, rows.len(), READ_BATCH_SIZE as usize,
        rows.last().map(|r| (r.settled_at,r.receipt_id)),
    );
    let mut debits = Vec::with_capacity(rows.len());
    for row in rows { debits.push(row.into_debit(account_id)?); }
    Ok((debits, new_cursor))
}

/// See the module doc's "Classifying source-id-less rows" section. Runs
/// inside the SAME scoped transaction the three axes above already opened —
/// no extra RLS problem to solve, just one more `SELECT` on an
/// already-open, already-scoped connection.
async fn classify_unthreaded_usage(
    tx: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    is_org: bool,
    as_of: DateTime<Utc>,
) -> Result<(i64, i64), String> {
    let since = as_of - CLASSIFICATION_WINDOW;

    let (cache_hit, other): (i64, i64) = if is_org {
        sqlx::query_as(
            "SELECT \
                COALESCE(SUM(CASE WHEN task LIKE '%\\_cached' ESCAPE '\\' THEN 1 ELSE 0 END), 0)::bigint, \
                COALESCE(SUM(CASE WHEN task NOT LIKE '%\\_cached' ESCAPE '\\' THEN 1 ELSE 0 END), 0)::bigint \
             FROM llm_usage \
             WHERE source_job_id IS NULL AND source_query_id IS NULL \
               AND source_journal_period IS NULL AND created_at >= $1 \
               AND task NOT LIKE 'jev\\_ask%' ESCAPE '\\'",
        )
        .bind(since)
        .fetch_one(&mut **tx)
        .await
    } else {
        sqlx::query_as(
            "SELECT \
                COALESCE(SUM(CASE WHEN task LIKE '%\\_cached' ESCAPE '\\' THEN 1 ELSE 0 END), 0)::bigint, \
                COALESCE(SUM(CASE WHEN task NOT LIKE '%\\_cached' ESCAPE '\\' THEN 1 ELSE 0 END), 0)::bigint \
             FROM llm_usage \
             WHERE account_id = $1 AND source_job_id IS NULL AND source_query_id IS NULL \
               AND source_journal_period IS NULL AND created_at >= $2 \
               AND task NOT LIKE 'jev\\_ask%' ESCAPE '\\'",
        )
        .bind(account_id)
        .bind(since)
        .fetch_one(&mut **tx)
        .await
    }
    .map_err(|e| format!("classification: failed to count unthreaded rows: {e}"))?;

    Ok((cache_hit, other))
}

// ── Write phase ───────────────────────────────────────────────────────────

/// Insert every debit for one account, idempotently, then upsert
/// `credit_balances` (running balance + all three cursor pairs) and advance
/// each attributed member's `member_allowance_periods.consumed_credits`, all in
/// the SAME transaction. Returns the number of ledger rows ACTUALLY inserted
/// (not `ON CONFLICT`-skipped) — this is the count `tick` sums for its
/// `Tick::worked(n)`/`Tick::idle()` decision.
async fn write_account_ledger(
    control: &PgPool,
    account_id: Uuid,
    read: &AccountRead,
    reference_cost_per_credit: Decimal,
) -> Result<u64, sqlx::Error> {
    // The in-process turn (#1497) before the control connection, then the
    // bounded lock; the turn is dropped right after the commit below. A
    // skip (queue or lock timeout) returns before any write: the cursor is
    // not advanced, so the next tick re-reads and re-applies this batch.
    let (turn, mut tx) = crate::billing::entry_queue::begin_entered(account_id, "billing_derivation", || {
        begin_account_scoped(control, account_id)
    })
    .await?;

    let (inserted, untracked) = apply_account_debits_tx(
        &mut tx, account_id, &read.debits, reference_cost_per_credit,
        CursorWrite::Advance(&read.new_cursor),
    ).await?;

    tx.commit().await?;
    drop(turn);

    if untracked > 0 {
        // Not an error and not a failed write: it means spend was attributed to
        // a member who holds no allowance period covering now — an account that
        // has never had a period rolled (no Stripe `invoice.paid` yet), or a
        // member added after the roll. Their spend still hits the org pool via
        // `credit_balances`; it is simply invisible to the per-seat gate, which
        // reads `false` for them. Worth saying out loud rather than letting
        // per-member accounting look complete when it is not.
        tracing::info!(
            account_id = %account_id,
            members_without_period = untracked,
            "billing derivation: attributed spend for members with no current allowance period — \
             counted against the org pool only, not against any seat allowance"
        );
    }

    Ok(inserted)
}

#[derive(Clone, Copy)]
enum CursorWrite<'a> { Preserve, Advance(&'a AccountCursor) }

async fn apply_account_debits_tx(
    tx: &mut PgConnection,
    account_id: Uuid,
    debits: &[LedgerDebit],
    reference_cost_per_credit: Decimal,
    cursor: CursorWrite<'_>,
) -> Result<(u64, usize), sqlx::Error> {
    let mut inserted = 0u64;
    let mut delta = Decimal::ZERO;
    // Per-member consumption for THIS batch, keyed by `attributed_user_id`.
    // Accumulated from the rows that were actually inserted — never from
    // `read.debits` directly — so a redelivered/duplicate unit of work that
    // `ON CONFLICT`s away is not counted against anyone's allowance a second
    // time. That is the same guarantee, resting on the same `Option` return,
    // that keeps `delta` correct for `credit_balances`.
    let mut consumed_by_member: HashMap<Uuid, Consumed> = HashMap::new();
    let mut managed_debited = Decimal::ZERO;
    let applied = crate::billing::managed_reservation::already_applied_tx(
        &mut *tx,
        debits.iter().filter_map(|debit| debit.funded.as_ref()),
    )
    .await?;
    // One planner for the batch: each debit is split (plan allowance vs extra
    // usage) against the state the previous ones left. The caller holds
    // `lock_period_rotation`.
    let mut planner = SettlementPlanner::new(account_id);

    for debit in debits {
        if let Some(terminal) = &debit.funded {
            if applied.contains(&terminal.request.receipt) {
                continue;
            }
            let changed = match terminal.charged_micro_usd {
                Some(cost) => crate::billing::managed_reservation::settle_tx_with(
                    &mut *tx, &terminal.request, cost, &mut planner,
                ).await?,
                None => crate::billing::managed_reservation::release_unsent_tx(
                    &mut *tx, &terminal.request,
                ).await?,
            };
            inserted += u64::from(changed);
            continue;
        }
        let credits = credits_for_cost(debit.cost_usd, reference_cost_per_credit);
        if let Some(applied) = insert_ledger_row(
            &mut *tx, account_id, debit, credits, reference_cost_per_credit, &mut planner,
        ).await? {
            inserted += 1;
            delta += applied;
            let managed = debit.entry_kind == ENTRY_KIND_MANAGED;
            if managed {
                managed_debited -= applied;
            }
            if let Some(user_id) = debit.attributed_user_id {
                // `applied` is NEGATIVE (see `credits_for_cost`);
                // `consumed_credits` counts UPWARD and carries a
                // `>= 0` CHECK, so negate.
                let member = consumed_by_member.entry(user_id).or_default();
                member.total -= applied;
                if managed {
                    member.managed -= applied;
                }
            }
        }
    }

    upsert_credit_balance(&mut *tx, account_id, delta, managed_debited, cursor).await?;
    let untracked =
        advance_member_consumption(&mut *tx, account_id, &consumed_by_member).await?;

    Ok((inserted, untracked))
}

#[cfg(test)]
pub(crate) async fn native_interactive_debit(
    control: &PgPool, account: Uuid, payer: Uuid, credits: Decimal,
) -> Result<u64, sqlx::Error> {
    let read = AccountRead {
        debits: vec![LedgerDebit {
            funded: None, entry_kind: ENTRY_KIND_QUERY, cost_usd: credits,
            source_job_id: None, source_query_id: Some(Uuid::new_v4()), graph_id: None,
            journal_period: None, journal_granularity: None, source_receipt_id: None,
            attributed_user_id: Some(payer),
        }],
        ..Default::default()
    };
    write_account_ledger(control,account,&read,Decimal::ONE).await
}

/// A job-axis debit with a caller-chosen `source_job_id`, so a test can replay
/// the same unit of work. `payer` may be `None` (unattributed spend).
#[cfg(test)]
pub(crate) async fn native_job_debit(
    control: &PgPool, account: Uuid, payer: Option<Uuid>, job: Uuid, credits: Decimal,
) -> Result<u64, sqlx::Error> {
    let read = AccountRead {
        debits: vec![LedgerDebit {
            funded: None, entry_kind: ENTRY_KIND_JOB, cost_usd: credits,
            source_job_id: Some(job), source_query_id: None, graph_id: None,
            journal_period: None, journal_granularity: None, source_receipt_id: None,
            attributed_user_id: payer,
        }],
        ..Default::default()
    };
    write_account_ledger(control,account,&read,Decimal::ONE).await
}

#[cfg(test)]
pub(crate) async fn native_journal_debit(
    control: &PgPool, account: Uuid, payer: Option<Uuid>, graph: Uuid, period: DateTime<Utc>, credits: Decimal,
) -> Result<u64, sqlx::Error> {
    let read = AccountRead {
        debits: vec![LedgerDebit {
            funded: None, entry_kind: ENTRY_KIND_JOURNAL, cost_usd: credits,
            source_job_id: None, source_query_id: None, graph_id: Some(graph),
            journal_period: Some(period), journal_granularity: Some("day".into()), source_receipt_id: None,
            attributed_user_id: payer,
        }],
        ..Default::default()
    };
    write_account_ledger(control,account,&read,Decimal::ONE).await
}

#[cfg(test)]
pub(crate) async fn native_receipt_derivation(
    control: &PgPool, pool: &PgPool, tenant: &TenantId, account: Uuid,
) -> Result<u64,String> {
    let mut tx = open_read_scope(pool,tenant,account).await.map_err(|e|e.to_string())?;
    let (debits,cursor) = read_receipt_axis(&mut tx,account,None,Utc::now()+chrono::Duration::seconds(1)).await?;
    tx.commit().await.map_err(|e|e.to_string())?;
    write_account_ledger(control,account,&AccountRead {
        debits,new_cursor: AccountCursor {receipt:cursor,..Default::default()},..Default::default()
    },Decimal::from(999)).await.map_err(|e|e.to_string())
}

#[derive(Clone, Copy, Debug, Default)]
struct Consumed {
    total: Decimal,
    managed: Decimal,
}

/// Add this batch's per-member consumption onto each member's CURRENT
/// `member_allowance_periods` row. Returns how many members had no such row.
///
/// # Why this exists
///
/// Migration `0161`'s header states that `consumed_credits` "is a running total
/// maintained by `workers::billing_derivation` as it writes debits". Until this
/// function, that code did not exist: every write of the column anywhere in the
/// tree was inside `billing::enforcement`'s own tests. The consequence was not
/// cosmetic — `enforcement::is_blocked` short-circuits on `consumed < granted`,
/// so a bucket stuck at zero made that test permanently true, and because a
/// present period row also SHADOWS the `org_pool_exhausted` fallback beneath it,
/// a member with a period row could never be blocked by anything at all. Phase
/// 2b split the buckets on the read side; this is the write side catching up.
///
/// # Why `UPDATE ... WHERE now()` rather than an upsert
///
/// A missing period row means "this member was never granted an allowance for
/// this period", which is a fact to report, not one to invent. Manufacturing a
/// row here would fabricate a `granted_credits` figure that no grant and no
/// `invoice.paid` ever authorised, and `renewal::roll_period` — the one thing
/// that may write these rows — would then collide with it. So a member with no
/// current period simply does not have their consumption tracked, and the count
/// is returned for the caller to log.
///
/// The `+=` is done in SQL (`consumed_credits = consumed_credits + $3`) rather
/// than read-modify-write in Rust, so concurrent ticks serialise on the row lock
/// instead of losing an update — the discipline `0161`'s header asks for.
async fn advance_member_consumption(
    tx: &mut PgConnection,
    account_id: Uuid,
    consumed_by_member: &HashMap<Uuid, Consumed>,
) -> Result<usize, sqlx::Error> {
    let mut untracked = 0usize;

    // Deterministic lock order. `consumed_by_member` is a `HashMap`, and its
    // iteration order differs between processes, so two concurrent ticks that
    // charge the same two members can take the two row locks in OPPOSITE
    // orders. Postgres resolves that with `40P01 deadlock detected`, aborting
    // one — and `tick`'s refuse-to-write-on-partial-failure discipline then
    // discards the whole batch. That is the same "loud in the logs, invisible
    // in the data" shape as the defect this module is being fixed for, so it
    // is worth closing here rather than discovering again from an empty table.
    // Two ticks really can overlap: deploys run the old and new machine
    // together, and Fly autostart can wake the second one.
    let mut members: Vec<(&Uuid, &Consumed)> = consumed_by_member.iter().collect();
    members.sort_unstable_by(|a, b| a.0.cmp(b.0));

    for (user_id, consumed) in members {
        if consumed.total.is_zero() {
            continue;
        }
        // Charges exactly the LATEST current period. The unique index is
        // `(account_id, user_id, period_start)`, which does not stop two rows
        // from both covering `now()`; without the `max(period_start)` pin this
        // UPDATE charges EVERY overlapping row, double-counting one unit of
        // consumption. Pinning to the newest matches what `roll_period` means
        // by "the current period", and keeps this in agreement with
        // `enforcement::read_member_period_scoped`, which reads the same row.
        //
        // THAT OVERLAP IS NOW IMPOSSIBLE. Phase 4b's migration `0163` added an
        // exclusion constraint on `(account_id, user_id, tstzrange(period_start,
        // period_end))`, so at most one row can cover `now()` and this
        // subquery's `max()` is taken over a single-row set. The pin is
        // therefore DEFENCE IN DEPTH and is kept deliberately: it costs one
        // indexed subquery, it is the behaviour that stays correct if the
        // constraint is ever dropped or deferred, and removing it would leave
        // this UPDATE and `read_member_period_scoped` disagreeing about which
        // row "the current period" means. `pg_tests::
        // overlapping_periods_are_rejected_by_the_database` asserts the
        // constraint; it replaced the test that used to assert this pin,
        // because the state that test set up can no longer be created.
        let affected = sqlx::query(
            "UPDATE member_allowance_periods \
                SET consumed_credits = consumed_credits + $3, \
                    managed_consumed_credits = managed_consumed_credits + $4, updated_at = now() \
              WHERE account_id = $1 AND user_id = $2 \
                AND period_start = ( \
                    SELECT max(period_start) FROM member_allowance_periods \
                     WHERE account_id = $1 AND user_id = $2 \
                       AND period_start <= now() AND period_end > now())",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(consumed.total)
        .bind(consumed.managed)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if affected == 0 {
            untracked += 1;
        }
    }

    Ok(untracked)
}

/// One idempotent insert, dispatched to the right `ON CONFLICT` target for
/// this debit's axis (exactly one of `source_job_id`/`source_query_id`/
/// `journal_period` is set, enforced by `credit_ledger`'s own
/// `credit_ledger_single_source_axis` CHECK). Returns `Some(credits)` if the
/// row was newly inserted, `None` if it already existed (conflict — this
/// unit of work was already charged, exactly the idempotency guarantee
/// `jobs.id`/`query_history.id`/the journal triple key on `credit_ledger`
/// exist to provide).
async fn insert_ledger_row(
    tx: &mut PgConnection,
    account_id: Uuid,
    debit: &LedgerDebit,
    credits: Decimal,
    reference_cost_per_credit: Decimal,
    planner: &mut SettlementPlanner,
) -> Result<Option<Decimal>, sqlx::Error> {
    if let Some(query_id) = debit.source_query_id {
        return append_query_debit(tx, account_id, query_id, debit, reference_cost_per_credit, planner).await;
    }
    // `credits` is the NEGATIVE of the charge. The plan ledger records only
    // what the plan allowance bears plus any unfunded remainder; the part
    // extra usage funds goes to `extra_usage_ledger` (same key, same tx).
    let split = plan_debit(tx, planner, debit, credits).await?;
    let written = -split.plan_credits();
    debug_assert!(written >= credits, "the plan ledger never bears more than the charge");
    let (key, job) = if let Some(receipt) = debit.source_receipt_id {
        (debit_key("receipt", account_id, receipt), None)
    } else if let Some(job) = debit.source_job_id {
        (debit_key("job", account_id, job), Some(job))
    } else {
        (
            debit_key(
                "journal",
                account_id,
                format!(
                    "{}:{}:{}",
                    debit.graph_id.map(|g| g.to_string()).unwrap_or_default(),
                    debit.journal_granularity.as_deref().unwrap_or(""),
                    debit.journal_period.map(|p| p.to_rfc3339()).unwrap_or_default(),
                ),
            ),
            None,
        )
    };
    let row: Option<(Decimal,)> = if debit.source_receipt_id.is_some() {
        sqlx::query_as(
            "INSERT INTO credit_ledger \
                 (account_id, entry_kind, credits, source_receipt_id, graph_id, cost_usd, \
                  attributed_user_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (source_receipt_id) WHERE source_receipt_id IS NOT NULL DO NOTHING \
             RETURNING credits",
        )
        .bind(account_id)
        .bind(debit.entry_kind)
        .bind(written)
        .bind(debit.source_receipt_id)
        .bind(debit.graph_id)
        .bind(debit.cost_usd)
        .bind(debit.attributed_user_id)
        .fetch_optional(&mut *tx)
        .await?
    } else if debit.source_job_id.is_some() {
        sqlx::query_as(
            "INSERT INTO credit_ledger \
                 (account_id, entry_kind, credits, source_job_id, cost_usd, attributed_user_id) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (source_job_id) WHERE source_job_id IS NOT NULL DO NOTHING \
             RETURNING credits",
        )
        .bind(account_id)
        .bind(debit.entry_kind)
        .bind(written)
        .bind(debit.source_job_id)
        .bind(debit.cost_usd)
        .bind(debit.attributed_user_id)
        .fetch_optional(&mut *tx)
        .await?
    } else {
        sqlx::query_as(
            "INSERT INTO credit_ledger \
                 (account_id, entry_kind, credits, graph_id, journal_granularity, journal_period, \
                  cost_usd, attributed_user_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (account_id, graph_id, journal_granularity, journal_period) \
                 WHERE journal_period IS NOT NULL DO NOTHING \
             RETURNING credits",
        )
        .bind(account_id)
        .bind(debit.entry_kind)
        .bind(written)
        .bind(debit.graph_id)
        .bind(debit.journal_granularity.as_deref())
        .bind(debit.journal_period)
        .bind(debit.cost_usd)
        .bind(debit.attributed_user_id)
        .fetch_optional(&mut *tx)
        .await?
    };

    // Only a NEWLY inserted plan-ledger row moves money anywhere else: a
    // replay conflicts above and neither the planner nor extra usage sees it.
    if row.is_some() {
        commit_debit(tx, planner, account_id, debit, &split, key, job).await?;
    }
    Ok(row.map(|(c,)| c))
}

/// Split one debit (`credits` is negative) with the planner. Reads only.
async fn plan_debit(
    tx: &mut PgConnection,
    planner: &mut SettlementPlanner,
    debit: &LedgerDebit,
    credits: Decimal,
) -> Result<Split, sqlx::Error> {
    // A revision never credits money back: `credits` is the (non-positive)
    // negation of a charge. A positive value would be silently clamped to a
    // zero charge below and the credit lost, so stop in debug builds.
    debug_assert!(credits <= Decimal::ZERO, "debit credits must not be positive: {credits}");
    let charge = (-credits).max(Decimal::ZERO);
    planner.plan(tx, debit.attributed_user_id, PeriodSel::Current, charge).await
}

/// After the plan-ledger row exists: update the planner's view and write the
/// extra-usage side of the split.
async fn commit_debit(
    tx: &mut PgConnection,
    planner: &mut SettlementPlanner,
    account_id: Uuid,
    debit: &LedgerDebit,
    split: &Split,
    idempotency_key: String,
    source_job_id: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    planner.record(split);
    write_extra_debit(
        tx,
        &ExtraDebit {
            account: account_id,
            split,
            idempotency_key,
            source_job_id,
            note: split.reason.map(|r| r.code()),
        },
    )
    .await
}

#[derive(sqlx::FromRow)]
struct QueryBillingState {
    baseline_cost_usd: Decimal,
    accounted_cost_usd: Decimal,
    incremental_credits: Decimal,
    reference_cost_per_credit: Decimal,
    next_revision: i64,
}

async fn append_query_debit(
    tx: &mut PgConnection,
    account_id: Uuid,
    query_id: Uuid,
    debit: &LedgerDebit,
    reference_cost_per_credit: Decimal,
    planner: &mut SettlementPlanner,
) -> Result<Option<Decimal>, sqlx::Error> {
    sqlx::query(
        "INSERT INTO query_billing_state \
             (account_id,query_id,baseline_cost_usd,accounted_cost_usd, \
              reference_cost_per_credit,next_revision) \
         SELECT $1,$2,COALESCE(sum(cost_usd),0),COALESCE(sum(cost_usd),0), \
                $3,COALESCE(max(query_revision)+1,0) \
           FROM credit_ledger WHERE account_id=$1 AND source_query_id=$2 \
         ON CONFLICT (account_id,query_id) DO NOTHING",
    )
    .bind(account_id).bind(query_id).bind(reference_cost_per_credit)
    .execute(&mut *tx).await?;
    let state: QueryBillingState = sqlx::query_as(
        "SELECT baseline_cost_usd,accounted_cost_usd,incremental_credits, \
                reference_cost_per_credit,next_revision \
           FROM query_billing_state WHERE account_id=$1 AND query_id=$2 FOR UPDATE",
    )
    .bind(account_id).bind(query_id).fetch_one(&mut *tx).await?;
    if debit.cost_usd <= state.accounted_cost_usd && state.next_revision > 0 {
        return Ok(None);
    }
    let cost_delta = debit.cost_usd - state.accounted_cost_usd;
    let target_credits = credits_for_cost(
        debit.cost_usd - state.baseline_cost_usd, state.reference_cost_per_credit,
    );
    let applied = target_credits - state.incremental_credits;
    // `query_billing_state` tracks the FULL credits for revision arithmetic;
    // the plan ledger row carries only the plan's share of this revision.
    let split = plan_debit(tx, planner, debit, applied).await?;
    let written = -split.plan_credits();
    sqlx::query(
        "INSERT INTO credit_ledger \
             (account_id,entry_kind,credits,source_query_id,query_revision,cost_usd,attributed_user_id) \
         VALUES($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(account_id).bind(ENTRY_KIND_QUERY).bind(written).bind(query_id)
    .bind(state.next_revision).bind(cost_delta).bind(debit.attributed_user_id)
    .execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE query_billing_state SET accounted_cost_usd=$3,incremental_credits=$4, \
                next_revision=next_revision+1 WHERE account_id=$1 AND query_id=$2",
    )
    .bind(account_id).bind(query_id).bind(debit.cost_usd).bind(target_credits)
    .execute(&mut *tx).await?;
    commit_debit(
        tx, planner, account_id, debit, &split,
        debit_key("query", account_id, format!("{query_id}:{}", state.next_revision)), None,
    ).await?;
    Ok(Some(written))
}

async fn upsert_credit_balance(
    tx: &mut PgConnection,
    account_id: Uuid,
    delta: Decimal,
    managed_debited: Decimal,
    cursor: CursorWrite<'_>,
) -> Result<(), sqlx::Error> {
    let CursorWrite::Advance(cursor) = cursor else {
        sqlx::query(
            "INSERT INTO credit_balances(account_id,balance,managed_debited_credits,updated_at) \
             VALUES($1,$2,$3,now()) ON CONFLICT(account_id) DO UPDATE SET \
             balance=credit_balances.balance+EXCLUDED.balance, \
             managed_debited_credits=credit_balances.managed_debited_credits+EXCLUDED.managed_debited_credits, \
             updated_at=now()",
        ).bind(account_id).bind(delta).bind(managed_debited).execute(&mut *tx).await?;
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO credit_balances \
             (account_id, balance, updated_at, \
              job_axis_cursor_finished_at, job_axis_cursor_job_id, \
              query_axis_cursor_created_at, query_axis_cursor_usage_id, \
              journal_axis_cursor_period, journal_axis_cursor_usage_id, \
              receipt_cursor_at, receipt_cursor_id, managed_debited_credits) \
         VALUES ($1, $2, now(), $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         ON CONFLICT (account_id) DO UPDATE SET \
             balance = credit_balances.balance + EXCLUDED.balance, \
             managed_debited_credits = \
                 credit_balances.managed_debited_credits + EXCLUDED.managed_debited_credits, \
             updated_at = now(), \
             job_axis_cursor_finished_at = EXCLUDED.job_axis_cursor_finished_at, \
             job_axis_cursor_job_id = EXCLUDED.job_axis_cursor_job_id, \
             (query_axis_cursor_created_at, query_axis_cursor_usage_id) = \
                 (SELECT latest.ts, latest.id FROM (VALUES \
                    (credit_balances.query_axis_cursor_created_at, credit_balances.query_axis_cursor_usage_id), \
                    (EXCLUDED.query_axis_cursor_created_at, EXCLUDED.query_axis_cursor_usage_id)) \
                    AS latest(ts,id) ORDER BY ts DESC NULLS LAST,id DESC NULLS LAST LIMIT 1), \
             journal_axis_cursor_period = EXCLUDED.journal_axis_cursor_period, \
             journal_axis_cursor_usage_id = EXCLUDED.journal_axis_cursor_usage_id, \
             receipt_cursor_at = EXCLUDED.receipt_cursor_at, \
             receipt_cursor_id = EXCLUDED.receipt_cursor_id",
    )
    .bind(account_id)
    .bind(delta)
    .bind(cursor.job.map(|(ts, _)| ts))
    .bind(cursor.job.map(|(_, id)| id))
    .bind(cursor.query.map(|(ts, _)| ts))
    .bind(cursor.query.map(|(_, id)| id))
    .bind(cursor.journal.map(|(ts, _)| ts))
    .bind(cursor.journal.map(|(_, id)| id))
    .bind(cursor.receipt.map(|(ts, _)| ts))
    .bind(cursor.receipt.map(|(_, id)| id))
    .bind(managed_debited)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

// ── Pure unit tests (no Postgres) ────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_job_kinds_are_not_job_axis_kinds() {
        for kind in crate::jobs::STANDING_EXTRACTION_KINDS {
            assert!(crate::managed::MANAGED_JOB_KINDS.contains(kind));
        }
        for kind in crate::managed::MANAGED_JOB_KINDS
            .iter()
            .chain(crate::managed::MANAGED_ENRICHMENT_JOB_KINDS)
        {
            assert!(crate::managed::is_managed_job_kind(kind));
            assert!(
                !JOB_AXIS_KINDS.contains(kind),
                "{kind} is charged from receipts and must not be billed again on the job axis"
            );
        }
        for kind in JOB_AXIS_KINDS {
            assert!(!crate::managed::is_managed_job_kind(kind), "{kind} is on the job axis");
        }
    }

    #[test]
    fn decimal_from_f64_avoids_binary_float_noise() {
        // 0.1 has no exact f64 representation; a naive `Decimal::from_f64_retain`
        // would carry that noise (0.1000000000000000055511151231257827021181583404541015625)
        // into an exact-decimal column. The fixed-precision string round-trip
        // this function uses must not.
        let d = decimal_from_f64(0.1);
        assert_eq!(d, "0.1".parse::<Decimal>().unwrap());
    }

    #[test]
    fn credits_for_cost_is_negative_and_exact() {
        let cost = Decimal::from(44) / Decimal::from(10000); // 0.0044
        let reference = Decimal::from(44) / Decimal::from(10000); // 0.0044/credit
        let credits = credits_for_cost(cost, reference);
        // $0.0044 of spend at $0.0044/credit is exactly 1 credit, debited.
        assert_eq!(credits, Decimal::from(-1));
    }

    #[test]
    fn credits_for_cost_handles_zero_conversion_rate_without_panicking() {
        assert_eq!(credits_for_cost(Decimal::from(5), Decimal::ZERO), Decimal::ZERO);
    }

    #[test]
    fn resolve_debit_attribution_no_rows_attributed_is_none_and_not_conflicted() {
        let seen = HashSet::new();
        assert_eq!(resolve_debit_attribution(&seen), (None, false));
    }

    #[test]
    fn resolve_debit_attribution_single_agreeing_member_wins() {
        let member = Uuid::new_v4();
        let mut seen = HashSet::new();
        seen.insert(member);
        assert_eq!(resolve_debit_attribution(&seen), (Some(member), false));
    }

    #[test]
    fn resolve_debit_attribution_disagreement_is_none_and_flagged_conflicted() {
        let mut seen = HashSet::new();
        seen.insert(Uuid::new_v4());
        seen.insert(Uuid::new_v4());
        let (resolved, conflicted) = resolve_debit_attribution(&seen);
        assert_eq!(resolved, None, "disagreement must never arbitrarily pick a member");
        assert!(conflicted, "disagreement must be flagged so the caller can log it");
    }

    #[test]
    fn advance_cursor_jumps_to_the_overlap_boundary_when_the_batch_is_drained() {
        let cutoff = chrono::DateTime::parse_from_rfc3339("2026-08-13T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let last_row = Some((cutoff - chrono::Duration::seconds(30), Uuid::new_v4()));
        // Batch came back under the limit (drained) — must jump to
        // `cutoff - OVERLAP_WINDOW`, NOT to `last_row`'s own position.
        let advanced = advance_cursor(None, cutoff, 1, 500, last_row);
        assert_eq!(advanced, Some((cutoff - OVERLAP_WINDOW, Uuid::nil())));
    }

    #[test]
    fn advance_cursor_never_moves_backward() {
        let cutoff = chrono::DateTime::parse_from_rfc3339("2026-08-13T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let already_ahead = Some((cutoff, Uuid::new_v4())); // already past the would-be target
        let advanced = advance_cursor(already_ahead, cutoff, 1, 500, Some((cutoff, Uuid::nil())));
        assert_eq!(advanced, already_ahead, "must not regress below an already-advanced cursor");
    }

    #[test]
    fn advance_cursor_uses_last_row_when_the_batch_is_truncated() {
        let cutoff = chrono::DateTime::parse_from_rfc3339("2026-08-13T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let last = (cutoff - chrono::Duration::seconds(400), Uuid::new_v4());
        // Batch hit the limit — backlog may remain between `last` and
        // `cutoff`; must NOT jump ahead to the overlap boundary yet.
        let advanced = advance_cursor(None, cutoff, 500, 500, Some(last));
        assert_eq!(advanced, Some(last));
    }
}

// ── Live-Postgres tests ──────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with the control
// migrations applied through at least `0152_llm_usage_source_query_id_
// cursor_index_concurrently.sql`) — same convention as `llm::usage::
// pg_tests` (see that module for the precedent this file follows). `cargo
// test --lib` with no env var set SKIPS these with an `eprintln!` marker
// rather than failing. Every test mints FRESH `Uuid::new_v4()` accounts/
// jobs/graphs, so tests never COLLIDE with each other.
//
// Fresh UUIDs are not a substitute for cleanup, though, and this header used
// to claim they were ("need no shared-fixture cleanup"). **Issue #642.** A
// fresh id prevents collision; it does nothing about ACCUMULATION. These
// suites run against ONE database, and anything that reads without an
// `account_id` filter sees every row every previous test ever left:
//
//   - a globally-unscoped query, like the unattributed-job alert in `tick`
//     (`with_job_cleanup` below already carves out this exact exception, and
//     its doc comment is where the reasoning was first written down);
//   - a constraint validation, which is table-wide by construction —
//     re-adding an exclusion constraint during Phase 4b failed with a real
//     `23P01` naming two orphaned rows, which reads as a code bug until you
//     realise it is leftover test state;
//   - a sibling suite counting anomalies, which is how this first surfaced.
//
// The damage is silent and delayed: the leaking suite stays green and the
// failure lands in whichever suite runs later. So every test here now calls
// `cleanup` before it returns.
//
// These tests run on the CONSUMER plane only (`TenantId::Consumer` against
// the control DB itself, which doubles as the consumer-content plane) — the
// CI job this suite rides in (`mcp-e2e`) applies only `migrations_control`,
// with no tenant-tree database to exercise the Org branch's
// `begin_tenant_system_scoped`/no-account_id-filter code paths against. The
// Org-specific SQL branches are covered by `cargo check`'s type-checking and
// by direct code review, not by a live run in this suite — flagged
// honestly rather than implied covered.
#[cfg(test)]
mod pg_tests {
    use std::sync::Arc;

    use super::*;
    use crate::store::{PostgresStore, Store};

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
                .max_connections(8)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    fn consumer_tenant(pool: PgPool) -> WorkerTenant {
        WorkerTenant {
            tenant: TenantId::Consumer,
            store: Arc::new(PostgresStore::new(pool.clone())),
            pool,
        }
    }

    /// A real `users` + `accounts` row pair — `credit_ledger`/`credit_balances`
    /// both FK to `accounts(id)`, so every test needs one.
    async fn seed_account(pool: &PgPool) -> Uuid {
        let user_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@billing-pr9.test"))
            .execute(pool)
            .await
            .expect("insert user");
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("insert account");
        account_id
    }

    /// Undo `seed_account`, leaving the shared database as it was found.
    /// **Issue #642** — see this module's header for why fresh UUIDs did not
    /// make this unnecessary.
    ///
    /// `accounts` cascades to the billing children — `credit_ledger`,
    /// `credit_balances`, `enforcement_events`, `member_allowance_periods`,
    /// `grants`, `account_prefs`, `graphs` (and `atoms` behind it) — so
    /// deleting the account covers most of the job.
    ///
    /// **It does NOT cover `jobs` or `llm_usage`.** Measured against a v163
    /// control DB: neither table has a foreign key to `accounts` at all
    /// (`SELECT ... FROM pg_constraint WHERE confrelid = 'accounts'::regclass`
    /// lists 36 children; those two are not among them). Deleting the account
    /// leaves every seeded job and usage row behind, which is exactly the
    /// residue that matters most — `jobs` is the table the globally-unscoped
    /// unattributed-job alert reads, and `llm_usage` is what the sibling
    /// `usage`/`usage_reconciliation` suites count. So both are deleted
    /// explicitly, by `account_id`.
    ///
    /// Jobs seeded with a NULL `account_id` (`insert_unattributed_done_job`)
    /// cannot be reached that way and remain `with_job_cleanup`'s job; the
    /// two mechanisms are complementary, not alternatives.
    ///
    /// The owner user is NOT a child — `accounts.owner_user_id` points AT it
    /// — so it has to be read off the account first and deleted after, in
    /// that order, or the FK refuses the delete. That ordering is the whole
    /// reason this is a helper rather than a line of SQL at each call site.
    ///
    /// `seed_account` returns only the account id, so callers cannot name
    /// the owner themselves; deriving it here is what lets every existing
    /// call site stay a one-liner. Tests that insert ADDITIONAL users (the
    /// attribution tests seed members beyond the owner) pass them in
    /// `extra_users`.
    ///
    /// Deletes are best-effort (`let _ =`): a test that already failed its
    /// assertions must not have its real failure masked by a teardown error.
    ///
    /// **Panic caveat, stated honestly.** This runs at the end of a test
    /// body, so a test that PANICS skips it and still leaks. That is
    /// deliberate and sufficient for #642, which is about the leak on a
    /// FULLY GREEN run — a panicking test means the suite is already red and
    /// being investigated. Where panic-safe teardown is genuinely
    /// load-bearing, use the `with_job_cleanup` wrapper below instead; it
    /// re-panics with the original payload after cleaning up.
    async fn cleanup(pool: &PgPool, account_id: Uuid, extra_users: &[Uuid]) {
        let owner: Option<Uuid> =
            sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
                .bind(account_id)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();

        // The two tables the cascade does not reach. Before the account, so
        // an FK added to either one later cannot turn this into an ordering
        // bug that only shows up on a future migration.
        //
        // `llm_usage` needs BOTH arms of that `OR`, and the job-id arm is the
        // one that actually matters: `insert_llm_usage_for_job` seeds
        // `account_id` as NULL (the real job-worker shape — derivation
        // resolves the account through `source_job_id`), so an
        // `account_id = $1` delete alone misses every job-axis row. Measured:
        // it left exactly 13 behind on an otherwise spotless run. The
        // query- and journal-axis helpers do pass `Some(account_id)`, which
        // the first arm covers.
        //
        // This must run BEFORE the `jobs` delete below — once the jobs are
        // gone the subquery matches nothing and the usage rows are stranded
        // for good.
        let _ = sqlx::query(
            "DELETE FROM llm_usage \
             WHERE account_id = $1 \
                OR source_job_id IN (SELECT id FROM jobs WHERE account_id = $1)",
        )
        .bind(account_id)
        .execute(pool)
        .await;

        let _ = sqlx::query("DELETE FROM jobs WHERE account_id = $1")
            .bind(account_id)
            .execute(pool)
            .await;

        let mut financial=begin_account_scoped(pool,account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial,account_id).await.unwrap();
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id)
            .execute(&mut *financial)
            .await.expect("compatible account cascade cleanup");
        financial.commit().await.unwrap();

        for user_id in owner.iter().chain(extra_users.iter()) {
            let _ = sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(pool)
                .await;
        }
    }

    /// A real `graphs` row (needed for the journal axis, which requires a
    /// non-NULL `graph_id`).
    async fn seed_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        let graph_id = Uuid::new_v4();
        sqlx::query("INSERT INTO graphs (id, account_id, name) VALUES ($1, $2, 'test ocean')")
            .bind(graph_id)
            .bind(account_id)
            .execute(pool)
            .await
            .expect("insert graph");
        graph_id
    }

    /// A `status='done', kind='ExtractEntities'` job, `finished_at` set to
    /// `now() - finished_at_ago_secs` via SQL interval math (never a bound
    /// `chrono::DateTime` — see this crate's own "never round-trip Utc::now()
    /// through TIMESTAMPTZ" lesson; here the reason is different but the
    /// discipline is the same: the cursor tests below need the ACTUAL
    /// Postgres `now()` as their reference point, not a Rust-side clock that
    /// could disagree with it by however long the test has been running).
    async fn insert_done_job(pool: &PgPool, id: Uuid, account_id: Uuid, idempotency_key: &str, finished_at_ago_secs: f64) {
        sqlx::query(
            "INSERT INTO jobs (id, kind, payload, status, idempotency_key, account_id, finished_at, started_at) \
             VALUES ($1, 'ExtractEntities', '{}'::jsonb, 'done', $2, $3, \
                     now() - ($4 * INTERVAL '1 second'), now() - ($4 * INTERVAL '1 second'))",
        )
        .bind(id)
        .bind(idempotency_key)
        .bind(account_id)
        .bind(finished_at_ago_secs)
        .execute(pool)
        .await
        .expect("insert done job");
    }

    /// A pool whose every connection has `SET ROLE` to a role that is SUBJECT
    /// to row-level security, i.e. one with `rolbypassrls = false`.
    ///
    /// Every other pool in this test module connects as the database owner,
    /// which bypasses RLS entirely. That is precisely why the production defect
    /// this guards against went unseen: policies are simply not evaluated for
    /// the role the tests use, so an unscoped read of an RLS-covered table
    /// looks perfectly healthy right up until it runs as `coralswarm_app`.
    ///
    /// Returns `None` when the role cannot be created (insufficient privilege),
    /// so the suite degrades to "skipped" rather than "failed" on a test
    /// database that is not owned by a superuser.
    async fn rls_subject_pool(pool: &PgPool, url: &str) -> Option<PgPool> {
        const ROLE: &str = "cs_rls_probe";

        // NOLOGIN: reached only via SET ROLE, never by connecting.
        let created = sqlx::query(&format!(
            "DO $$ BEGIN \
               IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN \
                 CREATE ROLE {ROLE} NOLOGIN; \
               END IF; \
             END $$;"
        ))
        .execute(pool)
        .await;
        if created.is_err() {
            eprintln!("skipping: cannot create the {ROLE} role on this test database");
            return None;
        }

        for stmt in [
            format!("GRANT USAGE ON SCHEMA public TO {ROLE}"),
            format!(
                "GRANT SELECT, INSERT, UPDATE ON credit_balances, credit_ledger, \
                 member_allowance_periods, grants TO {ROLE}"
            ),
            // SET ROLE requires the connecting role to be a member of the
            // target role.
            format!("GRANT {ROLE} TO CURRENT_USER"),
        ] {
            if sqlx::query(&stmt).execute(pool).await.is_err() {
                eprintln!("skipping: cannot grant to {ROLE} on this test database");
                return None;
            }
        }

        // ONE connection, so the negative control below is guaranteed to reuse
        // the same session the scoped transaction ran on — which is what makes
        // `app.account_id` a `''` placeholder rather than unset. See the test
        // for why that distinction is the whole point.
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query(&format!("SET ROLE {ROLE}")).execute(conn).await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .ok()
    }

    /// **The regression test for the defect that kept `credit_ledger` and
    /// `credit_balances` EMPTY in production for the derivation worker's entire
    /// life.**
    ///
    /// [`load_cursor`] read `credit_balances` on an unscoped connection. Under
    /// the owner role every other test here uses, RLS is bypassed and that read
    /// succeeds — which is exactly why no existing test caught it. Under
    /// `coralswarm_app` (`rolbypassrls = false`) the `account_isolation`
    /// policy's `current_setting('app.account_id')::uuid` sees the empty string
    /// and raises `invalid input syntax for type uuid: ""`, so every pass
    /// failed for every account and `tick`'s refuse-to-write-on-partial-failure
    /// discipline correctly wrote nothing. Loud in the logs, invisible in the
    /// data.
    ///
    /// So this test runs as a role RLS actually applies to, and asserts BOTH
    /// directions. The negative control is the load-bearing half: it proves the
    /// probe role really is non-bypassing, so a green result here means the
    /// scope is doing the work rather than the role quietly ignoring policies.
    ///
    /// The negative control deliberately runs the unscoped read on a connection
    /// that has ALREADY served a scoped transaction, because that is the state
    /// a pooled production connection is actually in — and the two states fail
    /// differently. `app.account_id` becomes a session-local placeholder the
    /// first time it is ever set and reverts to `''` (not "unset") once that
    /// transaction ends, so a reused connection raises `invalid input syntax
    /// for type uuid: ""` while a pristine one raises `unrecognized
    /// configuration parameter`. `0083_consumer_rls`'s header documents this,
    /// and `store::mod`'s journal-sweep trio was fixed for the same reason. The
    /// empty-string form is the one production logged, so it is the one
    /// reproduced here.
    #[tokio::test]
    async fn load_cursor_reads_under_rls_without_bypass() {
        let name = "load_cursor_reads_under_rls_without_bypass";
        let Some(pool) = pg_test_pool(name).await else { return };
        let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pg_test_pool");
        let Some(rls_pool) = rls_subject_pool(&pool, &url).await else { return };

        let account_id = seed_account(&pool).await;

        // Put the single pooled connection into the state a live one is in:
        // `app.account_id` set once, then released back to the pool.
        crate::tenancy::scope::begin_account_scoped(&rls_pool, account_id)
            .await
            .expect("scoped transaction on the probe pool")
            .commit()
            .await
            .expect("commit the scoped transaction");

        // ── Negative control ─────────────────────────────────────────────────
        // The exact query `load_cursor` used to run, on an UNSCOPED connection.
        // It must still fail, or this test proves nothing about the fix.
        let unscoped: Result<Option<(Option<Uuid>,)>, sqlx::Error> =
            sqlx::query_as("SELECT job_axis_cursor_job_id FROM credit_balances WHERE account_id = $1")
                .bind(account_id)
                .fetch_optional(&rls_pool)
                .await;
        let err = unscoped.expect_err(
            "the probe role is bypassing RLS, so this test cannot detect the defect it exists to \
             catch — check that cs_rls_probe has rolbypassrls = false",
        );
        assert!(
            err.to_string().contains("invalid input syntax for type uuid"),
            "expected the unscoped read to fail exactly as production did, on the empty \
             app.account_id GUC, got: {err}"
        );

        // ── The fix ──────────────────────────────────────────────────────────
        // Same table, same role, but `load_cursor` opens its own account scope.
        let cursor = load_cursor(&rls_pool, account_id)
            .await
            .expect("load_cursor must open its own account scope and succeed under RLS");
        assert_eq!(
            (cursor.job, cursor.query, cursor.journal),
            (None, None, None),
            "a freshly seeded account has no credit_balances row, which is an EMPTY cursor — \
             not an error, and not a fabricated position"
        );

        // Teardown goes through the bypass pool, not `rls_pool` — the probe
        // role deliberately cannot bypass RLS, so deleting through it would
        // silently remove nothing (#642).
        cleanup(&pool, account_id, &[]).await;
    }

    /// Seed one CURRENT allowance period for a member, the way
    /// `renewal::roll_period` would.
    async fn seed_allowance_period(pool: &PgPool, account_id: Uuid, user_id: Uuid, granted: i64) {
        let mut tx=begin_account_scoped(pool,account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx,account_id).await.unwrap();
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, now() - interval '1 day', now() + interval '29 days', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(Decimal::from(granted))
        .execute(&mut *tx)
        .await
        .expect("insert member_allowance_periods");
        tx.commit().await.unwrap();
    }

    async fn consumed_credits(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Option<Decimal> {
        sqlx::query_scalar(
            "SELECT consumed_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_optional(pool)
        .await
        .expect("read consumed_credits")
    }

    fn job_debit(source_job_id: Uuid, cost_usd: Decimal, user: Option<Uuid>) -> LedgerDebit {
        LedgerDebit {
            funded: None,
            entry_kind: ENTRY_KIND_JOB,
            cost_usd,
            source_job_id: Some(source_job_id),
            source_query_id: None,
            graph_id: None,
            journal_period: None,
            journal_granularity: None,
            source_receipt_id: None,
            attributed_user_id: user,
        }
    }

    fn read_of(debits: Vec<LedgerDebit>) -> AccountRead {
        AccountRead {
            debits,
            old_cursor: AccountCursor::default(),
            new_cursor: AccountCursor::default(),
            cache_hit_rows: 0,
            unthreaded_rows: 0,
        }
    }

    /// **The write half of the split-bucket model.**
    ///
    /// Migration `0161` says `consumed_credits` is "maintained by
    /// `workers::billing_derivation` as it writes debits". Before this PR no
    /// such code existed — every write of the column in the tree was inside
    /// `billing::enforcement`'s own tests — so the per-member bucket sat at
    /// zero forever. That is not a cosmetic gap: `is_blocked` short-circuits on
    /// `consumed < granted`, and a present period row also shadows the
    /// `org_pool_exhausted` fallback beneath it, so a member with a period row
    /// could never be blocked by anything at all.
    ///
    /// Asserts the three things that make the counter trustworthy: it moves for
    /// an attributed debit, it does NOT move twice when the same unit of work
    /// is re-derived (the `ON CONFLICT` path), and an UNATTRIBUTED debit still
    /// hits the org pool without being charged to anybody's seat.
    #[tokio::test]
    async fn debits_advance_the_attributed_members_consumed_credits() {
        let name = "debits_advance_the_attributed_members_consumed_credits";
        let Some(pool) = pg_test_pool(name).await else { return };

        let account_id = seed_account(&pool).await;
        let member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(member)
            .bind(format!("{member}@billing-p4c.test"))
            .execute(&pool)
            .await
            .expect("insert member user");
        seed_allowance_period(&pool, account_id, member, 3000).await;

        // $0.0044 at $0.0044/credit == exactly 1 credit (see
        // `credits_for_cost_is_negative_and_exact`), so the arithmetic below is
        // exact and the assertions can be equalities rather than ranges.
        let reference = Decimal::from(44) / Decimal::from(10_000);
        let job_id = Uuid::new_v4();
        let read = read_of(vec![job_debit(job_id, reference, Some(member))]);

        let n = write_account_ledger(&pool, account_id, &read, reference)
            .await
            .expect("write the ledger");
        assert_eq!(n, 1, "one debit, one ledger row");
        assert_eq!(
            consumed_credits(&pool, account_id, member).await,
            Some(Decimal::from(1)),
            "an attributed debit must advance that member's consumed_credits"
        );

        // ── Re-derivation must not double-charge ─────────────────────────────
        // Same job id, so `insert_ledger_row` hits its `ON CONFLICT` and
        // returns None. The consumption accumulator reads from that same
        // `Option`, so the member must not be charged a second time.
        let n = write_account_ledger(&pool, account_id, &read, reference)
            .await
            .expect("re-write the same ledger");
        assert_eq!(n, 0, "the duplicate unit of work must be skipped");
        assert_eq!(
            consumed_credits(&pool, account_id, member).await,
            Some(Decimal::from(1)),
            "a redelivered/re-derived debit must NOT be charged against the allowance twice — \
             this is the same ON CONFLICT guarantee that protects credit_balances"
        );

        // ── An unattributed debit charges the pool, nobody's seat ────────────
        let orphan = read_of(vec![job_debit(Uuid::new_v4(), reference, None)]);
        write_account_ledger(&pool, account_id, &orphan, reference)
            .await
            .expect("write the unattributed debit");
        assert_eq!(
            consumed_credits(&pool, account_id, member).await,
            Some(Decimal::from(1)),
            "a debit with no attributed_user_id must not land on an unrelated member's allowance"
        );
        let balance: Decimal =
            sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .expect("read the org pool");
        assert_eq!(
            balance,
            Decimal::from(-2),
            "the pool carries the TWO distinct debits at 1 credit each and is untouched by the \
             skipped duplicate — the org pool and the per-member bucket must agree about which \
             units of work were real"
        );

        cleanup(&pool, account_id, &[member]).await;
    }

    /// Two allowance periods can no longer both cover `now()` — the database
    /// refuses to store that state at all.
    ///
    /// # Why this test asserts prevention where it used to assert mitigation
    ///
    /// This was `overlapping_periods_charge_only_the_newest`, and it proved the
    /// `max(period_start)` pin in [`advance_member_consumption`] charged only
    /// the newest of two overlapping rows. That was the right test while an
    /// overlap was merely *tolerable*: the unique index is `(account_id,
    /// user_id, period_start)`, which does nothing to stop two rows from both
    /// covering `now()`, so the pin was all that stood between one unit of work
    /// and being billed twice.
    ///
    /// Phase 4b's migration `0163` added an exclusion constraint on
    /// `(account_id, user_id, tstzrange(period_start, period_end))`, which
    /// makes the overlap UNREPRESENTABLE rather than merely uncharged. The old
    /// test's setup can no longer run — its second INSERT now raises `23P01`
    /// before the worker is ever called — so continuing to assert the
    /// mitigation would mean asserting behaviour that no reachable state can
    /// produce. The honest replacement is to assert the stronger guarantee that
    /// replaced it, which is what this does.
    ///
    /// The `max(period_start)` pin is deliberately KEPT rather than reverted:
    /// it costs nothing, it agrees with
    /// `enforcement::read_member_period_scoped`'s matching `ORDER BY
    /// period_start DESC LIMIT 1`, and it is the behaviour that stays correct
    /// if the constraint is ever dropped or deferred. Its own doc comment
    /// records that it is now defence in depth.
    ///
    /// **Negative control.** Drop `member_allowance_periods_no_overlap` and
    /// this test FAILS at `expect_err` — the second insert succeeds and returns
    /// `PgQueryResult { rows_affected: 1 }`. Verified by dropping the
    /// constraint against the live test database and watching it fail, then
    /// restoring it and watching it pass.
    #[tokio::test]
    async fn overlapping_periods_are_rejected_by_the_database() {
        let name = "overlapping_periods_are_rejected_by_the_database";
        let Some(pool) = pg_test_pool(name).await else { return };

        let account_id = seed_account(&pool).await;
        let member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(member)
            .bind(format!("{member}@billing-p4c-overlap.test"))
            .execute(&pool)
            .await
            .expect("insert member user");

        let insert = |ago: &'static str, ahead: &'static str| {
            let pool = pool.clone();
            async move {
                let mut tx=begin_account_scoped(&pool,account_id).await.unwrap();
                crate::billing::renewal::lock_period_rotation(&mut tx,account_id).await.unwrap();
                let result=sqlx::query(&format!(
                    "INSERT INTO member_allowance_periods \
                         (account_id, user_id, period_start, period_end, granted_credits) \
                     VALUES ($1, $2, now() - interval '{ago}', now() + interval '{ahead}', $3)"
                ))
                .bind(account_id)
                .bind(member)
                .bind(Decimal::from(3000))
                .execute(&mut *tx)
                .await;
                if result.is_ok() {tx.commit().await.unwrap();} else {tx.rollback().await.unwrap();}
                result
            }
        };

        // The older period: started 10 days ago, still open.
        insert("10 days", "20 days").await.expect("the first period inserts normally");

        // A second period that also covers `now()`. Before 0163 this succeeded
        // and left the pin to sort out; now the database refuses it outright.
        let err = insert("1 day", "29 days")
            .await
            .expect_err("an overlapping period must be rejected by the exclusion constraint");
        let db = err.as_database_error().expect("a database error, not a protocol error");
        assert_eq!(db.code().as_deref(), Some("23P01"), "exclusion_violation, got {db:?}");
        assert_eq!(
            db.constraint(),
            Some("member_allowance_periods_no_overlap"),
            "the failure must name 0163's constraint, so a future reader knows which rule bit"
        );

        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM member_allowance_periods WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member)
        .fetch_one(&pool)
        .await
        .expect("count the member's periods");
        assert_eq!(rows, 1, "the rejected insert must leave exactly the original row");

        // Load-bearing beyond ordinary hygiene: these `member_allowance_
        // periods` rows are what the exclusion constraint validates over, and
        // a leaked pair is what made re-adding that constraint fail with a
        // real `23P01` during Phase 4b (#642).
        cleanup(&pool, account_id, &[member]).await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_llm_usage_for_job(
        pool: &PgPool,
        account_id: Option<Uuid>,
        graph_id: Option<Uuid>,
        source_job_id: Uuid,
        model: &str,
        input_tokens: i32,
        output_tokens: i32,
    ) {
        sqlx::query(
            "INSERT INTO llm_usage (account_id, graph_id, task, model, input_tokens, output_tokens, source_job_id) \
             VALUES ($1, $2, 'extract_entities', $3, $4, $5, $6)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(model)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(source_job_id)
        .execute(pool)
        .await
        .expect("insert llm_usage for job");
    }

    /// Billing Phase 2a: same shape as [`insert_llm_usage_for_job`] but also
    /// sets `attributed_user_id` — the field this PR adds — so tests can
    /// exercise the job axis's attribution-resolution path without a full
    /// `UsageContext`/`record` round trip.
    #[allow(clippy::too_many_arguments)]
    async fn insert_llm_usage_for_job_attributed(
        pool: &PgPool,
        account_id: Option<Uuid>,
        graph_id: Option<Uuid>,
        source_job_id: Uuid,
        model: &str,
        input_tokens: i32,
        output_tokens: i32,
        attributed_user_id: Option<Uuid>,
    ) {
        sqlx::query(
            "INSERT INTO llm_usage \
                 (account_id, graph_id, task, model, input_tokens, output_tokens, \
                  source_job_id, attributed_user_id) \
             VALUES ($1, $2, 'extract_entities', $3, $4, $5, $6, $7)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(model)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(source_job_id)
        .bind(attributed_user_id)
        .execute(pool)
        .await
        .expect("insert attributed llm_usage for job");
    }

    /// OpenRouter provider port, PR1: same shape as
    /// [`insert_llm_usage_for_job`] but with an explicit `provider` and
    /// `cost_usd` — used to prove (a) the job axis's `(provider, model)`
    /// threading into `PriceSheet::rate` and (b) the SQL-level `CASE WHEN
    /// bool_and(cost_usd IS NOT NULL)` NULL-safety fix (a bare
    /// `SUM(cost_usd)` silently ignores NULL rows in Postgres).
    #[allow(clippy::too_many_arguments)]
    async fn insert_llm_usage_for_job_with_provider_and_cost(
        pool: &PgPool,
        account_id: Option<Uuid>,
        graph_id: Option<Uuid>,
        source_job_id: Uuid,
        provider: &str,
        model: &str,
        input_tokens: i32,
        output_tokens: i32,
        cost_usd: Option<Decimal>,
    ) {
        sqlx::query(
            "INSERT INTO llm_usage \
                 (account_id, graph_id, task, provider, model, input_tokens, output_tokens, \
                  source_job_id, cost_usd) \
             VALUES ($1, $2, 'extract_entities', $3, $4, $5, $6, $7, $8)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(provider)
        .bind(model)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(source_job_id)
        .bind(cost_usd)
        .execute(pool)
        .await
        .expect("insert llm_usage for job with provider and cost");
    }

    async fn ledger_rows_for_account(pool: &PgPool, account_id: Uuid) -> Vec<(Option<Uuid>, Decimal, Decimal)> {
        sqlx::query_as(
            "SELECT source_job_id, credits, cost_usd FROM credit_ledger \
             WHERE account_id = $1 ORDER BY source_job_id NULLS LAST",
        )
        .bind(account_id)
        .fetch_all(pool)
        .await
        .expect("fetch ledger rows")
    }

    /// Billing Phase 2a: `attributed_user_id` alongside the job axis's own
    /// idempotency key, for tests that need to assert on the resolved
    /// attribution rather than just cost/credits.
    async fn ledger_job_attribution(
        pool: &PgPool,
        account_id: Uuid,
        source_job_id: Uuid,
    ) -> Option<Option<Uuid>> {
        sqlx::query_scalar(
            "SELECT attributed_user_id FROM credit_ledger \
             WHERE account_id = $1 AND source_job_id = $2",
        )
        .bind(account_id)
        .bind(source_job_id)
        .fetch_optional(pool)
        .await
        .expect("fetch ledger attribution")
    }

    async fn insert_llm_usage_for_query_attributed(
        pool: &PgPool,
        account_id: Option<Uuid>,
        source_query_id: Uuid,
        model: &str,
        input_tokens: i32,
        output_tokens: i32,
        attributed_user_id: Option<Uuid>,
    ) {
        sqlx::query(
            // `created_at` is backdated past CURSOR_LAG (180s) on purpose: a
            // row newer than `cutoff = now() - CURSOR_LAG` is deliberately
            // NOT billable yet, so a `now()` default would make this test
            // fail for a reason that has nothing to do with attribution.
            "INSERT INTO llm_usage \
                 (account_id, task, model, input_tokens, output_tokens, \
                  source_query_id, attributed_user_id, created_at) \
             VALUES ($1, 'synthesis_prose', $2, $3, $4, $5, $6, \
                     now() - interval '400 seconds')",
        )
        .bind(account_id)
        .bind(model)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(source_query_id)
        .bind(attributed_user_id)
        .execute(pool)
        .await
        .expect("insert attributed llm_usage for query");
    }

    async fn insert_llm_usage_for_journal_attributed(
        pool: &PgPool,
        account_id: Option<Uuid>,
        graph_id: Uuid,
        period: DateTime<Utc>,
        granularity: &str,
        model: &str,
        input_tokens: i32,
        output_tokens: i32,
        attributed_user_id: Option<Uuid>,
    ) {
        sqlx::query(
            // Backdated past CURSOR_LAG for the same reason as the query-axis
            // helper above — the journal axis applies the same `created_at <=
            // cutoff` filter alongside its `source_journal_period` ordering.
            "INSERT INTO llm_usage \
                 (account_id, graph_id, task, model, input_tokens, output_tokens, \
                  source_journal_period, source_journal_granularity, attributed_user_id, \
                  created_at) \
             VALUES ($1, $2, 'journal_compile', $3, $4, $5, $6, $7, $8, \
                     now() - interval '400 seconds')",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(model)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(period)
        .bind(granularity)
        .bind(attributed_user_id)
        .execute(pool)
        .await
        .expect("insert attributed llm_usage for journal");
    }

    async fn ledger_query_attribution(
        pool: &PgPool,
        account_id: Uuid,
        source_query_id: Uuid,
    ) -> Option<Option<Uuid>> {
        sqlx::query_scalar(
            "SELECT attributed_user_id FROM credit_ledger \
             WHERE account_id = $1 AND source_query_id = $2",
        )
        .bind(account_id)
        .bind(source_query_id)
        .fetch_optional(pool)
        .await
        .expect("fetch query-axis ledger attribution")
    }

    async fn ledger_journal_attribution(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        granularity: &str,
        period: DateTime<Utc>,
    ) -> Option<Option<Uuid>> {
        sqlx::query_scalar(
            "SELECT attributed_user_id FROM credit_ledger \
             WHERE account_id = $1 AND graph_id = $2 \
               AND journal_granularity = $3 AND journal_period = $4",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(granularity)
        .bind(period)
        .fetch_optional(pool)
        .await
        .expect("fetch journal-axis ledger attribution")
    }

    async fn balance_for_account(pool: &PgPool, account_id: Uuid) -> Option<Decimal> {
        sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .expect("fetch balance")
    }

    /// **The double-charge trap, positive proof.** Two DISTINCT `jobs` rows
    /// sharing one `idempotency_key` (exactly the state a real re-enqueue-
    /// after-completion produces — `idx_jobs_idem_pending` only protects
    /// PENDING rows, so two `done` rows CAN share a key), each with its own
    /// real `llm_usage` spend. Keying the ledger on `jobs.id` must charge
    /// BOTH distinctly — collapsing them via `idempotency_key` (which a
    /// naive implementation might be tempted to do, since it looks like "the
    /// same atom") would silently under-charge one of two genuine LLM calls.
    #[tokio::test]
    async fn job_axis_charges_two_distinct_jobs_sharing_one_idempotency_key() {
        let Some(pool) = pg_test_pool("job_axis_charges_two_distinct_jobs_sharing_one_idempotency_key").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let shared_key = format!("extract:{}", Uuid::new_v4());

        let job_a = Uuid::new_v4();
        let job_b = Uuid::new_v4();
        // Both safely past CURSOR_LAG (180s).
        insert_done_job(&pool, job_a, account_id, &shared_key, 400.0).await;
        insert_done_job(&pool, job_b, account_id, &shared_key, 300.0).await;
        // Different token counts so the two jobs' costs are distinguishable.
        insert_llm_usage_for_job(&pool, None, None, job_a, "claude-sonnet-4-6", 1_000_000, 0).await;
        insert_llm_usage_for_job(&pool, None, None, job_b, "claude-sonnet-4-6", 2_000_000, 0).await;

        let t = consumer_tenant(pool.clone());
        let tick_result = tick(&t, &pool).await.expect("tick succeeds");
        assert_eq!(tick_result, Tick::worked(2), "both distinct jobs must be charged");

        let rows = ledger_rows_for_account(&pool, account_id).await;
        assert_eq!(rows.len(), 2, "sharing an idempotency_key must not collapse two real jobs into one row");

        let by_job: HashMap<Uuid, (Decimal, Decimal)> = rows
            .into_iter()
            .map(|(job, credits, cost)| (job.expect("job axis row"), (credits, cost)))
            .collect();
        // claude-sonnet-4-6: $3/M input. 1M tokens -> $3; 2M tokens -> $6.
        let (credits_a, cost_a) = by_job[&job_a];
        let (credits_b, cost_b) = by_job[&job_b];
        assert_eq!(cost_a, Decimal::from(3), "job A: 1M input tokens @ $3/M");
        assert_eq!(cost_b, Decimal::from(6), "job B: 2M input tokens @ $3/M");
        assert!(credits_a < Decimal::ZERO, "a debit must be negative");
        assert!(credits_b < Decimal::ZERO, "a debit must be negative");
        assert_ne!(credits_a, credits_b, "the two jobs' distinct real costs must not have collided");

        cleanup(&pool, account_id, &[]).await;
    }

    /// #1497: one account whose financial lock is held (by a connection
    /// outside this process's entry queue) must not take the tick's ledger
    /// writes away from every account after it. A is enumerated before B
    /// (`enumerate_consumer_accounts` orders by id). With A's lock held, a
    /// tick reports partial, B's debit is written, A's is not and nothing of
    /// A's is marked; once the lock is free the next tick writes A's debit
    /// exactly once, and a further tick adds nothing.
    #[tokio::test]
    async fn a_contended_account_is_skipped_and_the_rest_of_the_tick_still_writes() {
        let Some(pool) = pg_test_pool("a_contended_account_is_skipped_and_the_rest_of_the_tick_still_writes").await else {
            return;
        };
        let (first, second) = (seed_account(&pool).await, seed_account(&pool).await);
        let (a, b) = if first < second { (first, second) } else { (second, first) };
        let (job_a, job_b) = (Uuid::new_v4(), Uuid::new_v4());
        insert_done_job(&pool, job_a, a, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_done_job(&pool, job_b, b, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job(&pool, None, None, job_a, "claude-sonnet-4-6", 1_000_000, 0).await;
        insert_llm_usage_for_job(&pool, None, None, job_b, "claude-sonnet-4-6", 1_000_000, 0).await;
        let t = consumer_tenant(pool.clone());

        let mut holder = crate::tenancy::scope::begin_account_scoped(&pool, a).await.expect("scope holder");
        crate::billing::renewal::lock_period_rotation(&mut holder, a).await.expect("hold A's lock");
        let partial = tick(&t, &pool).await;
        let Err(report) = partial else {
            panic!("a tick with one account's lock held must report partial, not succeed");
        };
        assert!(
            report.contains("skipped on the financial lock") && report.contains(&a.to_string()),
            "the partial report names the skipped account: {report}"
        );
        assert_eq!(ledger_rows_for_account(&pool, b).await.len(), 1, "B's debit was written past A's skip");
        assert_eq!(ledger_rows_for_account(&pool, a).await.len(), 0, "A's debit waits for the next tick");
        assert_eq!(balance_for_account(&pool, a).await, None, "nothing of A's was written or marked");
        holder.rollback().await.expect("release A's lock");

        tick(&t, &pool).await.expect("the next tick goes through");
        assert_eq!(ledger_rows_for_account(&pool, a).await.len(), 1, "A's debit written exactly once");
        assert_eq!(ledger_rows_for_account(&pool, b).await.len(), 1, "B's not repeated");
        tick(&t, &pool).await.expect("a further tick");
        assert_eq!(ledger_rows_for_account(&pool, a).await.len(), 1, "a further tick adds nothing for A");

        cleanup(&pool, a, &[]).await;
        cleanup(&pool, b, &[]).await;
    }

    /// **Item 2(a), cutover-blocking — a NULL-cost OpenRouter bucket prices
    /// at its OWN pinned rate, never the Anthropic blend.** Before this fix,
    /// `estimate_cost_usd` hardcoded `"anthropic"` internally — an
    /// OpenRouter row with no provider-reported cost (an errored/estimated
    /// finalize; `StreamUsageGuard::finalize_err`/`Drop`; a
    /// `usage_pending_sweep` orphan) would have priced at the Anthropic
    /// blend rate ($5.70/M) instead of DeepSeek's pinned $0.08/M — wrong in
    /// either direction depending on the model, and by orders of magnitude
    /// for a cheap background model like DeepSeek.
    #[tokio::test]
    async fn job_axis_prices_a_null_cost_openrouter_bucket_at_its_own_pinned_rate_not_the_anthropic_blend(
    ) {
        let Some(pool) = pg_test_pool(
            "job_axis_prices_a_null_cost_openrouter_bucket_at_its_own_pinned_rate_not_the_anthropic_blend",
        )
        .await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job_with_provider_and_cost(
            &pool,
            None,
            None,
            job_id,
            "openrouter",
            "deepseek/deepseek-v4-flash-0731",
            1_000_000,
            0,
            None, // no provider-reported cost — exercises the estimate fallback
        )
        .await;

        let t = consumer_tenant(pool.clone());
        let tick_result = tick(&t, &pool).await.expect("tick succeeds");
        assert_eq!(tick_result, Tick::worked(1));

        let rows = ledger_rows_for_account(&pool, account_id).await;
        assert_eq!(rows.len(), 1);
        let (_job, _credits, cost) = rows[0];
        assert_eq!(
            cost,
            "0.08".parse::<Decimal>().unwrap(),
            "must price at DeepSeek's pinned $0.08/M input rate (1M tokens), never the \
             Anthropic blend rate ($5.70/M) or a phantom $0"
        );

        cleanup(&pool, account_id, &[]).await;
    }

    /// **Item 2(b), cutover-blocking — the job axis's NULL-safe SQL sum.** A
    /// bucket with TWO rows sharing the same `(job, provider, model)` — one
    /// carrying a provider-reported `cost_usd`, one not — must fall through
    /// to the token-based estimate for the WHOLE bucket, never silently sum
    /// only the non-NULL row (`SUM(cost_usd)` in plain SQL ignores NULLs by
    /// default; `ModelBucket::cost_usd_sum`'s contract is "`Some` only when
    /// EVERY row carried cost"). The reported cost on the non-NULL row
    /// (`$5`) is deliberately absurd relative to the correct token-based
    /// estimate (`$0.08` for the combined 1M tokens) so the two outcomes
    /// are unmistakable.
    #[tokio::test]
    async fn job_axis_null_safe_sum_never_silently_drops_the_null_cost_rows_from_a_mixed_bucket() {
        let Some(pool) = pg_test_pool(
            "job_axis_null_safe_sum_never_silently_drops_the_null_cost_rows_from_a_mixed_bucket",
        )
        .await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job_with_provider_and_cost(
            &pool,
            None,
            None,
            job_id,
            "openrouter",
            "deepseek/deepseek-v4-flash-0731",
            500_000,
            0,
            Some(Decimal::from(5)),
        )
        .await;
        insert_llm_usage_for_job_with_provider_and_cost(
            &pool,
            None,
            None,
            job_id,
            "openrouter",
            "deepseek/deepseek-v4-flash-0731",
            500_000,
            0,
            None,
        )
        .await;

        let t = consumer_tenant(pool.clone());
        let tick_result = tick(&t, &pool).await.expect("tick succeeds");
        assert_eq!(tick_result, Tick::worked(1));

        let rows = ledger_rows_for_account(&pool, account_id).await;
        assert_eq!(rows.len(), 1);
        let (_job, _credits, cost) = rows[0];
        assert_eq!(
            cost,
            "0.08".parse::<Decimal>().unwrap(),
            "a bucket with ANY null-cost row must fall through to the token-based estimate for \
             the WHOLE bucket (1M total tokens @ $0.08/M) — never silently sum only the \
             non-NULL rows (which would wrongly report $5)"
        );

        cleanup(&pool, account_id, &[]).await;
    }

    /// **Negative control 1a — re-running the SAME window twice must not
    /// double-charge.** Continues from the same shape as the test above:
    /// run `tick` a second time with no new candidate jobs (the cursor
    /// already advanced past both). Ledger contents and the materialized
    /// balance must be byte-for-byte identical to the first run — this is
    /// the plan's own "Done + verified" headline property.
    #[tokio::test]
    async fn running_tick_twice_over_the_same_window_is_idempotent() {
        let Some(pool) =
            pg_test_pool("running_tick_twice_over_the_same_window_is_idempotent").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job(&pool, None, None, job_id, "claude-haiku-4-5", 500_000, 100_000).await;

        let t = consumer_tenant(pool.clone());
        let first = tick(&t, &pool).await.expect("first tick");
        assert_eq!(first, Tick::worked(1));
        let rows_after_first = ledger_rows_for_account(&pool, account_id).await;
        let balance_after_first = balance_for_account(&pool, account_id).await;
        assert_eq!(rows_after_first.len(), 1);

        let second = tick(&t, &pool).await.expect("second tick");
        assert_eq!(second, Tick::idle(), "no new candidates — nothing to charge");
        let rows_after_second = ledger_rows_for_account(&pool, account_id).await;
        let balance_after_second = balance_for_account(&pool, account_id).await;

        assert_eq!(rows_after_first, rows_after_second, "ledger contents must be identical after a re-run");
        assert_eq!(balance_after_first, balance_after_second, "balance must be identical after a re-run");

        cleanup(&pool, account_id, &[]).await;
    }

    #[tokio::test]
    async fn query_revisions_reject_stale_snapshots_and_preserve_cursor() {
        let Some(pool) = pg_test_pool("query_revisions_reject_stale_snapshots_and_preserve_cursor").await else {
            return;
        };
        let account = seed_account(&pool).await;
        let query = Uuid::new_v4();
        let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
        let debit = LedgerDebit {
            funded: None,
            entry_kind: ENTRY_KIND_QUERY,
            cost_usd: Decimal::from(3),
            source_query_id: Some(query),
            source_job_id: None,
            source_receipt_id: None,
            graph_id: None,
            journal_period: None,
            journal_granularity: None,
            attributed_user_id: None,
        };
        let mut read = AccountRead {
            debits: vec![debit],
            new_cursor: AccountCursor { query: Some((now, query)), ..Default::default() },
            ..Default::default()
        };
        assert_eq!(write_account_ledger(&pool, account, &read, Decimal::from(3)).await.unwrap(), 1);
        read.debits[0].cost_usd = Decimal::ONE;
        read.new_cursor.query = Some((now - chrono::Duration::minutes(1), Uuid::nil()));
        assert_eq!(write_account_ledger(&pool, account, &read, Decimal::from(9)).await.unwrap(), 0);
        assert_eq!(load_cursor(&pool, account).await.unwrap().query, Some((now, query)));
        read.debits[0].cost_usd = Decimal::from(6);
        assert_eq!(write_account_ledger(&pool, account, &read, Decimal::from(9)).await.unwrap(), 1);
        assert_eq!(balance_for_account(&pool, account).await.unwrap(), Decimal::from(-2));
        let rows = ledger_rows_for_account(&pool, account).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.iter().map(|r| r.2).sum::<Decimal>(), Decimal::from(6));
        cleanup(&pool, account, &[]).await;
    }

    /// **Negative control 1b — proving the idempotent insert is load-bearing,
    /// not incidental.** Re-runs the exact same `INSERT ... RETURNING
    /// credits` shape [`insert_ledger_row`] uses for the job axis, TWICE,
    /// for the identical `source_job_id` — first through the real
    /// `ON CONFLICT (source_job_id) ... DO NOTHING` guard (exactly one row
    /// results, second call gracefully no-ops), then through a
    /// deliberately-unguarded plain `INSERT` with no conflict clause at all.
    ///
    /// The unguarded half does NOT silently produce a second row — the
    /// unique index itself (`idx_credit_ledger_source_job_id_unique`) is
    /// what makes double-charging structurally impossible, and it enforces
    /// regardless of which INSERT shape violates it. What `ON CONFLICT ...
    /// DO NOTHING` actually buys is different, and just as load-bearing:
    /// **a graceful no-op on a legitimate re-run vs. a hard `23505` error
    /// that would abort the enclosing transaction.** This worker's whole
    /// design (see the module doc's "The cursor" section) DELIBERATELY
    /// re-derives and re-attempts to insert already-charged rows every
    /// tick (the overlap-window policy) — without the guard, EVERY such
    /// re-run would error out and poison the transaction instead of
    /// quietly confirming "already charged, nothing to do here". This test
    /// proves that concretely: the unguarded shape errors on its second
    /// attempt, the guarded shape does not.
    #[tokio::test]
    async fn job_axis_idempotency_guard_is_load_bearing() {
        let Some(pool) = pg_test_pool("job_axis_idempotency_guard_is_load_bearing").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let job_id = Uuid::new_v4();

        // The guarded path, called directly (bypassing tick's cursor so this
        // test isolates the INSERT's own idempotency, not the cursor's).
        let mut tx = begin_account_scoped(&pool, account_id).await.expect("open scope");
        crate::billing::renewal::lock_period_rotation(&mut tx,account_id).await.unwrap();
        let debit = LedgerDebit {
            funded: None,
            entry_kind: ENTRY_KIND_JOB,
            cost_usd: Decimal::from(5),
            source_job_id: Some(job_id),
            source_query_id: None,
            graph_id: None,
            journal_period: None,
            journal_granularity: None,
            source_receipt_id: None,
            attributed_user_id: None,
        };
        let mut planner = SettlementPlanner::new(account_id);
        let first = insert_ledger_row(&mut tx, account_id, &debit, Decimal::from(-1000), Decimal::ONE, &mut planner)
            .await
            .expect("first insert");
        let second = insert_ledger_row(&mut tx, account_id, &debit, Decimal::from(-1000), Decimal::ONE, &mut planner)
            .await
            .expect("second insert (same source_job_id) must NOT error");
        tx.commit().await.expect("commit — the guarded path never poisons the transaction");

        assert!(first.is_some(), "the first insert for a fresh job_id must land");
        assert!(second.is_none(), "the SAME source_job_id must be rejected as a conflict, not inserted again");
        let guarded_count: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .expect("count guarded rows");
        assert_eq!(guarded_count, 1, "exactly one row — the guard did its job");

        // The UNGUARDED shape: a plain INSERT with no ON CONFLICT clause at
        // all, run twice for a second, otherwise-identical job id. The FIRST
        // call succeeds identically to the guarded path; the SECOND call
        // must hard-error (23505, the same unique index) rather than
        // silently continuing — proving the guard above is what turns that
        // hard error into the graceful no-op this worker's re-run design
        // depends on, not an accident of some other check.
        let unguarded_job_id = Uuid::new_v4();
        let mut financial=begin_account_scoped(&pool,account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial,account_id).await.unwrap();
        sqlx::query(
            "INSERT INTO credit_ledger (account_id, entry_kind, credits, source_job_id, cost_usd) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(ENTRY_KIND_JOB)
        .bind(Decimal::from(-1000))
        .bind(unguarded_job_id)
        .bind(Decimal::from(5))
        .execute(&mut *financial)
        .await
        .expect("first unguarded insert succeeds");

        financial.commit().await.unwrap();
        let mut financial=begin_account_scoped(&pool,account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial,account_id).await.unwrap();
        let second_unguarded = sqlx::query(
            "INSERT INTO credit_ledger (account_id, entry_kind, credits, source_job_id, cost_usd) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(ENTRY_KIND_JOB)
        .bind(Decimal::from(-1000))
        .bind(unguarded_job_id)
        .bind(Decimal::from(5))
        .execute(&mut *financial)
        .await;

        financial.rollback().await.unwrap();

        let err = second_unguarded.expect_err(
            "without ON CONFLICT DO NOTHING, re-inserting the SAME job_id must hard-error — this is \
             exactly the outcome the guard exists to avoid on a legitimate re-run",
        );
        let db_err = err.as_database_error().expect("a real Postgres error, not a driver-level one");
        assert_eq!(
            db_err.code().as_deref(),
            Some("23505"),
            "must be the unique_violation on idx_credit_ledger_source_job_id_unique specifically"
        );

        let unguarded_count: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE source_job_id = $1")
            .bind(unguarded_job_id)
            .fetch_one(&pool)
            .await
            .expect("count unguarded rows");
        assert_eq!(unguarded_count, 1, "the failed second insert must not have landed a duplicate row either");

        cleanup(&pool, account_id, &[]).await;
    }

    /// **Negative control 2 (partial) — a read-phase failure must refuse to
    /// write anything, for the ENTIRE tick.** Forces a REAL, live-Postgres
    /// failure via the Org branch's account-resolution step (a
    /// `clerk_org_id` with no `tenant_databases` row — a legitimate failure
    /// mode, not a synthetic one) and confirms: `tick` returns `Err`, and a
    /// PRE-EXISTING, unrelated account's ledger/balance state is completely
    /// untouched by the attempt.
    ///
    /// **Honest scope note**: this proves "the read phase can fail and
    /// nothing is written", the core of the discipline, via the Org
    /// branch's single-account list. It does NOT reproduce "N accounts on
    /// the Consumer branch, one fails, the other N-1 succeed, and NOTHING
    /// is written for ANY of them" end-to-end against a live pool — doing
    /// that faithfully needs infrastructure-level fault injection (e.g. a
    /// second pool closed mid-test) that this suite does not build. Flagged
    /// here rather than silently claimed.
    ///
    /// Cleans up after itself deliberately (drains the bystander's job with
    /// a real, successful Consumer-plane tick at the end): every `pg_tests`
    /// test in this file shares ONE `RELINFER_TEST_DATABASE_URL` database
    /// and `TenantId::Consumer` sweeps EVERY account in it, so an orphaned,
    /// never-charged candidate job left behind by one test would silently
    /// inflate a LATER test's `Tick::worked(n)` count — exactly the kind of
    /// cross-test interference a shared-database suite has to guard against
    /// explicitly rather than assume away.
    #[tokio::test]
    async fn read_failure_refuses_to_write_anything() {
        let Some(pool) = pg_test_pool("read_failure_refuses_to_write_anything").await else {
            return;
        };
        // Unrelated pre-existing account, to prove it is untouched by the
        // failed attempt below. Its job is real and billable (proven by the
        // cleanup tick at the end) — the absence of ledger rows right after
        // the failed org tick is because the failure refused to write
        // anything, not because there was nothing to bill.
        let bystander = seed_account(&pool).await;
        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, bystander, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job(&pool, None, None, job_id, "claude-haiku-4-5", 100_000, 0).await;

        let missing_org = format!("org_does_not_exist_{}", Uuid::new_v4());
        let org_tenant = WorkerTenant {
            tenant: TenantId::Org(missing_org.clone()),
            pool: pool.clone(),
            store: Arc::new(PostgresStore::new(pool.clone())),
        };
        let result = tick(&org_tenant, &pool).await;
        assert!(result.is_err(), "resolving a nonexistent org's account must fail the whole tick");

        // The bystander account's data must be completely untouched — this
        // tick never got far enough to write anything for ANY account.
        let bystander_rows = ledger_rows_for_account(&pool, bystander).await;
        assert!(
            bystander_rows.is_empty(),
            "an unrelated account must see zero ledger rows from a tick that failed before the write phase"
        );
        assert_eq!(
            balance_for_account(&pool, bystander).await,
            None,
            "no credit_balances row should exist for the bystander either — nothing was ever written"
        );

        // Drain + proof the job was real: a genuine Consumer-plane tick
        // (a different tenant entirely from the failing Org one above) DOES
        // charge it. Named `drain_tick`, not `cleanup` — that binding would
        // shadow the `cleanup` helper this test calls below.
        let consumer = consumer_tenant(pool.clone());
        let drain_tick = tick(&consumer, &pool).await.expect("a real Consumer tick must succeed");
        assert!(drain_tick.processed >= 1, "the bystander's job must be billable once a working tick reaches it");
        assert_eq!(ledger_rows_for_account(&pool, bystander).await.len(), 1);

        cleanup(&pool, bystander, &[]).await;
    }

    /// **Negative control 3 — the cursor must not permanently lose a row
    /// that commits "late" with an earlier `finished_at` than one already
    /// consumed.** This is the exact bug this PR's own design work found
    /// and fixed mid-implementation: an EARLIER naive version of
    /// [`advance_cursor`] persisted the cursor at the LAST ROW SEEN rather
    /// than at `cutoff - OVERLAP_WINDOW`, which passes every OTHER test in
    /// this file but fails exactly this one — a cursor test that only moves
    /// forward proves nothing, per the task's own instruction.
    ///
    /// Job A (`finished_at` = 400s ago) is inserted and consumed by tick #1.
    /// Job B (`finished_at` = 405s ago — CHRONOLOGICALLY EARLIER than A) is
    /// only inserted AFTER tick #1 has already run and advanced the cursor —
    /// simulating a transaction that started before A's but committed after
    /// it. Tick #2 must still find and charge B.
    #[tokio::test]
    async fn cursor_picks_up_a_later_committing_but_earlier_finished_job() {
        let Some(pool) =
            pg_test_pool("cursor_picks_up_a_later_committing_but_earlier_finished_job").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let t = consumer_tenant(pool.clone());

        let job_a = Uuid::new_v4();
        insert_done_job(&pool, job_a, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job(&pool, None, None, job_a, "claude-haiku-4-5", 100_000, 0).await;

        tick(&t, &pool).await.expect("first tick");
        // Assert on THIS test's own jobs, never on the tick's global
        // `processed` count. `tick` sweeps every account in the tenant, so a
        // sibling suite's rows inflate that number and the assertion then
        // passes or fails depending on what ran before it — the same shared-
        // state hazard `usage_reconciliation`'s tests hit and fixed with
        // before/after deltas. Scoped counts are pollution-proof by
        // construction.
        assert_eq!(
            ledger_rows_for_account(&pool, account_id).await.len(),
            1,
            "job A charged on the first pass"
        );

        // Job B commits NOW (after tick #1), but its finished_at is EARLIER
        // than job A's — the non-monotonic-commit-order race.
        let job_b = Uuid::new_v4();
        insert_done_job(&pool, job_b, account_id, &format!("extract:{}", Uuid::new_v4()), 405.0).await;
        insert_llm_usage_for_job(&pool, None, None, job_b, "claude-haiku-4-5", 200_000, 0).await;

        tick(&t, &pool).await.expect("second tick");

        // The property under test is proven by the SCOPED assertions below —
        // job B has a ledger row keyed to this test's own account — not by the
        // tick's tenant-wide `processed` count, which sibling suites inflate.
        let rows = ledger_rows_for_account(&pool, account_id).await;
        let job_ids: std::collections::HashSet<Uuid> = rows.iter().filter_map(|(j, _, _)| *j).collect();
        assert!(job_ids.contains(&job_a), "job A must still be charged");
        assert!(job_ids.contains(&job_b), "job B must be charged too — this is the property under test");
        assert_eq!(rows.len(), 2, "exactly two rows total — B must not have been silently dropped OR double-charged");

        cleanup(&pool, account_id, &[]).await;
    }

    /// Journal axis: a `graph_id IS NULL` row must never be billed (the
    /// `credit_ledger_journal_axis_needs_graph` CHECK would reject it
    /// anyway), but the cursor must still make forward progress past it —
    /// see the module doc's "The three axes" section.
    #[tokio::test]
    async fn journal_axis_skips_null_graph_id_but_still_advances() {
        let Some(pool) = pg_test_pool("journal_axis_skips_null_graph_id_but_still_advances").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let period = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 13, 5, 0, 0)
            .single()
            .expect("valid fixed hour");

        // One attributable row (graph_id set) and one unattributable row
        // (graph_id NULL) in the SAME journal period — a plausible shape if
        // a caller ever forgets to thread graph_id through. `created_at` is
        // explicitly backdated past CURSOR_LAG (180s) — the DEFAULT now()
        // this table would otherwise use is too recent to pass the
        // lag-window filter, which would make this test vacuously find
        // nothing rather than actually exercising the skip-vs-bill split.
        sqlx::query(
            "INSERT INTO llm_usage (account_id, graph_id, task, model, input_tokens, output_tokens, \
                                     source_journal_period, source_journal_granularity, created_at) \
             VALUES ($1, $2, 'journal_compile', 'claude-sonnet-4-6', 1000000, 0, $3, 'hour', \
                     now() - INTERVAL '400 seconds')",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(period)
        .execute(&pool)
        .await
        .expect("insert attributable journal row");

        sqlx::query(
            "INSERT INTO llm_usage (account_id, graph_id, task, model, input_tokens, output_tokens, \
                                     source_journal_period, source_journal_granularity, created_at) \
             VALUES ($1, NULL, 'journal_compile', 'claude-sonnet-4-6', 500000, 0, $2, 'hour', \
                     now() - INTERVAL '400 seconds')",
        )
        .bind(account_id)
        .bind(period)
        .execute(&pool)
        .await
        .expect("insert unattributable journal row");

        let t = consumer_tenant(pool.clone());
        let result = tick(&t, &pool).await.expect("tick succeeds");
        assert_eq!(result, Tick::worked(1), "only the attributable row bills");

        let rows = ledger_rows_for_account(&pool, account_id).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].2, Decimal::from(3), "1M input tokens @ $3/M for claude-sonnet-4-6");

        cleanup(&pool, account_id, &[]).await;
    }

    // ── Billing Phase 2a: per-member attribution on the ledger ──────────────

    /// **A ledger debit carries the attributing member.** Two `llm_usage`
    /// rows for the SAME job, different models (so they land in different
    /// `GROUP BY source_job_id, model, attributed_user_id` buckets — see
    /// `read_job_axis`'s own comment on why that column is in the GROUP BY),
    /// both attributed to the SAME member. The resulting ledger row must
    /// carry that member — proving [`resolve_debit_attribution`]'s
    /// "exactly one distinct value" branch is what the real write path
    /// actually takes, not just what the pure unit tests below claim it
    /// does.
    #[tokio::test]
    async fn job_axis_debit_carries_the_attributing_member() {
        let Some(pool) = pg_test_pool("job_axis_debit_carries_the_attributing_member").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let member_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job_attributed(
            &pool, None, None, job_id, "claude-haiku-4-5", 100_000, 0, Some(member_id),
        )
        .await;
        insert_llm_usage_for_job_attributed(
            &pool, None, None, job_id, "claude-sonnet-4-6", 50_000, 0, Some(member_id),
        )
        .await;

        let t = consumer_tenant(pool.clone());
        let result = tick(&t, &pool).await.expect("tick succeeds");
        assert_eq!(result, Tick::worked(1));

        let attribution = ledger_job_attribution(&pool, account_id, job_id)
            .await
            .expect("ledger row must exist");
        assert_eq!(
            attribution,
            Some(member_id),
            "both llm_usage rows agreed on the member — the debit must carry it"
        );

        cleanup(&pool, account_id, &[]).await;
    }

    /// **The QUERY axis carries attribution too.** Added after a negative
    /// control proved the job-axis test alone did not cover it: breaking the
    /// query-axis and journal-axis `attributed_user_id` binds left the whole
    /// suite green, so those two production writes were asserting nothing.
    /// This is the request path — `routes::queries`/`services::ask` — which
    /// is the ONLY axis whose attribution comes from a live gate-verified
    /// caller rather than an atom's `ingested_by`, so a regression here
    /// silently unattributes exactly the interactive spend a per-seat limit
    /// is most expected to govern.
    #[tokio::test]
    async fn query_axis_debit_carries_the_attributing_member() {
        let Some(pool) = pg_test_pool("query_axis_debit_carries_the_attributing_member").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let member_id = Uuid::new_v4();
        let query_id = Uuid::new_v4();
        insert_llm_usage_for_query_attributed(
            &pool,
            Some(account_id),
            query_id,
            "claude-sonnet-4-6",
            80_000,
            2_000,
            Some(member_id),
        )
        .await;

        let t = consumer_tenant(pool.clone());
        tick(&t, &pool).await.expect("tick succeeds");

        let attribution = ledger_query_attribution(&pool, account_id, query_id)
            .await
            .expect("query-axis ledger row must exist");
        assert_eq!(
            attribution,
            Some(member_id),
            "a request-path debit must carry the acting member into the ledger"
        );

        cleanup(&pool, account_id, &[]).await;
    }

    /// **The JOURNAL axis carries attribution too** — the same uncovered
    /// production bind as the query axis above, and the more costly of the
    /// two to get wrong: per migration `0141`'s header the hourly journal
    /// compile is the highest-call-volume LLM site in the product, so an
    /// unattributed journal axis would silently strand the single largest
    /// block of per-member spend.
    #[tokio::test]
    async fn journal_axis_debit_carries_the_attributing_member() {
        let Some(pool) = pg_test_pool("journal_axis_debit_carries_the_attributing_member").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let member_id = Uuid::new_v4();
        let period = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 1, 15, 9, 0, 0)
            .single()
            .expect("valid fixed journal hour");
        insert_llm_usage_for_journal_attributed(
            &pool,
            Some(account_id),
            graph_id,
            period,
            "hour",
            "claude-haiku-4-5",
            30_000,
            1_000,
            Some(member_id),
        )
        .await;

        let t = consumer_tenant(pool.clone());
        tick(&t, &pool).await.expect("tick succeeds");

        let attribution = ledger_journal_attribution(&pool, account_id, graph_id, "hour", period)
            .await
            .expect("journal-axis ledger row must exist");
        assert_eq!(
            attribution,
            Some(member_id),
            "a journal-compile debit must carry the ocean member into the ledger"
        );

        cleanup(&pool, account_id, &[]).await;
    }

    /// **An atom with NULL `ingested_by` produces a NULL-attributed ledger
    /// row without erroring.** This is the everyday case for every job
    /// worker site that has no member in scope (or an atom ingested before
    /// migration `0119`'s Slack backfill) — `insert_llm_usage_for_job`
    /// (unlike the `_attributed` variant above) never sets
    /// `attributed_user_id`, so it defaults NULL exactly like a real
    /// unattributed worker call would. The ledger row must still land
    /// (`credit_ledger.attributed_user_id` is nullable, no CHECK requires
    /// it), `tick` must not error, and the ledger row's attribution must
    /// read back as NULL rather than some fabricated value.
    #[tokio::test]
    async fn null_ingested_by_produces_a_null_attributed_ledger_row_without_erroring() {
        let Some(pool) = pg_test_pool(
            "null_ingested_by_produces_a_null_attributed_ledger_row_without_erroring",
        )
        .await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        // No attribution — the pre-0119 / no-member-in-scope shape.
        insert_llm_usage_for_job(&pool, None, None, job_id, "claude-haiku-4-5", 100_000, 0).await;

        let t = consumer_tenant(pool.clone());
        let result = tick(&t, &pool).await.expect("tick must succeed — an unattributed row is not an error");
        assert_eq!(result, Tick::worked(1));

        let attribution = ledger_job_attribution(&pool, account_id, job_id)
            .await
            .expect("ledger row must exist despite carrying no attribution");
        assert_eq!(attribution, None, "NULL ingested_by must read back as a NULL-attributed ledger row");

        cleanup(&pool, account_id, &[]).await;
    }

    /// **Disagreement policy, proved against the real write path.** Two
    /// `llm_usage` rows for the SAME job attributed to TWO DIFFERENT
    /// members (a shape this PR's own module doc says should not occur in
    /// production for the job axis, but documents as handled defensively
    /// rather than assumed away — see the module doc's "Disagreement
    /// policy" section). The debit must still land (never refuse to write
    /// because of an enrichment field), `tick` must not error, and the
    /// resulting ledger row's attribution must be NULL — not an arbitrary
    /// pick of either member.
    #[tokio::test]
    async fn job_axis_disagreeing_attribution_lands_unattributed_not_erroring() {
        let Some(pool) =
            pg_test_pool("job_axis_disagreeing_attribution_lands_unattributed_not_erroring").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let member_a = Uuid::new_v4();
        let member_b = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job_attributed(
            &pool, None, None, job_id, "claude-haiku-4-5", 100_000, 0, Some(member_a),
        )
        .await;
        insert_llm_usage_for_job_attributed(
            &pool, None, None, job_id, "claude-sonnet-4-6", 50_000, 0, Some(member_b),
        )
        .await;

        let t = consumer_tenant(pool.clone());
        let result = tick(&t, &pool).await.expect("a conflicting attribution must never fail the tick");
        assert_eq!(result, Tick::worked(1), "the debit must still land");

        let attribution = ledger_job_attribution(&pool, account_id, job_id)
            .await
            .expect("ledger row must exist");
        assert_eq!(
            attribution, None,
            "disagreeing members must resolve to NULL, never an arbitrary pick of either one"
        );

        cleanup(&pool, account_id, &[]).await;
    }

    /// **The Phase 1 regression guard: `.with_attributed_user(...)` alone —
    /// the exact shape every Phase 2a job-worker site in this PR uses — must
    /// never also set the RLS scope selector.** `UsageContext::user_id` is
    /// the field `llm::usage::record`/`reserve`/`finalize` match on to choose
    /// `begin_tenant_scoped` (`Some`) vs `begin_account_scoped` (`None`) —
    /// see `llm/usage.rs`'s `record` function, `match user_id { Some(uid) =>
    /// begin_tenant_scoped(...), None => begin_account_scoped(...) }`. This
    /// is a live-Postgres test, not a pure struct-field check in isolation:
    /// it builds the context exactly the way `extract.rs`/
    /// `atom_title_backfill.rs`/etc. now do (`.with_source_job(...)
    /// .with_attributed_user(...)`, no `.with_user(...)` call) and runs it
    /// through the REAL production `record` function, then confirms the row
    /// landed — proving the `None` (`begin_account_scoped`) branch is the one
    /// that actually executed for this shape, not merely that the struct
    /// looks right.
    #[tokio::test]
    async fn attributed_user_alone_never_flips_the_scope_selector() {
        let Some(pool) = pg_test_pool("attributed_user_alone_never_flips_the_scope_selector").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let graph_id = seed_graph(&pool, account_id).await;
        let member_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();

        // The exact builder chain every new Phase 2a job-worker site uses —
        // see e.g. `extract.rs`'s `usage_ctx` construction.
        let ctx = crate::llm::usage::UsageContext::new(pool.clone(), Some(account_id), Some(graph_id))
            .with_source_job(job_id)
            .with_attributed_user(member_id);
        assert_eq!(
            ctx.user_id, None,
            "with_attributed_user must never touch the scope-selector field — \
             see llm/usage.rs's own 'Two roles named user_id' doc section"
        );

        crate::llm::usage::record(
            &pool,
            ctx.account_id,
            ctx.user_id,
            ctx.graph_id,
            "test_attribution_scope_guard",
            "claude-haiku-4-5",
            10,
            5,
            0,
            0,
            ctx.attributed_user_id,
        )
        .await;

        let row: Option<(Option<Uuid>,)> = sqlx::query_as(
            "SELECT attributed_user_id FROM llm_usage \
             WHERE account_id = $1 AND task = 'test_attribution_scope_guard'",
        )
        .bind(account_id)
        .fetch_optional(&pool)
        .await
        .expect("query llm_usage");

        // Delete the probe row BEFORE asserting, for two reasons. It is a
        // deliberately UNTHREADED row (`record` is the legacy path — no
        // source_job_id/source_query_id/journal axis) dated today, and
        // `workers::usage_reconciliation`'s `classify_unthreaded` counts every
        // such row for `created_at::date = today` across the whole tenant, not
        // per account. Left behind, it inflates `unthreaded_rows` for any
        // reconciliation test that later runs against the SAME shared test
        // database — which is exactly how CI runs `mcp-e2e`, and it turned
        // `cached_zero_token_row_is_not_counted_as_an_anomaly` red with
        // `left: 1, right: 0`. Deleting before the assertions (not after) also
        // means a failing assertion cannot leak the row.
        sqlx::query("DELETE FROM llm_usage WHERE account_id = $1 AND task = 'test_attribution_scope_guard'")
            .bind(account_id)
            .execute(&pool)
            .await
            .expect("delete the scope-guard probe row");

        let (attributed,) = row.expect(
            "record must have landed the row via begin_account_scoped (user_id: None) — \
             if this is missing, either record silently dropped the row (a real regression) \
             or user_id was non-None and this assertion needs to change",
        );
        assert_eq!(attributed, Some(member_id), "attribution must still land correctly");

        cleanup(&pool, account_id, &[]).await;
    }

    // ── PR10: enforcement observation on "the debit path" ───────────────────

    async fn enforcement_events_for_account(
        pool: &PgPool,
        account_id: Uuid,
    ) -> Vec<(bool, String)> {
        sqlx::query_as(
            "SELECT would_have_blocked, reason FROM enforcement_events \
             WHERE account_id = $1 ORDER BY created_at",
        )
        .bind(account_id)
        .fetch_all(pool)
        .await
        .expect("fetch enforcement_events")
    }

    /// Takes `&str` ("true"/"false") rather than `bool` for minimal call-site
    /// diff from before the flag moved off `account_prefs` — see
    /// `billing::enforcement`'s module doc's "Not a user preference" section
    /// for why it lives on `accounts` now.
    async fn set_enforcement_pref(pool: &PgPool, account_id: Uuid, value: &str) {
        sqlx::query("UPDATE accounts SET billing_enforcement_enabled = $2 WHERE id = $1")
            .bind(account_id)
            .bind(value == "true")
            .execute(pool)
            .await
            .expect("set billing_enforcement_enabled");
    }

    /// **Negative control 1, end-to-end through the real debit path.** An
    /// account with NO prior grant runs up real usage past what it has (no
    /// `credit_balances` row exists yet, so the very first debit already
    /// lands it at a negative balance), with its `billing_enforcement_enabled`
    /// flag ON. Runs `tick()` — the real call site
    /// `crate::billing::enforcement::check_and_record` is wired into — and
    /// asserts BOTH halves of the plan's own wording: the debit path RECORDS
    /// a `would_have_blocked = true` `enforcement_events` row, AND the
    /// operation (the debit itself: the `credit_ledger` row and the
    /// `credit_balances` write) still completed — `tick` returns
    /// `Tick::worked`, not an error, and the ledger/balance rows are present.
    /// If flipping the flag on ever caused `tick` to fail, skip the write, or
    /// return fewer rows than the flag-off case, that would be exactly the
    /// C2 violation the plan calls out by name.
    #[tokio::test]
    async fn enforcement_flag_on_records_would_have_blocked_but_never_blocks_the_debit() {
        let Some(pool) = pg_test_pool(
            "enforcement_flag_on_records_would_have_blocked_but_never_blocks_the_debit",
        )
        .await
        else {
            return;
        };
        std::env::remove_var(crate::billing::enforcement::KILL_SWITCH_ENV);

        let account_id = seed_account(&pool).await;
        set_enforcement_pref(&pool, account_id, "true").await;

        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        // A real, sizeable cost with no prior grant — balance goes negative
        // on this very first debit.
        insert_llm_usage_for_job(&pool, None, None, job_id, "claude-sonnet-4-6", 2_000_000, 0).await;

        let t = consumer_tenant(pool.clone());
        let result = tick(&t, &pool).await.expect("tick must still succeed with enforcement flag ON");
        assert_eq!(result, Tick::worked(1), "the debit must still be written");

        // The operation was allowed: the ledger row and the negative balance
        // both exist.
        let rows = ledger_rows_for_account(&pool, account_id).await;
        assert_eq!(rows.len(), 1, "the debit must land despite would_have_blocked");
        let balance = balance_for_account(&pool, account_id).await.expect("balance row must exist");
        assert!(balance < Decimal::ZERO, "balance must reflect the real debit: {balance}");

        // The observation was recorded, correctly reflecting the ON flag.
        let events = enforcement_events_for_account(&pool, account_id).await;
        assert_eq!(events.len(), 1, "exactly one zero/negative-balance observation this tick");
        assert!(events[0].0, "would_have_blocked must be true with the flag ON");
        assert_eq!(events[0].1, "usage_debit_zero_or_negative_balance");

        cleanup(&pool, account_id, &[]).await;
    }

    /// The flag-OFF counterpart to the test above, run against the SAME
    /// shape (a fresh account, no prior grant, one debit past zero) so the
    /// only variable is the flag. An `enforcement_events` row is still
    /// written (this module observes regardless of the flag — an operator
    /// needs to see "how many accounts would be affected" even before
    /// turning enforcement on for any of them), but `would_have_blocked` is
    /// `false`. The debit itself is, as always, unaffected either way.
    #[tokio::test]
    async fn enforcement_flag_off_still_records_the_observation_as_would_have_blocked_false() {
        let Some(pool) = pg_test_pool(
            "enforcement_flag_off_still_records_the_observation_as_would_have_blocked_false",
        )
        .await
        else {
            return;
        };
        std::env::remove_var(crate::billing::enforcement::KILL_SWITCH_ENV);

        let account_id = seed_account(&pool).await;
        // No account_prefs row at all — default-OFF by construction.

        let job_id = Uuid::new_v4();
        insert_done_job(&pool, job_id, account_id, &format!("extract:{}", Uuid::new_v4()), 400.0).await;
        insert_llm_usage_for_job(&pool, None, None, job_id, "claude-sonnet-4-6", 2_000_000, 0).await;

        let t = consumer_tenant(pool.clone());
        let result = tick(&t, &pool).await.expect("tick succeeds");
        assert_eq!(result, Tick::worked(1));

        let events = enforcement_events_for_account(&pool, account_id).await;
        assert_eq!(events.len(), 1);
        assert!(!events[0].0, "would_have_blocked must be false with no account_prefs row (default-OFF)");

        cleanup(&pool, account_id, &[]).await;
    }

    /// **Invariant C2, proved directly against the real Store write path
    /// `ingest.rs` uses** (`Store::insert_atoms_scoped`, `store/mod.rs`), not
    /// merely by absence of a balance check in that route (see the PR body
    /// for the structural grep evidence). An account is driven to a NEGATIVE
    /// balance (stronger than exactly zero — if a negative balance can't
    /// block writes, zero certainly can't either), then a real atom insert
    /// through the SAME RLS-scoped primitive the ingest route calls is
    /// attempted and must succeed unconditionally — no code path here even
    /// LOOKS at `credit_balances` before writing.
    #[tokio::test]
    async fn c2_zero_and_negative_balance_account_can_still_ingest() {
        let Some(pool) = pg_test_pool("c2_zero_and_negative_balance_account_can_still_ingest").await
        else {
            return;
        };
        // Seeded directly (not via `seed_account`) so `user_id` stays in
        // scope — `atoms.ingested_by` FKs to `users(id)`.
        let user_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@c2-ingest-pr10.test"))
            .execute(&pool)
            .await
            .expect("insert user");
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("insert account");
        let graph_id = seed_graph(&pool, account_id).await;

        // Drive the account well past zero.
        let mut financial=begin_account_scoped(&pool,account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial,account_id).await.unwrap();
        sqlx::query(
            "INSERT INTO credit_balances (account_id, balance) VALUES ($1, $2)",
        )
        .bind(account_id)
        .bind(Decimal::from(-1_000_000))
        .execute(&mut *financial)
        .await
        .expect("seed a deeply negative balance");

        financial.commit().await.unwrap();

        // Even with enforcement ON for this account, ingestion must still
        // succeed — the flag governs enforcement OBSERVATION only.
        set_enforcement_pref(&pool, account_id, "true").await;

        let store = PostgresStore::new(pool.clone());
        let atom_id = Uuid::new_v4();
        let mut tx = begin_account_scoped(&pool, account_id)
            .await
            .expect("open account-scoped transaction — the same primitive ingest.rs uses");
        // Exact call shape `ingest::submit` uses
        // (`ingest.rs:1327`) — `scope`/`fts_language` defaults match that
        // route's own `unwrap_or("private")`/`unwrap_or("english")`.
        let insert_result = store
            .insert_atoms_scoped(
                &mut tx,
                &[atom_id],
                graph_id,
                "note",
                &[format!("c2-test-{atom_id}")],
                &[format!("c2-test/{atom_id}")],
                &[],
                Utc::now(),
                user_id,
                "private",
                "english",
                &[None],
                &[None],
                &[None],
                &[None],
                None,
            )
            .await;
        assert!(
            insert_result.is_ok(),
            "ingest write must succeed at a negative balance: {insert_result:?}"
        );
        tx.commit().await.expect("commit the ingest write");

        let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&pool)
            .await
            .expect("verify the atom actually landed");
        assert_eq!(stored, 1, "the write must be durable, not merely non-erroring");

        // The atom FKs to both the graph and the user, and `graphs` cascades
        // from `accounts` — so deleting the account clears the atom too, and
        // only then can the user go (#642).
        cleanup(&pool, account_id, &[]).await;
    }

    // ── UNATTRIBUTED_ALERT_WINDOW — the alert's self-limiting bound ────────

    /// Minimal tracing-output capture — same `MakeWriter` technique as
    /// `routes::queries::graph_traversal_shadow_log_emits_the_promised_
    /// delta_fields`. `tick`'s return value (`Tick`) carries no signal at
    /// all about this alerting side-query (it is deliberately best-effort —
    /// see the call site's own comment), so the ONLY way to prove what the
    /// alert did or did not do is to capture what it actually logged.
    #[derive(Clone, Default)]
    struct LogBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for LogBuf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = LogBuf;
        fn make_writer(&'a self) -> LogBuf {
            self.clone()
        }
    }

    /// Run `tick` under a captured subscriber and return `(tick's own
    /// result, everything logged at WARN or above during the call)`. Both
    /// alert-window tests below need the identical capture dance, so it is
    /// factored out here rather than duplicated.
    async fn tick_capturing_logs(t: &WorkerTenant, pool: &PgPool) -> (Result<Tick, String>, String) {
        let buf = LogBuf::default();
        let sub = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        // Same `set_default` (not `with_default`) technique and rationale as
        // the `queries.rs` precedent: this needs to wrap an `.await`, and the
        // thread-local guard stays active across await points on this
        // single-threaded `#[tokio::test]` task.
        let _guard = tracing::subscriber::set_default(sub);
        let result = tick(t, pool).await;
        drop(_guard);
        let logged = String::from_utf8(buf.0.lock().unwrap().clone()).expect("captured log is UTF-8");
        (result, logged)
    }

    /// A `status='done', kind='ExtractEntities', account_id = NULL` job —
    /// the exact shape `UNATTRIBUTED_ALERT_WINDOW` exists to reason about.
    /// Deliberately NOT built on top of [`insert_done_job`] (whose
    /// `account_id` parameter is non-optional): a NULL `account_id` is the
    /// entire subject of this test cluster, not an edge case of that
    /// helper's usual shape. Same `now() - (… * INTERVAL '1 second')`
    /// discipline as `insert_done_job`'s own doc explains, for the same
    /// reason: the window math below is compared against Postgres's own
    /// `now()`, not a Rust-side clock that could disagree with it.
    async fn insert_unattributed_done_job(
        pool: &PgPool,
        id: Uuid,
        idempotency_key: &str,
        finished_at_ago_secs: f64,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, kind, payload, status, idempotency_key, account_id, finished_at, started_at) \
             VALUES ($1, 'ExtractEntities', '{}'::jsonb, 'done', $2, NULL, \
                     now() - ($3 * INTERVAL '1 second'), now() - ($3 * INTERVAL '1 second'))",
        )
        .bind(id)
        .bind(idempotency_key)
        .bind(finished_at_ago_secs)
        .execute(pool)
        .await
        .expect("insert unattributed done job");
    }

    /// Runs `body` (spawned as its own task), then UNCONDITIONALLY deletes
    /// `job_id` from `jobs`, then re-panics with `body`'s original payload —
    /// same panic-safe teardown discipline as `judgement_backfill_pg.rs`'s
    /// `with_cleanup`. Load-bearing here specifically (unlike most of this
    /// file's other pg_tests, whose own header explains fresh-UUID accounts
    /// need no cleanup): the alert query these two tests exercise is
    /// GLOBALLY unscoped by design (no `account_id` filter — see `tick`'s
    /// own comment), so a row left behind by one test is directly visible to
    /// — and corrupts — the very next test's count assertion, whether or not
    /// either test panics.
    async fn with_job_cleanup<Fut>(pool: PgPool, job_id: Uuid, body: Fut)
    where
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let result = tokio::spawn(body).await;
        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .ok();
        if let Err(e) = result {
            std::panic::resume_unwind(e.into_panic());
        }
    }

    /// **The alert must not fire on weeks-old historical residue.**
    ///
    /// Production measured 3,493 rows of exactly this shape (`status=
    /// 'done'`, `account_id IS NULL`, `finished_at` weeks in the past)
    /// tripping `alert = true` on EVERY derivation pass, forever — the
    /// defect `UNATTRIBUTED_ALERT_WINDOW` exists to fix. Seeds one such row
    /// 45 days old (comfortably past both the 24h window and the measured
    /// residue's own age) and asserts the alert line never appears.
    ///
    /// **Observed negative control** (reverting `tick`'s query to drop its
    /// `finished_at >= $2` lower bound — the pre-fix shape): this test FAILS
    /// with
    /// ```text
    /// the alert fired on 45-day-old residue — the window bound is not excluding it:
    /// ... billing derivation: completed ExtractEntities jobs with no account_id — ... count=1 ...
    /// ```
    /// Restoring the bound makes it PASS again. (Observed by hand while
    /// developing this test, against the exact query text this file ships
    /// with today — not re-run as part of every CI pass.)
    #[tokio::test]
    async fn unattributed_alert_ignores_weeks_old_residue() {
        let Some(pool) = pg_test_pool("unattributed_alert_ignores_weeks_old_residue").await else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let job_id = Uuid::new_v4();
        const FORTY_FIVE_DAYS_SECS: f64 = 45.0 * 86_400.0;
        insert_unattributed_done_job(
            &pool,
            job_id,
            &format!("extract:{}", Uuid::new_v4()),
            FORTY_FIVE_DAYS_SECS,
        )
        .await;

        // `with_job_cleanup` takes `pool` BY VALUE and `body_pool` is moved
        // into the body, so teardown needs its own handle (#642).
        let cleanup_pool = pool.clone();
        let body_pool = pool.clone();
        with_job_cleanup(pool, job_id, async move {
            // Sanity: prove the seeded row really is the shape under test —
            // a mistake here would make the rest of this test vacuous.
            let seeded_account_id: Option<Uuid> =
                sqlx::query_scalar("SELECT account_id FROM jobs WHERE id = $1")
                    .bind(job_id)
                    .fetch_one(&body_pool)
                    .await
                    .expect("read back the seeded job");
            assert_eq!(seeded_account_id, None, "sanity: the seeded row must be unattributed");

            let t = consumer_tenant(body_pool.clone());
            let (result, logged) = tick_capturing_logs(&t, &body_pool).await;
            result.expect("tick must succeed even with unattributed residue present");
            assert!(
                !logged.contains("completed ExtractEntities jobs with no account_id"),
                "the alert fired on 45-day-old residue — the window bound is not excluding it:\n{logged}"
            );
        })
        .await;

        cleanup(&cleanup_pool, account_id, &[]).await;
    }

    /// **The complementary negative control**: the alert must still fire for
    /// a genuinely NEW unattributed job — proving the window bound did not
    /// simply silence the alert altogether. Seeds a job finished 10 minutes
    /// ago: safely past `CURSOR_LAG` (180s, so it clears the alert's own
    /// UPPER bound too) and deep inside `UNATTRIBUTED_ALERT_WINDOW` (24h).
    ///
    /// **Observed negative control** (setting the effective window narrower
    /// than 10 minutes, or inverting the new bound to `finished_at <= $2`
    /// instead of `>=`): this test FAILS with
    /// ```text
    /// the alert did not fire for a job that finished only 10 minutes ago:
    /// (empty — no "completed ExtractEntities jobs with no account_id" line at all)
    /// ```
    /// Restoring the correct bound/direction makes it PASS again. (Observed
    /// by hand while developing this test.)
    #[tokio::test]
    async fn unattributed_alert_fires_for_a_recent_unattributed_job() {
        let Some(pool) = pg_test_pool("unattributed_alert_fires_for_a_recent_unattributed_job").await
        else {
            return;
        };
        let account_id = seed_account(&pool).await;
        let job_id = Uuid::new_v4();
        const TEN_MINUTES_SECS: f64 = 600.0;
        insert_unattributed_done_job(&pool, job_id, &format!("extract:{}", Uuid::new_v4()), TEN_MINUTES_SECS)
            .await;

        // `with_job_cleanup` takes `pool` BY VALUE and `body_pool` is moved
        // into the body, so teardown needs its own handle (#642).
        let cleanup_pool = pool.clone();
        let body_pool = pool.clone();
        with_job_cleanup(pool, job_id, async move {
            let t = consumer_tenant(body_pool.clone());
            let (result, logged) = tick_capturing_logs(&t, &body_pool).await;
            result.expect("tick must succeed — the alert is best-effort and must never fail the tick");
            assert!(
                logged.contains("completed ExtractEntities jobs with no account_id"),
                "the alert did not fire for a job that finished only 10 minutes ago:\n{logged}"
            );
            assert!(logged.contains("alert=true"), "missing the alert=true field:\n{logged}");
            assert!(
                logged.contains("count=1"),
                "missing count=1 — expected exactly the one seeded row:\n{logged}"
            );
        })
        .await;

        cleanup(&cleanup_pool, account_id, &[]).await;
    }
}

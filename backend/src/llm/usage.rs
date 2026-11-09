//! Per-call LLM usage / cost observability (audit S5).
//!
//! Every background LLM call (extraction, relationship extraction, entity
//! resolution, reef digest) records the task, model, and token counts into the
//! `llm_usage` table so cost can be attributed per tenant (account / ocean) at
//! scale. Synthesis already logs the equivalent fields to `query_history`; this
//! generalizes the same accounting to the non-interactive jobs.
//!
//! Recording is strictly **best-effort**: a failed insert (or a missing table on
//! an un-migrated DB) logs a warning and returns. It must never propagate an
//! error or fail the originating job.
//!
//! # Two roles named `user_id` — read this before touching either
//!
//! Billing Phase 1 (seats + per-member metering) added a SECOND, unrelated
//! id to this module's vocabulary, and the two are easy to conflate because
//! they share a name on one side of the split:
//!
//! * **[`UsageContext::user_id`] / `record`'s `user_id` parameter** — the
//!   TENANT-PLANE RLS SCOPE SELECTOR. It picks [`crate::tenancy::scope::
//!   begin_tenant_scoped`] vs [`crate::tenancy::scope::begin_account_scoped`]
//!   in `reserve`/`finalize`/`record` below, must be the gate-verified
//!   [`crate::tenancy::runtime::TenantContext::user_id`] — the one user the
//!   request's context is valid for, never a caller-supplied id (see
//!   `begin_tenant_scoped`'s own doc) — and is **NEVER stored** on
//!   `llm_usage` itself. `llm_usage_pending.user_id` (migration `0149`/
//!   `0055`) is this same scope selector, for the same reason, on the
//!   pending-row twin of this table.
//! * **[`UsageContext::attributed_user_id`] / `finalize`'s and `record`'s
//!   `attributed_user_id` parameter** (Billing Phase 1, migration `0157`/
//!   `0057`) — which member the call was FOR, **stored-only** on
//!   `llm_usage.attributed_user_id` and **never read by any `begin_*` scope
//!   function**. Reusing the scope-selector `user_id` for this purpose was
//!   considered and rejected: it would flip roughly two dozen background
//!   worker call sites from `begin_account_scoped` to `begin_tenant_scoped`
//!   on the consumer plane, and would set `app.user_id` — an RLS-relevant
//!   session GUC — from table data (e.g. a journal-compile candidate's
//!   owner) rather than a gate-verified request context. That is a real
//!   scope-widening bug class, not a style preference, which is why
//!   `attributed_user_id` is a wholly separate, independently-settable field
//!   (see [`UsageContext::with_attributed_user`]) rather than a second
//!   meaning layered onto `user_id`.
//!
//! `llm_usage_pending` does NOT get an `attributed_user_id` column — a
//! pending row is reserve-time-only bookkeeping (see the PR4 section below)
//! and is deleted, never read back, by the time `finalize` has an
//! `attributed_user_id` to write; there is nothing for it to attribute.
//!
//! **Populated at:** every request-path call site that already threads a
//! gate-verified acting user (`routes::queries`, `routes::nodes`,
//! `routes::transcript`, `services::ask`) passes the SAME id to both
//! `with_user` and `with_attributed_user` — the acting caller is who the
//! call is billed to. The two background `journal_sweep.rs` compile sites
//! pass `user_id: None` (no request context to scope to) but DO populate
//! `attributed_user_id` from the dirty-hour/dirty-group's own `user_id`
//! field (the ocean owner is already in hand — the hourly compile is
//! per-`(user, graph)` by construction).
//!
//! **Billing Phase 2a** resolved an owner for the job-worker sites that
//! process a specific atom, via [`with_attributed_user_opt`]:
//! the since-retired per-atom extraction workers,
//! `atom_title_backfill` (and `services::atom_title`, which receives the
//! context as a parameter rather than constructing its own), and `summary`
//! (via a chunk-atom lookup on the meeting's `source_id` prefix, since a
//! `meeting_transcripts` row has no `ingested_by` of its own — see that
//! worker's own comment). Each threads `atoms.ingested_by`, which is
//! genuinely `NULL` for every atom ingested before migration `0119`'s Slack
//! backfill and for any ingest path that never threaded it — `None` is a
//! legitimate, expected value at every one of these sites, never a bug to
//! chase down or a reason to fabricate a fallback owner.
//!
//! `entity_resolution`, `conflict_detection`, `infer_expertise`, and
//! `digest` remain `attributed_user_id: None` **by design, not by
//! deferral**: each of these adjudicates or summarizes MULTIPLE atoms /
//! entity pairs in one LLM call (a candidate pair of nodes, a pair of
//! conflicting decisions, a topic's evidence spanning many atoms, an
//! entity's several selected corals), so there is no single atom whose
//! `ingested_by` the call could honestly be billed to — see each worker's
//! own comment at its `usage_ctx` construction site for the specific reason
//! that call has no atom-level attribution to reach for.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::{CompletionRequest, CompletionResponse, LlmError, LlmProvider};

// ── PR4 (usage-metering plan): the reserve/finalize choke point ───────────────
//
// `record` above is the ORIGINAL best-effort write path: call the provider,
// then record actuals, with no visibility into a call that is still
// in-flight and no trace at all of a call whose process dies mid-request.
// That gap is exactly what this train's PR4 exists to close — every
// production call to `LlmProvider::complete`/`complete_via_stream`/
// `stream_complete` (barring `llm/batch.rs`'s Anthropic Batch API, which gets
// its own sibling in PR6 because its reserve and finalize are separated by up
// to ~24h, a genuinely different lifecycle) now goes through one of:
//
// * [`metered_complete`] — wraps [`LlmProvider::complete`].
// * [`metered_complete_via_stream`] — wraps [`super::complete_via_stream`].
// * [`reserve_for_stream`] + [`finalize_for_stream`] — hand-wired around a
//   RAW [`LlmProvider::stream_complete`] call whose caller consumes the
//   stream itself (`synthesis::synthesize_stream`, the interactive Ask SSE
//   path) and so cannot be wrapped by a function-shaped choke point: the
//   actuals only arrive in the terminal `StreamEvent::Final` frame, after
//   the last prose delta is already on the wire, so the reserve happens
//   before `stream_complete` is called and the finalize happens wherever the
//   caller handles that `Final` frame.
//
// # Reserve and finalize are SEPARATE transactions, not one
//
// A pending row written INSIDE the transaction that later finalizes it would
// never be visible to any other connection, and would vanish entirely if the
// process were killed mid-call — exactly the case this PR's own verification
// exists to test. So the sequence is always:
//
// 1. **Reserve** (private `reserve` below): insert a row into
//    `llm_usage_pending`, commit, and release the connection. Durable and
//    visible to any other connection BEFORE the provider is ever called.
// 2. **Call**: the provider round-trip runs holding NO pool connection at
//    all — an LLM call can run 60s or more, and parking a connection across
//    that is a pool-exhaustion hazard independent of the durability bug reserve
//    exists to fix.
// 3. **Finalize** (private `finalize` below): a SECOND, independent
//    transaction — insert the real `llm_usage` row with actuals AND delete
//    the pending row TOGETHER, so the swap is atomic. A crash between "the
//    row landed" and "the pending row is gone" is impossible: either both
//    happened (committed) or neither did (rolled back / the process never
//    got this far), never a state where a call was billed twice or where a
//    completed call left a live-looking pending row.
//
// A pending row is deliberately left behind (never voided or deleted here)
// whenever the process dies before finalize runs — genuinely ambiguous, since
// the database has no way to tell "still running" apart from "died mid-call."
// That is not a bug to work around in this PR — it is the entire reason PR6
// has a sweeper: `kind='sync'` pendings older than ~15 minutes are
// unambiguous orphans (a real call cannot legitimately still be in flight
// that long); `kind='batch'` pendings need a ~25h floor instead, since
// `wait_until_ended` polls for up to 24h.
//
// **Billing-correctness fix (stacked on this PR): a provider `Err` is no
// longer one of those left-behind cases.** `metered_complete`/
// `metered_complete_via_stream` now finalize their `Err` arm too (reserve
// estimate for input, zero output — see each function's own doc), and the
// raw-stream shape (`reserve_for_stream`+`finalize_for_stream`) gets an RAII
// backstop, [`StreamUsageGuard`], that finalizes with whatever the stream
// actually produced before an error OR a caller-future cancellation (a
// dropped/abandoned stream). The pending row is still left behind ONLY when
// there is truly nothing left to run any of that code — a killed process, or
// a guard dropped with no Tokio runtime handle available at all (see
// `StreamUsageGuard`'s own `Drop` doc). See
// `workers::usage_pending_sweep`'s module doc for how much smaller that
// remaining surface is now.
//
// # The `account_id: None` arm is an owner-scoped transaction, not ambient
// (usage-metering PR5)
//
// `reserve`, `finalize`, and `record` (below) all branch on `account_id`.
// `Some(acct)` was always scoped (`begin_account_scoped`/`begin_tenant_scoped`).
// `None` — the shape every ORG TENANT plane worker call passes
// (`JobScope::usage_account_id`/`scope.usage_account_id()` return `None` on
// the Org arm BY DESIGN, one org per tenant DB; see `record`'s own History
// section) — used to be a bare ambient insert (`record`) or a connectionless
// `pool.begin()` with no GUC at all (`finalize`). Both only ever worked
// because the app connects as the tenant tables' OWNER and
// `migrations_tenant/0009_tenant_rls` §7 leaves the owner exempt from
// `ENABLE`-only RLS — a real, silent gap once Phase 3 repoints the tenant DSN
// at the non-`BYPASSRLS` `coralswarm_tenant_app` role: `llm_usage`/
// `llm_usage_pending`'s tenant-plane policy is anchored on `graph_id` via
// `tenant_rls_visible_graph_ids()`, and an unscoped session sees (and can
// therefore write) nothing but the org commons.
//
// PR5 closes it: every `None`-account arm now opens a transaction via
// [`crate::tenancy::scope::begin_tenant_system_scoped_without_account`],
// which sets `app.tenant_actor='system'` — the same reviewed
// service-principal bypass `workers::mcp_task_sweep::org_tick` already uses
// for its own org-tenant-plane tick. A member-private or team ocean's
// `graph_id` is genuinely visible to the write because the GUC is set, not
// because RLS never got the chance to evaluate it. On the CONSUMER plane
// `account_id: None` remains exactly as broken as `record`'s own doc already
// documents (the strict `::uuid` cast on an unset `app.account_id` still
// fails `0083_consumer_rls`) — `app.tenant_actor` has no reader there, so
// this fix changes nothing about that (independently tracked) gap.
//
// # The reserve-step token estimate is never billed
//
// `estimate_reserve_input_tokens` is `request character length / 4`, clamped
// to `[1, RESERVE_TOKEN_ESTIMATE_CEILING]`. It exists ONLY for in-flight
// spend visibility and a future `check_and_debit` gate — finalize ALWAYS
// overwrites with the provider's real, metered token counts on `llm_usage`.
// Cheap and obviously approximate beats a tokenizer dependency for a number
// that never reaches a ledger.
//
// # `provider` — real introspection since the OpenRouter provider port (PR1)
//
// [`LlmProvider::name`] now exists, and every entry point that has a `&dyn
// LlmProvider` in scope (`metered_complete`, `metered_complete_via_stream`)
// records `llm.name()` rather than a hardcoded literal — a call routed
// through the OpenRouter-backed `OpenAiProvider` now genuinely records
// `"openrouter"`, not `"anthropic"`. [`DEFAULT_PROVIDER`] survives as
// `"anthropic"` for one narrower use where no `&dyn LlmProvider` is
// in scope to ask: this module's own `#[cfg(test)]` calls into the
// private `reserve`/`finalize` primitives directly.
// `reserve_for_stream`/`finalize_for_stream`/[`StreamUsageGuard`] take an
// explicit `provider: &str` parameter instead (their callers always have an
// `&dyn LlmProvider` in scope to read `.name()` off of).
pub const PENDING_KIND_SYNC: &str = "sync";

/// Task-name suffix marking an `llm_usage` row finalized from a genuinely
/// ABANDONED stream — [`StreamUsageGuard`]'s `Drop` path, or a
/// `StreamUsageGuard` dropped with no Tokio runtime handle to spawn onto.
/// Reuses the pre-existing `task`-suffix marker convention
/// (the historical `*_cached` rows `workers::billing_derivation`'s unthreaded-classification
/// query already pattern-matches) rather than a new schema column — no
/// migration needed, and every consumer that bills off `llm_usage`
/// (`billing_derivation`'s query/job/journal axes) keys on
/// `source_query_id`/`source_job_id`/the journal triple, never on the literal
/// `task` string, so these rows are billed exactly like a clean completion
/// would have been (correctly — this is real, already-incurred provider
/// spend) while staying auditable/distinguishable from one.
pub const ABANDONED_TASK_SUFFIX: &str = "_abandoned";

/// Same convention as [`ABANDONED_TASK_SUFFIX`], for a row finalized from the
/// `Err` arm of [`metered_complete`], [`metered_complete_via_stream`], or
/// [`StreamUsageGuard::finalize_err`] — the caller is still actively running
/// (unlike the abandoned case) and observed a provider error, which may be a
/// client-side timeout or a 5xx returned AFTER the model already generated.
pub const ERRORED_TASK_SUFFIX: &str = "_errored";

/// See "`provider` is a stopgap constant" above.
#[cfg(test)]
const DEFAULT_PROVIDER: &str = "anthropic";

/// Sanity ceiling for the reserve-step token estimate — generous enough to
/// cover every model's context window this codebase configures today, tight
/// enough that a pathological request can't write an absurd number into
/// `llm_usage_pending.estimated_input_tokens` (never billed, but still read
/// by a human via `/internal/admin` for in-flight visibility).
const RESERVE_TOKEN_ESTIMATE_CEILING: u32 = 200_000;

/// Unified cost-accounting context threaded through every metered LLM call
/// (PR4, replacing `AtomTitleUsageContext` / `EventWindowUsageContext` /
/// `SynthesisUsageContext`, which were three copies of the same shape).
///
/// **The `None`-context skip is preserved, and it still lives at
/// CONSTRUCTION sites, never here.** A caller with no Postgres pool
/// simply never builds a `UsageContext` at all — every
/// construction site follows the existing `pool.clone().map(|p| ...)`
/// pattern (four of five; see [`crate::workers::atom_title_backfill`] for the
/// one exception, which has an unconditional `PgPool`, not an
/// `Option<PgPool>| — and passes `Option<&UsageContext>` down to
/// [`metered_complete`] / [`metered_complete_via_stream`], both of which
/// call the provider directly with NO reserve/finalize at all when given
/// `None`. That mirrors `record`'s own `usage_ctx: Option<&Ctx>` contract
/// exactly — metering is always best-effort and must never gate, delay, or
/// fail the underlying call.
///
/// `account_id`/`user_id` widen from the narrower pre-PR4 structs
/// (`AtomTitleUsageContext`'s non-optional `account_id`;
/// `EventWindowUsageContext`'s non-optional `user_id`) to `Option<Uuid>` —
/// every worker call site now goes through this one type, and workers pass
/// `user_id: None` unconditionally (see `record`'s own doc, "Tenant RLS Phase
/// 2: `user_id`" section, for why that is deliberate rather than a gap).
#[derive(Clone, Debug)]
pub struct UsageContext {
    pub pool: PgPool,
    pub account_id: Option<Uuid>,
    /// The tenant-plane RLS scope selector (mirrors `record`'s own `user_id`
    /// parameter) — NOT a stored-attribution column on `llm_usage` itself.
    pub user_id: Option<Uuid>,
    /// Billing Phase 1: the member this call is FOR, stored on
    /// `llm_usage.attributed_user_id` and NEVER read by any `begin_*` scope
    /// function — see this module's doc, "Two roles named `user_id`" section,
    /// for the full distinction from `user_id` just above. `None` for every
    /// background job worker with no member in scope (deferred to a later
    /// phase — see each call site's own comment).
    pub attributed_user_id: Option<Uuid>,
    pub graph_id: Option<Uuid>,
    /// The `jobs.id` of the job whose execution made this call (PR2's
    /// `llm_usage.source_job_id`). `None` for request-path and journal-compile
    /// calls, which use `source_query_id`/`source_journal_*` instead.
    pub source_job_id: Option<Uuid>,
    /// The `query_history.id` of the HTTP request this call belongs to (PR2's
    /// `llm_usage.source_query_id`) — minted by the handler at request start
    /// (see `routes::queries`'s `spawn_query_history` and its callers), NOT
    /// generated here and NOT read back from a `RETURNING` after the fact
    /// (the row does not exist yet at reserve time).
    pub source_query_id: Option<Uuid>,
    /// The journal compile's own tier boundary (PR2's
    /// `llm_usage.source_journal_period`) — `journal_sweep::compile_hour`'s
    /// `candidate.hour_start` or `compile_tier_group`'s `period_start`.
    pub source_journal_period: Option<DateTime<Utc>>,
    /// `'hour' | 'day' | 'week' | 'month'` (PR2's
    /// `llm_usage.source_journal_granularity`) — required alongside
    /// `source_journal_period` because day/week/month tier boundaries alias
    /// onto hour boundaries; see migration `0141`'s header.
    pub source_journal_granularity: Option<String>,
}

impl UsageContext {
    /// The common case: pool + account attribution + ocean, no unit-of-work
    /// join key yet. Chain `.with_*` below to add one.
    pub fn new(pool: PgPool, account_id: Option<Uuid>, graph_id: Option<Uuid>) -> Self {
        Self {
            pool,
            account_id,
            user_id: None,
            attributed_user_id: None,
            graph_id,
            source_job_id: None,
            source_query_id: None,
            source_journal_period: None,
            source_journal_granularity: None,
        }
    }

    /// The gate-verified acting caller, for request-path callers that must
    /// satisfy tenant RLS Phase 2 (`record`'s own doc, "Request-path callers"
    /// bullet). Never a worker — workers pass `None` unconditionally.
    ///
    /// **This is the RLS scope selector, not attribution.** It never reaches
    /// `llm_usage` as a stored column. Pair it with
    /// [`with_attributed_user`](Self::with_attributed_user) when the same id
    /// is also who the call should be billed to — the two are independent
    /// and one does not imply the other.
    pub fn with_user(mut self, user_id: Uuid) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// Billing Phase 1: record which member this call is FOR —
    /// `llm_usage.attributed_user_id`, stored-only, never read by any
    /// `begin_*` scope function. See this module's doc, "Two roles named
    /// `user_id`" section, for the full distinction from
    /// [`with_user`](Self::with_user). Safe to call with the SAME id passed
    /// to `with_user` (the common request-path case) or with no matching
    /// `with_user` call at all (e.g. a background sweep that knows the
    /// ocean's owner but scopes its own transaction as the tenant system
    /// actor, not that owner).
    pub fn with_attributed_user(mut self, user_id: Uuid) -> Self {
        self.attributed_user_id = Some(user_id);
        self
    }

    /// Billing Phase 2a: [`with_attributed_user`](Self::with_attributed_user),
    /// but for the job-worker call sites (`atom_title_backfill`, `summary`)
    /// whose only source
    /// of a member is `atoms.ingested_by` — a column that is genuinely
    /// `NULL` for every atom ingested before migration `0119`'s Slack
    /// backfill, and for any ingest path that never threaded it at all. A
    /// `None` here leaves `attributed_user_id` unset (identical to never
    /// calling `with_attributed_user` at all), rather than forcing every
    /// call site to write its own `if let Some(id) = ingested_by { ctx =
    /// ctx.with_attributed_user(id); }`. Same "stored-only, never a scope
    /// selector" contract as `with_attributed_user` — this never touches
    /// `user_id`.
    pub fn with_attributed_user_opt(mut self, user_id: Option<Uuid>) -> Self {
        self.attributed_user_id = user_id;
        self
    }

    pub fn with_source_job(mut self, job_id: Uuid) -> Self {
        self.source_job_id = Some(job_id);
        self
    }

    pub fn with_source_query(mut self, query_id: Uuid) -> Self {
        self.source_query_id = Some(query_id);
        self
    }

    pub fn with_journal(mut self, period: DateTime<Utc>, granularity: impl Into<String>) -> Self {
        self.source_journal_period = Some(period);
        self.source_journal_granularity = Some(granularity.into());
        self
    }
}

/// `request character length / 4`, clamped to `[1, RESERVE_TOKEN_ESTIMATE_CEILING]`
/// — see this module's "reserve-step token estimate" doc section above.
fn estimate_reserve_input_tokens(req: &CompletionRequest) -> i32 {
    let mut chars: usize = req.system.as_deref().map(str::len).unwrap_or(0);
    for m in &req.messages {
        chars = chars.saturating_add(m.content.len());
    }
    let tokens = ((chars / 4).max(1) as u32).min(RESERVE_TOKEN_ESTIMATE_CEILING);
    tokens as i32
}

/// The OUTPUT-side counterpart of [`estimate_reserve_input_tokens`]'s `chars /
/// 4` heuristic — used ONLY by [`StreamUsageGuard`] as the fallback source of
/// truth when a stream is abandoned/errored before the provider's own
/// `message_delta`-reported `output_tokens` ever arrives (see that struct's
/// doc, "Token sourcing" section, for why that is the common case for an
/// interruption rather than the rare one: this codebase's `StreamEvent` only
/// ever surfaces a provider-reported count on the terminal `Final` frame).
///
/// Deliberately has NO `.max(1)` floor when `chars == 0`, unlike the
/// input-side estimate — a stream that produced no deltas at all genuinely
/// cost zero output tokens, and honestly saying so (rather than a fabricated
/// floor of 1) is exactly what this billing-correctness fix is for.
fn estimate_output_tokens_from_chars(chars: usize) -> u32 {
    if chars == 0 {
        return 0;
    }
    ((chars / 4).max(1) as u32).min(RESERVE_TOKEN_ESTIMATE_CEILING)
}

/// Insert a durable, visible-to-other-connections `llm_usage_pending` row and
/// commit — see this module's "Reserve and finalize are SEPARATE
/// transactions" doc section. Best-effort: `None` on any failure, exactly
/// like `record`'s own error handling (never propagated, never blocks the
/// caller from making the LLM call anyway).
///
/// RLS scoping mirrors `record` exactly (`llm_usage_pending` carries the same
/// `account_isolation` / `member_ocean_isolation` policies as `llm_usage` —
/// migration `0149_llm_usage_pending`/`0055_llm_usage_pending`), because a
/// pending row must satisfy the SAME predicate the `llm_usage` row it
/// finalizes into will.
#[allow(clippy::too_many_arguments)]
async fn reserve(
    pool: &PgPool,
    account_id: Option<Uuid>,
    user_id: Option<Uuid>,
    graph_id: Option<Uuid>,
    task: &str,
    provider: &str,
    model: &str,
    estimated_input_tokens: i32,
    kind: &str,
) -> Option<Uuid> {
    let id = Uuid::new_v4();
    let insert = sqlx::query(
        r#"INSERT INTO llm_usage_pending
               (id, account_id, user_id, graph_id, task, provider, model,
                estimated_input_tokens, kind)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
    )
    .bind(id)
    .bind(account_id)
    .bind(user_id)
    .bind(graph_id)
    .bind(task)
    .bind(provider)
    .bind(model)
    .bind(estimated_input_tokens)
    .bind(kind);

    let res = match account_id {
        Some(acct) => {
            let scoped_tx = match user_id {
                Some(uid) => crate::tenancy::scope::begin_tenant_scoped(pool, acct, uid).await,
                None => crate::tenancy::scope::begin_account_scoped(pool, acct).await,
            };
            match scoped_tx {
                Ok(mut tx) => match insert.execute(&mut *tx).await {
                    Ok(_) => tx.commit().await,
                    Err(e) => Err(e),
                },
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        task,
                        model,
                        "llm_usage_pending: failed to open account-scoped tx (continuing — \
                         reserve is best-effort)"
                    );
                    return None;
                }
            }
        }
        // ORG TENANT plane worker sites (`JobScope::usage_account_id`/
        // `scope.usage_account_id()` returning `None` on the Org arm BY
        // DESIGN — one org per tenant DB; see `record`'s own History
        // section). Usage-metering PR5's retrofit: an owner-scoped
        // transaction with `app.tenant_actor='system'` set, exactly like
        // `workers::mcp_task_sweep::org_tick`'s live precedent — NOT the bare
        // ambient autocommit insert this used to be.
        // `llm_usage_pending`'s tenant-plane policy (migration `0055`) is
        // anchored on `graph_id` via `tenant_rls_visible_graph_ids()`, the
        // SAME predicate `llm_usage` itself carries, so the system actor is
        // required to see (and therefore write) a call scoped to a
        // member-private or team ocean — an unscoped session denies
        // everything but the org commons (0009 §3's fail-closed default).
        // On the CONSUMER plane this `None` case is already a bug
        // independent of this arm (see `record`'s own doc's "None" bullet):
        // the insert still fails the `::uuid` cast on an unset
        // `app.account_id`, just now inside a transaction instead of
        // autocommit — same observable outcome (warn, dropped row), because
        // `app.tenant_actor` has no reader on the consumer plane's policy.
        None => match crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await
        {
            Ok(mut tx) => match insert.execute(&mut *tx).await {
                Ok(_) => tx.commit().await,
                Err(e) => Err(e),
            },
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    task,
                    model,
                    "llm_usage_pending: failed to open tenant-system-scoped tx (continuing — \
                     reserve is best-effort)"
                );
                return None;
            }
        },
    };

    match res {
        Ok(()) => Some(id),
        Err(e) => {
            tracing::warn!(
                error = %e,
                task,
                model,
                "llm_usage_pending: failed to reserve (continuing — the LLM call proceeds \
                 unmetered-in-flight; finalize below still records the completed call)"
            );
            None
        }
    }
}

/// Insert the real `llm_usage` row with actuals and delete the reserved
/// pending row TOGETHER, in one transaction — see this module's "Reserve and
/// finalize are SEPARATE transactions" doc section for why this is a SECOND
/// transaction from [`reserve`], never the same one.
///
/// `pending_id: None` (reserve itself failed, or this call has no reserve at
/// all) still records the `llm_usage` row — finalize's usage accounting must
/// not be held hostage to reserve's own best-effort failure. There is nothing
/// to anchor exactly-once on in this case; see the next paragraph.
///
/// # Exactly-once (billing-correctness fix)
///
/// A `pending_id` can now be finalized from TWO independent code paths racing
/// for the SAME row — an ordinary terminal-event finalize and
/// [`StreamUsageGuard`]'s `Drop`-triggered backstop can both be in flight for
/// the same call (e.g. the terminal `Final` frame arrives at the same moment
/// the caller's future is being cancelled for an unrelated reason). This
/// function is where that is made safe, not the callers: the pending-row
/// DELETE now runs FIRST, inside the same SAVEPOINT discipline described
/// below, and its rows-affected count gates the `llm_usage` INSERT —
/// whichever call's DELETE actually removes the row is the one that gets to
/// write `llm_usage`; a second, racing call for the same `pending_id` sees
/// ZERO rows deleted (already gone) and skips the INSERT entirely, so the
/// same call is never billed twice. Concurrency-safe by construction: a
/// `DELETE ... WHERE id = $1` takes a row lock, so a second, truly concurrent
/// finalize call blocks until the first commits (then sees zero matching
/// rows) or rolls back (then proceeds normally, since the row is still
/// there).
///
/// If the `llm_usage` insert ultimately fails, the WHOLE transaction —
/// including the delete that may have already run above — is never
/// committed (this function returns without calling `tx.commit()`, and an
/// uncommitted `sqlx::Transaction` rolls back on drop), so the pending row
/// survives intact for PR6's sweeper exactly as before this reordering; see
/// `pg_tests::finalize_failure_leaves_the_pending_row_intact`. If the insert
/// succeeds but the pending-row delete failed for an unrelated reason (not
/// "already deleted" — a real error, e.g. a trigger), the SAVEPOINT rollback
/// below still lets the `llm_usage` insert commit (never silently drops
/// billing data over a failed bookkeeping cleanup) and leaves the row to age
/// into a false-positive orphan the sweeper reaps later — a wasted row, not a
/// double charge.
#[allow(clippy::too_many_arguments)]
async fn finalize(
    pool: &PgPool,
    pending_id: Option<Uuid>,
    account_id: Option<Uuid>,
    user_id: Option<Uuid>,
    graph_id: Option<Uuid>,
    task: &str,
    provider: &str,
    model: &str,
    input_tokens: u32,
    output_tokens: u32,
    cache_creation_input_tokens: u32,
    cache_read_input_tokens: u32,
    cost_usd: Option<f64>,
    source_job_id: Option<Uuid>,
    source_query_id: Option<Uuid>,
    source_journal_period: Option<DateTime<Utc>>,
    source_journal_granularity: Option<&str>,
    // Billing Phase 1: STORED attribution only — see `UsageContext::
    // attributed_user_id`'s own doc. Deliberately threaded as its own
    // parameter, never derived from `user_id` above: `user_id` is the RLS
    // scope selector this function's transaction-opening `match account_id`
    // below branches on, and the two must stay independently settable (a
    // background sweep passes `user_id: None` — no request context to scope
    // to — while still knowing, and attributing to, the ocean's owner).
    attributed_user_id: Option<Uuid>,
    // OpenRouter retry fix (migration `0202`/`0075`): the ACTUAL upstream
    // inference host (OpenRouter's own `provider` field — see
    // `openai::OpenAiResponse::provider`'s doc) this call was served — or
    // last attempted to be served — by. Read off the [`crate::llm::
    // UPSTREAM_PROVIDER`] task-local by [`metered_complete`]/
    // [`metered_complete_via_stream`] for BOTH their Ok and Err arms, so a
    // failed, exhausted-retry row is attributable to a host, not just a
    // success. `None` for every non-OpenRouter provider (Anthropic
    // — this concept doesn't exist for them) and for the raw-stream shape's
    // `finalize_ok`/`finalize_err` (`StreamUsageGuard`) and
    // `finalize_for_stream`, which do not (yet) wrap
    // their own call in that scope — a deliberate, documented gap, not an
    // oversight; see each of those functions' own doc. NEVER the same thing
    // as `provider` above, which stays the constant `llm.name()` string
    // (`"openrouter"`/`"anthropic"`/…) rate lookups key on.
    upstream_provider: Option<&str>,
) {
    // The served model and upstream host are the provider's own strings,
    // bound as `text` (`pg_text`).
    let model = coralswarm_core::pg_text::sanitize(model);
    let model = model.as_ref();
    let upstream_provider = coralswarm_core::pg_text::sanitize_opt(upstream_provider);
    let upstream_provider = upstream_provider.as_deref();
    let scoped_tx = match account_id {
        Some(acct) => match user_id {
            Some(uid) => crate::tenancy::scope::begin_tenant_scoped(pool, acct, uid).await,
            None => crate::tenancy::scope::begin_account_scoped(pool, acct).await,
        },
        // Usage-metering PR5's retrofit — see `reserve`'s matching `None` arm
        // above for the full rationale (owner-scoped
        // `app.tenant_actor='system'` transaction, not a bare `pool.begin()`
        // with no GUC). This is the highest-value single fix in the
        // retrofit: EVERY metered worker call (`entity_resolution`,
        // `summary`, `journal_sweep` ×2, `conflict_detection`, `digest`, …)
        // now flows through `metered_complete`/`metered_complete_via_stream`
        // and therefore through this arm on the Org tenant plane — finalize's
        // own atomicity requirement (insert + pending-row delete together)
        // always needed a real transaction, it just wasn't a SCOPED one
        // until now.
        None => crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await,
    };

    let mut tx = match scoped_tx {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(
                error = %e,
                task,
                model,
                pending_id = ?pending_id,
                "llm_usage: failed to open finalize tx (continuing — accounting is best-effort; \
                 the pending row, if any, is left for PR6's sweeper rather than voided here)"
            );
            return;
        }
    };

    // ── Exactly-once anchor: delete FIRST, gate the insert on it ───────────
    // See this function's own "Exactly-once" doc section above. `proceed`
    // stays `true` when there is nothing to anchor on (`pending_id: None`)
    // or the delete found (and removed) exactly one row; it flips to `false`
    // ONLY when the delete found zero rows — i.e. a racing finalize already
    // claimed this `pending_id`.
    let mut proceed = true;
    if let Some(id) = pending_id {
        // SAVEPOINT, per the discipline documented at `routes/ingest.rs:472`.
        // Postgres aborts the WHOLE transaction on ANY statement error (25P02
        // `in_failed_sql_transaction`), so a bare `if let Err(e) = ... {
        // warn!(...) }` here would poison the transaction and take the
        // INSERT below (and the final commit) down with it. Confining the
        // delete to a savepoint is what actually makes "an unrelated delete
        // failure still lets the llm_usage row commit" true rather than
        // merely intended.
        let sp_ok = sqlx::query("SAVEPOINT finalize_pending_delete")
            .execute(&mut *tx)
            .await
            .is_ok();
        match sqlx::query("DELETE FROM llm_usage_pending WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
        {
            Ok(res) if res.rows_affected() == 0 => {
                // Already gone: a racing finalize for this EXACT pending_id
                // got there first (see "Exactly-once" above) — or a sweeper
                // reap raced it (`workers::usage_pending_sweep`). Either way,
                // skip the INSERT: this is the exactly-once guarantee firing
                // correctly, not a failure.
                if sp_ok {
                    let _ = sqlx::query("RELEASE SAVEPOINT finalize_pending_delete")
                        .execute(&mut *tx)
                        .await;
                }
                tracing::debug!(
                    task,
                    model,
                    pending_id = %id,
                    "llm_usage: pending row already gone at finalize — skipping a duplicate \
                     llm_usage insert (exactly-once)"
                );
                proceed = false;
            }
            Ok(_) => {
                // Exactly one row deleted — this call is the first (and, by
                // the row-lock argument above, only) finalize to reach this
                // pending_id.
                if sp_ok {
                    let _ = sqlx::query("RELEASE SAVEPOINT finalize_pending_delete")
                        .execute(&mut *tx)
                        .await;
                }
            }
            Err(e) => {
                // The delete itself failed for an UNRELATED reason (e.g. a
                // trigger) — roll back to the savepoint and still record the
                // usage row below; same "a failed pending-row delete must not
                // cost us the llm_usage row" contract this function has
                // always had.
                tracing::warn!(
                    error = %e,
                    task,
                    model,
                    pending_id = %id,
                    savepoint_established = sp_ok,
                    "llm_usage_pending: failed to delete pending row before finalize — rolling \
                     back to the savepoint so the llm_usage row below still commits; this \
                     pending row will age into a false-positive orphan for the sweeper"
                );
                if sp_ok {
                    let _ = sqlx::query("ROLLBACK TO SAVEPOINT finalize_pending_delete")
                        .execute(&mut *tx)
                        .await;
                } else {
                    // No savepoint to retreat to: the transaction is already
                    // poisoned and the insert/commit below will fail too, so
                    // the usage row IS lost here. Safe direction (the pending
                    // row survives for the sweeper) but worth saying out loud
                    // rather than implying the commit will succeed.
                    tracing::warn!(
                        task,
                        model,
                        pending_id = %id,
                        "llm_usage: no savepoint was established, so the finalize transaction is \
                         poisoned and the llm_usage row will NOT commit"
                    );
                }
            }
        }
    }

    if !proceed {
        if let Err(e) = tx.commit().await {
            tracing::warn!(
                error = %e,
                task,
                model,
                "llm_usage: failed to commit a no-op (already-finalized) finalize (continuing — \
                 accounting is best-effort)"
            );
        }
        return;
    }

    let insert_res = sqlx::query(
        r#"INSERT INTO llm_usage
               (account_id, graph_id, task, model, input_tokens, output_tokens,
                cache_creation_input_tokens, cache_read_input_tokens, provider, cost_usd,
                source_job_id, source_query_id, source_journal_period, source_journal_granularity,
                attributed_user_id, upstream_provider, diagnostics)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"#,
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(task)
    .bind(model)
    // Same INTEGER/BIGINT split as `record` above — see its own bind comments.
    .bind(input_tokens as i32)
    .bind(output_tokens as i32)
    .bind(cache_creation_input_tokens as i64)
    .bind(cache_read_input_tokens as i64)
    .bind(provider)
    // `cost_usd` is NUMERIC (migration 0141). Binding `Option<f64>` here on
    // the WRITE side is fine — Postgres applies the assignment cast from the
    // parameter's FLOAT8 wire type to the column's NUMERIC automatically —
    // but a naive `Option<f64>` on the READ side (a bare `SELECT cost_usd`
    // decoded via `query_as`) hard-errors: sqlx's runtime decode path checks
    // the returned wire type OID and refuses NUMERIC→f64 even though the
    // value would decode fine numerically. Verified against this module's
    // own `pg_tests::metered_complete_success_writes_llm_usage_and_deletes_pending`,
    // which had to add an explicit `cost_usd::float8` cast to its SELECT to
    // pass. Future readers of this column (PR9's derivation worker) must do
    // the same.
    .bind(cost_usd)
    .bind(source_job_id)
    .bind(source_query_id)
    .bind(source_journal_period)
    .bind(source_journal_granularity)
    // Billing Phase 1 (migration 0157/0057): STORED attribution, distinct
    // from `user_id` above — see this function's own parameter doc.
    .bind(attributed_user_id)
    // OpenRouter retry fix (migration 0202/0075) — see this function's own
    // parameter doc. Never the same value as `provider` above.
    .bind(upstream_provider)
    .bind(super::current_diagnostics().map(sqlx::types::Json))
    .execute(&mut *tx)
    .await;

    if let Err(e) = insert_res {
        tracing::warn!(
            error = %e,
            task,
            model,
            pending_id = ?pending_id,
            "llm_usage: failed to record finalized LLM usage (continuing — the WHOLE transaction, \
             including the pending-row delete above, is left uncommitted and rolls back on drop, \
             so the pending row — if any — survives intact for PR6's sweeper rather than \
             silently vanishing here)"
        );
        return;
    }

    if let Err(e) = tx.commit().await {
        tracing::warn!(
            error = %e,
            task,
            model,
            "llm_usage: failed to commit finalize (continuing — accounting is best-effort)"
        );
    }
}

fn finish_call_diagnostics(result: &Result<CompletionResponse, LlmError>, elapsed_ms: u64) {
    let _ = super::CALL_DIAGNOSTICS.try_with(|cell| {
        let mut diagnostics = cell.borrow_mut();
        diagnostics.elapsed_ms = Some(elapsed_ms);
        if let Err(error) = result {
            let (class, status) = super::error_metadata(error);
            diagnostics.error_class = Some(class);
            diagnostics.http_status = status;
            diagnostics.completion_reason.get_or_insert("provider_error");
        } else {
            diagnostics.completion_reason = Some("success");
        }
    });
}

// Log only typed metadata: error Display/Debug strings can contain response
// bodies, prompts, or credentials. Status 0 represents transport failure,
// not an HTTP response; Auth/ModelNotFound do not preserve an exact status.
fn log_completion_error(
    error: &LlmError,
    ctx: Option<&UsageContext>,
    task: &str,
    provider: &str,
    model: &str,
    elapsed_ms: u64,
) {
    let (error_class, status) = super::error_metadata(error);
    let diagnostics = super::current_diagnostics().unwrap_or_default();
    let source_query_id = ctx
        .and_then(|ctx| ctx.source_query_id)
        .map(|id| id.to_string());
    tracing::warn!(
        error_class,
        status,
        phase = diagnostics.phase,
        attempt_count = diagnostics.attempt_count,
        completion_reason = diagnostics.completion_reason,
        source_query_id = source_query_id.as_deref(),
        task,
        operation = "complete",
        provider,
        model,
        elapsed_ms,
        "llm provider call failed"
    );
}

/// The `metered_complete()` choke point (usage-metering plan PR4): reserve,
/// call [`LlmProvider::complete`] holding NO pool connection, finalize on
/// success. `ctx: None` (no pool — see [`UsageContext`]'s doc) skips
/// reserve/finalize entirely and calls the provider directly, unchanged from
/// every pre-PR4 call site's behavior in that case.
///
/// **Billing-correctness fix:** a provider `Err` — including a client-side
/// `LLM_TIMEOUT_SECS` expiry after the model already generated, or a 5xx
/// returned post-generation — used to record nothing at all here (the pending
/// row was silently left for PR6's sweeper). It now finalizes too: `complete()`
/// is not incremental, so there is no partial response to recover on error —
/// this records the reserve-time input ESTIMATE (the only number available)
/// and zero output tokens (honest: a call that failed before this function
/// can observe anything legitimately either generated nothing, or generated
/// something this level has no way to recover — see [`StreamUsageGuard`] for
/// the one call shape, the raw interactive-Ask stream, that CAN recover a
/// partial output count). Tagged [`ERRORED_TASK_SUFFIX`] so it is
/// distinguishable from a clean completion.
pub async fn metered_complete(
    llm: &dyn LlmProvider,
    req: CompletionRequest,
    ctx: Option<&UsageContext>,
    task: &str,
) -> Result<CompletionResponse, LlmError> {
    let diagnostics = super::CallDiagnostics {
        phase: crate::decision::current_ask_phase(),
        ..Default::default()
    };
    super::CALL_DIAGNOSTICS.scope(std::cell::RefCell::new(diagnostics),
        metered_complete_inner(llm, req, ctx, task, std::future::ready(true), |req| llm.complete(req))).await
}

pub const ADMISSION_WITHHELD: &str = "scoped input admission changed";

/// The fresh admission future is polled after asynchronous reservation and
/// immediately before dispatch. A withheld call never reaches the provider and
/// finalizes its reservation with known zero usage/cost, distinct from an
/// upstream error whose input usage can only be estimated.
pub async fn metered_complete_admitted(
    llm: &dyn LlmProvider, req: CompletionRequest, ctx: Option<&UsageContext>,
    task: &str, admission: impl std::future::Future<Output=bool> + Send,
) -> Result<CompletionResponse,LlmError> {
    let diagnostics=super::CallDiagnostics{phase:crate::decision::current_ask_phase(),..Default::default()};
    super::CALL_DIAGNOSTICS.scope(std::cell::RefCell::new(diagnostics),metered_complete_inner(llm,req,ctx,task,admission,|req| llm.complete(req))).await
}

/// A managed (background) call (COR-21). It is refused, with no usage row and
/// no send, when `llm` exposes no gated transport or the managed gate's pure
/// checks fail. Otherwise every HTTP attempt reserves on the managed ledger
/// before it is sent and settles after it. The row this writes is never
/// billed; the call's receipts are (COR-88).
///
/// With `aside`, the gate opens as a second logical call on the same window ([`crate::managed::ManagedGate::open_aside`]):
/// same purpose, unit, per-version cap and fresh fence, its own receipts.
/// Source extraction uses it to retry a standing window on a smaller span,
/// so the retry never reuses the pinned call's attempt numbers and every
/// attempt of both calls is reserved and settled exactly once.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn metered_complete_managed<T: Send>(
    llm: &dyn LlmProvider,
    req: CompletionRequest,
    usage: &UsageContext,
    task: &str,
    call: crate::managed::ManagedCall<'_>,
    aside: bool,
    validate: impl FnOnce(&CompletionResponse) -> T + Send,
) -> Result<(CompletionResponse, T), LlmError> {
    let Some(gated) = llm.gated() else {
        return Err(LlmError::Managed(super::Deny::Refuse(super::RefuseCode::Ungated)));
    };
    let gate = if aside {
        crate::managed::ManagedGate::open_aside(call, &req).await
    } else {
        crate::managed::ManagedGate::open(call, &req).await
    }
    .map_err(LlmError::Managed)?;
    let diagnostics = super::CallDiagnostics {
        phase: crate::decision::current_ask_phase(),
        ..Default::default()
    };
    let mut validation = None;
    let validation_slot = &mut validation;
    let usage = managed_usage_context(usage);
    let result = super::CALL_DIAGNOSTICS
        .scope(
            std::cell::RefCell::new(diagnostics),
            metered_complete_inner(llm, req, Some(&usage), task, std::future::ready(true), |req| async move {
                let response = gated.complete_gated(req, &gate).await?;
                *validation_slot = Some(validate(&response));
                Ok(response)
            }),
        )
        .await;
    result.map(|response| (response, validation.expect("successful dispatch validates its response")))
}

/// A managed call that is a second logical call on a standing window
/// (`ManagedGate::open_aside`): Jev's extraction-time binding question.
/// Same reserve/settle contract as [`metered_complete_managed`].
pub(crate) async fn metered_complete_managed_aside(
    llm: &dyn LlmProvider,
    req: CompletionRequest,
    usage: &UsageContext,
    task: &str,
    call: crate::managed::ManagedCall<'_>,
) -> Result<CompletionResponse, LlmError> {
    let Some(gated) = llm.gated() else {
        return Err(LlmError::Managed(super::Deny::Refuse(super::RefuseCode::Ungated)));
    };
    let gate = crate::managed::ManagedGate::open_aside(call, &req)
        .await
        .map_err(LlmError::Managed)?;
    let diagnostics = super::CallDiagnostics {
        phase: crate::decision::current_ask_phase(),
        ..Default::default()
    };
    let usage = managed_usage_context(usage);
    super::CALL_DIAGNOSTICS
        .scope(
            std::cell::RefCell::new(diagnostics),
            metered_complete_inner(llm, req, Some(&usage), task, std::future::ready(true), |req| async move {
                gated.complete_gated(req, &gate).await
            }),
        )
        .await
}

/// The usage row of a managed call keeps its job and member attribution but
/// never a query or journal key: billing derivation's query and journal axes
/// select rows by those keys, and a managed call is billed from its receipts
/// only. Without this, a journal or Ask consumer moved onto a managed purpose
/// would be charged twice.
fn managed_usage_context(usage: &UsageContext) -> UsageContext {
    let mut usage = usage.clone();
    usage.source_query_id = None;
    usage.source_journal_period = None;
    usage.source_journal_granularity = None;
    usage
}

/// COR-89(b): a retried call's earlier attempts may have been billed by the
/// provider. Their reported cost folds into the call's one usage row. An
/// attempt with no reported cost is not priced here (void, never fabricate).
fn fold_attempt_costs(
    result: &Result<CompletionResponse, LlmError>,
    log: &[coralswarm_core::send_gate::AttemptRecord],
) -> Option<f64> {
    let reported = |records: &[coralswarm_core::send_gate::AttemptRecord]| {
        records
            .iter()
            .filter_map(|r| r.outcome.reported_cost_usd())
            .fold(None, |acc: Option<f64>, c| Some(acc.unwrap_or(0.0) + c))
    };
    match result {
        Ok(resp) => {
            let earlier = &log[..log.len().saturating_sub(1)];
            resp.cost_usd
                .map(|cost| cost + reported(earlier).unwrap_or(0.0))
        }
        Err(_) => reported(log),
    }
}

async fn metered_complete_inner<D, F>(
    llm: &dyn LlmProvider,
    req: CompletionRequest,
    ctx: Option<&UsageContext>,
    task: &str,
    admission: impl std::future::Future<Output=bool> + Send,
    dispatch: D,
) -> Result<CompletionResponse, LlmError>
where
    D: FnOnce(CompletionRequest) -> F,
    F: std::future::Future<Output = Result<CompletionResponse, LlmError>> + Send,
{
    // An errored call has no response carrying a served model.
    let requested_model = req.model.clone();
    let Some(ctx) = ctx else {
        if !admission.await {return Err(LlmError::Auth(ADMISSION_WITHHELD.into()));}
        let started = std::time::Instant::now();
        let result = dispatch(req).await;
        finish_call_diagnostics(&result, started.elapsed().as_millis() as u64);
        if let Err(error) = &result {
            log_completion_error(
                error,
                None,
                task,
                llm.name(),
                &requested_model,
                started.elapsed().as_millis() as u64,
            );
        }
        return result;
    };

    let estimated_input_tokens = estimate_reserve_input_tokens(&req);
    let pending_id = reserve(
        &ctx.pool,
        ctx.account_id,
        ctx.user_id,
        ctx.graph_id,
        task,
        llm.name(),
        &req.model,
        estimated_input_tokens,
        PENDING_KIND_SYNC,
    )
    .await;

    if !admission.await {
        finalize(&ctx.pool,pending_id,ctx.account_id,ctx.user_id,ctx.graph_id,
            &format!("{task}_withheld"),llm.name(),&requested_model,0,0,0,0,Some(0.0),
            ctx.source_job_id,ctx.source_query_id,ctx.source_journal_period,
            ctx.source_journal_granularity.as_deref(),ctx.attributed_user_id,None).await;
        return Err(LlmError::Auth(ADMISSION_WITHHELD.into()));
    }

    // No pool connection held across this call — reserve already committed
    // and released its own connection above.
    //
    // OpenRouter retry fix: wrapped in a fresh `UPSTREAM_PROVIDER` task-local
    // scope so `openai::OpenAiProvider::complete`'s internal retry loop can
    // report which upstream host served (or last attempted to serve) this
    // call, whether it ultimately succeeds or exhausts its retries — see
    // `crate::llm::report_upstream_provider`'s own doc for why this is a
    // task-local rather than an `LlmError` field or a plain provider-struct
    // field. `take_upstream_provider()` runs INSIDE the scope (before this
    // `.await` resolves), which is required — the value is gone the instant
    // the scope's future completes.
    let started = std::time::Instant::now();
    let ((result, upstream_provider), attempts) = coralswarm_core::send_gate::with_attempt_log(
        crate::llm::UPSTREAM_PROVIDER.scope(std::cell::RefCell::new(None), async {
            let result = dispatch(req).await;
            let upstream_provider = crate::llm::take_upstream_provider();
            (result, upstream_provider)
        }),
    )
    .await;
    let cost_usd = fold_attempt_costs(&result, &attempts);

    if matches!(result, Err(LlmError::Managed(_))) && attempts.is_empty() {
        finalize(&ctx.pool,pending_id,ctx.account_id,ctx.user_id,ctx.graph_id,
            &format!("{task}_withheld"),llm.name(),&requested_model,0,0,0,0,Some(0.0),
            ctx.source_job_id,ctx.source_query_id,ctx.source_journal_period,
            ctx.source_journal_granularity.as_deref(),ctx.attributed_user_id,None).await;
        return result;
    }

    finish_call_diagnostics(&result, started.elapsed().as_millis() as u64);
    match &result {
        Ok(resp) => {
            finalize(
                &ctx.pool,
                pending_id,
                ctx.account_id,
                ctx.user_id,
                ctx.graph_id,
                task,
                llm.name(),
                &resp.model,
                resp.input_tokens,
                resp.output_tokens,
                resp.cache_creation_input_tokens,
                resp.cache_read_input_tokens,
                cost_usd,
                ctx.source_job_id,
                ctx.source_query_id,
                ctx.source_journal_period,
                ctx.source_journal_granularity.as_deref(),
                ctx.attributed_user_id,
                upstream_provider.as_deref(),
            )
            .await;
        }
        Err(error) => {
            // This runs inside Jev's metering task even if evaluate's caller
            // has been canceled, and before best-effort accounting finalize.
            log_completion_error(
                error,
                Some(ctx),
                task,
                llm.name(),
                &requested_model,
                started.elapsed().as_millis() as u64,
            );
            let errored_task = format!("{task}{ERRORED_TASK_SUFFIX}");
            finalize(
                &ctx.pool,
                pending_id,
                ctx.account_id,
                ctx.user_id,
                ctx.graph_id,
                &errored_task,
                llm.name(),
                &requested_model,
                estimated_input_tokens as u32,
                0,
                0,
                0,
                cost_usd,
                ctx.source_job_id,
                ctx.source_query_id,
                ctx.source_journal_period,
                ctx.source_journal_granularity.as_deref(),
                ctx.attributed_user_id,
                upstream_provider.as_deref(),
            )
            .await;
        }
    }

    result
}

/// COR-89(a): one paid embedding attempt, metered like a completion. Each
/// attempt that was billed, or written with no usable answer, becomes one
/// `llm_usage` row on `ctx`'s axis; an attempt that generated nothing writes
/// none. Called only by the embedding gateway's meter
/// (`crate::embedding::Purpose::Metered`), which sees every attempt.
pub(crate) async fn insert_embed_attempt_row(
    ctx: &UsageContext,
    task: &str,
    chars: usize,
    attempt: &coralswarm_core::send_gate::AttemptRecord,
) {
    use coralswarm_core::send_gate::SendOutcome;
    let (task, model, input_tokens, cost, upstream) = match &attempt.outcome {
        SendOutcome::Billed {
            reported_cost_usd,
            input_tokens,
            served_model,
            upstream,
            ..
        } => (
            task.to_string(),
            served_model.clone(),
            (*input_tokens).min(u64::from(u32::MAX)) as u32,
            *reported_cost_usd,
            upstream.clone(),
        ),
        SendOutcome::Ambiguous { .. } => (
            format!("{task}{ERRORED_TASK_SUFFIX}"),
            coralswarm_core::embed_openrouter::CLOUD_EMBED_MODEL.to_string(),
            estimate_output_tokens_from_chars(chars),
            None,
            None,
        ),
        SendOutcome::NotGenerated => return,
    };
    finalize(
        &ctx.pool,
        None,
        ctx.account_id,
        ctx.user_id,
        ctx.graph_id,
        &task,
        "openrouter",
        &model,
        input_tokens,
        0,
        0,
        0,
        cost,
        ctx.source_job_id,
        ctx.source_query_id,
        ctx.source_journal_period,
        ctx.source_journal_granularity.as_deref(),
        ctx.attributed_user_id,
        upstream.as_deref(),
    )
    .await;
}

/// [`metered_complete`]'s sibling for the `complete_via_stream` shape
/// (`synthesis.rs`'s `synthesize` retry pair, `workers::digest`) — same
/// reserve/call/finalize sequence, wrapping [`super::complete_via_stream`]
/// instead of [`LlmProvider::complete`] directly, because
/// `complete_via_stream` never itself calls `complete()` (it drives
/// `stream_complete` and accumulates), so a `complete()`-shaped wrapper alone
/// would silently miss every one of these call sites (usage-metering plan
/// §3 item 9).
///
/// **Billing-correctness fix:** same Err-arm finalize [`metered_complete`]
/// now has — see that function's doc for the full reasoning. Note
/// `complete_via_stream`'s own `?` on each `stream.next()` item means an
/// `Err` here discards any deltas it accumulated before failing (its local
/// `content` buffer is dropped with the early return), so — same as
/// `metered_complete` — there is no partial output to recover at this level;
/// zero output tokens is the honest answer, not an oversight.
pub async fn metered_complete_via_stream(
    llm: &dyn LlmProvider,
    req: CompletionRequest,
    ctx: Option<&UsageContext>,
    task: &str,
) -> Result<CompletionResponse, LlmError> {
    metered_complete_via_stream_with_delta(llm, req, ctx, task, &mut |_| {}).await
}

/// [`metered_complete_via_stream`] plus a per-delta hook. Reserve/finalize
/// are unchanged; only the inner stream accumulation notifies `on_delta`.
pub async fn metered_complete_via_stream_with_delta<F>(
    llm: &dyn LlmProvider,
    req: CompletionRequest,
    ctx: Option<&UsageContext>,
    task: &str,
    on_delta: &mut F,
) -> Result<CompletionResponse, LlmError>
where
    F: FnMut(&str) + Send,
{
    let Some(ctx) = ctx else {
        return super::complete_via_stream_with_delta(llm, req, on_delta).await;
    };

    let estimated_input_tokens = estimate_reserve_input_tokens(&req);
    let requested_model = req.model.clone();
    let pending_id = reserve(
        &ctx.pool,
        ctx.account_id,
        ctx.user_id,
        ctx.graph_id,
        task,
        llm.name(),
        &req.model,
        estimated_input_tokens,
        PENDING_KIND_SYNC,
    )
    .await;

    // OpenRouter retry fix: same task-local scope as `metered_complete` —
    // see that function's own comment for the full rationale.
    let ((result, upstream_provider), attempts) = coralswarm_core::send_gate::with_attempt_log(
        crate::llm::UPSTREAM_PROVIDER.scope(std::cell::RefCell::new(None), async {
            let result = super::complete_via_stream_with_delta(llm, req, on_delta).await;
            let upstream_provider = crate::llm::take_upstream_provider();
            (result, upstream_provider)
        }),
    )
    .await;
    let cost_usd = fold_attempt_costs(&result, &attempts);

    match &result {
        Ok(resp) => {
            finalize(
                &ctx.pool,
                pending_id,
                ctx.account_id,
                ctx.user_id,
                ctx.graph_id,
                task,
                llm.name(),
                &resp.model,
                resp.input_tokens,
                resp.output_tokens,
                resp.cache_creation_input_tokens,
                resp.cache_read_input_tokens,
                cost_usd,
                ctx.source_job_id,
                ctx.source_query_id,
                ctx.source_journal_period,
                ctx.source_journal_granularity.as_deref(),
                ctx.attributed_user_id,
                upstream_provider.as_deref(),
            )
            .await;
        }
        Err(_) => {
            let errored_task = format!("{task}{ERRORED_TASK_SUFFIX}");
            finalize(
                &ctx.pool,
                pending_id,
                ctx.account_id,
                ctx.user_id,
                ctx.graph_id,
                &errored_task,
                llm.name(),
                &requested_model,
                estimated_input_tokens as u32,
                0,
                0,
                0,
                cost_usd,
                ctx.source_job_id,
                ctx.source_query_id,
                ctx.source_journal_period,
                ctx.source_journal_granularity.as_deref(),
                ctx.attributed_user_id,
                upstream_provider.as_deref(),
            )
            .await;
        }
    }

    result
}

/// The reserve half of the hand-wired choke point for a RAW
/// [`LlmProvider::stream_complete`] call whose caller consumes the returned
/// stream itself and so cannot be wrapped by [`metered_complete_via_stream`]
/// — today, exactly `synthesis::synthesize_stream` (the interactive Ask SSE
/// path). Call this immediately before `stream_complete`, and pair it with
/// [`finalize_for_stream`] wherever the caller handles the terminal
/// `StreamEvent::Final` frame. For that ONE caller (`synthesize_stream`),
/// also construct a [`StreamUsageGuard`] right alongside this reserve — see
/// its own doc for why this primitive alone has no backstop for a caller that
/// never reaches the terminal frame at all.
///
/// `ctx: None` returns `None` — no reserve, matching every other entry
/// point's `None`-context skip.
pub async fn reserve_for_stream(
    ctx: Option<&UsageContext>,
    task: &str,
    provider: &str,
    req: &CompletionRequest,
    kind: &str,
) -> Option<Uuid> {
    let ctx = ctx?;
    let estimated_input_tokens = estimate_reserve_input_tokens(req);
    reserve(
        &ctx.pool,
        ctx.account_id,
        ctx.user_id,
        ctx.graph_id,
        task,
        provider,
        &req.model,
        estimated_input_tokens,
        kind,
    )
    .await
}

/// The finalize half of [`reserve_for_stream`] — call from wherever the
/// caller handles the terminal `StreamEvent::Final` frame. `ctx: None` is a
/// no-op (mirrors every other entry point).
///
/// `upstream_provider`: see [`finalize`]'s own parameter doc. The
/// interactive-Ask raw-stream shape (`synthesis::synthesize_stream`) does
/// NOT call this function at all — it goes through [`StreamUsageGuard`]
/// instead, which has the identical, identically-documented gap.
#[allow(clippy::too_many_arguments)]
pub async fn finalize_for_stream(
    ctx: Option<&UsageContext>,
    pending_id: Option<Uuid>,
    task: &str,
    provider: &str,
    model: &str,
    input_tokens: u32,
    output_tokens: u32,
    cache_creation_input_tokens: u32,
    cache_read_input_tokens: u32,
    cost_usd: Option<f64>,
    upstream_provider: Option<&str>,
) {
    let Some(ctx) = ctx else {
        return;
    };
    finalize(
        &ctx.pool,
        pending_id,
        ctx.account_id,
        ctx.user_id,
        ctx.graph_id,
        task,
        provider,
        model,
        input_tokens,
        output_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens,
        cost_usd,
        ctx.source_job_id,
        ctx.source_query_id,
        ctx.source_journal_period,
        ctx.source_journal_granularity.as_deref(),
        ctx.attributed_user_id,
        upstream_provider,
    )
    .await;
}

// ── Billing-correctness fix: the raw-stream shape's Drop-triggered backstop ───
//
// Everything above this section closes the gap for a provider `Err` an
// ACTIVELY RUNNING caller observes. It does nothing for a caller that never
// gets to observe anything at all because its own future is cancelled —
// `routes::queries::create_query_stream`'s own "H1" design deliberately keeps
// its worker task running to completion independent of the CLIENT'S TCP
// connection (see that function's `relay`/`tokio::spawn` comment), so the
// realistic trigger for this specific gap is NOT a literal client disconnect
// today — it is the caller's future being dropped while still suspended
// somewhere (process shutdown mid-stream, a task `.abort()`, or any future
// caller that DOES tie cancellation to disconnect). [`StreamUsageGuard`] is
// the backstop for exactly that.

/// RAII backstop for the raw-`stream_complete` shape's reserve/finalize pair
/// ([`reserve_for_stream`]/[`finalize_for_stream`] — see their own doc for why
/// this shape can't be a function-shaped choke point like [`metered_complete`]).
///
/// # The gap this closes
///
/// Before this existed, the ONLY way `synthesis::synthesize_stream`'s
/// reserved pending row got finalized was the caller explicitly handling the
/// terminal `StreamEvent::Final` frame. Anything that stopped the caller
/// reaching that arm WITHOUT going through an `Err` value either (this
/// module's section doc above explains why that is realistically a
/// cancellation, not a disconnect, in this codebase today) silently dropped
/// real, already-billed-by-the-provider spend with no trace beyond the
/// pending row `workers::usage_pending_sweep` eventually VOIDS (never
/// fabricates from). This guard turns that "cancelled future" case from data
/// loss into a finalize with whatever was genuinely observed.
///
/// # Why a plain RAII local, not a custom `Stream` wrapper
///
/// This guard is handed back on `synthesis::SynthesisStream` as an ordinary
/// struct field, not folded into the stream's own `poll_next`. A local
/// variable that is ALIVE across an `.await` point inside an async fn is
/// dropped exactly like a synchronous stack variable when that future is
/// cancelled — Rust's async cancellation model guarantees this — so this
/// guard only needs to sit in the CALLER's own loop (updated via
/// [`record_delta`](Self::record_delta) on every `Delta`, consumed via
/// EXACTLY ONE of [`finalize_ok`](Self::finalize_ok)/
/// [`finalize_err`](Self::finalize_err) on the terminal event) for `Drop` to
/// fire correctly on cancellation. A custom `Stream` impl doing the same
/// tracking inside `poll_next` would need unsafe pin projection to get this
/// exact same guarantee — this is simpler and needs none.
///
/// # Token sourcing
///
/// Input tokens: the reserve-time ESTIMATE ([`estimate_reserve_input_tokens`])
/// — this module's own doc already establishes real input tokens are only
/// ever known from the provider's response, and an abandoned/errored stream
/// never produces one. Output tokens: this codebase's [`super::StreamEvent`]
/// only ever surfaces a provider-reported count on the terminal `Final` frame
/// — which is exactly the frame
/// [`finalize_ok`](Self::finalize_ok) already consumes on the normal path. So
/// "prefer the provider's own reported count" is satisfied structurally by
/// [`finalize_ok`](Self::finalize_ok) itself; THIS guard's own fallback
/// (`chars / 4`, mirroring `estimate_reserve_input_tokens`'s heuristic — see
/// [`estimate_output_tokens_from_chars`]) only ever runs for
/// [`finalize_err`](Self::finalize_err)/`Drop`, i.e. exactly the case where no
/// `Final` frame ever arrived to report a real count.
///
/// # Exactly-once
///
/// This guard's `inner: Option<..>` bookkeeping (taken by
/// [`finalize_ok`](Self::finalize_ok)/[`finalize_err`](Self::finalize_err),
/// which is what disarms `Drop`) is an OPTIMIZATION — it skips a redundant
/// spawn + DB round-trip on the ordinary happy path — not the correctness
/// mechanism. [`finalize`]'s own SAVEPOINT-guarded pending-row DELETE (see its
/// doc) is what actually prevents a double `llm_usage` write if this guard's
/// `Drop` and an explicit `finalize_ok`/`finalize_err` ever somehow raced for
/// the same `pending_id`.
pub struct StreamUsageGuard {
    inner: Option<StreamUsageGuardInner>,
}

struct StreamUsageGuardInner {
    pool: PgPool,
    pending_id: Option<Uuid>,
    account_id: Option<Uuid>,
    user_id: Option<Uuid>,
    /// Billing Phase 1: carried through to every `finalize` call this guard
    /// makes (`finalize_ok`/`finalize_err`/`Drop`) — see `UsageContext::
    /// attributed_user_id`'s doc for the stored-attribution-vs-scope-selector
    /// distinction from `user_id` just above.
    attributed_user_id: Option<Uuid>,
    graph_id: Option<Uuid>,
    task: String,
    /// [`LlmProvider::name`] of the provider this guard's call is running
    /// against — see this module's "provider" doc section above.
    provider: String,
    requested_model: String,
    /// Running character count of every `StreamDelta::text` observed via
    /// [`StreamUsageGuard::record_delta`] — the fallback output-token source;
    /// see the struct doc's "Token sourcing" section.
    streamed_chars: usize,
    estimated_input_tokens: u32,
    cache_creation_input_tokens: u32,
    cache_read_input_tokens: u32,
    source_job_id: Option<Uuid>,
    source_query_id: Option<Uuid>,
    source_journal_period: Option<DateTime<Utc>>,
    source_journal_granularity: Option<String>,
}

impl StreamUsageGuard {
    /// Construct alongside [`reserve_for_stream`] — same `ctx`/`task`/`req`
    /// inputs (`req` BEFORE it is moved into `stream_complete`). `ctx: None`
    /// produces a fully inert guard (mirrors every other metered entry
    /// point's `None`-context skip: no pool to finalize against, so `Drop`
    /// has nothing to do).
    pub fn new(
        ctx: Option<&UsageContext>,
        pending_id: Option<Uuid>,
        task: &str,
        provider: &str,
        req: &CompletionRequest,
    ) -> Self {
        let Some(ctx) = ctx else {
            return Self { inner: None };
        };
        Self {
            inner: Some(StreamUsageGuardInner {
                pool: ctx.pool.clone(),
                pending_id,
                account_id: ctx.account_id,
                user_id: ctx.user_id,
                attributed_user_id: ctx.attributed_user_id,
                graph_id: ctx.graph_id,
                task: task.to_string(),
                provider: provider.to_string(),
                requested_model: req.model.clone(),
                streamed_chars: 0,
                estimated_input_tokens: estimate_reserve_input_tokens(req) as u32,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                source_job_id: ctx.source_job_id,
                source_query_id: ctx.source_query_id,
                source_journal_period: ctx.source_journal_period,
                source_journal_granularity: ctx.source_journal_granularity.clone(),
            }),
        }
    }

    /// Feed every `StreamEvent::Delta` the caller observes. Cheap (a length
    /// add, no allocation) — safe to call on every token.
    pub fn record_delta(&mut self, text: &str) {
        if let Some(inner) = self.inner.as_mut() {
            inner.streamed_chars = inner.streamed_chars.saturating_add(text.len());
        }
    }

    /// The normal happy path — call from the terminal `Ok(StreamEvent::Final)`
    /// arm with the frame's OWN actuals (never this guard's own fallback
    /// estimate; the provider's real numbers always win when available).
    /// Consumes `self`, which is what disarms `Drop` — see the struct doc's
    /// "Exactly-once" section for why that is an optimization, not the
    /// correctness mechanism.
    ///
    /// `upstream_provider`: see [`finalize`]'s own parameter doc — the
    /// caller's job to supply, same as every other actual on this call
    /// (`model`/`input_tokens`/…), since this guard has no way to observe it
    /// itself: unlike [`super::openai::OpenAiProvider::complete`], the RAW
    /// `stream_complete` shape this guard wraps
    /// (`synthesis::synthesize_stream`) does not run its consumption loop
    /// inside a [`crate::llm::UPSTREAM_PROVIDER`] scope, so there is nothing
    /// for `crate::llm::take_upstream_provider()` to read at this call's
    /// site today. Every current caller passes `None` — a deliberate,
    /// documented gap (see [`finalize_for_stream`]'s own doc), not a bug to
    /// silently paper over with a fabricated value.
    pub async fn finalize_ok(
        mut self,
        model: &str,
        input_tokens: u32,
        output_tokens: u32,
        cache_creation_input_tokens: u32,
        cache_read_input_tokens: u32,
        cost_usd: Option<f64>,
        upstream_provider: Option<&str>,
    ) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        finalize(
            &inner.pool,
            inner.pending_id,
            inner.account_id,
            inner.user_id,
            inner.graph_id,
            &inner.task,
            &inner.provider,
            model,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            cost_usd,
            inner.source_job_id,
            inner.source_query_id,
            inner.source_journal_period,
            inner.source_journal_granularity.as_deref(),
            inner.attributed_user_id,
            upstream_provider,
        )
        .await;
    }

    /// Call from an `Err(e)` arm — a provider error observed mid-stream while
    /// the caller is still actively running (as opposed to `Drop`, which
    /// fires only when the caller's own future is abandoned). Finalizes with
    /// whatever this guard tracked: the reserve-time input estimate and the
    /// `chars / 4` output fallback (see the struct doc's "Token sourcing"
    /// section), tagged [`ERRORED_TASK_SUFFIX`] so it is distinguishable from
    /// a clean completion. Consumes `self` — same disarm contract as
    /// [`finalize_ok`](Self::finalize_ok).
    pub async fn finalize_err(mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let output_tokens = estimate_output_tokens_from_chars(inner.streamed_chars);
        let task = format!("{}{ERRORED_TASK_SUFFIX}", inner.task);
        finalize(
            &inner.pool,
            inner.pending_id,
            inner.account_id,
            inner.user_id,
            inner.graph_id,
            &task,
            &inner.provider,
            &inner.requested_model,
            inner.estimated_input_tokens,
            output_tokens,
            inner.cache_creation_input_tokens,
            inner.cache_read_input_tokens,
            None,
            inner.source_job_id,
            inner.source_query_id,
            inner.source_journal_period,
            inner.source_journal_granularity.as_deref(),
            inner.attributed_user_id,
            // See `finalize_ok`'s own doc — this guard never observes an
            // upstream host at all (no scope to read one back from).
            None,
        )
        .await;
    }
}

impl Drop for StreamUsageGuard {
    /// The backstop itself — see the struct's own doc. Fires only when
    /// neither [`finalize_ok`](StreamUsageGuard::finalize_ok) nor
    /// [`finalize_err`](StreamUsageGuard::finalize_err) ever ran (both disarm
    /// by taking `inner`), i.e. the stream was genuinely abandoned.
    ///
    /// `Drop` is sync and [`finalize`] is async, so this spawns a detached
    /// task on the CURRENT Tokio runtime handle to do the actual write —
    /// `Handle::try_current()` (never the panicking `tokio::spawn`, which
    /// requires an ambient runtime context that is exactly what might be
    /// gone here), so a guard dropped with no runtime context available
    /// (process teardown after the runtime itself is already gone) degrades
    /// to a warning instead of a panic; the pending row, if any, is then left
    /// for `workers::usage_pending_sweep` exactly like any other unreachable
    /// finalize.
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let output_tokens = estimate_output_tokens_from_chars(inner.streamed_chars);
        let task = format!("{}{ABANDONED_TASK_SUFFIX}", inner.task);
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                tracing::warn!(
                    pending_id = ?inner.pending_id,
                    task = %task,
                    "llm_usage: StreamUsageGuard dropped with no Tokio runtime handle available — \
                     cannot spawn the backstop finalize; the pending row, if any, is left for \
                     workers::usage_pending_sweep"
                );
                return;
            }
        };
        tracing::warn!(
            pending_id = ?inner.pending_id,
            task = %task,
            model = %inner.requested_model,
            estimated_input_tokens = inner.estimated_input_tokens,
            output_tokens,
            "llm_usage: StreamUsageGuard dropped without a terminal finalize — spawning the \
             backstop finalize with tokens actually observed before the drop"
        );
        handle.spawn(async move {
            finalize(
                &inner.pool,
                inner.pending_id,
                inner.account_id,
                inner.user_id,
                inner.graph_id,
                &task,
                &inner.provider,
                &inner.requested_model,
                inner.estimated_input_tokens,
                output_tokens,
                inner.cache_creation_input_tokens,
                inner.cache_read_input_tokens,
                None,
                inner.source_job_id,
                inner.source_query_id,
                inner.source_journal_period,
                inner.source_journal_granularity.as_deref(),
                inner.attributed_user_id,
                // See `finalize_ok`'s own doc — no upstream host observed.
                None,
            )
            .await;
        });
    }
}

/// Record one LLM call's token usage. Best-effort — errors are logged, never
/// returned.
///
/// RLS scoping (org-tenancy, post-OPS-4). `llm_usage` carries an
/// `account_id`-keyed policy (migration `0083_consumer_rls`) whose predicate is
/// the STRICT `account_id = current_setting('app.account_id')::uuid`. That
/// makes `account_id` load-bearing on the consumer plane, not decorative:
///
/// * `Some(account)` — the insert runs inside a
///   [`crate::tenancy::scope::begin_account_scoped`] transaction, the GUC is
///   set, the predicate passes, the row lands. **This is the path every
///   consumer-plane caller must take.**
/// * `None` — a direct autocommit insert with no GUC set. On the consumer
///   plane that now FAILS: `current_setting` returns `''` and the `::uuid`
///   cast errors (`invalid input syntax for type uuid: ""`), so the row is
///   silently dropped after a warn. On the ORG TENANT plane `None` is still the
///   correct ATTRIBUTION (one org per tenant DB, and the column is nullable for
///   exactly that) — but the reason this doc used to give for it is now stale:
///   it said `migrations_tenant` applies no `0083` and whole-database isolation
///   replaces RLS. Whole-database isolation is still the CROSS-ORG story, and
///   `migrations_tenant/0009_tenant_rls` now puts a MEMBER-OCEAN policy on
///   `llm_usage`, anchored on `graph_id`. Post-cutover a tenant-plane insert is
///   therefore NOT unconditionally fine: see the failure-mode note below.
///
/// **History — do not re-derive this.** This doc used to record that
/// unattributed inserts were an accepted fail-closed loss. That was written
/// believing the `None` callers were incidental. They were not: at the OPS-4
/// cutover ALL FIVE were the background workers (`extract`,
/// `extract_relationships` ×2, `entity_resolution`, `infer_expertise`), which
/// run on every ingested atom and are therefore the majority of LLM spend.
/// The "accepted gap" was in practice most of cost accounting, and prod logged
/// the cast error every ~2s. Every one of those sites now passes
/// [`crate::workers::embed::JobScope::usage_account_id`] — the job's own W.1
/// `account_id` stamp, already in scope, so attribution costs no lookup (in
/// particular it does not need 0084's `resolve_graph_account_id` bootstrap).
/// The remaining `None` callers are exactly the org-tenant plane, where `None`
/// is the correct answer rather than a gap.
///
/// **Tenant plane, post-`0009_tenant_rls` cutover.** `llm_usage`'s tenant-side
/// policy resolves through `tenant_rls_visible_graph_ids()`, so an insert whose
/// `graph_id` is a member-private ocean requires the transaction to have set
/// `app.user_id` (or `app.tenant_actor`); a NULL `graph_id` is hidden by the
/// same predicate. Converting this write path is Phase 2 Category-B work
/// (`docs/tenant-rls-cutover-runbook.md` §2.2) and a Phase-3 prerequisite.
///
/// **The failure mode, in two layers — do not collapse them.** Postgres does
/// NOT drop the row quietly: a `WITH CHECK` violation raises `42501 new row
/// violates row-level security policy` and aborts the `INSERT`. THIS function
/// then swallows that error (it returns `()`, logs `warn!`, and continues,
/// because metering must never fail a user's request). Composed, that is a real
/// database error downgraded here into a WARN line and a missing row — so
/// detection is the log line and a drop in row rate, never an error-rate alert.
/// A caller that propagated the error would see a hard 42501 instead; do not
/// generalise this function's best-effort behaviour to other tenant-plane
/// writes.
///
/// If you are adding a call site: on the consumer plane, passing `None` is a
/// bug that will not fail your tests — it warns and continues. Resolve an
/// account.
///
/// `graph_id` stays optional and is pure metadata — it is not part of any
/// policy predicate on this table.
///
/// # Tenant RLS Phase 2: `user_id`
///
/// **This section is entirely about the RLS SCOPE SELECTOR, not stored
/// attribution.** See this module's own top-of-file doc, "Two roles named
/// `user_id`" section, for the full split from Billing Phase 1's
/// `attributed_user_id` — every `None` this section justifies below is still
/// correct after that PR; `attributed_user_id` is threaded independently and
/// does not change any of it.
///
/// `llm_usage` is ALSO RLS-covered on the tenant plane
/// (`migrations_tenant/0008_tenant_rls` §6(c), nullable `graph_id`). Two
/// distinct classes of caller, handled differently:
///
/// * **The four tenant-loop workers** (`extract`, `extract_relationships`,
///   `entity_resolution`, `infer_expertise`) pass `account_id` from
///   `JobScope::usage_account_id`, which is `None` on the Org arm by design
///   (see this function's own History section) — so they never reach the
///   `Some(acct)` branch on the tenant plane at all, and pass `user_id: None`
///   here unconditionally. This USED to be justified by `WorkerTenant.pool`
///   being a connection-scoped `app.tenant_actor=system` pool, so the ambient
///   `None => insert.execute(pool)` path satisfied `tenant_rls_is_system()` at
///   the connection level. **That justification is gone** — the startup-packet
///   GUC was removed because a connection pooler rejects it (see
///   `TenantRegistry::connect_and_verify`).
///
///   Until usage-metering PR5, the ambient insert only succeeded because the
///   app connects as the table owner and `0009` §7 leaves the owner exempt —
///   a REAL gap once Phase 3 lands, and a silent one: `record` returns `()`
///   and downgrades a `42501` to a `warn!` (see this function's error
///   handling), so nothing failed loudly and no test went red. **PR5 closed
///   it**: this function's own `None`-account arm (below) now opens an
///   owner-scoped transaction via
///   [`crate::tenancy::scope::begin_tenant_system_scoped_without_account`]
///   instead of inserting ambiently, and [`reserve`]/[`finalize`] carry the
///   identical fix for the `metered_complete`/`metered_complete_via_stream`/
///   `reserve_for_stream`+`finalize_for_stream` choke points. The write no
///   longer depends on the owner exemption at all — a member/team-ocean
///   `graph_id` is visible because `app.tenant_actor='system'` is genuinely
///   set, not because RLS never got the chance to run.
/// * **Request-path callers** (`routes::transcript`, `routes::nodes`,
///   `routes::queries`) resolve a REAL account and run on the per-request
///   pool (never the system-scoped worker pool), so they must pass
///   `user_id: Some(the gate-verified acting caller)` — without it, a write
///   into a member ocean's usage row would silently fail `0008_tenant_rls`'s
///   `graph_id`-anchored policy once Phase 3 cuts over.
///
/// The best-effort contract is unchanged and absolute: every failure path here
/// (scope open, insert, commit) warns and returns. This function must never
/// propagate an error, and never gates or slows the originating job.
///
/// Uses a runtime `sqlx::query` (not the compile-checked `query!` macro) so the
/// crate builds without a live database.
///
/// `attributed_user_id` (Billing Phase 1): STORED attribution only — see
/// `UsageContext::attributed_user_id`'s doc for the full distinction from
/// `user_id` above, which stays the RLS scope selector this function's own
/// `match account_id` below branches on. Its former production callers (the
/// cache-hit sites of the retired per-atom extraction workers) passed `None`
/// here; it has no production caller today.
#[allow(clippy::too_many_arguments)]
pub async fn record(
    pool: &PgPool,
    account_id: Option<Uuid>,
    user_id: Option<Uuid>,
    graph_id: Option<Uuid>,
    task: &str,
    model: &str,
    input_tokens: u32,
    output_tokens: u32,
    cache_creation_input_tokens: u32,
    cache_read_input_tokens: u32,
    attributed_user_id: Option<Uuid>,
) {
    let insert = sqlx::query(
        r#"INSERT INTO llm_usage
               (account_id, graph_id, task, model, input_tokens, output_tokens,
                cache_creation_input_tokens, cache_read_input_tokens, attributed_user_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(task)
    .bind(model)
    // input_tokens/output_tokens are still Postgres INTEGER (i32) columns
    // (migration 0044) — unchanged here, in-range for real token counts.
    .bind(input_tokens as i32)
    .bind(output_tokens as i32)
    // cache_creation_input_tokens/cache_read_input_tokens are BIGINT (i64) as
    // of migration 0073 — bind as i64 to match the column type exactly.
    .bind(cache_creation_input_tokens as i64)
    .bind(cache_read_input_tokens as i64)
    // Billing Phase 1 (migration 0157/0057) — see this function's own doc.
    .bind(attributed_user_id);

    let res = match account_id {
        Some(acct) => {
            let scoped_tx = match user_id {
                Some(uid) => crate::tenancy::scope::begin_tenant_scoped(pool, acct, uid).await,
                None => crate::tenancy::scope::begin_account_scoped(pool, acct).await,
            };
            match scoped_tx {
                Ok(mut tx) => match insert.execute(&mut *tx).await {
                    Ok(_) => tx.commit().await,
                    Err(e) => Err(e),
                },
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        task,
                        model,
                        "llm_usage: failed to open account-scoped tx (continuing — accounting is best-effort)"
                    );
                    return;
                }
            }
        }
        // Usage-metering PR5's retrofit: legacy `record()` callers passed
        // `scope.usage_account_id()`, which is `None` on the
        // Org arm BY DESIGN — same reasoning as `reserve`'s matching arm.
        // An owner-scoped `app.tenant_actor='system'` transaction replaces
        // the old bare autocommit insert, so a member/team-ocean `graph_id`
        // is actually visible to the write instead of resting on the
        // table-owner exemption alone (0009_tenant_rls policies `llm_usage`
        // on `graph_id`, and an unscoped session denies everything but the
        // org commons). On the CONSUMER plane this `None` case remains
        // exactly as broken as documented above (the `::uuid` cast still
        // fails — `app.tenant_actor` has no reader on 0083's policy), so
        // this change is a no-op there.
        None => match crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await
        {
            Ok(mut tx) => match insert.execute(&mut *tx).await {
                Ok(_) => tx.commit().await,
                Err(e) => Err(e),
            },
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    task,
                    model,
                    "llm_usage: failed to open tenant-system-scoped tx (continuing — accounting \
                     is best-effort)"
                );
                return;
            }
        },
    };

    if let Err(e) = res {
        tracing::warn!(
            error = %e,
            task,
            model,
            "llm_usage: failed to record LLM usage (continuing — accounting is best-effort)"
        );
    }
}

#[cfg(test)]
mod error_observability_tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct LogBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for LogBuf {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    struct FailingLlm(Mutex<Option<LlmError>>);

    #[async_trait::async_trait]
    impl LlmProvider for FailingLlm {
        async fn complete(&self, _: CompletionRequest) -> Result<CompletionResponse, LlmError> {
            Err(self.0.lock().unwrap().take().unwrap())
        }
        async fn stream_complete(
            &self,
            _: CompletionRequest,
        ) -> Result<
            std::pin::Pin<
                Box<dyn futures::Stream<Item = Result<super::super::StreamEvent, LlmError>> + Send>,
            >,
            LlmError,
        > {
            unreachable!()
        }
        fn name(&self) -> &'static str {
            "openrouter"
        }
    }

    #[tokio::test]
    async fn concurrent_call_diagnostics_are_isolated() {
        async fn call(phase: &'static str, attempts: u32) -> super::super::CallDiagnostics {
            crate::decision::in_ask_phase(Some(phase), async {
                super::super::CALL_DIAGNOSTICS.scope(std::cell::RefCell::new(
                    super::super::CallDiagnostics {
                        phase: crate::decision::current_ask_phase(), ..Default::default()
                    }), async {
                        super::super::report_attempt(attempts);
                        tokio::task::yield_now().await;
                        super::super::current_diagnostics().unwrap()
                    }).await
            }).await
        }
        let (pack, cite) = tokio::join!(call("pack", 3), call("cite", 1));
        assert_eq!((pack.phase, pack.attempt_count), (Some("pack"), 3));
        assert_eq!((cite.phase, cite.attempt_count), (Some("cite"), 1));
        assert!(super::super::current_diagnostics().is_none());
        assert!(crate::decision::current_ask_phase().is_none());
    }

    #[tokio::test]
    async fn metered_complete_errors_emit_safe_correlated_diagnostics() {
        // Closed pool makes reserve/finalize fail immediately without a DB.
        // The provider call and error arm still run through the real seam.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        pool.close().await;
        let query_id = Uuid::new_v4();
        let ctx = UsageContext::new(pool, None, None).with_source_query(query_id);
        let buf = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(buf.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let sensitive = "PRIVATE_PROVIDER_BODY sk-secret";
        let cases = [
            (
                LlmError::Upstream {
                    status: 503,
                    body: sensitive.into(),
                },
                "upstream",
                Some(503),
            ),
            (
                LlmError::Upstream {
                    status: 0,
                    body: sensitive.into(),
                },
                "upstream",
                None,
            ),
            (LlmError::Timeout, "timeout", None),
            (LlmError::Auth(sensitive.into()), "auth", None),
            (
                LlmError::RateLimit(sensitive.into()),
                "rate_limit",
                Some(429),
            ),
            (
                LlmError::ModelNotFound(sensitive.into()),
                "model_not_found",
                None,
            ),
            (LlmError::Parse(sensitive.into()), "parse", None),
        ];
        for (error, expected_class, expected_status) in cases {
            let expected_error = error.to_string();
            buf.0.lock().unwrap().clear();
            let req = CompletionRequest {
                model: "deepseek/deepseek-v4-flash-0731".into(),
                system: Some("PRIVATE_SYSTEM_PROMPT".into()),
                messages: vec![super::super::Message {
                    role: super::super::Role::User,
                    content: "PRIVATE_USER_QUESTION".into(),
                }],
                ..Default::default()
            };
            let result = crate::decision::in_ask_phase(Some("pack"), metered_complete(
                &FailingLlm(Mutex::new(Some(error))),
                req,
                Some(&ctx),
                "jev_ask",
            )).await;
            assert_eq!(result.unwrap_err().to_string(), expected_error);
            let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
            let events: Vec<serde_json::Value> = logs
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .filter(|event: &serde_json::Value| {
                    event["fields"]["message"] == "llm provider call failed"
                })
                .collect();
            assert_eq!(
                events.len(),
                1,
                "failure must emit exactly one diagnostic: {logs}"
            );
            let fields = &events[0]["fields"];
            assert_eq!(events[0]["level"], "WARN");
            assert_eq!(fields["source_query_id"], query_id.to_string());
            assert_eq!(fields["task"], "jev_ask");
            assert_eq!(fields["operation"], "complete");
            assert_eq!(fields["phase"], "pack");
            assert_eq!(fields["completion_reason"], "provider_error");
            assert_eq!(fields["provider"], "openrouter");
            assert_eq!(fields["model"], "deepseek/deepseek-v4-flash-0731");
            assert_eq!(fields["error_class"], expected_class);
            assert_eq!(fields["status"].as_u64(), expected_status);
            assert!(fields["elapsed_ms"].as_u64().is_some());
            for secret in [sensitive, "PRIVATE_SYSTEM_PROMPT", "PRIVATE_USER_QUESTION"] {
                assert!(!logs.contains(secret), "diagnostic leaked sensitive input");
            }
        }
    }
}

// ── PR4 live-Postgres tests ─────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with the control
// migrations applied, including `0149_llm_usage_pending`) — same convention
// as `jobs::queue`'s live tests (see that module's `track_b_test_pool`).
// `cargo test --lib` with no env var set SKIPS these with an `eprintln!`
// marker rather than failing.
//
// Every test mints a FRESH `Uuid::new_v4()` account/graph id and a unique
// `task` label, so tests never collide and need no shared-fixture cleanup
// between runs.
#[cfg(test)]
mod managed_tests {
    #[tokio::test]
    async fn managed_usage_rows_carry_no_query_or_journal_billing_key() {
        let pool = sqlx::postgres::PgPoolOptions::new().connect_lazy("postgres://localhost/unused").unwrap();
        let job = uuid::Uuid::new_v4();
        let user = uuid::Uuid::new_v4();
        let ctx = super::UsageContext::new(pool, Some(uuid::Uuid::new_v4()), Some(uuid::Uuid::new_v4()))
            .with_source_job(job)
            .with_source_query(uuid::Uuid::new_v4())
            .with_attributed_user(user)
            .with_journal(chrono::Utc::now(), "hour");
        let managed = super::managed_usage_context(&ctx);
        assert_eq!(managed.source_query_id, None, "the query axis must never bill a managed call");
        assert_eq!(managed.source_journal_period, None, "the journal axis must never bill a managed call");
        assert_eq!(managed.source_journal_granularity, None);
        assert_eq!(managed.source_job_id, Some(job), "job attribution is kept; the job axis bills by kind");
        assert_eq!(managed.attributed_user_id, Some(user));
        assert_eq!((managed.account_id, managed.graph_id), (ctx.account_id, ctx.graph_id));
    }

    use super::*;
    use crate::llm::{Deny, RefuseCode, StreamEvent};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Shaped like `decision::openrouter::DeadlineLlm`: forwards `complete`
    /// to a real transport and exposes no gated one of its own.
    struct Wrapper(crate::llm::openai::OpenAiProvider);

    #[async_trait::async_trait]
    impl LlmProvider for Wrapper {
        async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
            self.0.complete(req).await
        }
        async fn stream_complete(
            &self,
            req: CompletionRequest,
        ) -> Result<
            std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
            LlmError,
        > {
            self.0.stream_complete(req).await
        }
    }

    #[tokio::test]
    async fn managed_call_through_ungated_provider_is_refused() {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app = axum::Router::new().fallback(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        let inner = crate::llm::openai::OpenAiProvider::openrouter(reqwest::Client::new(), "k")
            .with_base_url(format!("http://{addr}"));
        let wrapper = Wrapper(crate::llm::openai::OpenAiProvider::openrouter(reqwest::Client::new(), "k")
            .with_base_url(format!("http://{addr}")));

        let lazy = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(500))
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .unwrap();
        let account = Uuid::new_v4();
        let scope = crate::workers::embed::JobScope::Consumer { pool: &lazy, account_id: account };
        let tenant = crate::tenancy::TenantId::Consumer;
        let call = || crate::managed::ManagedCall {
            plane: &scope,
            control: &lazy,
            tenant: &tenant,
            account_id: account,
            principal: Uuid::new_v4(),
            fallback_model: None,
            subject: crate::managed::Subject::Window {
                graph: Uuid::new_v4(),
                version: Uuid::new_v4(),
                profile: "source-atoms-v3",
                job: Uuid::new_v4(),
            },
        };
        let usage = UsageContext::new(lazy.clone(), Some(account), None);
        let req = || CompletionRequest {
            messages: vec![crate::llm::Message { role: crate::llm::Role::User, content: "x".into() }],
            model: "deepseek/deepseek-v4-flash-0731".into(),
            max_tokens: 16,
            ..Default::default()
        };

        let refused = metered_complete_managed(&wrapper, req(), &usage, "source_atoms", call(), false, |_| ()).await;
        assert!(
            matches!(refused, Err(LlmError::Managed(Deny::Refuse(RefuseCode::Ungated)))),
            "{refused:?}"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0, "nothing reached the provider");

        let gated = metered_complete_managed(&inner, req(), &usage, "source_atoms", call(), false, |_| ()).await;
        assert!(matches!(gated, Err(LlmError::Managed(Deny::Unavailable))), "{gated:?}");
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod pg_tests {
    use super::*;
    use crate::llm::{CompletionResponse, LlmError, Message, Role, StreamDelta, StreamEvent, StreamFinalEvent};

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

    async fn pending_count(pool: &PgPool, task: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM llm_usage_pending WHERE task = $1")
            .bind(task)
            .fetch_one(pool)
            .await
            .expect("count llm_usage_pending")
    }

    async fn usage_count(pool: &PgPool, task: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE task = $1")
            .bind(task)
            .fetch_one(pool)
            .await
            .expect("count llm_usage")
    }

    /// A controllable mock provider: `complete`/`stream_complete` each either
    /// succeed with fixed actuals (served model different from the requested
    /// one, real cache/cost figures — so a round-trip test can assert every
    /// field) or return a configured upstream error.
    struct MockLlm {
        fail: bool,
    }

    #[async_trait::async_trait]
    impl LlmProvider for MockLlm {
        async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
            if self.fail {
                return Err(LlmError::Upstream { status: 500, body: "mock failure".into() });
            }
            Ok(CompletionResponse {
                content: "ok".to_string(),
                input_tokens: 12,
                output_tokens: 34,
                model: "mock-served-model".to_string(),
                cache_creation_input_tokens: 1,
                cache_read_input_tokens: 2,
                stop_reason: Some("end_turn".to_string()),
                cost_usd: Some(0.0042),
            })
        }

        async fn stream_complete(
            &self,
            _req: CompletionRequest,
        ) -> Result<
            std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
            LlmError,
        > {
            if self.fail {
                return Err(LlmError::Upstream { status: 500, body: "mock failure".into() });
            }
            let events: Vec<Result<StreamEvent, LlmError>> = vec![
                Ok(StreamEvent::Delta(StreamDelta { text: "hi".into() })),
                Ok(StreamEvent::Final(StreamFinalEvent {
                    input_tokens: 12,
                    output_tokens: 34,
                    cache_creation_input_tokens: 1,
                    cache_read_input_tokens: 2,
                    stop_reason: Some("end_turn".to_string()),
                    model: Some("mock-served-model".to_string()),
                    cost_usd: Some(0.0042),
                })),
            ];
            Ok(Box::pin(futures::stream::iter(events)))
        }

        fn name(&self) -> &'static str {
            // Matches this test module's existing `row.2 == "anthropic"`
            // assertions — proves `metered_complete`/`metered_complete_via_
            // stream` actually thread `llm.name()` into `llm_usage.provider`
            // rather than a hardcoded literal (OpenRouter provider port,
            // PR1).
            "anthropic"
        }
    }

    fn req() -> CompletionRequest {
        CompletionRequest {
            messages: vec![Message {
                role: Role::User,
                content: "hello world, please respond".into(),
            }],
            model: "mock-requested-model".into(),
            max_tokens: 64,
            system: None,
            tool_schema: None,
            effort: None,
            reasoning: crate::llm::ReasoningSetting::Off,
        }
    }

    /// The positive path, end to end: reserve → call → finalize. Asserts
    /// every column lands, including the SERVED model (not the requested
    /// one), `provider`, `cost_usd` (the NUMERIC round-trip through `f64`),
    /// and `source_job_id`.
    #[tokio::test]
    async fn metered_complete_success_writes_llm_usage_and_deletes_pending() {
        let Some(pool) =
            pg_test_pool("metered_complete_success_writes_llm_usage_and_deletes_pending").await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let task = format!("pr4_test_success_{}", Uuid::new_v4());

        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id))
            .with_source_job(job_id);
        let llm = MockLlm { fail: false };

        let resp = metered_complete(&llm, req(), Some(&ctx), &task)
            .await
            .expect("mock call succeeds");
        assert_eq!(resp.model, "mock-served-model");

        assert_eq!(
            pending_count(&pool, &task).await,
            0,
            "finalize must delete the pending row on success"
        );
        assert_eq!(
            usage_count(&pool, &task).await,
            1,
            "exactly one llm_usage row must land"
        );

        let row: (
            Uuid,
            Option<Uuid>,
            String,
            String,
            i32,
            i32,
            i64,
            i64,
            String,
            Option<f64>,
        ) = sqlx::query_as(
            // `cost_usd` is NUMERIC — sqlx's runtime decode path checks the
            // wire type OID and refuses to decode NUMERIC straight into
            // `Option<f64>` even though the bytes are numerically fine (this
            // failed loudly here first: "mismatched types ... FLOAT8 ... is
            // not compatible with SQL type NUMERIC"). The explicit `::float8`
            // cast is required on every READ of this column — `finalize`'s
            // WRITE side binds `Option<f64>` straight through and needs no
            // such cast (Postgres applies the assignment cast automatically
            // on INSERT); this is a read-only gotcha future readers (PR9's
            // derivation worker) must repeat.
            "SELECT account_id, source_job_id, provider, model, input_tokens, output_tokens, \
                    cache_creation_input_tokens, cache_read_input_tokens, task, cost_usd::float8 \
             FROM llm_usage WHERE task = $1",
        )
        .bind(&task)
        .fetch_one(&pool)
        .await
        .expect("fetch the finalized row");
        assert_eq!(row.0, account_id);
        assert_eq!(row.1, Some(job_id), "source_job_id must round-trip");
        assert_eq!(row.2, "anthropic");
        assert_eq!(row.3, "mock-served-model", "must record the SERVED model");
        assert_eq!(row.4, 12);
        assert_eq!(row.5, 34);
        assert_eq!(row.6, 1);
        assert_eq!(row.7, 2);
        assert_eq!(row.8, task);
        assert_eq!(row.9, Some(0.0042), "cost_usd must round-trip through NUMERIC");
    }

    /// **OpenRouter provider port, PR1 — `llm_usage.provider` is real
    /// introspection, not a hardcoded literal.** A second mock provider
    /// (identical shape to [`MockLlm`], different [`LlmProvider::name`])
    /// proves `metered_complete` threads `llm.name()` through, not a
    /// constant — the two tests recording two DIFFERENT provider strings
    /// off the SAME `metered_complete` call site is the actual proof; a
    /// single test asserting one hardcoded string could pass even if the
    /// threading were still wrong.
    struct OpenRouterMockLlm;

    #[async_trait::async_trait]
    impl LlmProvider for OpenRouterMockLlm {
        async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
            Ok(CompletionResponse {
                content: "ok".to_string(),
                input_tokens: 7,
                output_tokens: 3,
                model: "deepseek/deepseek-v4-flash-0731".to_string(),
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                stop_reason: Some("end_turn".to_string()),
                cost_usd: Some(0.00011),
            })
        }

        async fn stream_complete(
            &self,
            _req: CompletionRequest,
        ) -> Result<
            std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
            LlmError,
        > {
            unimplemented!("not exercised by this test")
        }

        fn name(&self) -> &'static str {
            "openrouter"
        }
    }

    #[tokio::test]
    async fn metered_complete_records_the_openrouter_provider_name_not_anthropic() {
        let Some(pool) =
            pg_test_pool("metered_complete_records_the_openrouter_provider_name_not_anthropic")
                .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let task = format!(
            "pr_billing_test_openrouter_provider_name_{}",
            Uuid::new_v4()
        );
        let ctx = UsageContext::new(pool.clone(), Some(account_id), None);

        let resp = metered_complete(&OpenRouterMockLlm, req(), Some(&ctx), &task)
            .await
            .expect("mock complete should succeed");
        assert_eq!(resp.model, "deepseek/deepseek-v4-flash-0731");

        let provider: String =
            sqlx::query_scalar("SELECT provider FROM llm_usage WHERE task = $1")
                .bind(&task)
                .fetch_one(&pool)
                .await
                .expect("fetch the finalized row");
        assert_eq!(provider, "openrouter", "must record the ACTUAL provider, never a hardcoded default");
    }

    // ── OpenRouter retry fix: usage accounting stays truthful ──────────────────
    //
    // `openai::OpenAiProvider::complete`/`stream_complete` now retry a
    // body-level failure INTERNALLY (same `RetryPolicy`, one HTTP request
    // per attempt) before ever returning to this module. This mock
    // simulates exactly that shape — a `.complete()` call that may fail N
    // times and report an upstream host via `crate::llm::
    // report_upstream_provider` on EACH of its own internal "attempts"
    // before finally returning Ok or Err ONCE — to prove `metered_complete`
    // still writes EXACTLY ONE `llm_usage` row per call (no double
    // counting, no lost failed attempts: see this module's own top-of-file
    // "provider" doc section and the OpenRouter retry fix PR's item 2), and
    // that `upstream_provider` lands correctly on both a success row and an
    // exhausted-failure `_errored` row.
    struct RetryingMockLlm {
        /// How many of this mock's OWN internal "attempts" report a
        /// transient failure before the final one succeeds. `fail_times >=
        /// attempts_before_giving_up` (i.e. every attempt fails) simulates
        /// an EXHAUSTED retry — `.complete()` still returns exactly once.
        fail_times: u32,
        /// Reported via `crate::llm::report_upstream_provider` on every
        /// failed attempt.
        failing_provider: &'static str,
        /// Reported on the final (successful) attempt, when one occurs.
        succeeding_provider: &'static str,
    }

    #[async_trait::async_trait]
    impl LlmProvider for RetryingMockLlm {
        async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
            for _ in 0..self.fail_times {
                crate::llm::report_upstream_provider(Some(self.failing_provider));
            }
            if self.fail_times > 0 {
                return Err(LlmError::Upstream { status: 0, body: "mock: all attempts failed".into() });
            }
            crate::llm::report_upstream_provider(Some(self.succeeding_provider));
            Ok(CompletionResponse {
                content: "ok".to_string(),
                input_tokens: 12,
                output_tokens: 34,
                model: "z-ai/glm-5.3-flash".to_string(),
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                stop_reason: Some("end_turn".to_string()),
                cost_usd: Some(0.0001),
            })
        }

        async fn stream_complete(
            &self,
            _req: CompletionRequest,
        ) -> Result<
            std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
            LlmError,
        > {
            unimplemented!("not exercised by these tests")
        }

        fn name(&self) -> &'static str {
            "openrouter"
        }
    }

    /// The success half: a call that internally observed (and reported) a
    /// failing attempt's upstream host before finally succeeding must still
    /// write EXACTLY ONE `llm_usage` row — the success row, tagged with the
    /// FINAL (succeeding) attempt's upstream host, never the earlier failed
    /// one's, and never a second `_errored` row for the attempt that never
    /// reached `metered_complete` at all.
    #[tokio::test]
    async fn metered_complete_after_internal_retry_writes_exactly_one_success_row() {
        let Some(pool) =
            pg_test_pool("metered_complete_after_internal_retry_writes_exactly_one_success_row")
                .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_openrouter_retry_test_success_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id));
        let llm = RetryingMockLlm { fail_times: 0, failing_provider: "DeepInfra", succeeding_provider: "Fireworks" };

        let resp = metered_complete(&llm, req(), Some(&ctx), &task)
            .await
            .expect("the mock's own single call succeeds");
        assert_eq!(resp.model, "z-ai/glm-5.3-flash");

        assert_eq!(pending_count(&pool, &task).await, 0);
        assert_eq!(
            usage_count(&pool, &task).await,
            1,
            "exactly one llm_usage row — no double counting"
        );
        let errored_task = format!("{task}{ERRORED_TASK_SUFFIX}");
        assert_eq!(
            usage_count(&pool, &errored_task).await,
            0,
            "no _errored row must exist for an internally-retried call that ultimately succeeded"
        );
        let upstream_provider: Option<String> =
            sqlx::query_scalar("SELECT upstream_provider FROM llm_usage WHERE task = $1")
                .bind(&task)
                .fetch_one(&pool)
                .await
                .expect("fetch upstream_provider");
        assert_eq!(
            upstream_provider.as_deref(),
            Some("Fireworks"),
            "must record the FINAL (succeeding) attempt's upstream host"
        );
    }

    /// The failure half: every attempt fails (an exhausted retry) — exactly
    /// ONE `_errored` row must land, and it must carry the upstream host the
    /// failing attempts reported. This is "how the 16% was measured": a
    /// `GROUP BY upstream_provider` over `_errored` rows now has something
    /// to group on.
    #[tokio::test]
    async fn metered_complete_on_exhausted_retry_writes_exactly_one_errored_row_with_upstream_provider(
    ) {
        let Some(pool) = pg_test_pool(
            "metered_complete_on_exhausted_retry_writes_exactly_one_errored_row_with_upstream_provider",
        )
        .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_openrouter_retry_test_exhausted_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id));
        let llm = RetryingMockLlm { fail_times: 3, failing_provider: "DeepInfra", succeeding_provider: "unused" };

        let result = metered_complete(&llm, req(), Some(&ctx), &task).await;
        assert!(result.is_err(), "every attempt failing must surface as Err");

        assert_eq!(pending_count(&pool, &task).await, 0);
        assert_eq!(
            usage_count(&pool, &task).await,
            0,
            "no clean-completion row for a call that never succeeded"
        );
        let errored_task = format!("{task}{ERRORED_TASK_SUFFIX}");
        assert_eq!(
            usage_count(&pool, &errored_task).await,
            1,
            "exactly one _errored row — no double counting, no lost failed attempt"
        );
        let upstream_provider: Option<String> =
            sqlx::query_scalar("SELECT upstream_provider FROM llm_usage WHERE task = $1")
                .bind(&errored_task)
                .fetch_one(&pool)
                .await
                .expect("fetch upstream_provider");
        assert_eq!(
            upstream_provider.as_deref(),
            Some("DeepInfra"),
            "the _errored row must attribute the failure to the upstream host that produced it"
        );
    }

    /// **Negative control 1 (the task's "kill test"), billing-correctness
    /// fix version:** a provider call that never returns success used to
    /// leave EXACTLY ONE pending row and ZERO `llm_usage` rows (the pending
    /// row was left for the sweeper, and the real spend was never billed).
    /// It now finalizes: the pending row is CONSUMED (zero survive) and
    /// exactly one `llm_usage` row lands, carrying the reserve-time input
    /// ESTIMATE (never zero — `req()` below has non-trivial content) and
    /// ZERO output tokens (`complete()` is not incremental; there is nothing
    /// to recover on error), tagged [`ERRORED_TASK_SUFFIX`] so it is
    /// distinguishable from a clean completion. This is
    /// `two_finalizes_under_the_same_source_job_id_sum_rather_than_collide`'s
    /// sibling proof for the FAILURE axis: real spend must never be silently
    /// dropped just because the provider call didn't return `Ok`.
    #[tokio::test]
    async fn metered_complete_on_provider_error_finalizes_with_estimated_input_and_zero_output() {
        let Some(pool) = pg_test_pool(
            "metered_complete_on_provider_error_finalizes_with_estimated_input_and_zero_output",
        )
        .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_billing_test_kill_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id));
        let llm = MockLlm { fail: true };

        let result = metered_complete(&llm, req(), Some(&ctx), &task).await;
        assert!(
            result.is_err(),
            "the mock provider must fail for this control to mean anything"
        );

        assert_eq!(
            pending_count(&pool, &task).await,
            0,
            "the pending row must be CONSUMED by the new error-path finalize, not left for the \
             sweeper"
        );
        let errored_task = format!("{task}{ERRORED_TASK_SUFFIX}");
        assert_eq!(
            usage_count(&pool, &errored_task).await,
            1,
            "exactly one llm_usage row, tagged with the errored-task suffix"
        );
        let row: (i32, i32) = sqlx::query_as(
            "SELECT input_tokens, output_tokens FROM llm_usage WHERE task = $1",
        )
        .bind(&errored_task)
        .fetch_one(&pool)
        .await
        .expect("fetch the errored row");
        assert!(
            row.0 > 0,
            "input_tokens must be the non-trivial reserve-time estimate, not zero: {}",
            row.0
        );
        assert_eq!(
            row.1, 0,
            "a call that failed before returning has nothing to recover output tokens from"
        );
    }

    /// Same negative control, run against the STREAMING entry point — the
    /// task explicitly calls for this, not just the `complete()` shape.
    #[tokio::test]
    async fn metered_complete_via_stream_on_provider_error_finalizes_with_estimated_input_and_zero_output(
    ) {
        let Some(pool) = pg_test_pool(
            "metered_complete_via_stream_on_provider_error_finalizes_with_estimated_input_and_zero_output",
        )
        .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_billing_test_kill_stream_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id));
        let llm = MockLlm { fail: true };

        let result = metered_complete_via_stream(&llm, req(), Some(&ctx), &task).await;
        assert!(result.is_err());

        assert_eq!(pending_count(&pool, &task).await, 0);
        let errored_task = format!("{task}{ERRORED_TASK_SUFFIX}");
        assert_eq!(usage_count(&pool, &errored_task).await, 1);
        let row: (i32, i32) = sqlx::query_as(
            "SELECT input_tokens, output_tokens FROM llm_usage WHERE task = $1",
        )
        .bind(&errored_task)
        .fetch_one(&pool)
        .await
        .expect("fetch the errored row");
        assert!(row.0 > 0, "input_tokens must be the estimate, not zero: {}", row.0);
        assert_eq!(row.1, 0);
    }

    /// The BARE hand-wired raw-stream primitive (`reserve_for_stream` with no
    /// [`StreamUsageGuard`] attached — i.e. a caller that bypasses the guard
    /// entirely, or a true process kill, which runs no `Drop` impl at all):
    /// reserve happens, and nothing ever calls `finalize_for_stream`. Same
    /// assertion as always — one pending row, zero `llm_usage` rows — because
    /// THIS primitive alone has no backstop; see
    /// `stream_usage_guard_dropped_without_a_terminal_finalize_writes_the_tokens_actually_streamed`
    /// below for the REAL production shape (`synthesize_stream`'s
    /// `StreamUsageGuard`), which now DOES recover this case.
    #[tokio::test]
    async fn reserve_for_stream_without_a_matching_finalize_leaves_one_pending_and_zero_usage_rows() {
        let Some(pool) = pg_test_pool(
            "reserve_for_stream_without_a_matching_finalize_leaves_one_pending_and_zero_usage_rows",
        )
        .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let query_id = Uuid::new_v4();
        let task = format!("pr4_test_disconnect_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id))
            .with_source_query(query_id);

        let pending_id = reserve_for_stream(Some(&ctx), &task, DEFAULT_PROVIDER, &req(), PENDING_KIND_SYNC).await;
        assert!(
            pending_id.is_some(),
            "reserve must durably commit before the caller ever calls stream_complete"
        );

        // Simulate the client disconnect here: the stream is dropped and
        // `finalize_for_stream` never runs.
        assert_eq!(pending_count(&pool, &task).await, 1);
        assert_eq!(usage_count(&pool, &task).await, 0);
    }

    // ── Billing-correctness fix: `StreamUsageGuard` ─────────────────────────

    /// **The headline proof for this fix:** [`StreamUsageGuard`] dropped
    /// WITHOUT a terminal `finalize_ok`/`finalize_err` ever running — the
    /// real production shape for an abandoned stream (`synthesize_stream`'s
    /// guard, dropped when the caller's own future is cancelled mid-loop;
    /// see this module's "raw-stream shape" section doc for why that — not a
    /// literal client TCP disconnect — is the realistic trigger in THIS
    /// codebase today). Feeds it a few `record_delta` calls (real prose that
    /// genuinely streamed to the user, and was genuinely billed by Anthropic,
    /// before the drop), then drops it with NEITHER terminal method ever
    /// called. `Drop` spawns the backstop finalize on a background task, so
    /// this polls the DB briefly rather than asserting immediately after
    /// `drop()` returns (which only guarantees the spawn happened, not that
    /// the spawned task has run).
    #[tokio::test]
    async fn stream_usage_guard_dropped_without_a_terminal_finalize_writes_the_tokens_actually_streamed(
    ) {
        let Some(pool) = pg_test_pool(
            "stream_usage_guard_dropped_without_a_terminal_finalize_writes_the_tokens_actually_streamed",
        )
        .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_billing_test_guard_drop_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id));

        let pending_id = reserve_for_stream(Some(&ctx), &task, DEFAULT_PROVIDER, &req(), PENDING_KIND_SYNC).await;
        assert!(pending_id.is_some());

        {
            let mut guard = StreamUsageGuard::new(Some(&ctx), pending_id, &task, DEFAULT_PROVIDER, &req());
            // Real prose that streamed to the user before the drop — long
            // enough that `chars / 4` rounds to a clearly nonzero token count.
            guard.record_delta(&"The quick brown fox jumps over the lazy dog. ".repeat(4));
            // The drop itself: neither `finalize_ok` nor `finalize_err` is
            // ever called — this is what a cancelled/abandoned stream looks
            // like from this guard's point of view.
            drop(guard);
        }

        let abandoned_task = format!("{task}{ABANDONED_TASK_SUFFIX}");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if usage_count(&pool, &abandoned_task).await == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the Drop-spawned backstop finalize never landed within the timeout"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert_eq!(
            pending_count(&pool, &task).await,
            0,
            "the backstop finalize must consume the pending row"
        );
        let row: (i32, i32) =
            sqlx::query_as("SELECT input_tokens, output_tokens FROM llm_usage WHERE task = $1")
                .bind(&abandoned_task)
                .fetch_one(&pool)
                .await
                .expect("fetch the abandoned row");
        assert!(
            row.0 > 0,
            "input_tokens must be the reserve-time estimate, not zero: {}",
            row.0
        );
        assert!(
            row.1 > 0,
            "output_tokens must reflect the text actually streamed before the drop — NOT zero \
             and NOT fabricated from the (never-billed) reserve estimate: {}",
            row.1
        );
        assert_ne!(
            row.1, row.0,
            "output_tokens must come from the char-based fallback over the STREAMED text, not \
             accidentally echo the input-side estimate"
        );
    }

    /// [`StreamUsageGuard::finalize_err`] — the EXPLICIT error-path
    /// counterpart to the Drop backstop above, for a caller that is still
    /// ACTIVELY RUNNING (an `Err(e)` arm) rather than abandoned. Same
    /// fallback token source (`record_delta`'s accumulated chars), but
    /// immediate rather than Drop-triggered — this is
    /// `routes::queries::create_query_stream`'s own `Err(e)` arm's real
    /// shape.
    #[tokio::test]
    async fn stream_usage_guard_finalize_err_writes_exactly_one_row_with_fallback_output_tokens() {
        let Some(pool) = pg_test_pool(
            "stream_usage_guard_finalize_err_writes_exactly_one_row_with_fallback_output_tokens",
        )
        .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_billing_test_guard_err_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id));

        let pending_id = reserve_for_stream(Some(&ctx), &task, DEFAULT_PROVIDER, &req(), PENDING_KIND_SYNC).await;
        let mut guard = StreamUsageGuard::new(Some(&ctx), pending_id, &task, DEFAULT_PROVIDER, &req());
        guard.record_delta("partial answer before the provider errored out mid-stream");
        guard.finalize_err().await;

        assert_eq!(pending_count(&pool, &task).await, 0);
        let errored_task = format!("{task}{ERRORED_TASK_SUFFIX}");
        assert_eq!(usage_count(&pool, &errored_task).await, 1);
        let output_tokens: i32 =
            sqlx::query_scalar("SELECT output_tokens FROM llm_usage WHERE task = $1")
                .bind(&errored_task)
                .fetch_one(&pool)
                .await
                .expect("fetch output_tokens");
        assert!(
            output_tokens > 0,
            "must reflect the partial text actually observed, not zero: {output_tokens}"
        );
        // Same "Drop was genuinely disarmed" check as the finalize_ok test.
        let abandoned_task = format!("{task}{ABANDONED_TASK_SUFFIX}");
        assert_eq!(
            usage_count(&pool, &abandoned_task).await,
            0,
            "finalize_err must have disarmed Drop — no phantom _abandoned row"
        );
    }

    /// [`StreamUsageGuard::finalize_ok`] — the ordinary happy-path terminal
    /// call (`Ok(StreamEvent::Final)`). **Regression guard:** must write the
    /// PROVIDER'S actual reported actuals UNCHANGED, never this guard's own
    /// char-based fallback estimate — proves the guard doesn't quietly
    /// override a real number with its own approximation just because it
    /// happens to also be tracking `record_delta` calls along the way.
    #[tokio::test]
    async fn stream_usage_guard_finalize_ok_records_the_providers_actual_numbers_unchanged() {
        let Some(pool) = pg_test_pool(
            "stream_usage_guard_finalize_ok_records_the_providers_actual_numbers_unchanged",
        )
        .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_billing_test_guard_ok_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id));

        let pending_id = reserve_for_stream(Some(&ctx), &task, DEFAULT_PROVIDER, &req(), PENDING_KIND_SYNC).await;
        let mut guard = StreamUsageGuard::new(Some(&ctx), pending_id, &task, DEFAULT_PROVIDER, &req());
        // A short delta that would, on its own, produce only a tiny fallback
        // estimate — the terminal Final's REAL reported count (34) must win
        // regardless of what this guard tracked along the way.
        guard.record_delta("hi");
        guard
            .finalize_ok("served-model", 12, 34, 1, 2, Some(0.0042), None)
            .await;

        assert_eq!(pending_count(&pool, &task).await, 0);
        assert_eq!(
            usage_count(&pool, &task).await,
            1,
            "task is UNCHANGED (no suffix) on the happy path — this is a clean completion"
        );
        let row: (String, i32, i32, i64, i64, Option<f64>) = sqlx::query_as(
            "SELECT model, input_tokens, output_tokens, cache_creation_input_tokens, \
                    cache_read_input_tokens, cost_usd::float8 FROM llm_usage WHERE task = $1",
        )
        .bind(&task)
        .fetch_one(&pool)
        .await
        .expect("fetch the row");
        assert_eq!(row.0, "served-model");
        assert_eq!(row.1, 12);
        assert_eq!(
            row.2, 34,
            "must be the PROVIDER's reported count, not a 'hi'-based fallback estimate"
        );
        assert_eq!(row.3, 1);
        assert_eq!(row.4, 2);
        assert_eq!(row.5, Some(0.0042));
        // Directly proves `finalize_ok` disarmed `Drop` — NOT relying
        // transitively on `finalize`'s own exactly-once guard (which would
        // also mask a "forgot to disarm" bug, since a phantom second
        // finalize attempt for the same already-deleted `pending_id` would
        // just skip its insert). If `Drop` still fired here, it would use a
        // DIFFERENT task (the `_abandoned` suffix), so this checks that
        // table is genuinely empty rather than merely single-row.
        let abandoned_task = format!("{task}{ABANDONED_TASK_SUFFIX}");
        assert_eq!(
            usage_count(&pool, &abandoned_task).await,
            0,
            "finalize_ok must have disarmed Drop — no phantom _abandoned row"
        );
    }

    /// **Exactly-once (billing-correctness fix):** two independent finalize
    /// calls for the SAME `pending_id` — modelling
    /// [`StreamUsageGuard`]'s Drop-triggered backstop racing an explicit
    /// `finalize_ok`/`finalize_err` for the same call — must write exactly
    /// ONE `llm_usage` row, never two. This is what the reordered
    /// [`finalize`] (delete gates the insert — see that function's own
    /// "Exactly-once" doc section) exists to guarantee; the guard's own
    /// `inner`-disarming is only an optimization layered on top of it, not
    /// this guarantee's actual source.
    #[tokio::test]
    async fn two_finalizes_for_the_same_pending_id_write_exactly_one_row_not_two() {
        let Some(pool) =
            pg_test_pool("two_finalizes_for_the_same_pending_id_write_exactly_one_row_not_two")
                .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_billing_test_exactly_once_{}", Uuid::new_v4());

        let pending_id = reserve(
            &pool,
            Some(account_id),
            None,
            Some(graph_id),
            &task,
            DEFAULT_PROVIDER,
            "mock-model",
            10,
            PENDING_KIND_SYNC,
        )
        .await;
        assert!(pending_id.is_some());
        assert_eq!(pending_count(&pool, &task).await, 1);

        // First finalize: the "normal terminal path" — succeeds, deletes the
        // pending row, writes the real row.
        finalize(
            &pool,
            pending_id,
            Some(account_id),
            None,
            Some(graph_id),
            &task,
            DEFAULT_PROVIDER,
            "mock-model",
            10,
            34,
            1,
            2,
            Some(0.0042),
            None,
            None,
            None,
            None,
            None,
            Some("DeepInfra"),
        )
        .await;

        // Second finalize: the "Drop-triggered backstop" racing in
        // afterward — same pending_id, DELIBERATELY DIFFERENT actuals (so a
        // double-write would be detectable), must be a complete no-op.
        finalize(
            &pool,
            pending_id,
            Some(account_id),
            None,
            Some(graph_id),
            &task,
            DEFAULT_PROVIDER,
            "mock-model",
            999,
            999,
            0,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("Fireworks"),
        )
        .await;

        assert_eq!(pending_count(&pool, &task).await, 0);
        assert_eq!(
            usage_count(&pool, &task).await,
            1,
            "exactly one llm_usage row — the second, racing finalize must NOT have written a \
             duplicate"
        );
        let output_tokens: i32 =
            sqlx::query_scalar("SELECT output_tokens FROM llm_usage WHERE task = $1")
                .bind(&task)
                .fetch_one(&pool)
                .await
                .expect("fetch output_tokens");
        assert_eq!(
            output_tokens, 34,
            "the row that landed must be the FIRST finalize's actuals, not the second racer's"
        );
        let upstream_provider: Option<String> =
            sqlx::query_scalar("SELECT upstream_provider FROM llm_usage WHERE task = $1")
                .bind(&task)
                .fetch_one(&pool)
                .await
                .expect("fetch upstream_provider");
        assert_eq!(
            upstream_provider.as_deref(),
            Some("DeepInfra"),
            "upstream_provider must also be the FIRST finalize's value ('DeepInfra'), never the \
             second racer's ('Fireworks') — exactly-once applies to every column, not just \
             output_tokens"
        );
    }

    /// **Negative control 2:** force finalize itself to fail — a REAL DB-level
    /// failure (the `source_journal_granularity` CHECK constraint from
    /// migration `0141`/`0149`'s pairing), not a mock — and confirm the
    /// pending row SURVIVES rather than being silently lost. This is the
    /// entire point of separating reserve from finalize into two
    /// transactions: a failure inside finalize cannot retroactively
    /// un-commit reserve's own, already-committed transaction.
    #[tokio::test]
    async fn finalize_failure_leaves_the_pending_row_intact() {
        let Some(pool) = pg_test_pool("finalize_failure_leaves_the_pending_row_intact").await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr4_test_finalize_fail_{}", Uuid::new_v4());

        let pending_id = reserve(
            &pool,
            Some(account_id),
            None,
            Some(graph_id),
            &task,
            DEFAULT_PROVIDER,
            "mock-model",
            10,
            PENDING_KIND_SYNC,
        )
        .await;
        assert!(pending_id.is_some());
        assert_eq!(pending_count(&pool, &task).await, 1);

        // A deliberately invalid granularity violates the CHECK constraint —
        // the llm_usage INSERT fails, so finalize returns before ever
        // attempting the pending-row DELETE.
        finalize(
            &pool,
            pending_id,
            Some(account_id),
            None,
            Some(graph_id),
            &task,
            DEFAULT_PROVIDER,
            "mock-model",
            10,
            5,
            0,
            0,
            None,
            None,
            None,
            Some(Utc::now()),
            Some("not_a_real_granularity"),
            None,
            None,
        )
        .await;

        assert_eq!(
            pending_count(&pool, &task).await,
            1,
            "the pending row must survive a failed finalize — that is the whole point"
        );
        assert_eq!(
            usage_count(&pool, &task).await,
            0,
            "the CHECK violation must have blocked the llm_usage insert"
        );
    }

    /// A failed pending-row DELETE must NOT cost us the `llm_usage` row.
    ///
    /// Postgres aborts the whole transaction on any statement error (25P02), so
    /// without the SAVEPOINT around the delete, a warn-and-continue would
    /// poison the transaction and the COMMIT would fail — silently losing the
    /// accounting row finalize exists to write, in the one function the entire
    /// billing train funnels through. Induced here with a BEFORE DELETE trigger
    /// that raises, which is the only way to make a delete-by-primary-key fail
    /// on demand.
    #[tokio::test]
    async fn failed_pending_delete_still_commits_the_llm_usage_row() {
        let Some(pool) = pg_test_pool("failed_pending_delete_still_commits_the_llm_usage_row").await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr4_test_delete_fail_{}", Uuid::new_v4());

        let pending_id = reserve(
            &pool,
            Some(account_id),
            None,
            Some(graph_id),
            &task,
            DEFAULT_PROVIDER,
            "mock-model",
            10,
            PENDING_KIND_SYNC,
        )
        .await;
        assert_eq!(pending_count(&pool, &task).await, 1);

        // Two separate calls: sqlx sends these through the prepared-statement
        // path, which rejects multiple commands in one query (42601).
        sqlx::query(
            "CREATE OR REPLACE FUNCTION pr4_block_pending_delete() RETURNS trigger AS $$
             BEGIN RAISE EXCEPTION 'pr4 test: delete blocked'; END; $$ LANGUAGE plpgsql",
        )
        .execute(&pool)
        .await
        .expect("create the delete-blocking function");
        sqlx::query(
            "CREATE TRIGGER pr4_block_pending_delete BEFORE DELETE ON llm_usage_pending
             FOR EACH ROW EXECUTE FUNCTION pr4_block_pending_delete()",
        )
        .execute(&pool)
        .await
        .expect("install the delete-blocking trigger");

        finalize(
            &pool,
            pending_id,
            Some(account_id),
            None,
            Some(graph_id),
            &task,
            DEFAULT_PROVIDER,
            "mock-model",
            10,
            5,
            0,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

        sqlx::query("DROP TRIGGER IF EXISTS pr4_block_pending_delete ON llm_usage_pending;")
            .execute(&pool)
            .await
            .expect("drop the trigger");

        assert_eq!(
            usage_count(&pool, &task).await,
            1,
            "the llm_usage row MUST still commit — the savepoint is what confines the failed \
             delete instead of letting it poison the transaction and discard real accounting data"
        );
        assert_eq!(
            pending_count(&pool, &task).await,
            1,
            "the pending row survives (the delete was rolled back to the savepoint) and ages \
             into a false-positive orphan for PR6's sweeper — the safe direction"
        );
    }

    /// Task item (f): `workers::digest`'s `regenerate` may call
    /// `try_digest_completion` more than once per unit of work (primary +
    /// truncation/parse retry) — two successful finalizes under the SAME
    /// `source_job_id` must SUM (two `llm_usage` rows), never collide.
    /// There is deliberately no unique constraint on `source_job_id`.
    #[tokio::test]
    async fn two_finalizes_under_the_same_source_job_id_sum_rather_than_collide() {
        let Some(pool) =
            pg_test_pool("two_finalizes_under_the_same_source_job_id_sum_rather_than_collide").await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let task = format!("pr4_test_digest_retry_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id))
            .with_source_job(job_id);
        let llm = MockLlm { fail: false };

        metered_complete_via_stream(&llm, req(), Some(&ctx), &task)
            .await
            .expect("primary call");
        metered_complete_via_stream(&llm, req(), Some(&ctx), &task)
            .await
            .expect("retry call");

        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE source_job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .expect("count rows for this job_id");
        assert_eq!(n, 2, "two calls for the same job must sum, not collide");
    }

    /// The journal-compile axis (`source_journal_period`/`_granularity`)
    /// round-trips through finalize — PR9 groups on this triple instead of
    /// `source_job_id` for `journal_sweep`'s two call sites, which have no
    /// `jobs` row at all.
    #[tokio::test]
    async fn journal_axis_round_trips_through_finalize() {
        let Some(pool) = pg_test_pool("journal_axis_round_trips_through_finalize").await else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        // A FIXED, hour-aligned literal, deliberately not `Utc::now()`, for two
        // reasons. Semantically: a journal period is always a period boundary
        // (`compile_hour`'s `hour_start`, `compile_tier_group`'s `period_start`)
        // — never an arbitrary instant, so `now()` tested a shape the column
        // never actually holds. Mechanically: `Utc::now()` made this a
        // PLATFORM-DEPENDENT FLAKE. chrono carries nanosecond precision on
        // Linux while Postgres `TIMESTAMPTZ` stores microseconds, so the
        // round-trip truncated the sub-microsecond digits and this equality
        // failed in CI while passing on macOS, whose coarser clock granularity
        // hid the problem entirely.
        let period = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 13, 17, 0, 0)
            .single()
            .expect("valid fixed journal hour");
        let task = format!("pr4_test_journal_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id))
            .with_journal(period, "hour");
        let llm = MockLlm { fail: false };

        metered_complete(&llm, req(), Some(&ctx), &task)
            .await
            .expect("mock call");

        let row: (Option<DateTime<Utc>>, Option<String>) = sqlx::query_as(
            "SELECT source_journal_period, source_journal_granularity FROM llm_usage WHERE task = $1",
        )
        .bind(&task)
        .fetch_one(&pool)
        .await
        .expect("fetch the journal row");
        assert_eq!(row.0, Some(period));
        assert_eq!(row.1, Some("hour".to_string()));
    }

    /// `ctx: None` must be a complete no-op on both the reserve and finalize
    /// sides — no pending row, no llm_usage row, and the call still succeeds
    /// (mirrors every pool-less call site).
    #[tokio::test]
    async fn none_context_skips_reserve_and_finalize_entirely() {
        let Some(pool) = pg_test_pool("none_context_skips_reserve_and_finalize_entirely").await
        else {
            return;
        };
        let task = format!("pr4_test_none_ctx_{}", Uuid::new_v4());
        let llm = MockLlm { fail: false };

        let resp = metered_complete(&llm, req(), None, &task)
            .await
            .expect("None context must still call the provider");
        assert_eq!(resp.model, "mock-served-model");

        assert_eq!(pending_count(&pool, &task).await, 0);
        assert_eq!(usage_count(&pool, &task).await, 0);
    }

    // ── Billing Phase 1: `attributed_user_id` ───────────────────────────────
    //
    // These exercise the REAL production path (`metered_complete`/`record`,
    // never a hand-typed copy of `finalize`'s INSERT) and read the column
    // back — per the founder's standing lesson (`billing::entitlement`'s own
    // module doc is the worked example): a test that re-types the query it
    // claims to prove is testing itself. See this module's Billing Phase 1
    // negative control below for the sabotage-and-confirm-red proof that
    // these tests actually exercise `finalize`'s real bind, not a vacuous
    // assertion.

    /// A REQUEST-PATH-shaped call — `.with_user(x).with_attributed_user(x)`,
    /// the exact construction every one of the six real request-path call
    /// sites uses (`routes::queries`/`routes::nodes`/`routes::transcript`/
    /// `services::ask`) — finalizes an `llm_usage` row carrying the acting
    /// member's id in `attributed_user_id`.
    #[tokio::test]
    async fn attributed_user_id_round_trips_through_a_request_path_shaped_call() {
        let Some(pool) =
            pg_test_pool("attributed_user_id_round_trips_through_a_request_path_shaped_call").await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let acting_user = Uuid::new_v4();
        let task = format!("pr_billing_attr_request_{}", Uuid::new_v4());
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id))
            .with_user(acting_user)
            .with_attributed_user(acting_user);
        let llm = MockLlm { fail: false };

        metered_complete(&llm, req(), Some(&ctx), &task)
            .await
            .expect("mock call succeeds");

        let attributed: Option<Uuid> =
            sqlx::query_scalar("SELECT attributed_user_id FROM llm_usage WHERE task = $1")
                .bind(&task)
                .fetch_one(&pool)
                .await
                .expect("fetch the finalized row");
        assert_eq!(
            attributed,
            Some(acting_user),
            "a request-path call must land its acting member's id in attributed_user_id"
        );
    }

    /// A JOURNAL-SWEEP-shaped call — `user_id: None` (no request context to
    /// scope to, same as `journal_sweep.rs`'s own comment on both its
    /// `metered_complete` calls) but `.with_attributed_user(owner)`
    /// populated from the dirty-hour/dirty-group's own non-optional
    /// `user_id` field, the exact construction `journal_sweep::compile_hour`/
    /// `compile_tier_group` use — finalizes an `llm_usage` row attributed to
    /// the ocean owner despite carrying no RLS scope-selector `user_id` at
    /// all.
    #[tokio::test]
    async fn attributed_user_id_round_trips_through_a_journal_sweep_shaped_call() {
        let Some(pool) =
            pg_test_pool("attributed_user_id_round_trips_through_a_journal_sweep_shaped_call")
                .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let task = format!("pr_billing_attr_journal_{}", Uuid::new_v4());
        let period = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 14, 9, 0, 0)
            .single()
            .expect("valid fixed journal hour");
        let ctx = UsageContext::new(pool.clone(), Some(account_id), Some(graph_id))
            .with_attributed_user(owner)
            .with_journal(period, "hour");
        assert_eq!(
            ctx.user_id, None,
            "a journal-sweep-shaped ctx carries NO scope-selector user_id — sanity-checking the \
             test's own setup before trusting what it proves"
        );
        let llm = MockLlm { fail: false };

        metered_complete(&llm, req(), Some(&ctx), &task)
            .await
            .expect("mock call succeeds");

        let attributed: Option<Uuid> =
            sqlx::query_scalar("SELECT attributed_user_id FROM llm_usage WHERE task = $1")
                .bind(&task)
                .fetch_one(&pool)
                .await
                .expect("fetch the finalized row");
        assert_eq!(
            attributed,
            Some(owner),
            "a journal-sweep-shaped call must attribute to the ocean owner even with no RLS \
             scope-selector user_id"
        );
    }

    /// A WORKER-SITE-shaped call — [`record`], the exact function
    /// `extract`/`extract_relationships`/`extract_batch`'s cache-hit sites
    /// call, with no `attributed_user_id` supplied (`None`, exactly what
    /// every one of those sites passes today because none of them has a
    /// single member in scope) — stores NULL and does not error.
    #[tokio::test]
    async fn record_with_no_member_in_scope_stores_null_attributed_user_id() {
        let Some(pool) =
            pg_test_pool("record_with_no_member_in_scope_stores_null_attributed_user_id").await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let task = format!("pr_billing_attr_worker_{}", Uuid::new_v4());

        record(
            &pool,
            Some(account_id),
            None,
            Some(graph_id),
            &task,
            "mock-model",
            0,
            0,
            0,
            0,
            None,
        )
        .await;

        assert_eq!(
            usage_count(&pool, &task).await,
            1,
            "record() must not error or skip the row just because attributed_user_id is None"
        );
        let attributed: Option<Uuid> =
            sqlx::query_scalar("SELECT attributed_user_id FROM llm_usage WHERE task = $1")
                .bind(&task)
                .fetch_one(&pool)
                .await
                .expect("fetch the recorded row");
        assert_eq!(
            attributed, None,
            "no member was in scope for this worker site — attributed_user_id must be NULL"
        );
    }

    /// **The RLS negative control (billing Phase 1's own "kill test"):**
    /// setting `attributed_user_id` must NEVER change which `begin_*` scope
    /// function `finalize` opens — scope selection is driven ENTIRELY by
    /// `user_id`/`account_id`, exactly as before this PR. Proven
    /// BEHAVIORALLY, not by re-reading `finalize`'s own `match` as prose: a
    /// `BEFORE INSERT` trigger on `llm_usage`, scoped (via a `WHEN` clause
    /// baked with this test's own two task labels, never firing for any
    /// OTHER concurrently-running test's rows) to just the two calls below,
    /// captures the session's `app.user_id`/`app.tenant_actor` GUCs into a
    /// side table at the exact moment the real production INSERT runs — so
    /// this test observes the SAME transaction `finalize` actually opened,
    /// not an assertion on its source code.
    ///
    /// * Case A: `Some(account_id)` + `user_id: None` + `attributed_user_id:
    ///   Some(x)` must open [`crate::tenancy::scope::begin_account_scoped`]
    ///   (never `begin_tenant_scoped`) — `app.user_id` must stay unset even
    ///   though a real member id was populated in `attributed_user_id`. If a
    ///   future edit ever wires `attributed_user_id` into the scope-selection
    ///   `match` (e.g. `user_id.or(attributed_user_id)`), `app.user_id` comes
    ///   back equal to `attributed`'s text form instead of empty, and this
    ///   assertion catches it.
    /// * Case B: `None`-account arm must open
    ///   [`crate::tenancy::scope::begin_tenant_system_scoped_without_account`]
    ///   (`app.tenant_actor = 'system'`), never `begin_tenant_scoped`,
    ///   regardless of `attributed_user_id`.
    #[tokio::test]
    async fn attributed_user_id_never_changes_which_scope_function_finalize_opens() {
        let Some(pool) = pg_test_pool(
            "attributed_user_id_never_changes_which_scope_function_finalize_opens",
        )
        .await
        else {
            return;
        };
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let attributed = Uuid::new_v4();
        let task_a = format!("pr_billing_scope_a_{}", Uuid::new_v4());
        let task_b = format!("pr_billing_scope_b_{}", Uuid::new_v4());
        let suffix = Uuid::new_v4().simple().to_string();
        let capture_table = format!("pr_billing_scope_capture_{suffix}");
        let trigger_fn = format!("pr_billing_capture_scope_{suffix}");
        let trigger_name = format!("pr_billing_capture_scope_trigger_{suffix}");

        sqlx::query(&format!(
            "CREATE TABLE {capture_table} (task TEXT, app_user_id TEXT, app_tenant_actor TEXT)"
        ))
        .execute(&pool)
        .await
        .expect("create the capture table");
        sqlx::query(&format!(
            "CREATE OR REPLACE FUNCTION {trigger_fn}() RETURNS trigger AS $$
             BEGIN
                 INSERT INTO {capture_table} (task, app_user_id, app_tenant_actor)
                 VALUES (NEW.task, current_setting('app.user_id', true),
                         current_setting('app.tenant_actor', true));
                 RETURN NEW;
             END; $$ LANGUAGE plpgsql"
        ))
        .execute(&pool)
        .await
        .expect("create the capture trigger function");
        // The `WHEN` clause is what makes this trigger inert for every OTHER
        // concurrently-running test's inserts into the SAME shared
        // `llm_usage` table — it only ever fires for these two literal task
        // labels, baked in at CREATE TRIGGER time (both are fresh
        // `Uuid::new_v4()`-suffixed strings, so no other test can collide).
        sqlx::query(&format!(
            "CREATE TRIGGER {trigger_name} BEFORE INSERT ON llm_usage
             FOR EACH ROW WHEN (NEW.task IN ('{task_a}', '{task_b}'))
             EXECUTE FUNCTION {trigger_fn}()"
        ))
        .execute(&pool)
        .await
        .expect("install the capture trigger");

        finalize(
            &pool,
            None,
            Some(account_id),
            None,
            Some(graph_id),
            &task_a,
            DEFAULT_PROVIDER,
            "mock-model",
            5,
            5,
            0,
            0,
            None,
            None,
            None,
            None,
            None,
            Some(attributed),
            None,
        )
        .await;

        finalize(
            &pool,
            None,
            None,
            None,
            Some(graph_id),
            &task_b,
            DEFAULT_PROVIDER,
            "mock-model",
            5,
            5,
            0,
            0,
            None,
            None,
            None,
            None,
            None,
            Some(attributed),
            None,
        )
        .await;

        // Read BOTH captures, then tear the trigger down, and only THEN
        // assert. The assertions below are the whole point of this test — it
        // goes red exactly when a future edit wires `attributed_user_id` into
        // scope selection — so they are the assertions MOST likely to panic.
        // A panic before the cleanup would leave a `BEFORE INSERT` trigger on
        // `llm_usage` in the SHARED test database, silently corrupting every
        // later test that writes that table. Cleanup must not be reachable
        // only on the success path.
        let case_a: (Option<String>, Option<String>) = sqlx::query_as(&format!(
            "SELECT app_user_id, app_tenant_actor FROM {capture_table} WHERE task = $1"
        ))
        .bind(&task_a)
        .fetch_one(&pool)
        .await
        .expect("fetch case A's captured GUC state");
        let case_b: (Option<String>, Option<String>) = sqlx::query_as(&format!(
            "SELECT app_user_id, app_tenant_actor FROM {capture_table} WHERE task = $1"
        ))
        .bind(&task_b)
        .fetch_one(&pool)
        .await
        .expect("fetch case B's captured GUC state");

        sqlx::query(&format!("DROP TRIGGER IF EXISTS {trigger_name} ON llm_usage"))
            .execute(&pool)
            .await
            .expect("drop the capture trigger");
        sqlx::query(&format!("DROP FUNCTION IF EXISTS {trigger_fn}()"))
            .execute(&pool)
            .await
            .expect("drop the capture trigger function");
        sqlx::query(&format!("DROP TABLE IF EXISTS {capture_table}"))
            .execute(&pool)
            .await
            .expect("drop the capture table");

        assert!(
            case_a.0.as_deref().unwrap_or("").is_empty(),
            "Some(account_id)/user_id:None must open begin_account_scoped — app.user_id must stay \
             unset even though attributed_user_id was populated, got {:?}",
            case_a.0
        );
        assert_ne!(
            case_a.0.as_deref(),
            Some(attributed.to_string().as_str()),
            "app.user_id must NEVER be set from attributed_user_id — that is the exact \
             regression this control exists to catch"
        );
        assert!(
            case_b.0.as_deref().unwrap_or("").is_empty(),
            "the None-account arm must open begin_tenant_system_scoped_without_account — \
             app.user_id must stay unset, got {:?}",
            case_b.0
        );
        assert_eq!(
            case_b.1.as_deref(),
            Some("system"),
            "the None-account arm must set app.tenant_actor='system', got {:?}",
            case_b.1
        );
    }
}

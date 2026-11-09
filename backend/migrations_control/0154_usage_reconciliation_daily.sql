-- Migration: 0154_usage_reconciliation_daily
-- Description: usage-metering PR11 of the 13-PR train
--   (`.scratch/usage-metering/plan.md` §1 PR11) — the daily `llm_usage` vs
--   `query_history` cross-check's persisted output, one row per (day,
--   tenant), mirroring `platform_metrics_daily`'s exact shape (migration
--   `0095_platform_metrics.sql`) for the identical reason: a cross-tenant
--   operational fact that cannot be recomputed later once source rows age
--   out, keyed the same way (`tenant` = `'__consumer__'` for the shared
--   consumer plane, else the literal `clerk_org_id`), upserted intraday, and
--   carrying its own `computed_at` staleness column an operator can poll —
--   see `backend/src/workers/usage_reconciliation.rs`'s module doc for the
--   exact query, copied from `platform_rollup.rs`'s own documented one
--   (`SELECT tenant FROM platform_metrics_daily WHERE day = current_date AND
--   computed_at < now() - interval '2 hours'`).
--
--   ── Numbering note ──
--   Live heads at authoring time: control `0152` (PR9, merged), tenant
--   `0056`. Siblings PR10 and PR12 are independently in flight and may claim
--   `0153`/further numbers before this PR merges — per plan §0's "Numbering
--   reality" (assigned at MERGE time, not authoring time), this migration
--   took `0153` originally; sibling PR10 merged first and claimed it, so this
--   renumbered to `0154` at merge time exactly as plan §0 anticipates; `scripts/check_migrations.sh`
--   may report a gap or a renumber-on-merge need once the coordinator lands
--   the sibling PRs. That is expected, not a defect here — do not renumber
--   unilaterally.
--
--   ── Why a new table, not a new column on an existing one ──
--   The task brief says "check first whether an existing table already
--   carries a suitable column" before adding one. `credit_balances.updated_at`
--   (migration `0150_credit_ledger.sql`) is the closest candidate and was
--   rejected: it is PER-ACCOUNT balance-write freshness, not a per-(day,
--   tenant) reconciliation-PASS freshness — a tenant with zero credit_ledger
--   activity today would never touch it, so it cannot answer "did today's
--   reconciliation pass run for this tenant" the way this table's own
--   `computed_at` can. `platform_metrics_daily` itself was also considered
--   and rejected: its PRIMARY KEY and column set are an already-shipped,
--   already-consumed contract (nightly atom/reef/session/token counts);
--   bolting reconciliation-specific columns onto it would conflate two
--   unrelated rollups sharing only a `(day, tenant)` grain, and a partial
--   write by one job could shadow the other's `computed_at`.
--
--   ── Columns ──
--   `query_history_rows` — every `query_history` row created that UTC day,
--   this (day, tenant)'s total, informational.
--   `query_history_missing_usage` — the genuine cross-check anomaly: rows
--   among the above where `query_history.model IS NOT NULL` (i.e. synthesis
--   actually ran — see migration `0117`/`0120`'s header: a NULL model means
--   retrieval-only/abstention/no-synthesis-turn, which legitimately never
--   writes `llm_usage`, so counting those would manufacture false anomalies)
--   AND no `llm_usage` row carries a matching `source_query_id` — the choke
--   point (PR4) should have written one for every synthesis turn; a nonzero
--   count here means it did not.
--   `llm_usage_orphan_query_rows` — the reverse direction: `llm_usage` rows
--   with `source_query_id` set that name no `query_history.id` in this
--   window at all — a caller minted a query id and never persisted the turn
--   it belongs to.
--   `unknown_model_rows` — total `llm_usage` ROW COUNT this day whose model
--   did not resolve to a specific `model_rates` row (excludes the
--   `billing::rates::UNATTRIBUTED_SENTINEL` deliberately, per plan §3 item
--   4/§2 item 7 — the sentinel routes to the blend on purpose and is not an
--   anomaly). This is a raw count for the historical record; the actual
--   ALERT is emitted per-DISTINCT-`(provider, model)` pair, deduped via
--   `billing::unknown_model_alert::UnknownModelAlertTracker`, never per row
--   — see that module's doc for why a per-row alert is exactly the noise
--   this codebase has already been burned by once (`pool_failure_severity`'s
--   own history).
--   `cache_hit_rows` / `unthreaded_rows` — the SAME three-way classification
--   `workers::billing_derivation::classify_unthreaded_usage` already applies
--   at its own ~5-minute cadence (PR9); this table re-derives the identical
--   day-scoped totals for the historical record ONLY — it does not
--   independently alert on `unthreaded_rows`, since PR9 already owns that
--   alert at a much tighter cadence and duplicating it here would be a
--   second, redundant, undeduped signal for the same condition.
--   `computed_at` — set on every upsert, `DEFAULT now()`, the staleness
--   column: `WHERE day = current_date AND computed_at < now() - interval
--   'N hours'` finds every tenant a reconciliation pass silently failed to
--   refresh today, same query shape platform_rollup's own module doc
--   documents for `platform_metrics_daily`.
--
--   PRIMARY KEY (day, tenant): the worker upserts (`ON CONFLICT (day,
--   tenant) DO UPDATE`) since "today"'s row is recomputed on every tick
--   until the day is over, matching `platform_metrics_daily`'s own PK note.
--
--   CONTROL-ONLY, no tenant-tree counterpart, ever — same posture as
--   `platform_metrics_daily`/`platform_active_users`/`credit_ledger`/
--   `credit_balances`/`model_rates`: a cross-tenant rollup owned by the
--   control plane, which is where the two-pool worker (COLLECT from `t.pool`
--   per tenant, WRITE to `control`, same split as `platform_rollup`/
--   `billing_derivation`) writes it regardless of which plane it read from.
--   Added to `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` in this
--   same PR. No RLS: like `platform_metrics_daily`, this table has no
--   `account_id` column at all (`tenant` is an operator-facing TEXT
--   discriminator, not a per-account boundary), so it carries no
--   `account_isolation` policy and needs no `scripts/rls_coverage.manifest`
--   entry for ITS OWN reads/writes (the worker file IS listed there, because
--   it separately reads the RLS-covered `llm_usage`/`query_history` tables
--   on the tenant side of the split).
--
-- Depends on: 0095_platform_metrics.sql (the `platform_metrics_daily`
--   precedent this table's shape and PK convention directly copies),
--   0141_llm_usage_provider_cost.sql (source_job_id/source_query_id/
--   source_journal_period, the join keys this cross-check reads),
--   0019_query_history.sql (query_history.account_id/model).
-- Down:
--   DROP TABLE IF EXISTS usage_reconciliation_daily;

CREATE TABLE usage_reconciliation_daily (
    day                          DATE        NOT NULL,
    tenant                       TEXT        NOT NULL,  -- '__consumer__', else clerk_org_id
    query_history_rows           BIGINT      NOT NULL DEFAULT 0,
    query_history_missing_usage  BIGINT      NOT NULL DEFAULT 0,
    llm_usage_orphan_query_rows  BIGINT      NOT NULL DEFAULT 0,
    unknown_model_rows           BIGINT      NOT NULL DEFAULT 0,
    cache_hit_rows                BIGINT     NOT NULL DEFAULT 0,
    unthreaded_rows               BIGINT     NOT NULL DEFAULT 0,
    computed_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (day, tenant)
);

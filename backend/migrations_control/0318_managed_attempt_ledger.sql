-- COR-21 / COR-88 / COR-89: the managed attempt ledger, credits from its
-- receipts, and the cloud embedding rate.
--
-- One receipt per provider HTTP attempt, reserved before the send and settled
-- once after it. One budget row per (account, purpose, UTC day); a guarded
-- UPDATE keeps reserved + charged <= limit. The shared block below is paired
-- byte-for-byte with tenant 0149; only the RLS policies differ by plane.
-- Control also gains the credit axis keyed by receipt and the embedding rate.

CREATE TABLE managed_budget_periods (
  account_id UUID NOT NULL,
  purpose TEXT NOT NULL CHECK (purpose IN ('source_extraction','cloud_embedding')),
  period_start DATE NOT NULL,
  limit_micro_usd BIGINT NOT NULL CHECK (limit_micro_usd >= 0),
  reserved_micro_usd BIGINT NOT NULL DEFAULT 0 CHECK (reserved_micro_usd >= 0),
  charged_micro_usd BIGINT NOT NULL DEFAULT 0 CHECK (charged_micro_usd >= 0),
  attempts BIGINT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  PRIMARY KEY (account_id, purpose, period_start)
);
CREATE TABLE managed_attempt_receipts (
  receipt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id UUID NOT NULL, purpose TEXT NOT NULL, period_start DATE NOT NULL,
  graph_id UUID NOT NULL, unit_id UUID, source_version_id UUID, job_id UUID,
  attributed_user_id UUID,
  logical_call UUID NOT NULL,
  attempt_no SMALLINT NOT NULL CHECK (attempt_no BETWEEN 1 AND 8),
  candidate_models TEXT[] NOT NULL CHECK (cardinality(candidate_models) >= 1),
  input_bound_tokens BIGINT NOT NULL CHECK (input_bound_tokens >= 0),
  max_tokens INT NOT NULL CHECK (max_tokens >= 0),
  reserved_micro_usd BIGINT NOT NULL CHECK (reserved_micro_usd > 0),
  state TEXT NOT NULL DEFAULT 'reserved'
    CHECK (state IN ('reserved','billed','not_generated','ambiguous','overrun')),
  settled_micro_usd BIGINT,
  served_model TEXT, upstream_provider TEXT, generation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), settled_at TIMESTAMPTZ,
  CONSTRAINT managed_receipt_open_shape CHECK ((state = 'reserved') = (settled_at IS NULL)
                                           AND (state = 'reserved') = (settled_micro_usd IS NULL)),
  CONSTRAINT managed_receipt_charge_by_state CHECK (
       state = 'reserved'
    OR (state = 'not_generated' AND settled_micro_usd = 0)
    OR (state = 'ambiguous'     AND settled_micro_usd = reserved_micro_usd)
    OR (state = 'billed'        AND settled_micro_usd BETWEEN 0 AND reserved_micro_usd)
    OR (state = 'overrun'       AND settled_micro_usd > reserved_micro_usd)),
  UNIQUE (logical_call, attempt_no),
  FOREIGN KEY (account_id, purpose, period_start) REFERENCES managed_budget_periods
);
CREATE INDEX managed_attempt_receipts_open ON managed_attempt_receipts (created_at) WHERE state = 'reserved';
CREATE INDEX managed_attempt_receipts_charged
  ON managed_attempt_receipts (account_id, settled_at, receipt_id) WHERE settled_micro_usd > 0;
ALTER TABLE corpus_enrichment_units
  ADD COLUMN charged_micro_usd BIGINT NOT NULL DEFAULT 0 CHECK (charged_micro_usd >= 0);

-- The one settlement rule. Invoker rights: a live settle runs under the
-- job's RLS scope. The state='reserved' guard makes a second settle a no-op.
CREATE FUNCTION managed_settle(p_receipt UUID, p_state TEXT, p_cost BIGINT,
                               p_model TEXT, p_upstream TEXT, p_generation TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
DECLARE r managed_attempt_receipts;
BEGIN
  IF p_state NOT IN ('billed','not_generated','ambiguous') THEN
    RAISE EXCEPTION 'bad settle state %', p_state;
  END IF;
  IF p_state = 'billed' AND (p_cost IS NULL OR p_cost < 0) THEN
    RAISE EXCEPTION 'a billed settle needs a cost';
  END IF;
  UPDATE managed_attempt_receipts SET
      state = CASE WHEN p_state = 'billed' AND p_cost > reserved_micro_usd THEN 'overrun' ELSE p_state END,
      settled_micro_usd = CASE p_state WHEN 'not_generated' THEN 0
                                       WHEN 'ambiguous' THEN reserved_micro_usd ELSE p_cost END,
      served_model = p_model, upstream_provider = p_upstream, generation_id = p_generation,
      settled_at = now()
   WHERE receipt_id = p_receipt AND state = 'reserved'
  RETURNING * INTO r;
  IF NOT FOUND THEN RETURN false; END IF;
  UPDATE managed_budget_periods
     SET reserved_micro_usd = reserved_micro_usd - r.reserved_micro_usd,
         charged_micro_usd  = charged_micro_usd  + r.settled_micro_usd
   WHERE account_id = r.account_id AND purpose = r.purpose AND period_start = r.period_start;
  IF r.unit_id IS NOT NULL THEN
    UPDATE corpus_enrichment_units
       SET charged_micro_usd = charged_micro_usd - r.reserved_micro_usd + r.settled_micro_usd
     WHERE unit_id = r.unit_id;
  END IF;
  RETURN true;
END $$;

-- The reaper runs with owner rights so one call covers every account on the
-- plane. SKIP LOCKED leaves receipts whose live settle is in progress alone.
CREATE FUNCTION managed_reap_stale(p_older_than INTERVAL)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE id UUID; n INTEGER := 0;
BEGIN
  FOR id IN SELECT receipt_id FROM managed_attempt_receipts
             WHERE state = 'reserved'
               AND created_at < now() - GREATEST(p_older_than, interval '10 minutes')
             ORDER BY created_at LIMIT 500 FOR UPDATE SKIP LOCKED LOOP
    IF managed_settle(id, 'ambiguous', NULL, NULL, NULL, NULL) THEN n := n + 1; END IF;
  END LOOP;
  RETURN n;
END $$;
REVOKE ALL ON FUNCTION managed_settle(UUID,TEXT,BIGINT,TEXT,TEXT,TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION managed_reap_stale(INTERVAL) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
  GRANT SELECT,INSERT,UPDATE ON managed_budget_periods,managed_attempt_receipts TO coralswarm_app;
  GRANT EXECUTE ON FUNCTION managed_settle(UUID,TEXT,BIGINT,TEXT,TEXT,TEXT) TO coralswarm_app;
  GRANT EXECUTE ON FUNCTION managed_reap_stale(INTERVAL) TO coralswarm_app;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
  GRANT SELECT,INSERT,UPDATE ON managed_budget_periods,managed_attempt_receipts TO coralswarm_tenant_app;
  GRANT EXECUTE ON FUNCTION managed_settle(UUID,TEXT,BIGINT,TEXT,TEXT,TEXT) TO coralswarm_tenant_app;
  GRANT EXECUTE ON FUNCTION managed_reap_stale(INTERVAL) TO coralswarm_tenant_app;
 END IF;
END $$;

-- Consumer content plane: account isolation, like every account-keyed table.
ALTER TABLE managed_budget_periods ENABLE ROW LEVEL SECURITY;
ALTER TABLE managed_attempt_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON managed_budget_periods
  USING (account_id = current_setting('app.account_id', true)::uuid)
  WITH CHECK (account_id = current_setting('app.account_id', true)::uuid);
CREATE POLICY account_isolation ON managed_attempt_receipts
  USING (account_id = current_setting('app.account_id', true)::uuid)
  WITH CHECK (account_id = current_setting('app.account_id', true)::uuid);

-- COR-88: one credit debit per charged receipt. Receipts live on each content
-- plane; billing_derivation reads them and writes here.
ALTER TABLE credit_ledger ADD COLUMN source_receipt_id UUID;
ALTER TABLE credit_ledger DROP CONSTRAINT credit_ledger_single_source_axis,
  ADD CONSTRAINT credit_ledger_single_source_axis CHECK (
      (CASE WHEN source_job_id     IS NOT NULL THEN 1 ELSE 0 END)
    + (CASE WHEN source_query_id   IS NOT NULL THEN 1 ELSE 0 END)
    + (CASE WHEN journal_period    IS NOT NULL THEN 1 ELSE 0 END)
    + (CASE WHEN source_receipt_id IS NOT NULL THEN 1 ELSE 0 END) <= 1);
CREATE UNIQUE INDEX idx_credit_ledger_source_receipt_id_unique
  ON credit_ledger (source_receipt_id) WHERE source_receipt_id IS NOT NULL;
ALTER TABLE credit_balances ADD COLUMN receipt_cursor_at TIMESTAMPTZ, ADD COLUMN receipt_cursor_id UUID;

-- Background spend debits the same credits, but running the allowance out in
-- the background must never block interactive Ask (Alex, 2026-10-14). These
-- running totals of the managed share let billing::enforcement leave it out
-- of the interactive decision. billing_derivation adds to them in the same
-- transaction as the debit.
ALTER TABLE credit_balances
  ADD COLUMN managed_debited_credits NUMERIC NOT NULL DEFAULT 0 CHECK (managed_debited_credits >= 0);
ALTER TABLE member_allowance_periods
  ADD COLUMN managed_consumed_credits NUMERIC NOT NULL DEFAULT 0 CHECK (managed_consumed_credits >= 0);

-- COR-89: OpenRouter text-embedding-3-small at its card rate, so query
-- embeddings stop pricing at the blend fallback.
INSERT INTO model_rates (provider, model_pattern, priority, input_per_million, output_per_million,
  cache_write_multiplier, cache_read_multiplier, effective_from, effective_to, is_blend_fallback)
VALUES ('openrouter', 'text-embedding-3-small', 0, 0.02, 0.0, 1.0, 1.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false);

-- Existing query debits remain immutable revision zero. Later usage appends
-- only the previously uncharged cost, with conversion pinned per query.
ALTER TABLE credit_ledger ADD COLUMN query_revision BIGINT NOT NULL DEFAULT 0
  CHECK (query_revision >= 0 AND (query_revision = 0 OR source_query_id IS NOT NULL));
DROP INDEX idx_credit_ledger_source_query_id_unique;
CREATE UNIQUE INDEX idx_credit_ledger_query_revision_unique
  ON credit_ledger (account_id, source_query_id, query_revision)
  WHERE source_query_id IS NOT NULL;
CREATE TABLE query_billing_state (
  account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  query_id UUID NOT NULL,
  baseline_cost_usd NUMERIC NOT NULL CHECK (baseline_cost_usd >= 0),
  accounted_cost_usd NUMERIC NOT NULL CHECK (accounted_cost_usd >= baseline_cost_usd),
  incremental_credits NUMERIC NOT NULL DEFAULT 0 CHECK (incremental_credits <= 0),
  reference_cost_per_credit NUMERIC NOT NULL CHECK (reference_cost_per_credit >= 0),
  next_revision BIGINT NOT NULL CHECK (next_revision >= 0),
  PRIMARY KEY (account_id, query_id)
);
ALTER TABLE query_billing_state ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON query_billing_state
  USING (account_id = current_setting('app.account_id', true)::uuid)
  WITH CHECK (account_id = current_setting('app.account_id', true)::uuid);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
  GRANT SELECT,INSERT,UPDATE ON query_billing_state TO coralswarm_app;
 END IF;
END $$;

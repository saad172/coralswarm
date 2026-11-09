-- COR-21: the managed attempt ledger on org tenant databases. Paired with
-- control 0318; the shared block is identical and only the policies differ.
-- Credits stay on control: billing_derivation reads these receipts.

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

-- One org per tenant DB. The ledger is written by the system actor; receipts
-- are readable wherever their graph is.
ALTER TABLE managed_budget_periods ENABLE ROW LEVEL SECURITY;
ALTER TABLE managed_attempt_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY member_ocean_isolation ON managed_budget_periods
  USING (tenant_rls_is_system()) WITH CHECK (tenant_rls_is_system());
CREATE POLICY member_ocean_isolation ON managed_attempt_receipts
  USING (tenant_rls_is_system() OR graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
  WITH CHECK (tenant_rls_is_system());

LOCK TABLE managed_attempt_receipts IN ACCESS EXCLUSIVE MODE;

ALTER TABLE managed_attempt_receipts
  ADD COLUMN protocol_version SMALLINT NOT NULL DEFAULT 0 CHECK (protocol_version IN (0,1)),
  ADD COLUMN dispatch_phase TEXT CHECK (dispatch_phase IN ('prepared','armed','cancelled_unsent')),
  ADD COLUMN content_plane TEXT,
  ADD COLUMN request_hash TEXT,
  ADD CONSTRAINT managed_dispatch_shape CHECK (
    (protocol_version=0 AND dispatch_phase IS NULL AND content_plane IS NULL AND request_hash IS NULL)
    OR (protocol_version=1 AND dispatch_phase IS NOT NULL AND content_plane IS NOT NULL
        AND request_hash ~ '^[0-9a-f]{64}$' AND attributed_user_id IS NOT NULL));

CREATE FUNCTION managed_dispatch_immutable() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.protocol_version,NEW.content_plane,NEW.request_hash,NEW.account_id,NEW.purpose,
     NEW.period_start,NEW.graph_id,NEW.unit_id,NEW.source_version_id,NEW.job_id,
     NEW.attributed_user_id,NEW.logical_call,NEW.attempt_no,NEW.candidate_models,
     NEW.input_bound_tokens,NEW.max_tokens,NEW.reserved_micro_usd)
   IS DISTINCT FROM
    (OLD.protocol_version,OLD.content_plane,OLD.request_hash,OLD.account_id,OLD.purpose,
     OLD.period_start,OLD.graph_id,OLD.unit_id,OLD.source_version_id,OLD.job_id,
     OLD.attributed_user_id,OLD.logical_call,OLD.attempt_no,OLD.candidate_models,
     OLD.input_bound_tokens,OLD.max_tokens,OLD.reserved_micro_usd)
 THEN RAISE EXCEPTION 'managed receipt binding is immutable'; END IF;
 IF OLD.protocol_version=1 AND NEW.dispatch_phase IS DISTINCT FROM OLD.dispatch_phase
    AND NOT (OLD.dispatch_phase='prepared' AND NEW.dispatch_phase IN ('armed','cancelled_unsent'))
 THEN RAISE EXCEPTION 'managed dispatch phase is terminal'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER managed_dispatch_immutable BEFORE UPDATE ON managed_attempt_receipts
 FOR EACH ROW EXECUTE FUNCTION managed_dispatch_immutable();

CREATE FUNCTION managed_lock_v1(p_receipt UUID) RETURNS BOOLEAN
 LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
DECLARE r managed_attempt_receipts;
BEGIN
 SELECT * INTO r FROM managed_attempt_receipts WHERE receipt_id=p_receipt AND protocol_version=1;
 IF NOT FOUND THEN RETURN false; END IF;
 PERFORM 1 FROM managed_budget_periods WHERE account_id=r.account_id AND purpose=r.purpose
   AND period_start=r.period_start FOR UPDATE;
 IF r.unit_id IS NOT NULL THEN
   PERFORM 1 FROM corpus_enrichment_units WHERE unit_id=r.unit_id FOR UPDATE;
 END IF;
 PERFORM 1 FROM managed_attempt_receipts WHERE receipt_id=p_receipt FOR UPDATE;
 RETURN true;
END $$;

CREATE FUNCTION managed_arm_v1(p_receipt UUID) RETURNS BOOLEAN
 LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
BEGIN
 IF NOT managed_lock_v1(p_receipt) THEN RETURN false; END IF;
 UPDATE managed_attempt_receipts SET dispatch_phase='armed'
  WHERE receipt_id=p_receipt AND protocol_version=1 AND dispatch_phase='prepared' AND state='reserved';
 RETURN FOUND;
END $$;

CREATE FUNCTION managed_settle_v1(p_receipt UUID,p_state TEXT,p_cost BIGINT,
                                p_model TEXT,p_upstream TEXT,p_generation TEXT)
 RETURNS BOOLEAN LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
DECLARE r managed_attempt_receipts;
BEGIN
 IF p_state NOT IN ('billed','not_generated','ambiguous') OR p_state IS NULL
    OR (p_state='billed' AND (p_cost IS NULL OR p_cost<0)) THEN
   RAISE EXCEPTION 'invalid managed settlement';
 END IF;
 IF NOT managed_lock_v1(p_receipt) THEN RETURN false; END IF;
 SELECT * INTO r FROM managed_attempt_receipts WHERE receipt_id=p_receipt;
 IF r.state<>'reserved' THEN RETURN false; END IF;
 IF r.dispatch_phase NOT IN ('armed','cancelled_unsent')
    OR (r.dispatch_phase='cancelled_unsent' AND p_state<>'not_generated') THEN
   RAISE EXCEPTION 'managed receipt has no dispatched or cancelled outcome';
 END IF;
 UPDATE managed_attempt_receipts SET
   state=CASE WHEN p_state='billed' AND p_cost>reserved_micro_usd THEN 'overrun' ELSE p_state END,
   settled_micro_usd=CASE p_state WHEN 'not_generated' THEN 0
      WHEN 'ambiguous' THEN reserved_micro_usd ELSE p_cost END,
   served_model=p_model,upstream_provider=p_upstream,generation_id=p_generation,settled_at=now()
  WHERE receipt_id=p_receipt RETURNING * INTO r;
 UPDATE managed_budget_periods SET reserved_micro_usd=reserved_micro_usd-r.reserved_micro_usd,
   charged_micro_usd=charged_micro_usd+r.settled_micro_usd
  WHERE account_id=r.account_id AND purpose=r.purpose AND period_start=r.period_start;
 IF r.unit_id IS NOT NULL THEN
   UPDATE corpus_enrichment_units SET
     charged_micro_usd=charged_micro_usd-r.reserved_micro_usd+r.settled_micro_usd
    WHERE unit_id=r.unit_id;
 END IF;
 RETURN true;
END $$;

CREATE FUNCTION managed_cancel_unsent_v1(p_receipt UUID) RETURNS BOOLEAN
 LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
DECLARE phase TEXT;
BEGIN
 IF NOT managed_lock_v1(p_receipt) THEN RETURN false; END IF;
 SELECT dispatch_phase INTO phase FROM managed_attempt_receipts WHERE receipt_id=p_receipt;
 IF phase='cancelled_unsent' THEN RETURN true; END IF;
 IF phase<>'prepared' THEN RETURN false; END IF;
 UPDATE managed_attempt_receipts SET dispatch_phase='cancelled_unsent' WHERE receipt_id=p_receipt;
 PERFORM managed_settle_v1(p_receipt,'not_generated',0,NULL,NULL,NULL);
 RETURN true;
END $$;

CREATE FUNCTION managed_reap_stale_v1(p_older_than INTERVAL) RETURNS INTEGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE id UUID; n INTEGER := 0;
BEGIN
 FOR id IN SELECT receipt_id FROM managed_attempt_receipts
   WHERE protocol_version=1 AND state='reserved'
     AND created_at<now()-GREATEST(p_older_than,interval '10 minutes')
   ORDER BY account_id,purpose,period_start,unit_id,receipt_id LIMIT 500 LOOP
   IF managed_cancel_unsent_v1(id) OR managed_settle_v1(id,'ambiguous',NULL,NULL,NULL,NULL)
   THEN n := n+1; END IF;
 END LOOP;
 RETURN n;
END $$;
REVOKE ALL ON FUNCTION managed_lock_v1(UUID),managed_arm_v1(UUID),managed_cancel_unsent_v1(UUID),
 managed_settle_v1(UUID,TEXT,BIGINT,TEXT,TEXT,TEXT),managed_reap_stale_v1(INTERVAL) FROM PUBLIC;
DO $$ DECLARE role_name TEXT; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
   EXECUTE format('GRANT EXECUTE ON FUNCTION managed_lock_v1(UUID),managed_arm_v1(UUID),managed_cancel_unsent_v1(UUID),managed_settle_v1(UUID,TEXT,BIGINT,TEXT,TEXT,TEXT),managed_reap_stale_v1(INTERVAL) TO %I',role_name);
 END IF; END LOOP;
END $$;

CREATE OR REPLACE FUNCTION managed_settle(p_receipt UUID, p_state TEXT, p_cost BIGINT,
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
   WHERE receipt_id = p_receipt AND state = 'reserved' AND protocol_version=0
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
CREATE OR REPLACE FUNCTION managed_reap_stale(p_older_than INTERVAL)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE id UUID; n INTEGER := 0;
BEGIN
  FOR id IN SELECT receipt_id FROM managed_attempt_receipts
             WHERE state = 'reserved' AND protocol_version=0
               AND created_at < now() - GREATEST(p_older_than, interval '10 minutes')
             ORDER BY created_at LIMIT 500 FOR UPDATE SKIP LOCKED LOOP
    IF managed_settle(id, 'ambiguous', NULL, NULL, NULL, NULL) THEN n := n + 1; END IF;
  END LOOP;
  RETURN n;
END $$;

CREATE FUNCTION managed_source_receipt_history() RETURNS trigger
 LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.protocol_version=0 AND NEW.purpose='source_extraction' THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='source extraction requires funded dispatch generation';
  END IF;
  RETURN NEW;
 END IF;
 IF OLD.purpose='source_extraction' THEN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'source receipt history cannot be deleted'; END IF;
  IF (NEW.receipt_id,NEW.created_at) IS DISTINCT FROM (OLD.receipt_id,OLD.created_at)
   OR (OLD.state<>'reserved' AND NEW IS DISTINCT FROM OLD) THEN
   RAISE EXCEPTION 'source receipt identity and terminal history are immutable';
  END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION managed_source_receipt_history() FROM PUBLIC;
CREATE TRIGGER managed_source_receipt_history BEFORE INSERT OR UPDATE OR DELETE
 ON managed_attempt_receipts FOR EACH ROW EXECUTE FUNCTION managed_source_receipt_history();
CREATE FUNCTION managed_refuse_truncate() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN RAISE EXCEPTION 'protected financial and source history cannot be truncated'; END $$;
REVOKE ALL ON FUNCTION managed_refuse_truncate() FROM PUBLIC;
CREATE TRIGGER managed_receipts_no_truncate BEFORE TRUNCATE ON managed_attempt_receipts
 FOR EACH STATEMENT EXECUTE FUNCTION managed_refuse_truncate();

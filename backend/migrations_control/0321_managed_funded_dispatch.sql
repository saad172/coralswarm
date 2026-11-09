LOCK TABLE accounts,credit_balances,credit_conversion,credit_ledger,credit_packs,grant_seat_tiers,grants,
 managed_attempt_receipts,member_allowance_periods,query_billing_state,
 model_rates,plans,seat_tiers,stripe_subscriptions,stripe_webhook_events,tenant_databases IN ACCESS EXCLUSIVE MODE;

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

CREATE TABLE managed_credit_reservations (
 plane TEXT NOT NULL CHECK (plane='consumer' OR plane LIKE 'org:%'),
 receipt_id UUID NOT NULL,
 account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
 payer_id UUID NOT NULL,graph_id UUID NOT NULL,logical_call UUID NOT NULL,
 request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
 bound_micro_usd BIGINT NOT NULL CHECK (bound_micro_usd>0),
 bound_credits NUMERIC CHECK (bound_credits>0),
 member_period_id UUID REFERENCES member_allowance_periods(id),
 conversion_effective_from TIMESTAMPTZ,
 reference_cost_per_credit NUMERIC CHECK (reference_cost_per_credit>0),
 funding_basis TEXT CHECK (funding_basis IN ('allowance','pool')),
 state TEXT NOT NULL DEFAULT 'held' CHECK (state IN ('held','settled','released')),
 settled_micro_usd BIGINT CHECK (settled_micro_usd>=0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),settled_at TIMESTAMPTZ,
 PRIMARY KEY(plane,receipt_id),UNIQUE(receipt_id),
 CHECK ((state='held')=(settled_at IS NULL) AND (state='held')=(settled_micro_usd IS NULL)),
 CHECK (state='released' OR (bound_credits IS NOT NULL AND conversion_effective_from IS NOT NULL
     AND reference_cost_per_credit IS NOT NULL AND funding_basis IS NOT NULL)),
 CHECK (state<>'released' OR settled_micro_usd=0),
 CHECK (funding_basis IS DISTINCT FROM 'allowance' OR member_period_id IS NOT NULL)
);
CREATE INDEX managed_credit_reservations_outstanding
 ON managed_credit_reservations(account_id,member_period_id) INCLUDE(bound_credits) WHERE state='held';
ALTER TABLE managed_credit_reservations ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON managed_credit_reservations
 USING(account_id=current_setting('app.account_id',true)::uuid)
 WITH CHECK(account_id=current_setting('app.account_id',true)::uuid);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
 GRANT SELECT,INSERT,UPDATE ON managed_credit_reservations TO coralswarm_app;
 END IF;
END $$;

CREATE INDEX managed_credit_reservations_call ON managed_credit_reservations(account_id,plane,logical_call,created_at,receipt_id);

CREATE FUNCTION managed_credit_binding_immutable() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.plane,NEW.receipt_id,NEW.account_id,NEW.payer_id,NEW.graph_id,NEW.logical_call,
     NEW.request_hash,NEW.bound_micro_usd,NEW.bound_credits,NEW.member_period_id,
     NEW.conversion_effective_from,NEW.reference_cost_per_credit,NEW.funding_basis,NEW.created_at)
   IS DISTINCT FROM
    (OLD.plane,OLD.receipt_id,OLD.account_id,OLD.payer_id,OLD.graph_id,OLD.logical_call,
     OLD.request_hash,OLD.bound_micro_usd,OLD.bound_credits,OLD.member_period_id,
     OLD.conversion_effective_from,OLD.reference_cost_per_credit,OLD.funding_basis,OLD.created_at)
   OR (OLD.state<>'held' AND NEW IS DISTINCT FROM OLD)
 THEN RAISE EXCEPTION 'managed financial binding is immutable'; END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION managed_credit_binding_immutable() FROM PUBLIC;
CREATE TRIGGER managed_credit_binding_immutable BEFORE UPDATE ON managed_credit_reservations
 FOR EACH ROW EXECUTE FUNCTION managed_credit_binding_immutable();

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

DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_billing_entry_owner') THEN
  CREATE ROLE coralswarm_billing_entry_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_billing_entry_owner'
  AND (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls))
  OR EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.roleid
    WHERE r.rolname='coralswarm_billing_entry_owner') THEN
  RAISE EXCEPTION 'billing entry owner must be non-assumable and unprivileged';
 END IF;
END $$;
CREATE SCHEMA billing_private AUTHORIZATION coralswarm_billing_entry_owner;
REVOKE ALL ON SCHEMA billing_private FROM PUBLIC;
CREATE TABLE billing_private.account_entry(account_id UUID PRIMARY KEY,top_xid xid8 NOT NULL);
ALTER TABLE billing_private.account_entry OWNER TO coralswarm_billing_entry_owner;
REVOKE ALL ON billing_private.account_entry FROM PUBLIC;
GRANT SELECT ON accounts,grants TO coralswarm_billing_entry_owner;
GRANT SELECT,DELETE ON grant_seat_tiers TO coralswarm_billing_entry_owner;
CREATE FUNCTION billing_enter_account(p_account UUID) RETURNS VOID
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF nullif(current_setting('app.account_id',true),'')::uuid IS DISTINCT FROM p_account THEN
  RAISE EXCEPTION 'financial account scope mismatch';
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('billing_period_rotation:'||p_account::text,0));
 IF NOT EXISTS(SELECT 1 FROM public.accounts WHERE id=p_account) THEN
  RAISE EXCEPTION 'financial account does not exist';
 END IF;
 INSERT INTO billing_private.account_entry(account_id,top_xid) VALUES(p_account,pg_current_xact_id())
 ON CONFLICT(account_id) DO UPDATE SET top_xid=EXCLUDED.top_xid;
END $$;
CREATE FUNCTION billing_require_current_entry(p_account UUID) RETURNS VOID
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF nullif(current_setting('app.account_id',true),'')::uuid IS DISTINCT FROM p_account
  OR NOT EXISTS(SELECT 1 FROM billing_private.account_entry
    WHERE account_id=p_account AND top_xid=pg_current_xact_id()) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='compatible financial entry required';
 END IF;
END $$;
ALTER FUNCTION billing_enter_account(UUID) OWNER TO coralswarm_billing_entry_owner;
ALTER FUNCTION billing_require_current_entry(UUID) OWNER TO coralswarm_billing_entry_owner;
REVOKE ALL ON FUNCTION billing_enter_account(UUID),billing_require_current_entry(UUID) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION billing_enter_account(UUID) TO CURRENT_USER;
DO $$ DECLARE role_name TEXT; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
   EXECUTE format('GRANT EXECUTE ON FUNCTION billing_enter_account(UUID) TO %I',role_name);
   EXECUTE format('REVOKE ALL ON SCHEMA billing_private FROM %I',role_name);
   EXECUTE format('REVOKE ALL ON billing_private.account_entry FROM %I',role_name);
  END IF;
 END LOOP;
END $$;
CREATE FUNCTION billing_guard_account_row() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE a UUID;
BEGIN
 a=CASE WHEN TG_OP='INSERT' THEN NEW.account_id ELSE OLD.account_id END;
 IF TG_OP='UPDATE' AND NEW.account_id IS DISTINCT FROM OLD.account_id THEN
  RAISE EXCEPTION 'financial account moves are forbidden';
 END IF;
 PERFORM public.billing_require_current_entry(a);
 IF TG_TABLE_NAME='grants' AND TG_OP='DELETE' THEN
  DELETE FROM public.grant_seat_tiers WHERE grant_id=OLD.id;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
ALTER FUNCTION billing_guard_account_row() OWNER TO coralswarm_billing_entry_owner;
REVOKE ALL ON FUNCTION billing_guard_account_row() FROM PUBLIC;
DO $$ DECLARE t TEXT; BEGIN
 FOREACH t IN ARRAY ARRAY['credit_ledger','credit_balances','query_billing_state',
   'member_allowance_periods','managed_credit_reservations','grants','stripe_subscriptions',
   'stripe_webhook_events'] LOOP
  EXECUTE format('CREATE TRIGGER billing_current_generation BEFORE INSERT OR UPDATE OR DELETE ON public.%I FOR EACH ROW EXECUTE FUNCTION public.billing_guard_account_row()',t);
  EXECUTE format('CREATE TRIGGER billing_no_truncate BEFORE TRUNCATE ON public.%I FOR EACH STATEMENT EXECUTE FUNCTION public.managed_refuse_truncate()',t);
 END LOOP;
END $$;
CREATE FUNCTION billing_guard_seat_row() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE a UUID; g UUID;
BEGIN
 g=CASE WHEN TG_OP='INSERT' THEN NEW.grant_id ELSE OLD.grant_id END;
 IF TG_OP='UPDATE' AND NEW.grant_id IS DISTINCT FROM OLD.grant_id THEN
  RAISE EXCEPTION 'grant seat account moves are forbidden';
 END IF;
 SELECT account_id INTO a FROM public.grants WHERE id=g;
 IF a IS NULL THEN RAISE EXCEPTION 'grant seat requires its actual parent'; END IF;
 PERFORM public.billing_require_current_entry(a);
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
ALTER FUNCTION billing_guard_seat_row() OWNER TO coralswarm_billing_entry_owner;
REVOKE ALL ON FUNCTION billing_guard_seat_row() FROM PUBLIC;
CREATE TRIGGER billing_current_generation BEFORE INSERT OR UPDATE OR DELETE ON grant_seat_tiers
 FOR EACH ROW EXECUTE FUNCTION billing_guard_seat_row();
CREATE TRIGGER billing_no_truncate BEFORE TRUNCATE ON grant_seat_tiers
 FOR EACH STATEMENT EXECUTE FUNCTION managed_refuse_truncate();
CREATE FUNCTION billing_guard_account_identity() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='UPDATE' THEN
  IF NEW.id IS DISTINCT FROM OLD.id THEN RAISE EXCEPTION 'account id is immutable'; END IF;
  IF (NEW.clerk_org_id,NEW.is_org_account) IS NOT DISTINCT FROM (OLD.clerk_org_id,OLD.is_org_account) THEN RETURN NEW; END IF;
 END IF;
 PERFORM public.billing_require_current_entry(OLD.id);
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
ALTER FUNCTION billing_guard_account_identity() OWNER TO coralswarm_billing_entry_owner;
REVOKE ALL ON FUNCTION billing_guard_account_identity() FROM PUBLIC;
CREATE TRIGGER billing_identity_current_generation BEFORE UPDATE OR DELETE ON accounts
 FOR EACH ROW EXECUTE FUNCTION billing_guard_account_identity();
CREATE TRIGGER billing_no_truncate BEFORE TRUNCATE ON accounts
 FOR EACH STATEMENT EXECUTE FUNCTION managed_refuse_truncate();
ALTER TABLE tenant_databases ADD CONSTRAINT tenant_databases_account_key UNIQUE(account_id);
CREATE FUNCTION billing_guard_tenant_binding() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE a UUID; o TEXT;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF (NEW.account_id,NEW.clerk_org_id) IS DISTINCT FROM (OLD.account_id,OLD.clerk_org_id) THEN
   RAISE EXCEPTION 'tenant account identity is immutable';
  END IF;
  IF NEW.db_secret_ref IS NOT DISTINCT FROM OLD.db_secret_ref THEN RETURN NEW; END IF;
 END IF;
 a=CASE WHEN TG_OP='DELETE' THEN OLD.account_id ELSE NEW.account_id END;
 PERFORM public.billing_require_current_entry(a);
 IF TG_OP<>'DELETE' THEN
  SELECT clerk_org_id INTO o FROM public.accounts WHERE id=a AND is_org_account;
  IF o IS DISTINCT FROM NEW.clerk_org_id THEN RAISE EXCEPTION 'tenant account binding mismatch'; END IF;
  RETURN NEW;
 END IF;
 RETURN OLD;
END $$;
ALTER FUNCTION billing_guard_tenant_binding() OWNER TO coralswarm_billing_entry_owner;
REVOKE ALL ON FUNCTION billing_guard_tenant_binding() FROM PUBLIC;
CREATE TRIGGER billing_current_generation BEFORE INSERT OR UPDATE OR DELETE ON tenant_databases
 FOR EACH ROW EXECUTE FUNCTION billing_guard_tenant_binding();
CREATE TRIGGER billing_no_truncate BEFORE TRUNCATE ON tenant_databases
 FOR EACH STATEMENT EXECUTE FUNCTION managed_refuse_truncate();
CREATE FUNCTION billing_receipt_debit_history() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF OLD.source_receipt_id IS NOT NULL AND OLD.entry_kind='usage_managed' AND OLD.cost_usd>0 THEN
  IF TG_OP='DELETE' AND NOT EXISTS(SELECT 1 FROM public.accounts WHERE id=OLD.account_id) THEN RETURN OLD; END IF;
  IF TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'managed debit history is immutable'; END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
ALTER FUNCTION billing_receipt_debit_history() OWNER TO coralswarm_billing_entry_owner;
REVOKE ALL ON FUNCTION billing_receipt_debit_history() FROM PUBLIC;
CREATE TRIGGER billing_receipt_debit_history BEFORE UPDATE OR DELETE ON credit_ledger
 FOR EACH ROW EXECUTE FUNCTION billing_receipt_debit_history();
DO $$ DECLARE t TEXT; role_name TEXT; BEGIN
 FOREACH t IN ARRAY ARRAY['credit_conversion','plans','seat_tiers','model_rates','credit_packs'] LOOP
  EXECUTE format('REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON public.%I FROM PUBLIC',t);
  FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
   IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
    EXECUTE format('REVOKE ALL ON public.%I FROM %I',t,role_name);
    EXECUTE format('GRANT SELECT ON public.%I TO %I',t,role_name);
    IF has_table_privilege(role_name,'public.'||t,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
     OR has_any_column_privilege(role_name,'public.'||t,'INSERT,UPDATE,REFERENCES') THEN
     RAISE EXCEPTION 'application inherits global catalog write authority: % %',role_name,t;
    END IF;
   END IF;
  END LOOP;
 END LOOP;
END $$;
DO $$ DECLARE role_name TEXT; t TEXT; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
   IF has_schema_privilege(role_name,'public','CREATE')
    OR has_schema_privilege(role_name,'billing_private','USAGE,CREATE')
    OR has_table_privilege(role_name,'billing_private.account_entry','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
    OR pg_has_role(role_name,'coralswarm_billing_entry_owner','MEMBER')
    OR EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)) THEN
    RAISE EXCEPTION 'application carries incompatible billing owner/schema authority: %',role_name;
   END IF;
   FOREACH t IN ARRAY ARRAY['credit_ledger','credit_balances','query_billing_state','member_allowance_periods',
     'managed_credit_reservations','grants','grant_seat_tiers','stripe_subscriptions','stripe_webhook_events',
     'accounts','tenant_databases','managed_attempt_receipts'] LOOP
    IF pg_has_role(role_name,(SELECT relowner FROM pg_class WHERE oid=('public.'||t)::regclass),'MEMBER') THEN
     RAISE EXCEPTION 'application can assume protected table owner: % %',role_name,t;
    END IF;
   END LOOP;
  END IF;
 END LOOP;
END $$;

-- PAIRED WITH: migrations_control/0333_managed_enrichment_scaffold.sql.
--
-- Source-agnostic pipeline PR 3: the `managed_enrichment` purpose scaffold.
-- One shared purpose and one daily cap (MANAGED_DAILY_CAP_USD_ENRICHMENT,
-- default $2 per account) for graph-level consumers. No job kind is admitted
-- by this change; workers move onto the purpose one at a time later, behind
-- ENRICHMENT_PROCESSING_ENABLED=true.
--
-- The shared block below is byte-identical on both planes; only the RLS
-- policies at the end differ. Grants go to coralswarm_app and
-- coralswarm_tenant_app. The production tenant runtime connects as the table
-- OWNER, which owns this table (owner bypass, no FORCE) and inherits
-- coralswarm_tenant_app since tenant 0159.

-- 1. The purpose CHECK (0318/0149) gains the shared graph-level purpose. The
--    table holds one row per account, purpose and UTC day, so the rewrite
--    scan is tiny.
ALTER TABLE managed_budget_periods
  DROP CONSTRAINT managed_budget_periods_purpose_check,
  ADD CONSTRAINT managed_budget_periods_purpose_check
    CHECK (purpose IN ('source_extraction','cloud_embedding','managed_enrichment'));

-- 2. Enrichment receipts get the same seal as source receipts (0321/0153):
--    funded dispatch only, no deletion, terminal history immutable. CREATE
--    OR REPLACE keeps the function's oid, owner and ACL, so the trigger and
--    billing derivation's seal check (`verify_source_seal`) are unchanged.
CREATE OR REPLACE FUNCTION managed_source_receipt_history() RETURNS trigger
 LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.protocol_version=0 AND NEW.purpose='source_extraction' THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='source extraction requires funded dispatch generation';
  END IF;
  IF NEW.protocol_version=0 AND NEW.purpose='managed_enrichment' THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='managed enrichment requires funded dispatch generation';
  END IF;
  RETURN NEW;
 END IF;
 IF OLD.purpose IN ('source_extraction','managed_enrichment') THEN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'source receipt history cannot be deleted'; END IF;
  IF (NEW.receipt_id,NEW.created_at) IS DISTINCT FROM (OLD.receipt_id,OLD.created_at)
   OR (OLD.state<>'reserved' AND NEW IS DISTINCT FROM OLD) THEN
   RAISE EXCEPTION 'source receipt identity and terminal history are immutable';
  END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;

-- 3. All-input receipts for graph-level calls. Every input a call reads is
--    written with its receipt before the send is armed, so publish can
--    persist outputs only while every input is unchanged, and deleting an
--    input is one indexed UPDATE. Generic over input kinds (atom, node,
--    semantic_atom, journal hour, ...); sized for a journal hour (~150) and a
--    digest (~30) with headroom.
CREATE TABLE managed_enrichment_inputs (
  receipt_id UUID NOT NULL,
  account_id UUID NOT NULL,
  graph_id UUID NOT NULL,
  purpose TEXT NOT NULL DEFAULT 'managed_enrichment' CHECK (purpose = 'managed_enrichment'),
  ordinal SMALLINT NOT NULL CHECK (ordinal BETWEEN 0 AND 1023),
  input_kind TEXT NOT NULL CHECK (input_kind ~ '^[a-z][a-z0-9_]{0,47}$'),
  input_id UUID NOT NULL,
  content_revision BIGINT NOT NULL CHECK (content_revision >= 0),
  content_sha256 BYTEA NOT NULL CHECK (octet_length(content_sha256) = 32),
  outcome TEXT NOT NULL DEFAULT 'sent'
    CHECK (outcome IN ('sent','written','discarded_stale','invalidated')),
  PRIMARY KEY (receipt_id, ordinal),
  UNIQUE (receipt_id, input_kind, input_id),
  FOREIGN KEY (receipt_id, account_id, graph_id, purpose)
    REFERENCES managed_attempt_receipts (receipt_id, account_id, graph_id, purpose)
);
CREATE INDEX managed_enrichment_inputs_by_input
  ON managed_enrichment_inputs (input_kind, input_id);

-- The binding is immutable and an outcome leaves 'sent' once. Rows are
-- history: no delete, no truncate.
CREATE FUNCTION managed_enrichment_input_history() RETURNS trigger
 LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'managed enrichment input history cannot be deleted'; END IF;
 IF (NEW.receipt_id,NEW.account_id,NEW.graph_id,NEW.purpose,NEW.ordinal,NEW.input_kind,
     NEW.input_id,NEW.content_revision,NEW.content_sha256)
   IS DISTINCT FROM
    (OLD.receipt_id,OLD.account_id,OLD.graph_id,OLD.purpose,OLD.ordinal,OLD.input_kind,
     OLD.input_id,OLD.content_revision,OLD.content_sha256)
 THEN RAISE EXCEPTION 'managed enrichment input binding is immutable'; END IF;
 IF NEW.outcome IS DISTINCT FROM OLD.outcome AND OLD.outcome<>'sent' THEN
  RAISE EXCEPTION 'managed enrichment input outcome is terminal';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION managed_enrichment_input_history() FROM PUBLIC;
CREATE TRIGGER managed_enrichment_input_history BEFORE UPDATE OR DELETE
 ON managed_enrichment_inputs FOR EACH ROW EXECUTE FUNCTION managed_enrichment_input_history();
CREATE TRIGGER managed_enrichment_inputs_no_truncate BEFORE TRUNCATE ON managed_enrichment_inputs
 FOR EACH STATEMENT EXECUTE FUNCTION managed_refuse_truncate();

ALTER TABLE managed_enrichment_inputs ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
  GRANT SELECT,INSERT,UPDATE ON managed_enrichment_inputs TO coralswarm_app;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
  GRANT SELECT,INSERT,UPDATE ON managed_enrichment_inputs TO coralswarm_tenant_app;
 END IF;
END $$;

-- Tenant plane: written by the system actor, readable wherever its graph is
-- (the same split as managed_embedding_inputs, tenant 0152).
CREATE POLICY member_ocean_isolation ON managed_enrichment_inputs FOR ALL
  USING (tenant_rls_is_system()) WITH CHECK (tenant_rls_is_system());
CREATE POLICY member_ocean_read ON managed_enrichment_inputs FOR SELECT
  USING (tenant_rls_is_system() OR graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

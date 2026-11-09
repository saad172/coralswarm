-- PAIRED WITH: migrations_control/0337_managed_interactive_ask.sql.
--
-- Jev at Ask time moves under a managed budget (founder decision, 2026-10-18).
-- A new purpose, `interactive_ask`, carries every Jev call made inside a
-- user-initiated Ask (catalog scan, pack, cite, the capture Noul and
-- contradiction pairs). It has its own per-account daily cap
-- (MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK, default $1), separate from the
-- shared background `managed_enrichment` cap: an Ask is interactive, so a busy
-- enrichment day never holds it and a burst of Asks never starves enrichment.
-- Each provider attempt reserves before the send and settles once after,
-- through the existing funded gate; the receipts are the bill.
--
-- The shared block below is byte-identical on both planes. No new table and
-- no policy change: receipts and budget rows reuse managed_attempt_receipts
-- and managed_budget_periods with their existing RLS and grants. The
-- production tenant runtime connects as the table OWNER (owner bypass, no
-- FORCE) and inherits coralswarm_tenant_app since tenant 0159.

-- 1. The purpose CHECK (0318/0149, widened by 0333/0166) gains the Ask
--    purpose. One row per account, purpose and UTC day: a tiny rewrite scan.
ALTER TABLE managed_budget_periods
  DROP CONSTRAINT managed_budget_periods_purpose_check,
  ADD CONSTRAINT managed_budget_periods_purpose_check
    CHECK (purpose IN ('source_extraction','cloud_embedding','managed_enrichment','interactive_ask'));

-- 2. Ask receipts get the same seal as source and enrichment receipts:
--    funded dispatch only, no deletion, terminal history immutable. CREATE OR
--    REPLACE keeps the function's oid, owner and ACL, so the trigger and
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
  IF NEW.protocol_version=0 AND NEW.purpose='interactive_ask' THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='interactive ask requires funded dispatch generation';
  END IF;
  RETURN NEW;
 END IF;
 IF OLD.purpose IN ('source_extraction','managed_enrichment','interactive_ask') THEN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'source receipt history cannot be deleted'; END IF;
  IF (NEW.receipt_id,NEW.created_at) IS DISTINCT FROM (OLD.receipt_id,OLD.created_at)
   OR (OLD.state<>'reserved' AND NEW IS DISTINCT FROM OLD) THEN
   RAISE EXCEPTION 'source receipt identity and terminal history are immutable';
  END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;

-- PAIRED WITH: migrations_control/0345_managed_receipt_recovery_queue.sql.
--
-- The billing worker's managed recovery pass (`reconcile_managed_tx` with
-- include_funded) replayed every terminal funded source-extraction receipt
-- the account had ever settled, on every tick, while holding the account's
-- billing_enter_account lock that every managed admission also takes. Its
-- cost grew with all settled history. This queue makes it grow only with
-- what changed since the last pass.
--
-- managed_receipt_recovery_queue holds the funded (protocol 1)
-- source-extraction receipts on this plane whose terminal fact the recovery
-- pass has not yet confirmed on control. The invariant, for every terminal
-- funded source-extraction receipt R on this plane: R has a queue row, OR a
-- committed control transaction has recorded R's terminal fact exactly in
-- managed_credit_reservations (settled at R's charge, or released unsent).
--
-- * It holds once this migration commits: the trigger is installed and every
--   receipt already terminal is enqueued in the same transaction. The SHARE
--   ROW EXCLUSIVE lock waits out every in-flight receipt writer and blocks
--   new ones until commit, so no transition can commit between the backfill's
--   snapshot and the trigger going live.
-- * A receipt becomes terminal exactly once and its terminal history is
--   immutable (managed_source_receipt_history refuses any later change and
--   any delete; managed_receipts_no_truncate refuses TRUNCATE). The AFTER
--   trigger enqueues it in the SAME transaction as that transition, so a
--   committed terminal receipt always has its queue row.
-- * The recovery pass removes a row only after it has confirmed or applied
--   that receipt's fact: in the same control transaction for control-plane
--   rows, and only after that control transaction committed for tenant-plane
--   rows. A crash or failure between the two leaves the row queued, and the
--   next pass re-confirms it (already applied, so it changes nothing) and
--   then removes it. A reservation's terminal state is immutable too
--   (managed_credit_binding_immutable), so a confirmed fact stays confirmed.
-- * A receipt whose control fact conflicts (a missing reservation, a
--   mismatched binding, a settled amount that differs) makes the pass fail
--   and roll back, so its row stays queued and it fails again every pass.
--   It is never skipped.
-- The worker checks that this trigger is installed and enabled on every
-- plane before it trusts the queue, and refuses the pass otherwise.
--
-- Legacy (protocol 0) receipts are sealed and still replayed in full; they
-- are not queued. The trigger also fires on a no-op UPDATE of a terminal row
-- (the only UPDATE the history seal allows); re-enqueueing is harmless
-- because the pass confirms and removes it.
--
-- The trigger function is SECURITY DEFINER under the migration owner (the
-- table owner on every plane), so whoever settles a receipt (the job's scope,
-- the system actor or the owner-rights reaper) enqueues it without holding
-- INSERT on the queue. The application roles only read and remove rows,
-- under the policy below. The production tenant runtime connects as the
-- table OWNER (owner bypass, no FORCE) and inherits coralswarm_tenant_app
-- since tenant 0159.

-- Shared block (byte-identical on both planes).
LOCK TABLE managed_attempt_receipts IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE managed_receipt_recovery_queue (
  receipt_id UUID PRIMARY KEY,
  account_id UUID NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX managed_receipt_recovery_queue_account
  ON managed_receipt_recovery_queue (account_id, receipt_id);

CREATE FUNCTION managed_receipt_recovery_enqueue() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 INSERT INTO public.managed_receipt_recovery_queue(receipt_id,account_id)
 VALUES (NEW.receipt_id,NEW.account_id) ON CONFLICT (receipt_id) DO NOTHING;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION managed_receipt_recovery_enqueue() FROM PUBLIC;
CREATE TRIGGER managed_receipt_recovery_queue AFTER INSERT OR UPDATE ON managed_attempt_receipts
 FOR EACH ROW WHEN (NEW.protocol_version=1 AND NEW.purpose='source_extraction' AND NEW.state<>'reserved')
 EXECUTE FUNCTION managed_receipt_recovery_enqueue();

INSERT INTO managed_receipt_recovery_queue(receipt_id,account_id)
SELECT receipt_id,account_id FROM managed_attempt_receipts
 WHERE protocol_version=1 AND purpose='source_extraction' AND state<>'reserved'
ON CONFLICT (receipt_id) DO NOTHING;

ALTER TABLE managed_receipt_recovery_queue ENABLE ROW LEVEL SECURITY;
DO $$ DECLARE role_name TEXT; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
   EXECUTE format('GRANT SELECT,DELETE ON managed_receipt_recovery_queue TO %I',role_name);
  END IF;
 END LOOP;
END $$;
-- End shared block.

-- Tenant policy: the system actor only, like managed_budget_periods (0149)
-- and the receipt write policy (0150). The recovery pass reads and drains
-- the queue through begin_tenant_system_scoped.
CREATE POLICY member_ocean_isolation ON managed_receipt_recovery_queue
 USING (tenant_rls_is_system())
 WITH CHECK (tenant_rls_is_system());

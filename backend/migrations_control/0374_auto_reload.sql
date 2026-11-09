-- Migration: 0374_auto_reload
-- Description: Settings launch plan PR 3.5 (extra-usage auto-reload). Three
--   small additions to the 0367 schema; no new table.
--
--   ── extra_usage_reloads: what was decided, stored with the decision ──
--   stripe_customer_id / stripe_payment_method_id
--       The customer and the card the reload charges, written in the same
--       statement that claims the row. A reload's PaymentIntent is created
--       under the stable idempotency key `reload:<id>` (0367). Stripe rejects
--       a reused key whose parameters differ, so every retry, crash recovery
--       or reaper pass must send byte-identical parameters. If the card were
--       looked up again on each attempt, a customer who changed their default
--       card between attempts would turn a safe retry into an idempotency
--       conflict (and we could not tell whether the first attempt charged).
--       The decision is therefore persisted once.
--   disabled_auto_reload
--       True on the reload whose failure turned auto-reload off (N failures
--       in a row). The failure email says so; it is read when the email is
--       sent, so the flag must outlive the transaction that set it.
--
--   ── extra_usage_settings.auto_reload_reset_at ──
--   When the customer last turned auto-reload ON (off -> on), and only then.
--   The consecutive-failure count and its backoff start from here. They used
--   to start from `updated_at`, which ANY settings save bumps, so six declines
--   with a save between each never reached "three in a row" and auto-reload
--   never turned itself off: six charge attempts at a failing card. A save
--   that does not switch auto-reload on leaves this alone. NULL = never turned
--   on through the settings route (counts from the beginning of time).
--
--   ── two cross-account scans for the worker ──
--   The worker and the reaper need "which accounts?" before they can open an
--   account-scoped transaction, and `extra_usage_*` carry the strict
--   `account_isolation` policy (0367), so a bare read sees nothing (or errors
--   on the unset `app.account_id`) for a non-owner role. Two SECURITY DEFINER
--   functions answer only that question and return ids, nothing else:
--     extra_usage_reload_candidates()  accounts with auto-reload on, balance
--         under the threshold and no reload in flight. A cheap prefilter; the
--         worker re-decides everything under the account lock.
--     extra_usage_stale_reloads(...)   in-flight reloads older than a bound,
--         for the reaper.
--   Same grant pattern as 0321's billing_enter_account (CURRENT_USER plus the
--   app roles when they exist), so a prod deploy that connects as the table
--   owner is not left without EXECUTE.
--
-- Depends on: 0367_extra_usage_schema.
-- Down:
--   DROP FUNCTION IF EXISTS extra_usage_stale_reloads(interval, interval, int);
--   DROP FUNCTION IF EXISTS extra_usage_reload_candidates(int);
--   ALTER TABLE extra_usage_settings DROP COLUMN IF EXISTS auto_reload_reset_at;
--   ALTER TABLE extra_usage_reloads DROP COLUMN IF EXISTS disabled_auto_reload,
--       DROP COLUMN IF EXISTS stripe_payment_method_id,
--       DROP COLUMN IF EXISTS stripe_customer_id;

SET LOCAL lock_timeout = '3s';

ALTER TABLE extra_usage_reloads
    ADD COLUMN IF NOT EXISTS stripe_customer_id       TEXT CHECK (stripe_customer_id IS NULL OR stripe_customer_id <> ''),
    ADD COLUMN IF NOT EXISTS stripe_payment_method_id TEXT CHECK (stripe_payment_method_id IS NULL OR stripe_payment_method_id <> ''),
    ADD COLUMN IF NOT EXISTS disabled_auto_reload     BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE extra_usage_settings
    ADD COLUMN IF NOT EXISTS auto_reload_reset_at TIMESTAMPTZ;

CREATE OR REPLACE FUNCTION extra_usage_reload_candidates(p_limit int DEFAULT 200)
RETURNS SETOF UUID
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT s.account_id
      FROM public.extra_usage_settings s
      LEFT JOIN public.extra_usage_balances b ON b.account_id = s.account_id
     WHERE s.enabled
       AND s.auto_reload_enabled
       AND COALESCE(b.balance_micro_usd, 0) < s.auto_reload_threshold_micro_usd
       AND NOT EXISTS (
           SELECT 1 FROM public.extra_usage_reloads r
            WHERE r.account_id = s.account_id AND r.state IN ('created', 'submitted'))
     -- Random, so a pile of accounts that cannot reload right now (no card,
     -- limit reached) never starves the ones that can.
     ORDER BY random()
     LIMIT GREATEST(p_limit, 1)
$$;

CREATE OR REPLACE FUNCTION extra_usage_stale_reloads(p_created_age interval, p_submitted_age interval, p_limit int DEFAULT 100)
RETURNS TABLE (account_id UUID, reload_id UUID)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT r.account_id, r.id
      FROM public.extra_usage_reloads r
     WHERE (r.state = 'created'   AND r.created_at   < now() - p_created_age)
        OR (r.state = 'submitted' AND r.submitted_at < now() - p_submitted_age)
     -- Least recently attempted first: the reaper touches `updated_at` after an
     -- attempt that settled nothing, so a row Stripe cannot resolve goes to the
     -- back of the queue instead of starving the rest.
     ORDER BY r.updated_at
     LIMIT GREATEST(p_limit, 1)
$$;

REVOKE ALL ON FUNCTION extra_usage_reload_candidates(int) FROM PUBLIC;
REVOKE ALL ON FUNCTION extra_usage_stale_reloads(interval, interval, int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION extra_usage_reload_candidates(int) TO CURRENT_USER;
GRANT EXECUTE ON FUNCTION extra_usage_stale_reloads(interval, interval, int) TO CURRENT_USER;
DO $$ DECLARE role_name TEXT; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
   EXECUTE format('GRANT EXECUTE ON FUNCTION extra_usage_reload_candidates(int) TO %I', role_name);
   EXECUTE format('GRANT EXECUTE ON FUNCTION extra_usage_stale_reloads(interval, interval, int) TO %I', role_name);
  END IF;
 END LOOP;
END $$;

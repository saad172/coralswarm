-- Migration: 0373_billing_notice_delivery
-- Description: Settings launch plan PR 3.6. Delivery bookkeeping for
--   `billing_notices` (0363), so a notice can be claimed once and delivered
--   with retry, without ever being sent twice.
--
--   The claim is the row itself: `INSERT ... ON CONFLICT (account_id,
--   notice_kind, dedupe_key) DO NOTHING` decides the one winner. The columns
--   added here let a worker then DELIVER the claimed notice off the request
--   and debit paths:
--
--   next_attempt_at  When a worker may next take the notice. A worker takes
--                    it by moving this forward (a lease), so two workers
--                    never send the same notice at once, and a worker that
--                    dies mid-send hands the notice back by simply not
--                    finishing: it becomes due again when the lease runs out.
--                    A failed send sets it to now() + backoff.
--   last_attempt_at  NULL until a worker first takes the notice. `attempts`
--                    (0363, starts at 1) counts the attempt in flight, so the
--                    first lease must not bump it; every later one does.
--   delivered_to     Users already emailed for this notice. A notice goes to
--                    several admins; when one address fails and the notice is
--                    retried, the admins who already got it are skipped, not
--                    emailed again.
--
--   No data change, no new table, no RLS change (`billing_notices` keeps the
--   `account_isolation` policy from 0363). Additive columns with defaults, so
--   rows already claimed stay valid.

SET LOCAL lock_timeout = '3s';

ALTER TABLE billing_notices
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS delivered_to    UUID[]      NOT NULL DEFAULT '{}';

-- no-transaction
-- Migration: 0172_credit_ledger_checkout_session_index_concurrently
-- Description: build the unique index backing 0171's per-CHECKOUT-SESSION
--   dedup (`credit_ledger.stripe_checkout_session_id`) WITHOUT blocking
--   writes on `credit_ledger` while it builds. Split out of
--   `0171_credit_ledger_checkout_session.sql` (which adds the column) for
--   the same reason `0142`/`0059`/`0158`/`0160` were split out of their
--   respective column-add migrations — see `0142`'s header for the full
--   explanation of why `CREATE INDEX CONCURRENTLY` must be alone in a
--   `-- no-transaction` file (sqlx sends a migration's SQL as one
--   wire-protocol Query message, and Postgres implicitly wraps multiple
--   statements in such a message in a transaction, which `CONCURRENTLY`
--   refuses to run inside). `credit_ledger` already carries one prior
--   `CONCURRENTLY` precedent for exactly this reason (`0160`, the
--   `attributed_user_id` index) — this follows the same split rather than
--   documenting a one-off exception (CodeRabbit review round 1, `0171:83`).
--
--   Global unique, partial on `IS NOT NULL` — see 0171's header for why the
--   index is not scoped to `(account_id, stripe_checkout_session_id)` and why
--   the column (hence this index's predicate) is nullable.
--
--   `IF NOT EXISTS` + `CONCURRENTLY`: a failed concurrent build leaves an
--   INVALID index behind, and `IF NOT EXISTS` will then happily skip it
--   forever. If this migration errors, drop
--   `idx_credit_ledger_stripe_checkout_session_id_unique` before re-running
--   rather than assuming the skip means success — the same operational
--   caveat every `CONCURRENTLY` migration in this tree carries
--   (0059/0074/0075/0099/0142/0143/0144/0146/0147/0152/0158/0160).
--
--   No tenant twin — `credit_ledger` is CONTROL-ONLY (see 0171's header).
--
-- Depends on: 0171_credit_ledger_checkout_session.sql (creates
--   stripe_checkout_session_id).
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_credit_ledger_stripe_checkout_session_id_unique;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_credit_ledger_stripe_checkout_session_id_unique
    ON credit_ledger (stripe_checkout_session_id)
    WHERE stripe_checkout_session_id IS NOT NULL;

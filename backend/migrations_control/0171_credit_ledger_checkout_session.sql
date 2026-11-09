-- Migration: 0171_credit_ledger_checkout_session
-- Description: billing self-serve launch PR D fix (finding 10, review round) —
--   per-CHECKOUT-SESSION dedup for a pack-purchase `credit_ledger` row.
--
--   ── Numbering note — read before assuming a bug ──
--   This is 0171, not the numerically-next-looking 0170: `0170_team_plan_
--   activate.sql` is already claimed by the sibling PR E branch
--   (cs-billing-renewal-tiers, PR #655), which converged and is merging
--   ahead of this one. Taking 0171 here, rather than colliding on 0170,
--   keeps both trees' migration sequence gapless and ordered correctly once
--   both PRs land on `main`.
--
--   ── Why this axis is missing from 0150's three idempotency keys ──
--   `0150_credit_ledger.sql`'s three partial unique indexes
--   (`source_job_id`/`source_query_id`/the journal axis) are all UUID columns
--   keyed against TENANT-plane primary keys (`jobs.id`, `query_history.id`) —
--   see that migration's own "Cross-tenant uniqueness" note. A Stripe
--   Checkout Session id (`cs_...`) is neither a UUID nor a tenant-plane row;
--   none of the three existing axes can hold it, and
--   `credit_ledger_single_source_axis`'s CHECK is deliberately scoped to just
--   those three debit axes (a grant-type row already legitimately carries all
--   three NULL — see 0150's header). A pack purchase is the same shape as a
--   grant: no job, no query, no journal period, and (until now) no way to
--   name the ONE Stripe object that actually caused it.
--
--   ── The bug this closes ──
--   `routes::stripe_webhook::apply_checkout_completed_payment` dedups
--   `checkout.session.completed` (payment mode) SOLELY on
--   `stripe_webhook_events.event_id` (0162's claim). Stripe's own guidance is
--   that `event_id` is unique PER DELIVERY, not per underlying object — a
--   dashboard-triggered resend, or (documented, if rare) more than one event
--   id observing the same session, both carry a DIFFERENT `event_id` for the
--   SAME `session_id`. Each one indepedently passes the `event_id` claim and
--   independently credits `credit_balances` — a double-credit for one
--   purchase, silent because both inserts "succeed".
--
--   ── The fix: tag, then dedup on the tag ──
--   `credit_ledger.stripe_checkout_session_id` names the ONE session a
--   pack-purchase credit row came from. The unique index built in the
--   companion migration `0172` (split out — see that file's header for why
--   `CREATE UNIQUE INDEX CONCURRENTLY` cannot share a transaction with this
--   column-add) makes a second row for the SAME session a hard `23505`, not
--   merely discouraged — `routes::stripe_webhook`'s INSERT catches that
--   specific code and treats it as an already-credited no-op (see that
--   module's own comment at the call site), the same "the constraint IS the
--   dedup, the claim above is a fast path" shape 0161's UNIQUE index already
--   established for `member_allowance_periods`.
--
--   Global (not `(account_id, stripe_checkout_session_id)`) for the same
--   reason 0150's three existing indexes are global: a Stripe Checkout
--   Session id is already globally unique (Stripe's own id space, not a
--   per-tenant sequence), so scoping it to `account_id` would only weaken the
--   guarantee — two rows that happen to collide on `session_id` could never
--   legitimately belong to two different accounts in the first place.
--
--   Nullable, and deliberately NOT added to `credit_ledger_single_source_axis`:
--   this is a CREDIT-axis tag (which purchase produced this grant-shaped
--   row), not a DEBIT-axis idempotency key (which unit of billable work
--   produced this row) — the two questions are orthogonal, and a debit row
--   from `workers::billing_derivation` will simply never carry this column,
--   the same way a grant row never carries a `source_job_id`. Widening that
--   CHECK to also count this column would reject nothing meaningful (no debit
--   path will ever populate it) while coupling two independent concerns.
--
--   ── CHECK is a separately-named constraint (CodeRabbit review round 1,
--   `0171:79`) ──
--   `ADD COLUMN IF NOT EXISTS` is a no-op on a re-run where the column
--   already exists — which would silently also skip an inline `CHECK`
--   attached to the same `ADD COLUMN` clause, and an inline CHECK on a
--   multi-column ALTER is unnamed anyway (a later re-add attempt would then
--   create a second, differently-named constraint instead of recognizing the
--   one already there). Naming the constraint and adding it in its own
--   `ALTER TABLE ... DROP CONSTRAINT IF EXISTS ... ADD CONSTRAINT` pair below
--   makes the guarantee independent of whether the column-add branch ran.
--
-- CONTROL-PLANE ONLY — `credit_ledger` already is (0150); this migration adds
--   a column to an existing table, so no new entry is needed in
--   `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, and no new RLS
--   policy is needed (the table's existing `account_isolation` policy already
--   covers every column on the row).
--
-- Depends on: 0150_credit_ledger.sql (`credit_ledger`, the three existing
--   idempotency-key indexes this one joins as a fourth, independent axis).
-- Followed by: 0172_credit_ledger_checkout_session_index_concurrently.sql
--   (the unique index over this column).
-- Down:
--   ALTER TABLE credit_ledger DROP CONSTRAINT IF EXISTS credit_ledger_checkout_session_id_nonempty;
--   ALTER TABLE credit_ledger DROP COLUMN IF EXISTS stripe_checkout_session_id;

SET LOCAL lock_timeout = '3s';

ALTER TABLE credit_ledger
    ADD COLUMN IF NOT EXISTS stripe_checkout_session_id TEXT;

ALTER TABLE credit_ledger
    DROP CONSTRAINT IF EXISTS credit_ledger_checkout_session_id_nonempty;
ALTER TABLE credit_ledger
    ADD CONSTRAINT credit_ledger_checkout_session_id_nonempty
        CHECK (stripe_checkout_session_id IS NULL OR stripe_checkout_session_id <> '');

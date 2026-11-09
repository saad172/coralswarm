-- Migration: 0230_slack_unmatched_email_notices_per_person_cooldown
-- Description: D4 (docs/plans/org-onboarding-from-the-invite.md §3 item 2 /
--   "D4 — One email, one account, chosen deterministically") — the
--   unmatched-recipient nudge's persisted cooldown moves from
--   `(account_id, provider, email)` to **`(provider, email)`**: "recently
--   notified" is a fact about the PERSON, not about which of possibly
--   several CoralSwarm accounts (three orgs, or an org plus a personal
--   account) also has a bot in the same Slack channel.
--
--   Before this migration, `handle_member_joined` (`routes/slack_events.rs`)
--   fans out over every CoralSwarm account mapped to a Slack workspace, and
--   the invite enqueue sits inside that per-account fan-out, while the
--   cooldown was keyed `UNIQUE (account_id, provider, email)`. One real
--   person sitting in one Slack channel that three separate accounts have
--   each added their own agent to could receive three separate nudge
--   emails within the same cooldown week, each naming a different org —
--   see that route's own doc comment for the production shape of this bug
--   (the identical "loop over every matched account, one side effect per
--   account" class of leak #501 already had to fix for seat reconciliation
--   and huddle capture; this is the SEND-side instance of it). D4's fix has
--   two parts: this migration (the cooldown/notice row is per PERSON,
--   full stop) and `workers::slack_unmatched_email::process_job`'s new
--   `connectors::slack_unmatched_email::select_notice_account` call (this
--   same PR, CS-427), which picks exactly ONE of the several accounts that
--   could plausibly send this person a notice, deterministically, rather
--   than leaving "who wins" to whichever account's worker tick happens to
--   run its claim first — this migration's new `(provider, email)` UNIQUE
--   constraint is what makes that claim atomic and safe even if the
--   selection ever raced anyway (belt AND suspenders, not either/or).
--
--   `account_id` stays as a plain, NOT NULL column — dropped from the
--   UNIQUE constraint, not from the table. It still records which account's
--   send most recently claimed/renewed this row (observability), and
--   `services::unmatched_recipient_email::claim_notice_slot`'s `ON CONFLICT
--   DO UPDATE` now refreshes it on every renewal (`account_id =
--   EXCLUDED.account_id`), so a later claim by a DIFFERENT (D4-preferred)
--   account overwrites the old value rather than leaving a stale one behind.
--
--   ── "Recently" is seven days ──
--   Unchanged by this migration — `COOLDOWN = "7 days"` in
--   `services/unmatched_recipient_email.rs`, stated in prose next to that
--   constant (see its own doc comment) and mirrored in 0227's header. Noted
--   here only because D4 restates it explicitly as part of the same
--   decision this migration implements.
--
--   ── Collapsing pre-existing rows ──
--   Two (or more) rows can already share a `(provider, email)` today — one
--   per account that had separately claimed a slot for the SAME real person
--   under the OLD key. This migration keeps the row with the most recent
--   `last_notified_at` (the identical "who saw them last" answer the new
--   `ON CONFLICT (provider, email) DO UPDATE` will keep going forward) and
--   deletes the rest; a tie (two rows with an identical `last_notified_at`,
--   which two `now()`-defaulted concurrent legacy INSERTs could in
--   principle produce) breaks on `id` for a deterministic, idempotent
--   choice — re-running this migration's own `DELETE` a second time changes
--   nothing, since the `NOT EXISTS` predicate below only ever matches a row
--   that has a STRICTLY newer sibling, and by the second run there is at
--   most one row left per `(provider, email)`.
--
--   Any `unsubscribe_token`/`signup_prefill_token` already delivered to an
--   inbox for a row this migration DROPS stops resolving afterward
--   (`resolve_unmatched_email_notice_account_id[_by_prefill_token]` returns
--   `NULL` for it) — the SAME generic outcome `routes::email_unsubscribe`/
--   `routes::email_prefill` already give for any unknown token, by their
--   own stated enumeration-resistance contract (0228's/0229's headers), not
--   a new failure mode this migration introduces. Accepted: the surviving
--   row's own token (the most recently sent copy) still resolves; rewriting
--   an already-delivered email's own links after the fact is not possible.
--
--   ── Additive/idempotent, same convention as this table's own family
--   (0228/0229's headers) ── safe to run again, and safe to run against a
--   database with zero pre-existing rows (the `DELETE` and the constraint
--   swap are then no-ops beyond the swap itself).
--
--   ── RLS unaffected ──
--   The table's own `account_isolation` policy (migration 0227) is
--   untouched — still a bare, account-scoped `USING`/`WITH CHECK` on
--   `account_id`, which remains a real, populated column on every row.
--   Swapping which columns the UNIQUE constraint covers has no bearing on
--   RLS at all; `scripts/rls_coverage.manifest`'s existing entry for this
--   table needs no change.
--
-- Depends on: 0227_slack_unmatched_email_notices (the table and its
--             original `(account_id, provider, email)` uniqueness),
--             0228/0229 (the token columns this migration's row collapse
--             must not corrupt — collapse only ever REMOVES whole rows; no
--             column here is altered).
-- Down:
--   ALTER TABLE slack_unmatched_email_notices DROP CONSTRAINT IF EXISTS slack_unmatched_email_notices_provider_email_key;
--   ALTER TABLE slack_unmatched_email_notices ADD CONSTRAINT slack_unmatched_email_notices_account_id_provider_email_key UNIQUE (account_id, provider, email);

-- Collapse duplicates BEFORE the new constraint can be added — keep the row
-- with the most recent `last_notified_at` per `(provider, email)`, tie-broken
-- on `id` for a deterministic survivor (see this migration's own header).
DELETE FROM slack_unmatched_email_notices a
WHERE EXISTS (
    SELECT 1 FROM slack_unmatched_email_notices b
    WHERE b.provider = a.provider
      AND b.email = a.email
      AND (b.last_notified_at, b.id) > (a.last_notified_at, a.id)
);

ALTER TABLE slack_unmatched_email_notices
    DROP CONSTRAINT IF EXISTS slack_unmatched_email_notices_account_id_provider_email_key;

ALTER TABLE slack_unmatched_email_notices
    ADD CONSTRAINT slack_unmatched_email_notices_provider_email_key UNIQUE (provider, email);

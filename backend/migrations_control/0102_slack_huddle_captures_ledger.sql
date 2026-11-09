-- Migration: 0102_slack_huddle_captures_ledger
-- Description: `slack_huddle_captures` collapses from a queue (with a
--   `status` lifecycle) to a terminal-outcome ledger, as part of the huddle
--   capture rewrite (docs/connectors/slack.md §3.1/§3.3 — MEASURED WRONG, see
--   `backend/tests/fixtures/slack_huddle/README.md` for the real trigger
--   shape).
--
--   ── Why this is no longer a queue ──
--   Retry-safety moved to the generic `jobs` table
--   (`jobs::JobKind::SlackHuddleCapture`, processed by the rewritten
--   `workers::huddle`) — the SAME infrastructure every other worker already
--   uses for at-least-once delivery + bounded backoff (`jobs::queue::fail`).
--   This table no longer needs `status`/`processing`/`pending` to coordinate
--   a poller claiming work; it exists purely to record, once, what happened
--   to a given huddle:
--     * ingested — `processed_at` is set, `skip_reason` is NULL.
--     * skipped, for a recorded reason — `skip_reason` is set, `processed_at`
--       is NULL.
--   There are no lifecycle transitions between those two terminal states, so
--   there is no `status` column to model them. Enforced with
--   `CHECK (processed_at IS NOT NULL OR skip_reason IS NOT NULL)` — every row
--   must land in one terminal state or the other, never neither (see the
--   backfill section below for why this is safe to add against the rows this
--   migration inherits).
--
--   ── Why idempotency still matters despite no natural retry ──
--   The trigger (`message_changed` with `room.transcript_file_id` present, see
--   `routes::slack_events::huddle_capture_trigger`) is not naturally retried
--   by Slack — but `message_changed` itself fires repeatedly for the same
--   huddle (3 observed for one real huddle in the fixtures), and a later edit
--   can re-present the same `transcript_file_id`. The pre-existing
--   `UNIQUE (account_id, channel_id, huddle_thread_ts)` is EXACTLY the
--   idempotency key needed and is kept unchanged. A row for a SKIPPED huddle
--   must also suppress re-fetching — that is why `skip_reason` exists at all,
--   rather than simply not inserting a row on a skip.
--
--   ── Columns removed, and the ORDER this migration removes them in ──
--   `status` (and its CHECK constraint) — no lifecycle left to encode, see
--   above. `error` — folded into the more general `skip_reason` (a huddle
--   that permanently fails to ingest is recorded the same way as one that is
--   deliberately skipped; both are "we saw this huddle and did not ingest
--   it, here is why" — see `workers::huddle::capture_huddle`'s doc comment
--   for the retryable-vs-terminal split: a genuinely transient failure does
--   NOT write a row here at all, it is left for the `jobs` table's own
--   backoff to retry). `opted_by` — this was the Slack user id who reacted
--   :coral:/:coralswarm: under the deleted reaction-based opt-in flow
--   (`REACTION_NAMES`, removed from `routes/slack_events.rs` in the same PR);
--   huddle capture is no longer opt-in, so there is no "opted by" actor to
--   record.
--
--   These columns carry LIVE DATA on any pre-existing row (this migration's
--   very first application on `coralswarm_dev` already produced legacy rows
--   in every one of the five old `status` values). Naively dropping `error`
--   before `skip_reason` exists would destroy that data with no way to
--   recover it outside a backup restore, and would leave every
--   not-yet-`processed` legacy row with `processed_at IS NULL AND
--   skip_reason IS NULL` — the exact undefined third state this ledger's
--   design says cannot exist (`capture_already_recorded`'s bare
--   `SELECT EXISTS` would then treat those rows as already-recorded forever,
--   permanently suppressing capture of huddles a user explicitly opted into,
--   invisible to every health query since neither terminal predicate matches
--   them). So this migration is ordered to make the eventual state true
--   BEFORE anything is destroyed, not after:
--     1. ADD `skip_reason` (nothing destroyed yet).
--     2. BACKFILL every not-yet-terminal legacy row's `skip_reason` — from
--        `error` where present, else an honest synthesized reason — so the
--        CHECK constraint below is already satisfied for every existing row
--        before it is added.
--     3. DROP `status`/`error`/`opted_by` (now safe — their data already
--        migrated).
--     4. ADD the `CHECK` constraint (now guaranteed to hold).
--
--   ── Column added ──
--   `skip_reason TEXT NULL` — human-readable reason a huddle was NOT
--   ingested (e.g. "canvas has no Summary section", "Slack's placeholder
--   summary...", "no mappable destination ocean for this org account...").
--   Nullable: NULL means ingested (check `processed_at IS NOT NULL`
--   instead). The founder's "skip, but record why" decision — a skip must
--   never be silent.
--
--   `created_at` and `processed_at` are both KEPT unchanged (`processed_at`
--   now doubles as "the row reached a terminal ingested state", rather than
--   "left the 'processing' status").
--
-- Depends on: 0024_huddle_captures (original table).
--
-- Paired with: migrations_tenant/0014_slack_huddle_captures_ledger.sql — this
--   is one of the shared content tables (it is part of the tenant baseline,
--   `migrations_tenant/0001_tenant_baseline.sql`), so altering only one tree
--   fails `scripts/check_schema_parity.sh` by design. `slack_huddle_captures`
--   DOES carry RLS on this (control/consumer) plane — migration
--   `0083_consumer_rls` enables it and creates `account_isolation` on it —
--   this migration does not change that; huddle capture is CONSUMER-only
--   (`workers::tenant_harness`'s decision table), so the tenant-tree copy of
--   this table stays inert (no `member_ocean_isolation` policy,
--   `DELIBERATELY_UNCOVERED` in `backend/tests/tenant_rls_pg.rs`) exactly as
--   it already was.
--
-- Down:
--   -- NOT a true inverse — see the ADD COLUMN lines below. `error` is
--   -- reconstructable from `skip_reason` (lossy: the synthesized-vs-original
--   -- distinction is gone) but `status` cannot be reconstructed at all (this
--   -- ledger has no lifecycle to invert it from), so it is recreated as
--   -- 'processed'/'failed' by inspecting `processed_at`, not blindly
--   -- defaulted to 'pending' (that would mark every already-ingested huddle
--   -- as unprocessed work). `opted_by` has no source data whatsoever (the
--   -- reaction-based opt-in flow that populated it is gone) and 0024 defines
--   -- it `NOT NULL` with NO default, so it cannot be added back without
--   -- supplying a value for every existing row — done here with the empty
--   -- string, which is a real behavior change (0024 never allowed that),
--   -- flagged rather than silently matched.
--   ALTER TABLE slack_huddle_captures DROP CONSTRAINT IF EXISTS slack_huddle_captures_terminal_state_check;
--   ALTER TABLE slack_huddle_captures ADD COLUMN error TEXT;
--   UPDATE slack_huddle_captures SET error = skip_reason WHERE skip_reason IS NOT NULL;
--   ALTER TABLE slack_huddle_captures ADD COLUMN status TEXT;
--   UPDATE slack_huddle_captures SET status = CASE WHEN processed_at IS NOT NULL THEN 'processed' ELSE 'failed' END;
--   ALTER TABLE slack_huddle_captures ALTER COLUMN status SET NOT NULL;
--   ALTER TABLE slack_huddle_captures ALTER COLUMN status SET DEFAULT 'pending';
--   ALTER TABLE slack_huddle_captures ADD CONSTRAINT slack_huddle_captures_status_check
--       CHECK (status IN ('pending', 'processing', 'processed', 'failed', 'cancelled'));
--   ALTER TABLE slack_huddle_captures ADD COLUMN opted_by TEXT NOT NULL DEFAULT '';
--   ALTER TABLE slack_huddle_captures DROP COLUMN IF EXISTS skip_reason;

-- Step 1: add the new column FIRST — nothing destroyed yet.
ALTER TABLE slack_huddle_captures ADD COLUMN IF NOT EXISTS skip_reason TEXT;

-- Step 2: backfill BEFORE dropping anything. Every row that is not already a
-- terminal "ingested" row (`processed_at IS NULL`) gets an honest
-- `skip_reason`: `error`'s content where one was recorded, else a synthesized
-- reason that names the legacy status it came from, so no row is left in the
-- undefined third state (BLOCKER 3 — MEASURED: four of five legacy rows
-- seeded in the old `pending`/`processing`/`failed`/`cancelled`/`processed`
-- statuses came out `processed_at IS NULL AND skip_reason IS NULL` without
-- this step).
UPDATE slack_huddle_captures
SET    skip_reason = COALESCE(NULLIF(error, ''), 'legacy pre-ledger row (status=' || status || ')')
WHERE  processed_at IS NULL
  AND  skip_reason IS NULL;

-- Step 3: now safe to drop the old lifecycle columns — their live data has
-- already been carried into `skip_reason` above.
ALTER TABLE slack_huddle_captures DROP COLUMN IF EXISTS status;
ALTER TABLE slack_huddle_captures DROP COLUMN IF EXISTS error;
ALTER TABLE slack_huddle_captures DROP COLUMN IF EXISTS opted_by;

-- Step 4: enforce the ledger's whole design invariant at the database level
-- — every row is either ingested or explained, never neither. Guaranteed to
-- hold for every pre-existing row by the backfill in step 2 above (verified
-- against a MEASURED five-row legacy fixture — see this PR's report).
ALTER TABLE slack_huddle_captures
    ADD CONSTRAINT slack_huddle_captures_terminal_state_check
    CHECK (processed_at IS NOT NULL OR skip_reason IS NOT NULL);

-- The old 'pending'-only partial index is dead weight now there is no
-- 'pending' status left to filter on.
DROP INDEX IF EXISTS idx_huddle_captures_pending;

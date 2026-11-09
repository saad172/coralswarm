-- Migration: 0192_outlook_everything_mode
-- Description: m365 enterprise train PR 3.4 ("Outlook everything mode").
--   Per-(account, owner) OPT-IN state for the mailbox archetype's
--   everything-mode carve-out (`docs/connectors/CONNECTOR_CONTRACT.md` §4
--   "Two rules that fall out of the matrix", §7 "Everything-mode carve-out",
--   §6 amended test #9): `workers::outlook_everything_mode`'s tenant-loop
--   tick sweeps this table to decide, per (account, member), whether to pull
--   that member's WHOLE mailbox (not just opted-in threads) into their own
--   private ocean.
--
--   ── This is opt-in, not default-on — the load-bearing difference from
--   its structural sibling `microsoft_calendar_sync_state` (0189/0067) ──
--   Calendar is self-owned, non-sensitive data (contract §4's `sync()`
--   rule): default-on, with a per-member OFF switch. Mail is sensitive
--   correspondence; the contract's carve-out permits `sync()`-shaped
--   whole-mailbox ingest ONLY when "the mailbox owner configured it
--   themselves — never an admin, never a default" (§7). So this table's
--   `enabled` column defaults FALSE, and — more importantly — NO code path
--   in this codebase ever INSERTs a row here except `routes::
--   outlook_everything_mode::set_everything_mode`, gated on the acting
--   user enabling it FOR THEMSELVES (see that function's own doc for the
--   actor==target enforcement, which does not rely on RLS alone — RLS here
--   only proves internal consistency of a row that already exists, not who
--   was allowed to write it). Unlike `microsoft_calendar_sync_state`, this
--   worker's tick never lazily upserts a fresh row on discovery: a mailbox
--   with no row here has never had everything mode touched, which is
--   exactly the "nobody's mailbox starts in everything mode" fact
--   conformance test #9 (§6) asserts.
--
--   ── Column choices — same shape as 0189, see that migration's header for
--   the full column-by-column rationale on `owner_user_id`/`graph_id` being
--   `NOT NULL` and why that keeps the tenant twin's RLS policy sound ──
--   `enabled` — `BOOLEAN NOT NULL DEFAULT FALSE`. The off-by-default column
--   default IS the "never default" requirement, not app logic layered on a
--   nullable column — the mirror image of 0189's `sync_enabled DEFAULT
--   TRUE` reasoning, for the opposite carve-out shape.
--
--   `resume_cursor` — persisted Graph `@odata.nextLink` (`connectors::
--   microsoft::BackfillPage::next_cursor`) from `list_recent_messages`, the
--   SAME whole-mailbox pagination helper CS-118 workflow backfill already
--   uses (`BACKFILL_SCAN_CAP`-bounded pages per call — contract §6 test #6
--   discipline, not the calendar worker's cursorless `$top=20` gap). `NULL`
--   means "start from the newest message" — both the initial state and the
--   state after a page reports `complete = true` (Graph's own end reached;
--   the next tick re-scans from the top, which is fine: `atoms`' unique
--   `(graph_id, source_type, source_id)` constraint makes re-ingesting an
--   already-seen message a cheap no-op, exactly the reasoning `docs/
--   connectors/outlook.md`'s calendar-cursor-gap paragraph already gives for
--   the analogous re-fetch).
--
--   `claimed_until` — CodeRabbit round 2 on PR #700 (F1): an atomic claim so
--   two backend processes sweeping the same fleet can never both pick the
--   SAME owner in the same window. Without this, two concurrent `resolve_
--   outlook_everything_mode_due()`/`enabled_owners_for_account` reads could
--   both return the same due owner, and the LATER of the two `record_
--   progress` writes would silently overwrite a NEWER `resume_cursor` with a
--   stale one — worse than the plain duplicate-work case the analogous gap
--   in `microsoft_calendar_sync`/`microsoft_calendar_sync_state` tolerates
--   (that table has no cursor to corrupt; re-fetching its cursorless
--   `$top=20` window twice is merely wasteful, not lossy — see that table's
--   own migration for why it deliberately has no claim column). `NULL` or a
--   past timestamp means unclaimed/reclaimable; the claiming query (below,
--   and this table's tenant-tree twin's `enabled_owners_for_account`) sets it
--   to `now() + interval '15 minutes'` in the SAME statement that selects the
--   row, via `FOR UPDATE SKIP LOCKED`.
--
--   RELEASE-ON-SUCCESS, BACKOFF-ON-FAILURE (CodeRabbit round 3, F2 —
--   corrected from round 2's unconditional release): `record_progress`
--   (`workers::outlook_everything_mode`) clears `claimed_until` back to
--   `NULL` once a tick's work on the row ends in SUCCESS (a page fetched
--   with no ingest error), so a healthy owner is immediately reclaimable.
--   `record_progress`'s error branch and `mark_failed_no_progress` (a
--   pre-fetch failure — creds/token/Graph error) instead stamp `claimed_
--   until = now() + interval '30 minutes'`. Round 2's `NULL`-on-failure was
--   itself a bug: neither failure path ever advances `last_synced_at`, so a
--   permanently-failing owner sorts to the FRONT of `ORDER BY last_synced_at
--   ASC NULLS FIRST` forever — releasing its claim immediately let it be
--   reclaimed on literally the next tick, so a handful of permanently-
--   failing owners could monopolize every `LIMIT 5` batch and starve every
--   healthy owner behind them. The 30-minute backoff fixes this by
--   excluding the row from the claim predicate (`claimed_until IS NULL OR
--   claimed_until < now()`) — a `WHERE` filter that runs before the `ORDER
--   BY`/`LIMIT` — without touching `last_synced_at`, ordering, or index
--   usage at all; this was chosen over reordering by e.g. `GREATEST(last_
--   synced_at, last_error_at)` specifically to leave the success path's
--   proven behavior untouched. Either way, a crashed/stuck process's claim
--   still self-expires after its original 15 minutes rather than orphaning
--   the row forever.
--
--   `claim_id` — FENCING TOKEN (CodeRabbit round 4 on PR #700, F3): the
--   claim shape above (`claimed_until` alone) still let a STALE claim holder
--   corrupt a NEWER claimant's state. Sequence: worker A claims (15-minute
--   TTL) and then stalls (GC pause, slow Graph response, etc.) past that
--   TTL; worker B's discovery sees the expired `claimed_until` and reclaims
--   the SAME row; worker A eventually wakes up and calls `record_progress`/
--   `mark_failed_no_progress` — unfenced, those writes matched on bare
--   `(account_id, owner_user_id)` and would silently overwrite B's newer
--   `resume_cursor`/error state, and A's failure-path 30-minute backoff
--   could even stomp B's still-ACTIVE claim out from under it. `claim_id` is
--   a random token (`gen_random_uuid()`) stamped in the SAME claiming
--   `UPDATE` that sets `claimed_until` (both the bootstrap function below
--   and its tenant-tree twin's `enabled_owners_for_account`), and returned
--   to the caller alongside the existing claim columns. Every completion/
--   failure write (`record_progress`, `mark_failed_no_progress`) now adds
--   `AND claim_id = $n` to its `WHERE` clause: worker A's stale token no
--   longer matches B's row once B has reclaimed it (a fresh `gen_random_
--   uuid()` on every claim, TTL-expired or not), so A's write affects ZERO
--   rows instead of clobbering B's state — `workers::
--   outlook_everything_mode`'s callers treat a zero-row update as "another
--   worker owns this row now," log a warn, and return without touching
--   anything else. `claimed_until` alone could not serve as its own fence:
--   it is a timestamp, not a token — two racing claims can (and, in the bug
--   this closes, do) both look "currently claimed" from the stale holder's
--   point of view, so the fence needs an identity, not just a deadline.
--
-- Depends on: 0189_microsoft_calendar_sync_state (structural precedent),
--             0181_google_watch_channels_bootstrap_function (the
--             SECURITY DEFINER cross-account sweep pattern this migration's
--             own bootstrap function below follows, for the CONSUMER-plane
--             tick — org tenant ticks already know their one account_id and
--             never call it, same reasoning 0181's header gives).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_outlook_everything_mode_due() FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_outlook_everything_mode_due() FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_outlook_everything_mode_due();
--   DROP POLICY IF EXISTS account_isolation ON outlook_everything_mode;
--   DROP TABLE IF EXISTS outlook_everything_mode;

CREATE TABLE IF NOT EXISTS outlook_everything_mode (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    owner_user_id    UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    graph_id         UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    enabled          BOOLEAN     NOT NULL DEFAULT FALSE,
    resume_cursor    TEXT,
    claimed_until    TIMESTAMPTZ,
    claim_id         UUID,
    last_synced_at   TIMESTAMPTZ,
    last_error       TEXT,
    last_error_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, owner_user_id)
);

ALTER TABLE outlook_everything_mode ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON outlook_everything_mode;
CREATE POLICY account_isolation ON outlook_everything_mode
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND EXISTS (
            SELECT 1 FROM graphs
            WHERE graphs.id = outlook_everything_mode.graph_id
              AND graphs.account_id = current_setting('app.account_id')::uuid
              AND graphs.owner_user_id = outlook_everything_mode.owner_user_id
              AND graphs.kind = 'member'
              AND graphs.archived_at IS NULL
        )
    );

-- CONSUMER-plane cross-account sweep discovery (see this migration's own
-- header, and 0181's header for the full incident this pattern avoids): the
-- personal/consumer-plane tick has no single account to scope a plain
-- `SELECT ... FROM outlook_everything_mode` against, and a bare read under
-- `app.account_id` unset would silently return zero rows. Bare ids only
-- (`account_id`, `owner_user_id`, `graph_id`) — never `resume_cursor` or any
-- other column — consumed purely to discover WHICH accounts have enabled
-- everything mode; each is re-read (and re-verified `enabled = true`) inside
-- its own `begin_account_scoped` transaction by `workers::
-- outlook_everything_mode`'s consumer-plane sweep, so the only thing
-- crossing an account boundary here is the identifier the sweep needs to
-- open a scope with.
--
-- ATOMIC CLAIM (CodeRabbit round 2 on PR #700, F1): this used to be a plain
-- `SELECT ... LIMIT 5`, `STABLE`. With more than one backend process running
-- this sweep, two processes could read the same due owner in the same
-- window; the LATER of their two `record_progress` writes then overwrites a
-- NEWER `resume_cursor` with a stale one, corrupting that owner's resumable
-- pagination (worse than the merely-wasteful duplicate-ingest case — `atoms`'
-- own `(graph_id, source_type, source_id)` uniqueness makes a re-ingested
-- message a cheap no-op, but a clobbered cursor can silently skip a whole
-- page of never-ingested messages). This function is now the CLAIM itself,
-- not just discovery: the inner `SELECT ... FOR UPDATE SKIP LOCKED` locks up
-- to 5 eligible rows (`enabled = true` and unclaimed/claim-expired) and the
-- outer `UPDATE` stamps `claimed_until = now() + interval '15 minutes'` in
-- the SAME statement, so no second caller can observe an eligible-but-not-
-- yet-claimed row between two round trips. `workers::outlook_everything_
-- mode`'s `record_progress`/`mark_failed_no_progress` release the claim
-- (`claimed_until = NULL`) on success or back it off (`claimed_until = now()
-- + interval '30 minutes'`) on failure once that tick's work on the row is
-- done — see this migration's own header, "release-on-success,
-- backoff-on-failure", for why the failure case is a backoff and not a
-- release (CodeRabbit round 3, F2). An abandoned claim (crashed process)
-- simply expires after its original 15 minutes either way, and the row
-- becomes claimable again. No longer `STABLE` — a claiming function modifies
-- data.
--
-- FENCING TOKEN (CodeRabbit round 4, F3 — see this migration's own header,
-- "claim_id"): the SAME `UPDATE` also stamps `claim_id = gen_random_uuid()`
-- and returns it. `record_progress`/`mark_failed_no_progress` gate their
-- completion/failure writes on `AND claim_id = $n`, so a worker that stalls
-- past its 15-minute `claimed_until` and wakes up AFTER another worker has
-- reclaimed the row can no longer clobber that newer claim's cursor/error/
-- claimed_until state — its stale token simply matches zero rows.
CREATE OR REPLACE FUNCTION resolve_outlook_everything_mode_due()
RETURNS TABLE (account_id uuid, owner_user_id uuid, graph_id uuid, claim_id uuid)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
    UPDATE outlook_everything_mode oem
    SET claimed_until = now() + interval '15 minutes',
        claim_id = gen_random_uuid()
    FROM (
        SELECT id
        FROM outlook_everything_mode
        WHERE enabled = true
          AND (claimed_until IS NULL OR claimed_until < now())
        -- Least-recently-synced first + a hard per-tick cap (PR 3.4
        -- CodeRabbit round 1): the consumer sweep is fleet-wide, and each
        -- returned owner can cost a full BACKFILL_SCAN_CAP page of
        -- serialized ingests — an unbounded result would hold the sweep slot
        -- for O(N x page). NULLS FIRST makes the cap self-rotating: owners
        -- skipped this tick sort to the front of the next one once anyone
        -- else syncs.
        ORDER BY last_synced_at ASC NULLS FIRST
        LIMIT 5
        FOR UPDATE SKIP LOCKED
    ) claimable
    WHERE oem.id = claimable.id
    RETURNING oem.account_id, oem.owner_user_id, oem.graph_id, oem.claim_id
$$;

REVOKE EXECUTE ON FUNCTION resolve_outlook_everything_mode_due() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_outlook_everything_mode_due() TO coralswarm_app, CURRENT_USER;

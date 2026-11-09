-- Migration: 0197_gmail_everything_mode
-- Description: gsuite enterprise train PR G2.4 ("Gmail everything mode").
--   Per-(account, owner) OPT-IN state for the Mailbox archetype's
--   everything-mode carve-out (`docs/connectors/CONNECTOR_CONTRACT.md` §4
--   "Two rules that fall out of the matrix", §7 "Everything-mode carve-out",
--   §6 amended conformance test #9): `workers::gmail_everything_mode`'s
--   tenant-loop tick sweeps this table to decide, per (account, member),
--   whether to pull that member's WHOLE mailbox (not just opted-in threads)
--   into their own private ocean.
--
--   Gmail's tenant-loop counterpart to `migrations_control/
--   0192_outlook_everything_mode.sql` (m365 enterprise train PR 3.4 — the
--   FIRST implementation of this carve-out, per founder decision recorded in
--   `.scratch/gsuite-enterprise/plan.md`: "Q2 = NO — G2.4 waits for Outlook
--   3.4"). Byte-identical column set, byte-identical claim/fencing-token
--   mechanics, ported verbatim rather than re-derived: PR #700's four
--   CodeRabbit rounds already found and fixed every race this shape is
--   vulnerable to (F1 atomic claim, F2 release-on-success/backoff-on-
--   failure, F3 claim_id fencing token) — see 0192's own header for the full
--   incident-by-incident history. Only what genuinely differs for Gmail is
--   called out below; everything else restates 0192's reasoning unchanged.
--
--   ── This is opt-in, not default-on — same load-bearing distinction 0192
--   draws against `microsoft_calendar_sync_state` ── Gmail's own opt-in
--   per-thread ingest (`google_gmail.rs`, `gmail_watched_threads`) already
--   established the Mailbox archetype's normal posture; everything mode is
--   the founder-gated exception, so `enabled` defaults FALSE and — more
--   importantly — NO code path in this codebase ever INSERTs a row here
--   except `routes::gmail_everything_mode::set_everything_mode`, gated on
--   the acting user enabling it FOR THEMSELVES (actor==target enforcement in
--   that function itself, not merely RLS — see 0192's header for why RLS
--   alone is insufficient: it only proves internal consistency of a row
--   that already exists, never who was allowed to write it).
--   `workers::gmail_everything_mode`'s tick never lazily upserts a fresh row
--   on discovery: a mailbox with no row here has never had everything mode
--   touched — exactly the "nobody's mailbox starts in everything mode" fact
--   conformance test #9 (§6) asserts.
--
--   `resume_cursor` — persisted Gmail `nextPageToken` from `connectors::
--   google_gmail::fetch_everything_mode_mail`'s `users.messages.list` pull.
--   Deliberately NOT `google_watch_channels.sync_token`/`gmail_watched_
--   threads` — everything mode does not reuse the opt-in/live-follow
--   `history.list` machinery at all (see that function's own doc for why:
--   reusing it would couple this founder-gated, opt-in-only feature into the
--   SAME incremental path every other Gmail ingest caller shares, the exact
--   coupling `workers::outlook_everything_mode`'s own module doc gives as
--   the reason Outlook's worker calls a dedicated, non-trait connector
--   function instead of widening `ConnectorProvider::sync`). `NULL` means
--   "start from the newest message" — both the initial state and the state
--   after a page reports `complete = true` (Gmail's own `nextPageToken`
--   absence); `atoms`' `(graph_id, source_type, source_id)` uniqueness makes
--   a re-scan's already-seen messages a cheap no-op, the identical argument
--   0192's header gives for Outlook's `resume_cursor`.
--
--   `claimed_until` / `claim_id` — atomic claim + fencing token, byte-
--   identical mechanics to 0192's own columns. See that migration's header,
--   "claimed_until" and "claim_id", for the full incident history (two
--   processes racing the same due owner corrupting a resumable cursor; a
--   stale worker's late write clobbering a newer claimant's state). Ported
--   here from the START rather than re-discovered, since this table is new
--   and #700 already proved the shape end to end.
--
--   ── Fixes applied BEFORE merge, from adversarial review (unmerged
--   migration — edited in place, same license #677 round 1 and #700's own
--   rounds already used on their own unmerged files) ──
--
--   `last_attempted_at` — NEW column, absent from 0192's own precedent.
--   0192's discovery ordering (`ORDER BY last_synced_at ASC NULLS FIRST`)
--   only advances `last_synced_at` on SUCCESS; neither failure path
--   (`record_progress`'s error branch, `mark_failed_no_progress`) touches
--   it. A never-succeeding owner (revoked credentials, permanent Gmail
--   error) therefore sorts to the front of every claim query FOREVER — its
--   sort key never moves forward while every healthy owner's does — and a
--   fistful of such owners can occupy every slot the per-tick budget below
--   allows, on every tick, starving every healthy owner out indefinitely.
--   Reviewed and reproduced against real Postgres (adversarial review
--   round, 2026-08-25) before merge. Fixed here, not inherited: `workers::
--   gmail_everything_mode` now stamps `last_attempted_at = now()` on EVERY
--   outcome (success AND failure) and orders discovery by IT instead —
--   `last_synced_at` keeps its narrower "last successful sync" meaning
--   (preserved for any future status surface), while `last_attempted_at` is
--   the pure rotation key, monotonically advancing for every owner touched
--   this tick regardless of outcome.
--
--   `WITH CHECK` — the `kind = 'member'` arm below (byte-identical to
--   0192's own) is CORRECT for the Org plane, where `resolve_primary_graph`
--   always resolves a `kind='member'` ocean — but WRONG, unconditionally,
--   for the Personal/consumer plane: `resolve_primary_graph`'s Personal arm
--   (`routes/slack.rs`) creates/resolves a `kind='personal', is_primary=
--   true` graph with `owner_user_id` left NULL (there is exactly one owner
--   per personal account, so `kind='personal'` IS the ownership anchor
--   there — no member-scoped row ever exists on this plane). Since this
--   table's OWN doc and `workers::gmail_everything_mode`'s doc both state
--   "a personal-plane user can enable it for their own mailbox exactly as
--   an org member can for their own" as a design requirement, and 0192's
--   Outlook precedent has the IDENTICAL gap (confirmed against real
--   Postgres + the real `coralswarm_app` role under the adversarial review
--   — Outlook's own everything-mode enable is equally broken in prod today,
--   not something this PR is responsible for fixing), the WITH CHECK below
--   adds a second, Personal-plane arm rather than porting 0192's
--   member-only shape unchanged.
--
--   `NOT gmail_everything_mode.enabled OR EXISTS (...)` — the graph-shape
--   check below applies ONLY when the row is being written with `enabled =
--   true`; a DISABLE (the row's `enabled` column set to `false`) always
--   passes, account-match aside. Two rounds of adversarial review found two
--   DIFFERENT ways the original always-checked version broke a disable that
--   never even touches `graph_id`:
--
--   Round 1 found: the only legitimate writer, `routes::
--   gmail_everything_mode::set_everything_mode`, always resolves a fresh,
--   LIVE graph via `resolve_primary_graph` before writing — so the check
--   was never load-bearing at ENABLE time — but a disable/claim UPDATE
--   leaves `graph_id` unchanged, so Postgres re-evaluates the SAME WITH
--   CHECK against whatever the graph looks like NOW on every later write.
--   An owner whose member ocean is later archived (offboarding,
--   `member_ocean_reconciler`'s archive arm) therefore could not reach
--   their own OFF switch, and — worse — the worker's per-tick claim UPDATE
--   (`enabled_owners_for_account`) failed for the WHOLE BATCH it is part
--   of, since one violating row aborts an entire multi-row `UPDATE`,
--   single-org-tenant-wide. Round 1's fix dropped `archived_at IS NULL`
--   from the check.
--
--   Round 2 found that was insufficient: this migration's own consuming
--   test (`disabling_succeeds_even_when_the_owners_ocean_cannot_be_
--   resolved_pg`) seeds a graph `kind='personal', is_primary=false` — a
--   valid personal graph, just not the CURRENT resolvable primary (the
--   shape a stale/orphaned reference could realistically leave behind) —
--   and disabling against it STILL failed WITH CHECK's `is_primary = true`
--   arm even after archived_at was dropped. The general fix is the `NOT
--   enabled OR` gate above: a disable needs NO graph validation at all,
--   for the identical "already validated at enable time, graph_id doesn't
--   change" reasoning round 1 already established — round 1 just closed
--   one specific way that validation could go stale (archival) instead of
--   removing it for the whole disable path. This does not weaken the
--   carve-out: kind/owner matching still fully prevents ever pairing this
--   table with a team ocean or a different member's ocean on any write
--   that ACTUALLY sets `enabled = true`.
--
-- Depends on: 0192_outlook_everything_mode (structural + mechanical
--             precedent — the FIRST implementation of this carve-out, which
--             this migration consumes rather than re-derives), 0186_
--             google_gmail_watch_owner_and_threads (Gmail's own per-member
--             token/ownership precedent on this connector — `owner_user_id`
--             here is always a real member, never account-wide, matching
--             `connectors::google::Surface::Gmail.token_owner_for`).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_gmail_everything_mode_due() FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_gmail_everything_mode_due() FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_gmail_everything_mode_due();
--   DROP POLICY IF EXISTS account_isolation ON gmail_everything_mode;
--   DROP TABLE IF EXISTS gmail_everything_mode;

CREATE TABLE IF NOT EXISTS gmail_everything_mode (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    owner_user_id    UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    graph_id         UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    enabled          BOOLEAN     NOT NULL DEFAULT FALSE,
    resume_cursor    TEXT,
    claimed_until    TIMESTAMPTZ,
    claim_id         UUID,
    last_synced_at   TIMESTAMPTZ,
    last_attempted_at TIMESTAMPTZ,
    last_error       TEXT,
    last_error_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, owner_user_id)
);

ALTER TABLE gmail_everything_mode ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON gmail_everything_mode;
CREATE POLICY account_isolation ON gmail_everything_mode
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND (
            -- DISABLING skips the graph-shape check entirely (round 2 of
            -- the adversarial review fix, superseding the archived_at-only
            -- version above): a disable UPDATE never changes `graph_id`, so
            -- re-validating "is this still a live, correctly-shaped
            -- ocean?" on every disable serves no purpose the original
            -- ENABLE-time check didn't already serve, and actively breaks
            -- legitimate disables whenever the referenced graph's shape has
            -- since changed for ANY reason — not just archival (dropping
            -- ONLY `archived_at IS NULL` still left a real failure: a graph
            -- seeded `kind='personal', is_primary=false` — a valid, if
            -- non-primary, personal graph a stale/orphaned row might
            -- reference — still failed the `is_primary = true` arm below,
            -- reproduced by this migration's own consuming test,
            -- `disabling_succeeds_even_when_the_owners_ocean_cannot_be_
            -- resolved_pg`). `enabled` is a column on the NEW row itself, so
            -- WITH CHECK can reference it directly with no extra lookup.
            NOT gmail_everything_mode.enabled
            OR EXISTS (
                SELECT 1 FROM graphs
                WHERE graphs.id = gmail_everything_mode.graph_id
                  AND graphs.account_id = current_setting('app.account_id')::uuid
                  AND (
                      -- Org plane: the owner's own private member ocean.
                      (graphs.kind = 'member' AND graphs.owner_user_id = gmail_everything_mode.owner_user_id)
                      -- Personal plane: the account's single primary ocean —
                      -- `resolve_primary_graph`'s Personal arm never sets
                      -- `owner_user_id` (one account, one owner;
                      -- `kind='personal'` IS the ownership anchor there).
                      OR (graphs.kind = 'personal' AND graphs.is_primary = true)
                  )
            )
        )
    );

-- CONSUMER-plane cross-account sweep discovery + atomic claim — byte-
-- identical mechanics to 0192's own `resolve_outlook_everything_mode_due()`;
-- see that migration's header (and 0181's, for the original bootstrap-
-- resolver incident both patterns descend from) for the full rationale this
-- comment does not repeat.
--
-- Two fixes applied before merge (adversarial review, 2026-08-25):
-- (1) `ORDER BY last_attempted_at ASC NULLS FIRST`, not `last_synced_at` —
--     see this migration's own header, "last_attempted_at", for the
--     starvation incident this closes (a permanently-failing owner's sort
--     key must advance on EVERY attempt, not just a successful one).
-- (2) `LIMIT 25`, not `LIMIT 5` — 0192's own `LIMIT 5` (ported here
--     unchanged in this PR's first draft) is a FLEET-WIDE cap on this
--     CONSUMER-plane bootstrap: unlike the Org-plane claim below, which is
--     scoped to one account's own membership per tenant-loop tick, this
--     function serves every personal account sharing the consumer database
--     in ONE query per tick. At `LIMIT 5` / `POLL_INTERVAL` = 30 minutes,
--     the whole fleet's personal-plane throughput ceiling is 10 owners/hour
--     regardless of how many have opted in — 200 opted-in personal users
--     would each get roughly one `EVERYTHING_MODE_PAGE_SIZE`-sized page
--     every 20 hours. Raised 5x or the same worst-case-bounded-tick
--     reasoning `OWNERS_PER_TICK`'s own comment gives, still finite and
--     still self-rotating via `NULLS FIRST`.
CREATE OR REPLACE FUNCTION resolve_gmail_everything_mode_due()
RETURNS TABLE (account_id uuid, owner_user_id uuid, graph_id uuid, claim_id uuid)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
    UPDATE gmail_everything_mode gem
    SET claimed_until = now() + interval '15 minutes',
        claim_id = gen_random_uuid()
    FROM (
        SELECT id
        FROM gmail_everything_mode
        WHERE enabled = true
          AND (claimed_until IS NULL OR claimed_until < now())
        ORDER BY last_attempted_at ASC NULLS FIRST
        LIMIT 25
        FOR UPDATE SKIP LOCKED
    ) claimable
    WHERE gem.id = claimable.id
    RETURNING gem.account_id, gem.owner_user_id, gem.graph_id, gem.claim_id
$$;

REVOKE EXECUTE ON FUNCTION resolve_gmail_everything_mode_due() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_gmail_everything_mode_due() TO coralswarm_app, CURRENT_USER;

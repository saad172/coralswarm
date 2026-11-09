-- Migration 0206: google_meet_sync_state (gsuite Train G3 PR 2 — Meet
--   ingest, `docs/connectors/google-workspace.md` §11).
--
--   Per-(account, connected-member) pull cursor for the Meet ingest sweep
--   (`workers::google_meet_sweep`). Meet ships PULL-ONLY in this PR (the
--   doc's own §3 C7 row: "Push planned — Workspace Events API, same family.
--   Not built in this PR") — this table exists so the sweep has somewhere to
--   remember "how far back has this connected member's meetings already been
--   checked", the identical bookkeeping role `google_watch_channels` plays
--   for the push-based surfaces, without pretending Meet has a push
--   subscription of its own.
--
--   ── Why NOT `google_watch_channels` (column overload) ──
--   Calendar (`0182`/`0063`) and Gmail (`0186`/`0066`) both extended
--   `google_watch_channels` in place ("new columns, not a new table") because
--   a watch CHANNEL and its sync cursor share one identity/lifecycle for
--   those two surfaces. Meet has no channel to share an identity with: this
--   PR registers no `google_watch_channels` row, ever, and
--   `workers::google_watch_renewal` has no `'meet'` arm and gets none here —
--   adding a `surface='meet'` row to that table would either need a synthetic
--   `expires_at` the renewal worker would then try (and fail) to act on, or a
--   new carve-out inside a renewal worker whose entire reason to exist is
--   renewing PUSH channels. A dedicated table with no `expires_at`/
--   `channel_id`/`topic_name` columns at all is the honest shape for a
--   pull-only cursor — same reasoning `gmail_everything_mode` (`0197`) used
--   to justify NOT reusing `google_watch_channels`/`gmail_watched_threads`
--   for its own, structurally different, whole-mailbox cursor.
--
--   ── Column choices ──
--   `last_synced_end_time` — the watermark: the latest
--   `conferenceRecord.endTime` this owner's sweep has already processed
--   (through). `NULL` means "never swept" — the sweep's own first-run
--   fallback seeds a bounded lookback window (see
--   `connectors::google_meet::DEFAULT_LOOKBACK` and its own doc) rather than
--   attempting to enumerate a member's entire Meet history, mirroring the
--   "no bulk historical pull on first connect" posture Gmail's opt-in model
--   already established for a different reason (consent, not retention) —
--   here the reason is retention (`ConferenceRecord.expireTime` is 30 days
--   after the meeting ends per Google's own API reference, so nothing older
--   than that is reachable regardless).
--
--   `claimed_until` / `claim_id` — the same atomic-claim + fencing-token
--   shape `resolve_gmail_everything_mode_due()` (migration `0197`) and
--   `resolve_outlook_everything_mode_due()` (migration `0192`) already
--   established for a CONSUMER-plane cross-account sweep; ported here rather
--   than re-derived, since both precedents already survived their own
--   adversarial review rounds for this exact race.
--
--   `last_attempted_at` — the discovery sort key (`ORDER BY ... ASC NULLS
--   FIRST` in `resolve_google_meet_sync_due()` below), advanced on EVERY
--   outcome (success and failure) from day one — `gmail_everything_mode`'s
--   own header documents the starvation incident that shipped WITHOUT this
--   column and had to be patched in; this table starts with the fix already
--   applied rather than repeating that incident a third time.
--
--   ── Row lifecycle ──
--   `GoogleConnector::post_store`'s `Surface::Meet` arm upserts an initial
--   row (idempotent) the moment a member's `google_meet` OAuth connect
--   completes, `last_synced_end_time = NULL` — the sweep's own bootstrap,
--   there is no separate opt-in step the way `gmail_everything_mode` has
--   (Meet ingest is not founder-gated the way the whole-mailbox carve-out is;
--   the `ORG_GOOGLE_MEET_ENABLED` connector flag plus the OAuth consent
--   itself are the only gates). Disconnecting Meet (`routes::oauth::
--   delete_connection`) deletes this row (see `connectors::google_disconnect`
--   PR change) — stopping the sweep is what "disconnect stops ingest" means
--   here, mirroring the "disconnecting Chat stops Chat" precedent
--   (`google-workspace.md` §9's own closing bullet), even though this
--   surface's cursor is a plain row rather than an upstream subscription to
--   revoke.
--
-- Depends on: none (bare table, only `accounts`/`users` FKs).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_google_meet_sync_due() FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_google_meet_sync_due() FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_google_meet_sync_due();
--   DROP POLICY IF EXISTS account_isolation ON google_meet_sync_state;
--   DROP TABLE IF EXISTS google_meet_sync_state;

CREATE TABLE IF NOT EXISTS google_meet_sync_state (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id             UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    owner_user_id          UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    last_synced_end_time   TIMESTAMPTZ,
    claimed_until          TIMESTAMPTZ,
    claim_id               UUID,
    last_attempted_at      TIMESTAMPTZ,
    last_synced_at         TIMESTAMPTZ,
    last_error             TEXT,
    last_error_at          TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, owner_user_id)
);

-- Plain account-scoped bookkeeping on the shared consumer/control DB — no
-- graph_id/ocean anchor (this table records a per-owner CURSOR, never a
-- destination; see `connectors::google_meet`'s own module doc for why the
-- fan-out destination is resolved fresh on every ingest instead of stored).
ALTER TABLE google_meet_sync_state ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON google_meet_sync_state;
CREATE POLICY account_isolation ON google_meet_sync_state
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

-- CONSUMER-plane cross-account sweep discovery + atomic claim — byte-
-- identical mechanics to `resolve_gmail_everything_mode_due()` (migration
-- 0197): `workers::google_meet_sweep`'s consumer-plane tick needs this
-- because it has no single account_id to scope a plain SELECT against (the
-- same reason 0181's original bootstrap resolver exists). Unlike Gmail's
-- version there is no `enabled` gate here — every row this table holds
-- exists because a member completed a `google_meet` OAuth connect (see this
-- migration's own header, "Row lifecycle"); there is no separate opt-in
-- toggle to filter on.
CREATE OR REPLACE FUNCTION resolve_google_meet_sync_due()
RETURNS TABLE (account_id uuid, owner_user_id uuid, claim_id uuid)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
    UPDATE google_meet_sync_state gms
    SET claimed_until = now() + interval '15 minutes',
        claim_id = gen_random_uuid()
    FROM (
        SELECT id
        FROM google_meet_sync_state
        WHERE (claimed_until IS NULL OR claimed_until < now())
        ORDER BY last_attempted_at ASC NULLS FIRST
        LIMIT 25
        FOR UPDATE SKIP LOCKED
    ) claimable
    WHERE gms.id = claimable.id
    RETURNING gms.account_id, gms.owner_user_id, gms.claim_id
$$;

REVOKE EXECUTE ON FUNCTION resolve_google_meet_sync_due() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_google_meet_sync_due() TO coralswarm_app, CURRENT_USER;

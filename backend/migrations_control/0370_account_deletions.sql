-- Migration: 0370_account_deletions
-- Description: Settings launch plan PR 2.4 (decisions 14, 17, 19, 24). The
--   durable, resumable record of a "delete my account" request, with a 7-day
--   cancellable grace period.
--
--   ── STATE MACHINE ──
--     grace      POST accepted. The user is locked out (except status and
--                cancel), the Stripe subscription is ended, bearer tokens are
--                revoked and the user is emailed a cancel link. NOTHING
--                destructive has run. Lasts until `purge_after` (created_at
--                + 7 days; created_at for a Clerk-initiated delete, where the
--                Clerk user is already gone and there is nothing to cancel).
--     running    `purge_after` passed (or webhook-initiated). The destructive
--                steps run. No way back: cancel is refused.
--     completed  Every step done; the row is the minimal non-PII tombstone.
--     cancelled  The user (or an eligibility re-check) cancelled during
--                grace. Access is restored; the subscription stays ended and
--                tokens stay revoked. The row is kept for audit and no longer
--                blocks anything (the unique indexes and the gate ignore it),
--                so a later request starts a fresh deletion.
--   grace -> running and grace -> cancelled are both guarded UPDATEs on
--   `state = 'grace'`, so a cancel racing the transition has exactly one
--   winner.
--
--   ── WHAT THE ROW IS ──
--   Job and tombstone in one row. `completed_steps` is the checkpoint: the
--   worker (`workers::account_deletion`) resumes at the first step not
--   recorded, and every step is safe to run twice. A completed tombstone
--   holds opaque ids only (user, account, Clerk user), timestamps, the
--   initiator, what Stripe did and the extra-usage amount forfeited
--   (decision 19). No email, name, IP or content; the typed-email
--   confirmation is verified in memory and discarded.
--
--   ── WHY THE ACCOUNT ROW IS KEPT (the financial fence) ──
--   `accounts` and `users` are NOT deleted. `credit_ledger`, `extra_usage_*`,
--   `stripe_*`, `managed_attempt_receipts`, `llm_usage` and friends are the
--   financial audit trail, and the 0321 entry fence plus the no-truncate and
--   "managed debit history is immutable" triggers refuse to let them be
--   removed (the cascade from `DELETE FROM accounts` would trip them, and
--   disabling a fence to get around that is not an option). Keeping the
--   account row keeps every one of those rows valid and untouched; the
--   `users` row is anonymized instead. Hence RESTRICT on both foreign keys.
--
--   ── search_path ──
--   Every SECURITY DEFINER function pins `search_path = pg_catalog, public,
--   pg_temp` (pg_temp LAST, so a session's temporary tables can never shadow a
--   real one) and schema-qualifies every relation and function it touches, so
--   the app role cannot redirect them with `CREATE TEMP TABLE
--   account_deletions` and a forged `running` row.
--
--   ── RLS, and the app role ──
--   Prod connects as `coralswarm_app`: not the owner, subject to RLS, and with
--   0083's deliberate revokes (escrow_shares, node_case_fold_log) and the
--   append-only revokes (retraction_audit_log, corpus_capture_admissions,
--   reef_identity_corrections). The job therefore does its privileged deletes
--   through SECURITY DEFINER functions below, each with a pinned search_path,
--   EXECUTE revoked from PUBLIC and granted to the app roles only, and each
--   refusing to act unless a deletion row for exactly that id is in the state
--   that permits it:
--     account_deletion_claim(lease)            worker claim (SKIP LOCKED)
--     account_deletion_state_for_clerk_user()  request-time gate (auth door)
--     account_deletion_state_for_account()     billing webhook gate
--     account_deletion_note_clerk_deleted()    Clerk `user.deleted` webhook
--     account_deletion_cancel(id)              grace -> cancelled
--     account_deletion_purge_restricted(id)    the app role cannot delete these
--     account_deletion_purge_user_rows(id)     the user's rows in EVERY account,
--                                              including accounts RLS hides
--     account_deletion_stuck(hours)            alert sweep
--   Writers on the request path use begin_account_scoped as usual.
--
--   ── NUMBERING ──
--   Main is at 0363. Claimed ahead of this: 0364-0366 (Library read-model,
--   #1458), 0367 (#1460, extra-usage schema), 0368 (#1473), 0369 (#1472).
--   This is the next free number, 0370. check_migrations.sh requires those to
--   land first (it forbids gaps).
--
-- Depends on: 0001_init (users, accounts), 0083_consumer_rls, 0233 (policy
--             convention).
-- Down:
--   DROP FUNCTION IF EXISTS account_deletion_claim(INT),
--       account_deletion_state_for_clerk_user(TEXT),
--       account_deletion_state_for_account(UUID),
--       account_deletion_note_clerk_deleted(TEXT),
--       account_deletion_cancel(UUID),
--       account_deletion_deletable_accounts(UUID),
--       account_deletion_user_keyed_tables(),
--       account_deletion_purge_restricted(UUID),
--       account_deletion_purge_user_rows(UUID),
--       account_deletion_stuck(INT);
--   DROP TABLE IF EXISTS account_deletions;

SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS account_deletions (
    id                               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id                          UUID        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    -- The user's personal account (the one the request was made against).
    -- Any other personal-org account the same user owns is handled by the
    -- same job; see `services::account_deletion::personal_account_ids`.
    account_id                       UUID        NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    -- Opaque Clerk id. Kept after anonymization on purpose: it is what stops
    -- a stale JWT for the deleted Clerk user from re-provisioning a fresh
    -- user, and what makes the `user.deleted` webhook idempotent. Clerk never
    -- reuses ids, and the id resolves to nothing once the Clerk user is gone.
    clerk_user_id                    TEXT        CHECK (clerk_user_id IS NULL OR clerk_user_id <> ''),
    initiated_by                     TEXT        NOT NULL CHECK (initiated_by IN ('user', 'clerk_webhook')),
    state                            TEXT        NOT NULL DEFAULT 'grace'
        CHECK (state IN ('grace', 'running', 'completed', 'cancelled')),
    -- Ordered steps already finished. See services::account_deletion::STEPS.
    completed_steps                  TEXT[]      NOT NULL DEFAULT '{}',
    -- True once Clerk no longer has the user: the `user.deleted` webhook set
    -- it (or the job was created from that webhook). The `clerk` step is then
    -- a no-op instead of a delete that would 404.
    clerk_user_deleted               BOOLEAN     NOT NULL DEFAULT false,
    -- What the billing step did, for the audit trail.
    stripe_outcome                   TEXT,
    -- Decision 19: purchased extra-usage credits are forfeited, not refunded.
    forfeited_extra_usage_micro_usd  BIGINT      NOT NULL DEFAULT 0 CHECK (forfeited_extra_usage_micro_usd >= 0),
    attempts                         INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error                       TEXT,
    next_attempt_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until                      TIMESTAMPTZ,
    -- Destructive steps may not start before this (decision 24).
    purge_after                      TIMESTAMPTZ NOT NULL DEFAULT now() + interval '7 days',
    running_since                    TIMESTAMPTZ,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at                     TIMESTAMPTZ,
    cancelled_at                     TIMESTAMPTZ,

    CONSTRAINT account_deletions_steps_known CHECK (
        completed_steps <@ ARRAY['detach', 'stripe_cancel', 'revoke_tokens', 'notify_started',
                                 'forfeit', 'connectors', 'keys', 'data', 'tenant_planes',
                                 'stripe_customer', 'clerk', 'blob_drain', 'notify_completed',
                                 'tombstone']::TEXT[]
    ),
    CONSTRAINT account_deletions_completed_shape CHECK (
        (state = 'completed') = (completed_at IS NOT NULL)
    ),
    CONSTRAINT account_deletions_cancelled_shape CHECK (
        (state = 'cancelled') = (cancelled_at IS NOT NULL)
    ),
    -- Destructive work only ever happens in `running` (or after it).
    CONSTRAINT account_deletions_running_since CHECK (
        (state IN ('running', 'completed')) = (running_since IS NOT NULL)
        OR state = 'cancelled'
    ),
    -- A completed tombstone has run every step (tombstone is last).
    CONSTRAINT account_deletions_completed_has_tombstone CHECK (
        state <> 'completed' OR 'tombstone' = ANY (completed_steps)
    ),
    -- Nothing destructive is recorded while still in grace.
    CONSTRAINT account_deletions_grace_is_not_destructive CHECK (
        state <> 'grace' OR NOT (completed_steps && ARRAY['forfeit', 'connectors', 'keys', 'data',
            'tenant_planes', 'stripe_customer', 'clerk', 'blob_drain', 'notify_completed',
            'tombstone']::TEXT[])
    )
);

-- One live (not cancelled) deletion per user, per account, and per Clerk user.
CREATE UNIQUE INDEX IF NOT EXISTS account_deletions_user_idx
    ON account_deletions (user_id) WHERE state <> 'cancelled';
CREATE UNIQUE INDEX IF NOT EXISTS account_deletions_account_idx
    ON account_deletions (account_id) WHERE state <> 'cancelled';
CREATE UNIQUE INDEX IF NOT EXISTS account_deletions_clerk_user_idx
    ON account_deletions (clerk_user_id) WHERE clerk_user_id IS NOT NULL AND state <> 'cancelled';
-- The worker's claim: runnable rows by due time.
CREATE INDEX IF NOT EXISTS account_deletions_runnable_idx
    ON account_deletions (next_attempt_at) WHERE state IN ('grace', 'running');

ALTER TABLE account_deletions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON account_deletions;
CREATE POLICY account_isolation ON account_deletions
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

-- ── Worker claim ─────────────────────────────────────────────────────────
-- Claims at most one runnable deletion: in grace or running, due, and not
-- leased by a live worker. The lease makes a crashed worker's claim expire
-- instead of wedging the job; SKIP LOCKED lets several workers coexist.
-- Counting the attempt here means a crash mid-step still counts.
CREATE OR REPLACE FUNCTION public.account_deletion_claim(p_lease_secs INT)
RETURNS SETOF public.account_deletions
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    UPDATE public.account_deletions d
       SET lease_until = now() + make_interval(secs => GREATEST(p_lease_secs, 1)),
           attempts = d.attempts + 1,
           updated_at = now()
     WHERE d.id = (
            SELECT id FROM public.account_deletions
             WHERE state IN ('grace', 'running')
               AND next_attempt_at <= now()
               AND (lease_until IS NULL OR lease_until < now())
             ORDER BY next_attempt_at, created_at
             LIMIT 1
             FOR UPDATE SKIP LOCKED)
    RETURNING d.*;
$$;

-- ── Request-time gate ────────────────────────────────────────────────────
-- 'grace' / 'running' / 'completed' / NULL for a Clerk subject. Read by the
-- auth door in the same statement as the rest of the identity prefetch. A
-- cancelled deletion is invisible: the user is a normal user again.
CREATE OR REPLACE FUNCTION public.account_deletion_state_for_clerk_user(p_clerk_user_id TEXT)
RETURNS TEXT
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT state FROM public.account_deletions WHERE clerk_user_id = p_clerk_user_id AND state <> 'cancelled';
$$;

-- Same, by account: the Stripe webhook resolves an account from a customer id
-- and must refuse billing events for a deleted account. Answers only for an
-- account that really is the deleted user's own (see below).
CREATE OR REPLACE FUNCTION public.account_deletion_state_for_account(p_account UUID)
RETURNS TEXT
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT d.state
      FROM public.account_deletions d
      JOIN public.accounts a ON a.id = p_account
     WHERE d.state <> 'cancelled'
       -- Only an account that is the deleted user's own: they own it and no one
       -- else is a member. A personal account they merely co-owned with another
       -- user (the deletion may even be anchored on it) is NOT theirs to refuse
       -- billing for.
       AND a.owner_user_id = d.user_id
       AND NOT EXISTS (SELECT 1 FROM public.account_members m WHERE m.account_id = a.id AND m.user_id <> d.user_id);
$$;

-- ── Clerk `user.deleted` webhook ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.account_deletion_note_clerk_deleted(p_clerk_user_id TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE found_id UUID;
BEGIN
    UPDATE public.account_deletions
       SET clerk_user_deleted = true, updated_at = now()
     WHERE clerk_user_id = p_clerk_user_id AND state <> 'cancelled'
    RETURNING id INTO found_id;
    RETURN found_id IS NOT NULL;
END $$;

-- ── Cancel (grace only) ──────────────────────────────────────────────────
-- 'cancelled' | 'too_late' (already running / completed / cancelled) |
-- 'not_found'. The single guarded UPDATE is what makes a cancel racing the
-- worker's grace -> running transition have exactly one winner.
CREATE OR REPLACE FUNCTION public.account_deletion_cancel(p_id UUID)
RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE st TEXT;
BEGIN
    UPDATE public.account_deletions
       SET state = 'cancelled', cancelled_at = now(), lease_until = NULL, updated_at = now()
     WHERE id = p_id AND state = 'grace' AND initiated_by = 'user';
    IF FOUND THEN RETURN 'cancelled'; END IF;
    SELECT state INTO st FROM public.account_deletions WHERE id = p_id;
    IF st IS NULL THEN RETURN 'not_found'; END IF;
    RETURN 'too_late';
END $$;

-- ── The personal accounts a deletion may wipe ────────────────────────────
-- Accounts the user owns as their personal home (the plain personal account
-- and, once adopted, the one standing for their personal org) that NO other
-- user is a live member of. A shared personal account (POST /v1/account/
-- members) holds other people's data and is never wiped by one member's
-- deletion.
CREATE OR REPLACE FUNCTION public.account_deletion_deletable_accounts(p_user UUID)
RETURNS SETOF UUID
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT a.id FROM public.accounts a
     WHERE a.owner_user_id = p_user
       AND (a.clerk_org_id IS NULL OR EXISTS (
              SELECT 1 FROM public.personal_orgs p JOIN public.users u ON u.clerk_user_id = p.clerk_user_id
               WHERE p.clerk_org_id = a.clerk_org_id AND u.id = p_user))
       AND NOT EXISTS (SELECT 1 FROM public.account_members m WHERE m.account_id = a.id AND m.user_id <> p_user)
     ORDER BY (a.clerk_org_id IS NOT NULL), a.created_at, a.id;
$$;

-- ── Tables the app role cannot delete from ───────────────────────────────
-- escrow_shares (0083 revoke), node_case_fold_log (0083 revoke),
-- corpus_capture_admissions and reef_identity_corrections (append-only
-- grants). Callable only for a deletion in `running`, and only for that
-- deletion's own deletable accounts and user.
CREATE OR REPLACE FUNCTION public.account_deletion_purge_restricted(p_id UUID)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE d public.account_deletions; accts UUID[];
BEGIN
    SELECT * INTO d FROM public.account_deletions WHERE id = p_id AND state = 'running';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'account deletion % is not in its destructive phase', p_id;
    END IF;
    SELECT COALESCE(array_agg(x), '{}') INTO accts FROM public.account_deletion_deletable_accounts(d.user_id) x;
    DELETE FROM public.escrow_shares WHERE account_id = ANY (accts) OR admin_user_id = d.user_id;
    DELETE FROM public.node_case_fold_log WHERE graph_id IN (SELECT id FROM public.graphs WHERE account_id = ANY (accts));
    DELETE FROM public.reef_identity_corrections WHERE graph_id IN (SELECT id FROM public.graphs WHERE account_id = ANY (accts));
    DELETE FROM public.corpus_capture_admissions WHERE account_id = ANY (accts);
END $$;

-- ── The user's rows, in every account ────────────────────────────────────
-- Emulates the ON DELETE CASCADE from `users` (the row itself is kept): one
-- (table, column) per cascading foreign key, plus the two user-keyed tables
-- with no foreign key. The app role cannot see rows in accounts it is not
-- scoped to (RLS), which is exactly where a user's leftovers from organizations
-- they already left live, so this runs as the owner. Refuses while the user
-- still belongs to a live team organization: detaching comes first, and the
-- purge never touches a team org the user is still in.
CREATE OR REPLACE FUNCTION public.account_deletion_user_keyed_tables()
RETURNS TABLE (tbl TEXT, col TEXT)
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, public, pg_temp AS $$
    VALUES
    ('account_members','user_id'), ('addin_microsoft_links','user_id'),
    ('addin_microsoft_sessions','user_id'), ('addin_pairing_codes','owner_user_id'),
    ('addin_tokens','owner_user_id'), ('conversations','user_id'),
    ('desktop_tokens','owner_user_id'), ('escrow_recovery_log','recovered_user_id'),
    ('escrow_shares','admin_user_id'), ('escrow_wrapped_keys','user_id'),
    ('gmail_everything_mode','owner_user_id'), ('gmail_hidden_messages','owner_user_id'),
    ('gmail_watched_threads','ingested_by'), ('google_chat_watched_spaces','owner_user_id'),
    ('google_chat_watched_spaces','reader_user_id'),
    ('google_meet_ingested_conferences','owner_user_id'), ('google_meet_sync_state','owner_user_id'),
    ('google_watch_channels','owner_user_id'), ('journal_rollups','user_id'),
    ('mcp_tasks','owner_user_id'), ('mcp_tenant_selection','user_id'),
    ('microsoft_calendar_sync_state','owner_user_id'), ('org_auto_adds','user_id'),
    ('org_join_request_digests','admin_user_id'), ('org_join_requests','user_id'),
    ('org_membership_tombstones','user_id'), ('outlook_everything_mode','owner_user_id'),
    ('outlook_watched_conversations','ingested_by'), ('outlook_workflows','user_id'),
    ('queries','user_id'), ('query_history','user_id'), ('sessions','user_id'),
    ('slack_member_channels','user_id'), ('slack_watched_channels','user_id'),
    ('slot_reservations','user_id'), ('team_member_key_envelopes','user_id'),
    ('team_members','user_id'), ('team_seat_grants','user_id'),
    ('teams_container_watchers','user_id'), ('teams_watched_resources','user_id'),
    ('user_access_log','user_id'), ('user_account_prefs','user_id'),
    ('user_active_graph','user_id'), ('user_prefs','user_id'),
    ('user_private_keys','user_id'), ('connector_identities','user_id'),
    ('platform_active_users','user_id'), ('mcp_tool_calls','user_id');
$$;

CREATE OR REPLACE FUNCTION public.account_deletion_purge_user_rows(p_id UUID)
RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE d public.account_deletions; t RECORD; n BIGINT; total BIGINT := 0;
BEGIN
    SELECT * INTO d FROM public.account_deletions WHERE id = p_id AND state = 'running';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'account deletion % is not in its destructive phase', p_id;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.account_members am JOIN public.accounts a ON a.id = am.account_id
         WHERE am.user_id = d.user_id AND a.clerk_org_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM public.org_revocations r WHERE r.clerk_org_id = a.clerk_org_id)
           AND NOT EXISTS (SELECT 1 FROM public.personal_orgs p JOIN public.users u ON u.clerk_user_id = p.clerk_user_id
                            WHERE p.clerk_org_id = a.clerk_org_id AND u.id = am.user_id)) THEN
        RAISE EXCEPTION 'user still belongs to a team organization; detach first';
    END IF;
    FOR t IN SELECT * FROM public.account_deletion_user_keyed_tables() LOOP
        EXECUTE format('DELETE FROM public.%I WHERE %I = $1', t.tbl, t.col) USING d.user_id;
        GET DIAGNOSTICS n = ROW_COUNT;
        total := total + n;
    END LOOP;
    -- Free-text copies of the address.
    DELETE FROM public.auth_codes WHERE lower(email) = lower((SELECT email FROM public.users WHERE id = d.user_id));
    DELETE FROM public.waitlist_snapshot WHERE email_normalized = lower((SELECT email FROM public.users WHERE id = d.user_id));
    RETURN total;
END $$;

-- ── Alert sweep ──────────────────────────────────────────────────────────
-- Deletions that should have finished: destructive phase running longer than
-- p_hours, or grace whose first phase (lockout, Stripe, tokens) has not
-- completed within an hour. The worker logs each at ERROR with alert = true.
CREATE OR REPLACE FUNCTION public.account_deletion_stuck(p_hours INT)
RETURNS TABLE (id UUID, state TEXT, current_step_count INT, age_hours DOUBLE PRECISION, last_error TEXT)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT d.id, d.state, cardinality(d.completed_steps),
           EXTRACT(EPOCH FROM (now() - COALESCE(d.running_since, d.created_at))) / 3600.0,
           d.last_error
      FROM public.account_deletions d
     WHERE (d.state = 'running' AND d.running_since < now() - make_interval(hours => GREATEST(p_hours, 1)))
        OR (d.state = 'grace' AND d.created_at < now() - interval '1 hour'
            AND NOT (d.completed_steps @> ARRAY['detach', 'stripe_cancel', 'revoke_tokens', 'notify_started']::TEXT[]));
$$;

REVOKE ALL ON FUNCTION account_deletion_claim(INT),
    account_deletion_state_for_clerk_user(TEXT),
    account_deletion_state_for_account(UUID),
    account_deletion_note_clerk_deleted(TEXT),
    account_deletion_cancel(UUID),
    account_deletion_deletable_accounts(UUID),
    account_deletion_user_keyed_tables(),
    account_deletion_purge_restricted(UUID),
    account_deletion_purge_user_rows(UUID),
    account_deletion_stuck(INT) FROM PUBLIC;
DO $$ DECLARE role_name TEXT; BEGIN
    FOREACH role_name IN ARRAY ARRAY['coralswarm_app', 'coralswarm_tenant_app'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('GRANT EXECUTE ON FUNCTION account_deletion_claim(INT), account_deletion_state_for_clerk_user(TEXT), account_deletion_state_for_account(UUID), account_deletion_note_clerk_deleted(TEXT), account_deletion_cancel(UUID), account_deletion_deletable_accounts(UUID), account_deletion_user_keyed_tables(), account_deletion_purge_restricted(UUID), account_deletion_purge_user_rows(UUID), account_deletion_stuck(INT) TO %I', role_name);
        END IF;
    END LOOP;
END $$;

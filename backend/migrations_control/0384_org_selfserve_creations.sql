-- Migration: 0384_org_selfserve_creations
-- Description: Self-serve org signup, PR A2 (supersedes #1400's 0355) —
--   `POST /v1/orgs`, the owner's provisioning retry, and the sweep that
--   cleans up orgs that were never paid for or whose provisioning failed.
--
--   1. `org_selfserve_creations` — one row per `POST /v1/orgs` attempt,
--      keyed by (creator, Idempotency-Key) and written BEFORE the Clerk
--      organization is created. It is:
--        * the idempotency record: a double-submit finds the row and returns
--          the same org and the same Checkout (`checkout_session_id`,
--          `checkout_url`); an attempt that died after Clerk answered is
--          recognised by the `private_metadata` marker carrying this row's
--          id and adopted, never re-created;
--        * the request's LEASE (`lease_token`, `lease_until`): a request
--          claims the row in one short transaction, makes its Clerk and
--          Stripe calls holding NO database connection, and records each
--          result with a write guarded by its token. A second request for
--          the same row while the lease is live is answered 409
--          `create_in_progress` (security review S1: the earlier design held
--          an advisory lock on a pooled connection across the network calls,
--          and enough parallel requests deadlocked the pool);
--        * what the creation caps count (persisted, so a restart or a second
--          machine does not reset them): per creator, per email domain
--          (`cap_bucket`; free-mail domains share one bucket), and a global
--          soft alert-and-throttle threshold (S8);
--        * the abandoned-org sweep's own state (`sweep_state`, an explicit
--          state column, `tenancy::org_selfserve_cleanup::SweepState`):
--
--              none ──► deleting ──► deleted
--                │         │
--                ▼         ▼
--               kept      none (released: the Checkout completed after all)
--
--          `kept` is terminal for the sweep: the org's trial moved past
--          checkout, so it is a paying (or trialing) org the sweep never
--          touches again. `deleting` is the atomic claim, taken only while no
--          request holds the lease (and a request takes the lease only from
--          `none`), so a create replay and the sweep never act at once (S5).
--          A claim older than the stale window is taken over by the next pass
--          (crash recovery). `sweep_next_check_at` orders the scan, so rows
--          the sweep leaves alone are pushed back instead of starving it (N4);
--        * the paid-but-held bookkeeping (`collection_paused_at`, S7): when a
--          paid org's provisioning is `held`, the sweep pauses collection on
--          its subscription at Stripe and records it here;
--        * the retry rate limit (`retry_count`, `last_retry_at`).
--      No RLS: a control-plane bookkeeping table read and written only by
--      `routes::orgs_create` and the sweep, with no user content beyond the
--      org name the creator typed. The rows exist before any account does,
--      so there is no account to scope them to. Registered in
--      `scripts/check_schema_parity.sh`'s CONTROL_TABLES in this same PR.
--      `user_id` is ON DELETE SET NULL (S9): the row outlives its creator so
--      the sweep can still clean up the org.
--
--   2. `tenant_provisioning_jobs.clerk_org_cleanup` gains `deleting` and
--      `held` (A1's CHECK allowed only none/pending/done; #1517's N6 note):
--
--          none ──(job fails, A1)──► pending ──claim──► deleting ──► done
--                                      ▲                   │
--                                      └──── release ──────┤
--                                                          ├──► held  (paid, auto-requeue budget spent,
--                                                          │           or the org is already routed)
--                                                          └──► none  (requeue: paid after all; the job runs again)
--
--      plus `clerk_org_cleanup_claimed_at` (the claim's clock: the claim
--      TOKEN every later write of the pass must present, and the stale
--      takeover's clock) and `clerk_org_cleanup_requeues` (how many times the
--      sweep has put a paid org's job back; capped in the definer, because
--      each run can mint a Neon project).
--
--   3. Definers (`pg_catalog, public, pg_temp`, schema-qualified, per 0376):
--      * NEW `tenant_provisioning_claim_clerk_org_cleanup(org, stale)`:
--        `pending → deleting` in one conditional write (or takes over a
--        `deleting` claim older than `stale`). Only a `failed` job. Returns
--        the claim token (`clerk_org_cleanup_claimed_at`), NULL when not
--        claimed.
--      * NEW `tenant_provisioning_release_clerk_org_cleanup(org, outcome,
--        claim, max_requeues)`: out of `deleting`, only for the holder of
--        `claim`: back to `pending`, to `held`, or `requeue` (the same reset
--        as `retry`, counted). A requeue past `max_requeues`, or of an org
--        that already has a routing row in another status, goes to `held`
--        instead (N7, N8). Returns what it did: `pending`, `held`,
--        `held_cap`, `held_routed`, `requeued`, or `lost` (not the holder).
--      * REPLACED `tenant_provisioning_retry`: also refuses a job whose
--        Clerk org is being deleted (`deleting`), so a concurrent owner retry
--        and the sweep can never both act on one org. Otherwise unchanged.
--      * REPLACED `tenant_provisioning_mark_clerk_org_cleanup_done`: now
--        `(org, claim)`, and only from `deleting` held by `claim` (the
--        0381 one-argument form is dropped), so nothing can mark an org
--        cleaned up without holding the CURRENT claim.
--      All four are app-role definers (the sweep and the retry route run as
--      `coralswarm_app`); none writes `tenant_databases` or the credential
--      table, and none moves a job past `verify_org`. The app role still has
--      SELECT only on `tenant_provisioning_jobs`;
--      `scripts/check_provisioning_privileges.sh` lists the two new ones as
--      app-callable.
--
-- Down:
--   DROP FUNCTION IF EXISTS tenant_provisioning_release_clerk_org_cleanup(text, text, timestamptz, integer);
--   DROP FUNCTION IF EXISTS tenant_provisioning_claim_clerk_org_cleanup(text, interval);
--   DROP FUNCTION IF EXISTS tenant_provisioning_mark_clerk_org_cleanup_done(text, timestamptz);
--   -- (re-create 0381's tenant_provisioning_retry and
--   --  tenant_provisioning_mark_clerk_org_cleanup_done(text) bodies and grants)
--   UPDATE tenant_provisioning_jobs SET clerk_org_cleanup = 'pending'
--    WHERE clerk_org_cleanup IN ('deleting', 'held');
--   ALTER TABLE tenant_provisioning_jobs DROP CONSTRAINT tenant_provisioning_jobs_cleanup_claim_has_clock;
--   ALTER TABLE tenant_provisioning_jobs DROP CONSTRAINT tenant_provisioning_jobs_clerk_org_cleanup_check;
--   ALTER TABLE tenant_provisioning_jobs ADD CONSTRAINT tenant_provisioning_jobs_clerk_org_cleanup_check
--     CHECK (clerk_org_cleanup IN ('none', 'pending', 'done'));
--   ALTER TABLE tenant_provisioning_jobs DROP COLUMN clerk_org_cleanup_claimed_at,
--     DROP COLUMN clerk_org_cleanup_requeues;
--   DROP TABLE IF EXISTS org_selfserve_creations;

SET LOCAL lock_timeout = '3s';

-- ── 1. org_selfserve_creations ───────────────────────────────────────────────

CREATE TABLE public.org_selfserve_creations (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- The creator. SET NULL (S9): the row is rate-limit history and the
    -- sweep's bookkeeping for an org that still exists in Clerk; it must
    -- never block deleting the person, and it must outlive them so the
    -- sweep can still delete an abandoned org.
    user_id             UUID        REFERENCES public.users(id) ON DELETE SET NULL,
    -- The client's `Idempotency-Key` header, verbatim (bounded).
    idempotency_key     TEXT        NOT NULL CHECK (length(idempotency_key) BETWEEN 8 AND 200),
    name                TEXT        NOT NULL CHECK (length(name) BETWEEN 1 AND 256),
    seats               INTEGER     NOT NULL CHECK (seats >= 2),
    -- The per-domain cap's bucket (S8): the creator's verified email domain,
    -- lowercased, or '*freemail' for the shared free-mail bucket. NULL only
    -- for a row nothing counts by domain.
    cap_bucket          TEXT        CHECK (cap_bucket IS NULL OR length(cap_bucket) BETWEEN 1 AND 255),
    -- creating: reserved, Clerk not yet answered (or the attempt died);
    -- created: the Clerk org exists; refused: Clerk refused the create (or
    -- the sweep gave up on a dead attempt), so nothing exists and the row no
    -- longer counts against the caps.
    status              TEXT        NOT NULL DEFAULT 'creating'
                                    CHECK (status IN ('creating', 'created', 'refused')),
    clerk_org_id        TEXT        CHECK (clerk_org_id IS NULL OR clerk_org_id ~ '^org_[A-Za-z0-9]{1,64}$'),
    -- SET NULL: the sweep deletes the account of an abandoned org and keeps
    -- this row (it still counts against the creator's daily cap).
    account_id          UUID        REFERENCES public.accounts(id) ON DELETE SET NULL,
    -- The Checkout this attempt issued, so a double-submit gets the same one.
    checkout_session_id TEXT        CHECK (checkout_session_id IS NULL OR checkout_session_id ~ '^cs_[A-Za-z0-9_]{1,200}$'),
    checkout_url        TEXT        CHECK (checkout_url IS NULL OR checkout_url ~ '^https?://'),
    -- The request lease (S1, see the header). Both set or both NULL.
    lease_token         UUID,
    lease_until         TIMESTAMPTZ,
    -- The abandoned-org sweep (see the header).
    sweep_state         TEXT        NOT NULL DEFAULT 'none'
                                    CHECK (sweep_state IN ('none', 'kept', 'deleting', 'deleted')),
    sweep_claimed_at    TIMESTAMPTZ,
    sweep_next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at          TIMESTAMPTZ,
    -- S7: when the sweep paused collection on a held org's subscription.
    collection_paused_at TIMESTAMPTZ,
    -- `POST /v1/orgs/{org}/provisioning/retry`'s rate limit.
    retry_count         INTEGER     NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_retry_at       TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT org_selfserve_creations_user_key UNIQUE (user_id, idempotency_key),
    CONSTRAINT org_selfserve_creations_org_key UNIQUE (clerk_org_id),
    CONSTRAINT org_selfserve_creations_account_key UNIQUE (account_id),
    -- A created row always names its Clerk org; a refused one never does.
    CONSTRAINT org_selfserve_creations_status_shape
        CHECK ((status = 'created') = (clerk_org_id IS NOT NULL)),
    CONSTRAINT org_selfserve_creations_lease_shape
        CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
    -- Only a created org can be swept, and a claim always carries its clock.
    CONSTRAINT org_selfserve_creations_sweep_shape
        CHECK ((sweep_state = 'none' OR status = 'created')
               AND ((sweep_state = 'deleting') = (sweep_claimed_at IS NOT NULL))
               AND ((sweep_state = 'deleted') = (deleted_at IS NOT NULL)))
);

-- The caps: per creator per day / pending, per domain bucket, and per hour
-- across everyone.
CREATE INDEX org_selfserve_creations_user_created_idx
    ON public.org_selfserve_creations (user_id, created_at)
    WHERE status <> 'refused';
CREATE INDEX org_selfserve_creations_bucket_created_idx
    ON public.org_selfserve_creations (cap_bucket, created_at)
    WHERE status <> 'refused';
CREATE INDEX org_selfserve_creations_created_idx
    ON public.org_selfserve_creations (created_at)
    WHERE status <> 'refused';
-- The sweep's scan: created orgs it has not settled, by when to look next.
CREATE INDEX org_selfserve_creations_sweep_idx
    ON public.org_selfserve_creations (sweep_next_check_at)
    WHERE status = 'created' AND sweep_state IN ('none', 'deleting');
-- The sweep's dead-attempt scan (N2).
CREATE INDEX org_selfserve_creations_creating_idx
    ON public.org_selfserve_creations (created_at)
    WHERE status = 'creating';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_app') THEN
        REVOKE ALL ON public.org_selfserve_creations FROM coralswarm_app;
        GRANT SELECT, INSERT, UPDATE ON public.org_selfserve_creations TO coralswarm_app;
    END IF;
END $$;

-- ── 2. clerk_org_cleanup gains `deleting` and `held` ─────────────────────────

ALTER TABLE public.tenant_provisioning_jobs
    DROP CONSTRAINT tenant_provisioning_jobs_clerk_org_cleanup_check;
ALTER TABLE public.tenant_provisioning_jobs
    ADD CONSTRAINT tenant_provisioning_jobs_clerk_org_cleanup_check
    CHECK (clerk_org_cleanup IN ('none', 'pending', 'deleting', 'held', 'done'));
ALTER TABLE public.tenant_provisioning_jobs
    ADD COLUMN clerk_org_cleanup_claimed_at TIMESTAMPTZ,
    ADD COLUMN clerk_org_cleanup_requeues   INTEGER NOT NULL DEFAULT 0
        CHECK (clerk_org_cleanup_requeues >= 0);
ALTER TABLE public.tenant_provisioning_jobs
    ADD CONSTRAINT tenant_provisioning_jobs_cleanup_claim_has_clock
    CHECK ((clerk_org_cleanup = 'deleting') = (clerk_org_cleanup_claimed_at IS NOT NULL));

-- ── 3. Definers ──────────────────────────────────────────────────────────────

-- 3a. The sweep's atomic claim. `p_stale` (at least a minute) is how old a
-- `deleting` claim must be before another pass may take it over: the
-- claimer crashed between the claim and its release/done. Returns the claim
-- token (its clock, `clock_timestamp()`, so a takeover always differs from
-- the claim it replaces), or NULL when nothing was claimed.
CREATE FUNCTION public.tenant_provisioning_claim_clerk_org_cleanup(p_clerk_org_id text, p_stale interval)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    token timestamptz;
BEGIN
    IF p_stale IS NULL OR p_stale < interval '1 minute' THEN
        RAISE EXCEPTION 'stale window must be at least a minute' USING ERRCODE = '22023';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET clerk_org_cleanup = 'deleting', clerk_org_cleanup_claimed_at = clock_timestamp(), updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id
       AND status = 'failed'
       AND (clerk_org_cleanup = 'pending'
            OR (clerk_org_cleanup = 'deleting' AND clerk_org_cleanup_claimed_at < now() - p_stale))
    RETURNING clerk_org_cleanup_claimed_at INTO token;
    RETURN token;
END
$$;

-- 3b. Leave `deleting` without deleting anything, only for the holder of
-- `p_claim` (a pass whose claim was taken over writes nothing: 'lost'):
--   'pending' — give the claim back (a transient failure; the next pass
--               decides again);
--   'held'    — the org is paid but cannot be requeued: an operator (or the
--               owner's retry) decides;
--   'requeue' — the org is paid after all (a late ACH/SEPA settlement, #1517
--               S9): the same reset as `tenant_provisioning_retry`, counted
--               in `clerk_org_cleanup_requeues`. Past `p_max_requeues`
--               ('held_cap'), or for an org that already has a routing row
--               in any status but `provisioning` ('held_routed'), it goes to
--               `held` instead of failing and leaving the claim behind.
CREATE FUNCTION public.tenant_provisioning_release_clerk_org_cleanup(
    p_clerk_org_id text, p_outcome text, p_claim timestamptz, p_max_requeues integer)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
    routed text;
    held_as text;
BEGIN
    IF p_outcome IS NULL OR p_outcome NOT IN ('pending', 'held', 'requeue') THEN
        RAISE EXCEPTION 'unknown release outcome %', p_outcome USING ERRCODE = '22023';
    END IF;
    IF p_claim IS NULL OR p_max_requeues IS NULL OR p_max_requeues < 0 THEN
        RAISE EXCEPTION 'a claim token and a non-negative requeue cap are required' USING ERRCODE = '22023';
    END IF;
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND OR job.status <> 'failed' OR job.clerk_org_cleanup <> 'deleting'
       OR job.clerk_org_cleanup_claimed_at IS DISTINCT FROM p_claim THEN
        RETURN 'lost';
    END IF;
    held_as := NULL;
    IF p_outcome = 'requeue' THEN
        SELECT status INTO routed FROM public.tenant_databases WHERE clerk_org_id = p_clerk_org_id;
        IF FOUND AND routed <> 'provisioning' THEN
            held_as := 'held_routed';
        ELSIF job.clerk_org_cleanup_requeues >= p_max_requeues THEN
            held_as := 'held_cap';
        END IF;
    END IF;
    IF p_outcome IN ('pending', 'held') OR held_as IS NOT NULL THEN
        UPDATE public.tenant_provisioning_jobs
           SET clerk_org_cleanup = CASE WHEN p_outcome = 'pending' THEN 'pending' ELSE 'held' END,
               clerk_org_cleanup_claimed_at = NULL, updated_at = now()
         WHERE clerk_org_id = p_clerk_org_id;
        RETURN coalesce(held_as, p_outcome);
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET status = 'provisioning', step = 'verify_org', attempts = 0, last_error = NULL,
           next_attempt_at = now(), failed_at = NULL, clerk_org_cleanup = 'none',
           clerk_org_cleanup_claimed_at = NULL,
           clerk_org_cleanup_requeues = clerk_org_cleanup_requeues + 1,
           cancel_requested_at = NULL, subscription_wait_started_at = NULL,
           version_wait_started_at = NULL, lease_owner = NULL, lease_expires_at = NULL,
           updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN 'requeued';
END
$$;

-- 3c. 0381's retry, plus: refused while the sweep holds the org's claim
-- (`deleting`). A `held` org may be retried (it is paid). Everything else
-- is unchanged.
CREATE OR REPLACE FUNCTION public.tenant_provisioning_retry(p_clerk_org_id text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    job public.tenant_provisioning_jobs;
    routed text;
BEGIN
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = p_clerk_org_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no provisioning job for org %', p_clerk_org_id USING ERRCODE = 'P0002';
    END IF;
    IF job.status <> 'failed' THEN
        RAISE EXCEPTION 'provisioning job for org % is %; only a failed job is retried', p_clerk_org_id, job.status
            USING ERRCODE = '23514';
    END IF;
    IF job.clerk_org_cleanup = 'done' THEN
        RAISE EXCEPTION 'the Clerk org for failed job % was already cleaned up; it cannot be retried', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF job.clerk_org_cleanup = 'deleting' THEN
        RAISE EXCEPTION 'the Clerk org for failed job % is being cleaned up; it cannot be retried', p_clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    SELECT status INTO routed FROM public.tenant_databases WHERE clerk_org_id = p_clerk_org_id;
    IF FOUND AND routed <> 'provisioning' THEN
        RAISE EXCEPTION 'org % has a routing row in status %; refusing to re-provision', p_clerk_org_id, routed
            USING ERRCODE = '23505';
    END IF;
    UPDATE public.tenant_provisioning_jobs
       SET status = 'provisioning', step = 'verify_org', attempts = 0, last_error = NULL,
           next_attempt_at = now(), failed_at = NULL, clerk_org_cleanup = 'none',
           clerk_org_cleanup_claimed_at = NULL,
           cancel_requested_at = NULL, subscription_wait_started_at = NULL,
           version_wait_started_at = NULL, lease_owner = NULL, lease_expires_at = NULL,
           updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id;
    RETURN true;
END
$$;

-- 3d. 0381's "A2 has acted on the orphaned Clerk org", now only from the
-- sweep's own CURRENT claim (`deleting`, held by `p_claim`). The 0381
-- one-argument form is dropped so nothing can call it without the token.
DROP FUNCTION public.tenant_provisioning_mark_clerk_org_cleanup_done(text);

CREATE FUNCTION public.tenant_provisioning_mark_clerk_org_cleanup_done(p_clerk_org_id text, p_claim timestamptz)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    UPDATE public.tenant_provisioning_jobs
       SET clerk_org_cleanup = 'done', clerk_org_cleanup_claimed_at = NULL, updated_at = now()
     WHERE clerk_org_id = p_clerk_org_id AND status = 'failed' AND clerk_org_cleanup = 'deleting'
       AND clerk_org_cleanup_claimed_at = p_claim;
    RETURN FOUND;
END
$$;

REVOKE ALL ON FUNCTION public.tenant_provisioning_claim_clerk_org_cleanup(text, interval) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.tenant_provisioning_release_clerk_org_cleanup(text, text, timestamptz, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.tenant_provisioning_mark_clerk_org_cleanup_done(text, timestamptz) FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_app') THEN
        GRANT EXECUTE ON FUNCTION public.tenant_provisioning_claim_clerk_org_cleanup(text, interval) TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION public.tenant_provisioning_release_clerk_org_cleanup(text, text, timestamptz, integer) TO coralswarm_app;
        GRANT EXECUTE ON FUNCTION public.tenant_provisioning_mark_clerk_org_cleanup_done(text, timestamptz) TO coralswarm_app;
        -- A1's privilege model, re-asserted: the app role still writes the
        -- job table only through definers.
        IF has_table_privilege('coralswarm_app', 'public.tenant_provisioning_jobs', 'INSERT,UPDATE,DELETE') THEN
            RAISE EXCEPTION 'coralswarm_app must not hold table write on tenant_provisioning_jobs';
        END IF;
        IF has_table_privilege('coralswarm_app', 'public.org_selfserve_creations', 'DELETE,TRUNCATE,REFERENCES,TRIGGER') THEN
            RAISE EXCEPTION 'coralswarm_app must hold only SELECT, INSERT, UPDATE on org_selfserve_creations';
        END IF;
    END IF;
END $$;

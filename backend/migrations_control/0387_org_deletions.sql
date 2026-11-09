-- Migration: 0387_org_deletions
-- Description: In-app ORG deletion (docs/plans/org-offboarding-design.md,
--   supersedes the #1395 draft; founder decisions 2026-10-27 items 4, 8, 11,
--   12, 13). The durable, resumable record of "delete this organization",
--   modelled on account deletion (0370): one row is the job, its checkpoint
--   and, once done, the tombstone.
--
--   ── STATE MACHINE ──
--     requested          The owner asked (or the system did: 30 days unpaid,
--                        or a Clerk-dashboard delete). Every member except
--                        the owner is locked out from this moment (the auth
--                        door reads `org_deletion_state_for_org`). The worker
--                        ends the trial and the Stripe subscription NOW (no
--                        proration, no final invoice) and emails the owner.
--     grace              7 days. The owner may export or undo; nobody else
--                        may use the org. Nothing destructive has run.
--     awaiting_approval  Grace is over and an operator must approve before
--                        anything is destroyed (the first N prod deletions,
--                        `ORG_DELETION_APPROVAL_FIRST_N`, default 10).
--     destroying         Revoke access, record the source manifest, write the
--                        30-day export, delete the routing row, the encrypted
--                        DSN and the tenant database (provisioner role,
--                        name-checked), delete the Clerk org. No way back.
--     done               The tombstone. Kept forever with the `accounts` row.
--     canceled           Undone during requested/grace (by the owner) or
--                        during awaiting_approval (by an operator). Access is
--                        restored; the Stripe subscription STAYS cancelled
--                        (they resubscribe). Invisible to the gate and to the
--                        unique indexes, so a later request starts afresh.
--   Every transition is a guarded UPDATE on the expected state, so a cancel
--   racing the worker has exactly one winner.
--
--   ── APPROVAL AND THE STATE MACHINE, ENFORCED IN THE DATABASE ──
--   The serving role (`coralswarm_app`, RLS on in prod) can UPDATE its own
--   account's row, so the rules that matter are not left to application code.
--   The `org_deletions_guard_approval` trigger refuses, for any role that is
--   not (a member of) the table owner:
--     * a row that does not start `requested`, unapproved, with no steps, a
--       `grace_ends_at` at least 7 days after `created_at`, `created_at =
--       now()`, and an `account_id` that is the account of `clerk_org_id`;
--     * any change to `account_id`, `clerk_org_id`, `created_at`,
--       `grace_ends_at`, `initiated_by`, `requested_by_user_id`;
--     * any transition outside requested->grace (the 3 request-phase steps
--       checkpointed), grace->awaiting_approval|destroying (only once
--       `now() >= grace_ends_at`), grace|awaiting_approval->canceled,
--       awaiting_approval->destroying (only when approved), destroying->done;
--     * steps being removed from `completed_steps`;
--     * `approved_at`/`approved_by` being written;
--     * `approval_required` being decided other than NULL->true, or NULL->false
--       while fewer than `org_deletion_policy.approval_first_n` (seeded 10)
--       deletions are `done`. That table is writable by the table owner only;
--       the application's `ORG_DELETION_APPROVAL_FIRST_N` can only RAISE the
--       threshold (the worker takes the larger), never lower it.
--   A CHECK also refuses `destroying`/`done` without the approval it needed.
--   The owner role (the migrator / an operator DSN) is exempt: it is how an
--   operator approves, cancels or extends a deletion.
--   `org_deletion_approve(id, operator)` is the operator entry point; EXECUTE
--   is granted to no application role.
--
--   ── ROUTING ROW + CREDENTIAL ──
--   `org_deletion_delete_routing(id)` deletes an ACTIVE routing row, which
--   0381's `tenant_provisioning_delete_routing` deliberately never does, and
--   only for an org whose deletion is `destroying` with access already
--   revoked, whose self-serve provisioning job is `ready` and bound to the
--   same account. Granted to `coralswarm_provisioner` ONLY (the worker's own
--   login), like every 0381 routing writer; `check_provisioning_privileges.sh`
--   lists it as provisioner-only. The credential is then deleted with 0381's
--   own `tenant_provisioning_delete_credential`.
--
--   ── search_path ──
--   Every SECURITY DEFINER function pins `search_path = pg_catalog, public,
--   pg_temp` (pg_temp LAST) and schema-qualifies every relation it touches.
--
--   ── NUMBERING ──
--   Main ends at 0386 (#1526); this takes the next free number, 0387.
--
-- Depends on: 0001 (accounts, users), 0082 (org_revocations), 0162
--             (stripe_subscriptions), 0321 (billing_enter_account), 0381
--             (tenant_provisioning_jobs, coralswarm_provisioner).
-- Down:
--   DROP FUNCTION IF EXISTS org_deletion_claim(INT), org_deletion_state_for_org(TEXT),
--       org_deletion_completed_count(), org_deletion_approval_first_n(),
--       org_deletion_may_relax_approval(), org_deletion_unpaid_forget(UUID),
--       org_deletion_unpaid_sweep(INT, INT),
--       org_deletion_adoptable_clerk_deletions(INT, INT), org_deletion_expired_exports(INT),
--       org_deletion_stuck(INT), org_deletion_approval_overdue(INT, INT),
--       org_deletion_delete_routing(UUID),
--       org_deletion_approve(UUID, TEXT), org_deletions_guard_approval();
--   DROP TABLE IF EXISTS org_deletion_policy;
--   DROP TABLE IF EXISTS org_unpaid_watch;
--   DROP TABLE IF EXISTS org_deletions;

SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS public.org_deletions (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- The ORG's account (never a personal one). Kept forever: the billing FK
    -- anchor (founder decision 13).
    account_id            UUID        NOT NULL REFERENCES public.accounts(id) ON DELETE RESTRICT,
    clerk_org_id          TEXT        NOT NULL CHECK (clerk_org_id <> ''),
    -- owner: the in-app owner asked. unpaid: 30 days unpaid (decision 4).
    -- clerk_dashboard: the org was deleted in Clerk; the tenant still gets
    -- the grace, the membership does not (decision 13).
    initiated_by          TEXT        NOT NULL CHECK (initiated_by IN ('owner', 'unpaid', 'clerk_dashboard')),
    requested_by_user_id  UUID        REFERENCES public.users(id) ON DELETE SET NULL,
    state                 TEXT        NOT NULL DEFAULT 'requested'
        CHECK (state IN ('requested', 'grace', 'awaiting_approval', 'destroying', 'done', 'canceled')),
    completed_steps       TEXT[]      NOT NULL DEFAULT '{}',
    grace_ends_at         TIMESTAMPTZ NOT NULL DEFAULT now() + interval '7 days',
    -- NULL until grace ends, when the worker decides it.
    approval_required     BOOLEAN,
    approved_at           TIMESTAMPTZ,
    approved_by           TEXT        CHECK (approved_by IS NULL OR approved_by <> ''),
    trial_outcome         TEXT,
    stripe_outcome        TEXT,
    -- Counts only (per source_type), never content. The full (source_type,
    -- source_id) list lives inside the encrypted export.
    source_manifest       JSONB,
    -- The 30-day export: an R2 object encrypted under a fresh per-export
    -- key, that key wrapped under MASTER_KEK_HEX. Destroying the wrapped key
    -- at day 30 makes the object unreadable even if a copy survives.
    export_object_key     TEXT,
    export_key_wrapped    BYTEA,
    export_sha256         TEXT,
    export_bytes          BIGINT      CHECK (export_bytes IS NULL OR export_bytes >= 0),
    export_expires_at     TIMESTAMPTZ,
    export_destroyed_at   TIMESTAMPTZ,
    -- What the database step found and deleted (audit).
    provider              TEXT,
    provider_database_id  TEXT,
    database_name         TEXT,
    attempts              INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error            TEXT,
    next_attempt_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until           TIMESTAMPTZ,
    -- Fencing token: `org_deletion_claim` mints a new one per claim and every
    -- worker write is `WHERE lease_id = <the token it was handed>`, so a worker
    -- whose lease expired and was re-claimed can no longer change the row.
    lease_id              UUID,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    destroying_since      TIMESTAMPTZ,
    done_at               TIMESTAMPTZ,
    canceled_at           TIMESTAMPTZ,
    canceled_by           TEXT,
    -- Members already emailed that the org is being deleted (the
    -- `notify_members` step). Appended one user at a time, right after each
    -- send, so a retry skips who already got it: one email per member per
    -- deletion. The Resend Idempotency-Key covers the crash window between
    -- the send and this append.
    members_notified      UUID[]      NOT NULL DEFAULT '{}',
    -- When the deletion entered `awaiting_approval` (the operator-approval
    -- wait), and when the operator was last alerted that it has waited too
    -- long (`org_deletion_approval_overdue`).
    awaiting_approval_since TIMESTAMPTZ,
    approval_alerted_at   TIMESTAMPTZ,

    CONSTRAINT org_deletions_steps_known CHECK (
        completed_steps <@ ARRAY['trial_end', 'stripe_cancel', 'notify_requested', 'notify_members',
                                 'stripe_sweep', 'revoke_access', 'manifest', 'export', 'routing',
                                 'credential', 'database', 'clerk', 'notify_done']::TEXT[]
    ),
    CONSTRAINT org_deletions_done_shape CHECK ((state = 'done') = (done_at IS NOT NULL)),
    CONSTRAINT org_deletions_canceled_shape CHECK ((state = 'canceled') = (canceled_at IS NOT NULL)),
    CONSTRAINT org_deletions_destroying_since CHECK (
        state NOT IN ('destroying', 'done') OR destroying_since IS NOT NULL
    ),
    -- Nothing destructive is ever recorded before `destroying`.
    CONSTRAINT org_deletions_not_destructive_before_destroying CHECK (
        state IN ('destroying', 'done')
        OR NOT (completed_steps && ARRAY['stripe_sweep', 'revoke_access', 'manifest', 'export', 'routing',
                                         'credential', 'database', 'clerk', 'notify_done']::TEXT[])
    ),
    -- Destruction only with the approval it needed.
    CONSTRAINT org_deletions_destroy_needs_approval CHECK (
        state NOT IN ('destroying', 'done')
        OR (approval_required IS NOT NULL AND (NOT approval_required OR approved_at IS NOT NULL))
    ),
    CONSTRAINT org_deletions_approval_shape CHECK ((approved_at IS NULL) = (approved_by IS NULL)),
    CONSTRAINT org_deletions_export_shape CHECK (
        export_key_wrapped IS NULL OR export_object_key IS NOT NULL
    ),
    CONSTRAINT org_deletions_done_is_complete CHECK (
        state <> 'done' OR completed_steps @> ARRAY['revoke_access', 'routing', 'credential',
                                                     'database', 'stripe_sweep', 'clerk']::TEXT[]
    )
);

-- One live (not canceled) deletion per org; `done` counts as live: it is the
-- tombstone that makes every later request, sweep and webhook a no-op.
CREATE UNIQUE INDEX IF NOT EXISTS org_deletions_org_idx
    ON public.org_deletions (clerk_org_id) WHERE state <> 'canceled';
CREATE UNIQUE INDEX IF NOT EXISTS org_deletions_account_idx
    ON public.org_deletions (account_id) WHERE state <> 'canceled';
CREATE INDEX IF NOT EXISTS org_deletions_runnable_idx
    ON public.org_deletions (next_attempt_at)
    WHERE state IN ('requested', 'grace', 'awaiting_approval', 'destroying');
CREATE INDEX IF NOT EXISTS org_deletions_export_expiry_idx
    ON public.org_deletions (export_expires_at)
    WHERE export_key_wrapped IS NOT NULL;

ALTER TABLE public.org_deletions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON public.org_deletions;
CREATE POLICY account_isolation ON public.org_deletions
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

-- ── Unpaid watch (decision 4: 30 days unpaid -> this flow) ──────────────
-- One row per self-serve org whose subscription is not live. `unpaid_since`
-- is when the sweep FIRST saw it unpaid (so an org already unpaid when the
-- flag is turned on gets the full 30 days), and the row is dropped as soon
-- as the subscription is live again. `not_before` is set when the owner
-- undoes an unpaid deletion: they get that long to pay before it re-enters.
CREATE TABLE IF NOT EXISTS public.org_unpaid_watch (
    account_id    UUID        PRIMARY KEY REFERENCES public.accounts(id) ON DELETE RESTRICT,
    clerk_org_id  TEXT        NOT NULL CHECK (clerk_org_id <> ''),
    unpaid_since  TIMESTAMPTZ NOT NULL DEFAULT now(),
    not_before    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.org_unpaid_watch ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON public.org_unpaid_watch;
CREATE POLICY account_isolation ON public.org_unpaid_watch
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

-- ── Approval policy (owner-writable only) ───────────────────────────────
-- The first N completed deletions need an operator's approval. N lives here,
-- in a table the serving role can neither read nor write, so no application
-- bug or compromise can lower it. Raise or relax it with an owner-level DSN:
--   UPDATE org_deletion_policy SET approval_first_n = 0;
CREATE TABLE IF NOT EXISTS public.org_deletion_policy (
    singleton        BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    approval_first_n INT     NOT NULL CHECK (approval_first_n >= 0)
);
INSERT INTO public.org_deletion_policy (singleton, approval_first_n) VALUES (true, 10) ON CONFLICT DO NOTHING;
ALTER TABLE public.org_deletion_policy ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.org_deletion_policy FROM PUBLIC;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_app') THEN
        REVOKE ALL ON public.org_deletion_policy FROM coralswarm_app;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
        REVOKE ALL ON public.org_deletion_policy FROM coralswarm_tenant_app;
    END IF;
END $$;

-- The threshold (read-only to callers).
CREATE OR REPLACE FUNCTION public.org_deletion_approval_first_n()
RETURNS INT
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT COALESCE((SELECT p.approval_first_n FROM public.org_deletion_policy p WHERE p.singleton), 2147483647);
$$;

-- May a deletion's approval requirement be decided `false` right now? Only
-- once at least `approval_first_n` deletions have completed.
CREATE OR REPLACE FUNCTION public.org_deletion_may_relax_approval()
RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT (SELECT count(*) FROM public.org_deletions d WHERE d.state = 'done')
           >= public.org_deletion_approval_first_n();
$$;

-- ── State-machine and approval guard ────────────────────────────────────
-- Not SECURITY DEFINER: `current_user` is whoever wrote the row. See the
-- header for the rules. Only a role that is (or is a member of) the table
-- owner is exempt.
CREATE OR REPLACE FUNCTION public.org_deletions_guard_approval()
RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE
    owner_oid OID;
    request_steps CONSTANT TEXT[] := ARRAY['trial_end', 'stripe_cancel', 'notify_requested', 'notify_members'];
BEGIN
    SELECT c.relowner INTO owner_oid FROM pg_catalog.pg_class c WHERE c.oid = 'public.org_deletions'::regclass;
    IF pg_catalog.pg_has_role(current_user, owner_oid, 'USAGE') THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'requested' OR NEW.approved_at IS NOT NULL OR NEW.approved_by IS NOT NULL
           OR NEW.approval_required IS NOT NULL OR cardinality(NEW.completed_steps) <> 0
           OR NEW.destroying_since IS NOT NULL OR NEW.done_at IS NOT NULL OR NEW.canceled_at IS NOT NULL
           OR NEW.export_key_wrapped IS NOT NULL OR NEW.export_object_key IS NOT NULL THEN
            RAISE EXCEPTION 'an organization deletion starts requested and unapproved' USING ERRCODE = '42501';
        END IF;
        IF NEW.created_at <> now() OR NEW.grace_ends_at < NEW.created_at + interval '7 days' THEN
            RAISE EXCEPTION 'an organization deletion gets the full 7-day grace' USING ERRCODE = '42501';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM public.accounts a
                        WHERE a.id = NEW.account_id AND a.clerk_org_id = NEW.clerk_org_id) THEN
            RAISE EXCEPTION 'the account is not the organization''s account' USING ERRCODE = '42501';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.account_id IS DISTINCT FROM OLD.account_id
       OR NEW.clerk_org_id IS DISTINCT FROM OLD.clerk_org_id OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR NEW.grace_ends_at IS DISTINCT FROM OLD.grace_ends_at OR NEW.initiated_by IS DISTINCT FROM OLD.initiated_by
       OR NEW.requested_by_user_id IS DISTINCT FROM OLD.requested_by_user_id THEN
        RAISE EXCEPTION 'account, organization, grace and requester of a deletion cannot be changed' USING ERRCODE = '42501';
    END IF;
    IF NEW.approved_at IS DISTINCT FROM OLD.approved_at OR NEW.approved_by IS DISTINCT FROM OLD.approved_by THEN
        RAISE EXCEPTION 'only an operator may approve an organization deletion' USING ERRCODE = '42501';
    END IF;
    IF NOT (NEW.completed_steps @> OLD.completed_steps) THEN
        RAISE EXCEPTION 'a completed deletion step cannot be taken back' USING ERRCODE = '42501';
    END IF;
    IF NEW.approval_required IS DISTINCT FROM OLD.approval_required THEN
        IF OLD.approval_required IS NOT NULL OR OLD.state <> 'grace'
           OR NEW.state NOT IN ('awaiting_approval', 'destroying')
           OR NOT (NEW.approval_required IS TRUE OR (NEW.approval_required IS FALSE
                                                     AND public.org_deletion_may_relax_approval())) THEN
            RAISE EXCEPTION 'the approval requirement of a deletion cannot be relaxed' USING ERRCODE = '42501';
        END IF;
    END IF;
    IF NEW.state IS DISTINCT FROM OLD.state THEN
        IF NOT ((OLD.state, NEW.state) IN (
                ('requested', 'grace'), ('grace', 'awaiting_approval'), ('grace', 'destroying'),
                ('grace', 'canceled'), ('awaiting_approval', 'destroying'),
                ('awaiting_approval', 'canceled'), ('destroying', 'done'))) THEN
            RAISE EXCEPTION 'organization deletion cannot go from % to %', OLD.state, NEW.state USING ERRCODE = '42501';
        END IF;
        IF OLD.state = 'requested' AND NOT (NEW.completed_steps @> request_steps) THEN
            RAISE EXCEPTION 'the request phase is not finished' USING ERRCODE = '42501';
        END IF;
        IF OLD.state = 'grace' AND NEW.state IN ('awaiting_approval', 'destroying') AND now() < OLD.grace_ends_at THEN
            RAISE EXCEPTION 'the grace period is not over' USING ERRCODE = '42501';
        END IF;
        IF NEW.state = 'destroying' AND (NEW.approval_required IS NULL
                OR (NEW.approval_required AND NEW.approved_at IS NULL)) THEN
            RAISE EXCEPTION 'destruction needs the approval the deletion required' USING ERRCODE = '42501';
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS org_deletions_guard_approval ON public.org_deletions;
CREATE TRIGGER org_deletions_guard_approval
    BEFORE INSERT OR UPDATE ON public.org_deletions
    FOR EACH ROW EXECUTE FUNCTION public.org_deletions_guard_approval();

-- ── Operator approval ───────────────────────────────────────────────────
-- 'approved' | 'already_approved' | 'not_found' | 'not_approvable'. INVOKER:
-- the trigger above refuses any caller that does not own the table, so
-- EXECUTE alone grants nothing. Callable during requested/grace (approve
-- ahead; destruction still waits for grace to end) and awaiting_approval.
CREATE OR REPLACE FUNCTION public.org_deletion_approve(p_id UUID, p_operator TEXT)
RETURNS TEXT
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE st TEXT; appr TIMESTAMPTZ;
BEGIN
    IF p_operator IS NULL OR btrim(p_operator) = '' THEN
        RAISE EXCEPTION 'name the approving operator' USING ERRCODE = '22023';
    END IF;
    SELECT state, approved_at INTO st, appr FROM public.org_deletions WHERE id = p_id FOR UPDATE;
    IF st IS NULL THEN RETURN 'not_found'; END IF;
    IF appr IS NOT NULL THEN RETURN 'already_approved'; END IF;
    IF st NOT IN ('requested', 'grace', 'awaiting_approval') THEN RETURN 'not_approvable'; END IF;
    UPDATE public.org_deletions
       SET approved_at = now(), approved_by = btrim(p_operator), next_attempt_at = now(), updated_at = now()
     WHERE id = p_id;
    RETURN 'approved';
END $$;

-- ── Worker claim ────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.org_deletion_claim(p_lease_secs INT)
RETURNS SETOF public.org_deletions
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    UPDATE public.org_deletions d
       SET lease_until = now() + make_interval(secs => GREATEST(p_lease_secs, 1)),
           lease_id = gen_random_uuid(),
           attempts = d.attempts + 1,
           updated_at = now()
     WHERE d.id = (
            SELECT id FROM public.org_deletions
             WHERE state IN ('requested', 'grace', 'awaiting_approval', 'destroying')
               AND next_attempt_at <= now()
               AND (lease_until IS NULL OR lease_until < now())
             ORDER BY next_attempt_at, created_at
             LIMIT 1
             FOR UPDATE SKIP LOCKED)
    RETURNING d.*;
$$;

-- ── Request-time gate ───────────────────────────────────────────────────
-- The live state for an org, NULL when none (or only canceled ones). Read by
-- the auth door in the same statement as the identity prefetch.
CREATE OR REPLACE FUNCTION public.org_deletion_state_for_org(p_clerk_org_id TEXT)
RETURNS TEXT
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT state FROM public.org_deletions WHERE clerk_org_id = p_clerk_org_id AND state <> 'canceled';
$$;

-- How many org deletions have completed: the approval requirement applies
-- to the first N.
CREATE OR REPLACE FUNCTION public.org_deletion_completed_count()
RETURNS BIGINT
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT count(*) FROM public.org_deletions WHERE state = 'done';
$$;

-- ── Unpaid sweep ────────────────────────────────────────────────────────
-- Self-serve orgs only (a `ready` provisioning job: a concierge org is never
-- auto-deleted), not revoked, with no live deletion. Unpaid = a subscription
-- row that is `past_due` or `canceled`. Starts the watch for newly unpaid
-- orgs, drops it for orgs that are paid again (or gone), and returns the
-- orgs unpaid for at least p_days whose `not_before` has passed.
CREATE OR REPLACE FUNCTION public.org_deletion_unpaid_sweep(p_days INT, p_limit INT)
RETURNS TABLE (account_id UUID, clerk_org_id TEXT, unpaid_since TIMESTAMPTZ)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
#variable_conflict use_column
BEGIN
    DELETE FROM public.org_unpaid_watch w
     WHERE NOT EXISTS (
            SELECT 1 FROM public.stripe_subscriptions s
             WHERE s.account_id = w.account_id AND s.status IN ('past_due', 'canceled'))
        OR EXISTS (SELECT 1 FROM public.org_revocations r WHERE r.clerk_org_id = w.clerk_org_id)
        OR EXISTS (SELECT 1 FROM public.grants g JOIN public.plans p ON p.id = g.plan_id
                    WHERE g.account_id = w.account_id AND g.revoked_at IS NULL AND p.code = 'founder_comp');
    INSERT INTO public.org_unpaid_watch (account_id, clerk_org_id)
    SELECT j.account_id, j.clerk_org_id
      FROM public.tenant_provisioning_jobs j
      JOIN public.accounts a ON a.id = j.account_id AND a.clerk_org_id = j.clerk_org_id
      JOIN public.stripe_subscriptions s ON s.account_id = j.account_id
     WHERE j.status = 'ready'
       AND s.status IN ('past_due', 'canceled')
       AND NOT EXISTS (SELECT 1 FROM public.org_revocations r WHERE r.clerk_org_id = j.clerk_org_id)
       -- A live operator/comped grant is never "unpaid".
       AND NOT EXISTS (SELECT 1 FROM public.grants g JOIN public.plans p ON p.id = g.plan_id
                        WHERE g.account_id = j.account_id AND g.revoked_at IS NULL AND p.code = 'founder_comp')
       AND NOT EXISTS (SELECT 1 FROM public.org_deletions d
                        WHERE d.account_id = j.account_id AND d.state <> 'canceled')
    ON CONFLICT ON CONSTRAINT org_unpaid_watch_pkey DO NOTHING;
    RETURN QUERY
    SELECT w.account_id, w.clerk_org_id, w.unpaid_since
      FROM public.org_unpaid_watch w
     WHERE w.unpaid_since <= now() - make_interval(days => GREATEST(p_days, 1))
       AND (w.not_before IS NULL OR w.not_before <= now())
       AND NOT EXISTS (SELECT 1 FROM public.grants g JOIN public.plans p ON p.id = g.plan_id
                        WHERE g.account_id = w.account_id AND g.revoked_at IS NULL AND p.code = 'founder_comp')
       AND NOT EXISTS (SELECT 1 FROM public.org_deletions d
                        WHERE d.account_id = w.account_id AND d.state <> 'canceled')
     ORDER BY w.unpaid_since
     LIMIT GREATEST(p_limit, 1);
END $$;

-- The worker confirmed with Stripe that this org's subscription is LIVE (the
-- mirror lagged): forget the unpaid clock so the 30 days start again if it
-- ever lapses. SECURITY DEFINER bypasses the table's RLS, so the function
-- re-applies it: it only acts on the account the caller has entered
-- (`app.account_id`), and is a no-op (0) for any other account. The sweep
-- worker enters each account before calling.
CREATE OR REPLACE FUNCTION public.org_deletion_unpaid_forget(p_account_id UUID)
RETURNS INTEGER
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    WITH gone AS (DELETE FROM public.org_unpaid_watch w
                  WHERE w.account_id = p_account_id
                    AND w.account_id = NULLIF(current_setting('app.account_id', true), '')::uuid
                  RETURNING 1)
    SELECT count(*)::int FROM gone;
$$;

-- ── Clerk-dashboard deletes ─────────────────────────────────────────────
-- A self-serve org revoked through `organization.deleted` (its Clerk org is
-- gone) that still has a routing row and no live deletion (an owner who
-- undid a deletion earlier does not stop a later dashboard delete): the
-- tenant data gets the same grace and the same destroy step. Only
-- revocations from the last p_days are adopted, so turning the flag on never
-- sweeps up old history.
CREATE OR REPLACE FUNCTION public.org_deletion_adoptable_clerk_deletions(p_days INT, p_limit INT)
RETURNS TABLE (account_id UUID, clerk_org_id TEXT)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT j.account_id, j.clerk_org_id
      FROM public.org_revocations r
      JOIN public.tenant_provisioning_jobs j ON j.clerk_org_id = r.clerk_org_id AND j.status = 'ready'
      JOIN public.tenant_databases t ON t.clerk_org_id = j.clerk_org_id AND t.account_id = j.account_id
     WHERE r.revoked_at >= now() - make_interval(days => GREATEST(p_days, 1))
       AND NOT EXISTS (SELECT 1 FROM public.org_deletions d
                        WHERE d.clerk_org_id = r.clerk_org_id AND d.state <> 'canceled')
     ORDER BY r.revoked_at
     LIMIT GREATEST(p_limit, 1);
$$;

-- ── Export retention ────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.org_deletion_expired_exports(p_limit INT)
RETURNS TABLE (id UUID, account_id UUID, export_object_key TEXT)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT d.id, d.account_id, d.export_object_key
      FROM public.org_deletions d
     WHERE d.export_key_wrapped IS NOT NULL AND d.export_expires_at <= now()
     ORDER BY d.export_expires_at
     LIMIT GREATEST(p_limit, 1);
$$;

-- ── Alert sweep ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.org_deletion_stuck(p_hours INT)
RETURNS TABLE (id UUID, state TEXT, steps INT, age_hours DOUBLE PRECISION, last_error TEXT)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    SELECT d.id, d.state, cardinality(d.completed_steps),
           EXTRACT(EPOCH FROM (now() - COALESCE(d.destroying_since, d.created_at))) / 3600.0,
           d.last_error
      FROM public.org_deletions d
     WHERE (d.state = 'destroying' AND d.destroying_since < now() - make_interval(hours => GREATEST(p_hours, 1)))
        OR (d.state = 'requested' AND d.created_at < now() - interval '1 hour')
        OR (d.state = 'awaiting_approval' AND d.grace_ends_at < now() - interval '1 day');
$$;

-- A deletion that has sat in `awaiting_approval` (waiting for an operator)
-- for more than p_days days, not yet alerted on or last alerted more than
-- p_repeat_hours ago. Stamps `approval_alerted_at` in the same statement, so
-- two sweeps never both return it: the alert fires once, then repeats at the
-- repeat interval until the deletion is approved or canceled. Nothing here
-- changes state; the owner of the table is exempt from the approval trigger,
-- which guards other columns.
CREATE OR REPLACE FUNCTION public.org_deletion_approval_overdue(p_days INT, p_repeat_hours INT)
RETURNS TABLE (id UUID, clerk_org_id TEXT, days_waiting DOUBLE PRECISION, first_alert BOOLEAN)
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    WITH due AS (
        SELECT d.id, d.approval_alerted_at IS NULL AS first_alert
          FROM public.org_deletions d
         WHERE d.state = 'awaiting_approval' AND d.approved_at IS NULL
           AND COALESCE(d.awaiting_approval_since, d.grace_ends_at)
               < now() - make_interval(days => GREATEST(p_days, 1))
           AND (d.approval_alerted_at IS NULL
                OR d.approval_alerted_at < now() - make_interval(hours => GREATEST(p_repeat_hours, 1)))
           FOR UPDATE OF d SKIP LOCKED
    ), upd AS (
        UPDATE public.org_deletions d SET approval_alerted_at = now()
          FROM due WHERE d.id = due.id
        RETURNING d.id, d.clerk_org_id,
                  EXTRACT(EPOCH FROM (now() - COALESCE(d.awaiting_approval_since, d.grace_ends_at))) / 86400.0 AS days_waiting
    )
    SELECT upd.id, upd.clerk_org_id, upd.days_waiting, due.first_alert FROM upd JOIN due ON due.id = upd.id;
$$;

-- ── Routing row delete (provisioner only) ───────────────────────────────
-- Deletes the org's ACTIVE routing row, for a deletion that is destroying
-- and has already revoked access. Enters the account for 0321's DELETE
-- check. Returns rows deleted (0 = already gone: a resumed job converging).
CREATE OR REPLACE FUNCTION public.org_deletion_delete_routing(p_id UUID)
RETURNS INTEGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE
    d public.org_deletions;
    job public.tenant_provisioning_jobs;
    row public.tenant_databases;
    n INTEGER;
BEGIN
    SELECT * INTO d FROM public.org_deletions WHERE id = p_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no organization deletion %', p_id USING ERRCODE = 'P0002';
    END IF;
    IF d.state <> 'destroying' OR NOT ('revoke_access' = ANY (d.completed_steps)) THEN
        RAISE EXCEPTION 'organization deletion % is not destroying with access revoked', p_id USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.org_revocations r WHERE r.clerk_org_id = d.clerk_org_id) THEN
        RAISE EXCEPTION 'organization % is not revoked', d.clerk_org_id USING ERRCODE = '23514';
    END IF;
    SELECT * INTO job FROM public.tenant_provisioning_jobs WHERE clerk_org_id = d.clerk_org_id;
    IF NOT FOUND OR job.status <> 'ready' OR job.account_id <> d.account_id THEN
        RAISE EXCEPTION 'organization % has no ready self-serve provisioning job for this account', d.clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    SELECT * INTO row FROM public.tenant_databases WHERE clerk_org_id = d.clerk_org_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;
    -- A self-serve routing row and its job always both carry a secret ref;
    -- a missing one on either side is a row this definer must not delete.
    IF row.db_secret_ref IS NULL OR job.db_secret_ref IS NULL THEN
        RAISE EXCEPTION 'routing row for organization % has no secret ref to bind to its job', d.clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    IF row.account_id IS DISTINCT FROM d.account_id OR row.db_secret_ref IS DISTINCT FROM job.db_secret_ref THEN
        RAISE EXCEPTION 'routing row for organization % is not bound to its job', d.clerk_org_id
            USING ERRCODE = '23514';
    END IF;
    PERFORM set_config('app.account_id', row.account_id::text, true);
    PERFORM public.billing_enter_account(row.account_id);
    DELETE FROM public.tenant_databases WHERE clerk_org_id = d.clerk_org_id;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END $$;

REVOKE ALL ON FUNCTION public.org_deletion_claim(INT),
    public.org_deletion_state_for_org(TEXT),
    public.org_deletion_completed_count(),
    public.org_deletion_approval_first_n(),
    public.org_deletion_may_relax_approval(),
    public.org_deletion_unpaid_forget(UUID),
    public.org_deletion_unpaid_sweep(INT, INT),
    public.org_deletion_adoptable_clerk_deletions(INT, INT),
    public.org_deletion_expired_exports(INT),
    public.org_deletion_stuck(INT),
    public.org_deletion_approval_overdue(INT, INT),
    public.org_deletion_delete_routing(UUID),
    public.org_deletion_approve(UUID, TEXT) FROM PUBLIC;

-- The serving role reads and writes its own rows through RLS (the default
-- privileges 0083 set up), but never deletes one: a `done` row is the
-- tombstone, and a canceled row is the audit trail.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_app') THEN
        REVOKE DELETE, TRUNCATE ON public.org_deletions FROM coralswarm_app;
        GRANT EXECUTE ON FUNCTION public.org_deletion_claim(INT),
            public.org_deletion_state_for_org(TEXT),
            public.org_deletion_completed_count(),
            public.org_deletion_approval_first_n(),
            public.org_deletion_may_relax_approval(),
            public.org_deletion_unpaid_forget(UUID),
            public.org_deletion_unpaid_sweep(INT, INT),
            public.org_deletion_adoptable_clerk_deletions(INT, INT),
            public.org_deletion_expired_exports(INT),
            public.org_deletion_stuck(INT),
            public.org_deletion_approval_overdue(INT, INT) TO coralswarm_app;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
        GRANT EXECUTE ON FUNCTION public.org_deletion_state_for_org(TEXT) TO coralswarm_tenant_app;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_provisioner') THEN
        GRANT EXECUTE ON FUNCTION public.org_deletion_delete_routing(UUID) TO coralswarm_provisioner;
    END IF;
END $$;

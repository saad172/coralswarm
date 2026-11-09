-- Migration: 0389_org_deletion_no_undo_after_clerk_delete
-- Description: A deletion cannot be canceled once Clerk has deleted the
--   organization (an `org_revocations` row exists). #1538 leaves billing to
--   the in-app flow when `organization.deleted` arrives during requested or
--   grace; canceling then would leave a deleted org with a live subscription.
--   The owner undo route and `org_deletion_admin cancel` refuse in Rust; this
--   replaces the 0387 guard so a direct UPDATE to canceled is refused too, for
--   every role (the check sits ABOVE the owner-role exemption).
--
-- Rollback: re-apply the 0387 definition of public.org_deletions_guard_approval().

CREATE OR REPLACE FUNCTION public.org_deletions_guard_approval()
RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE
    owner_oid OID;
    request_steps CONSTANT TEXT[] := ARRAY['trial_end', 'stripe_cancel', 'notify_requested', 'notify_members'];
BEGIN
    -- Once Clerk has deleted the organization (an `org_revocations` row) there
    -- is no undoing the deletion for ANYONE, the table owner included: the
    -- Clerk-side delete is irreversible and the in-app flow is the only thing
    -- that ends billing for it.
    IF TG_OP = 'UPDATE' AND NEW.state = 'canceled' AND OLD.state <> 'canceled'
       AND EXISTS (SELECT 1 FROM public.org_revocations r WHERE r.clerk_org_id = OLD.clerk_org_id) THEN
        RAISE EXCEPTION 'org already deleted in Clerk; deletion can''t be undone' USING ERRCODE = '42501';
    END IF;
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

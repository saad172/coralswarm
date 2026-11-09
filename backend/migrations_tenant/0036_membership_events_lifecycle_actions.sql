-- 0036: widen membership_events.action for team-ocean lifecycle management.
--
-- Tenant twin of migrations_control/0129 — the two DDL statements below are
-- byte-identical to the control tree's, same as the paired 0125/0032
-- originals. Adds 'archived', 'restored', 'renamed', 'left' to the action
-- vocabulary. Originally drafted as an in-place edit to
-- 0032_membership_events.sql while it was still unmerged; 0032 merged to
-- main first (PR #532), making it append-only, so the widening lands here
-- as its own migration instead. Full semantics of the four new values are
-- documented in the control twin's header — team-level events
-- ('archived'/'restored'/'renamed') use the self-referencing
-- target_user_id / NULL-role convention, and 'left' is deliberately
-- distinct from 'removed' (self-initiated departure vs an admin's roster
-- write).

ALTER TABLE membership_events
    DROP CONSTRAINT membership_events_action_check;

ALTER TABLE membership_events
    ADD CONSTRAINT membership_events_action_check
    CHECK (action IN ('added', 'removed', 'role_changed', 'cascade_removed',
                      'created', 'archived', 'restored', 'renamed', 'left'));

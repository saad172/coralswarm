-- 0129: widen membership_events.action for team-ocean lifecycle management.
--
-- Adds four values to the action vocabulary: 'archived', 'restored',
-- 'renamed', 'left'. This was originally drafted as an in-place edit to
-- 0125_membership_events.sql while that migration was still unmerged on the
-- same branch — but 0125 merged to main first (PR #532), making it
-- append-only, so the widening lands here as its own migration instead
-- (see 0089's header: corrections are NEW migrations, never edits).
--
-- Semantics of the new values (tenant twin: migrations_tenant/0036):
--   * 'archived'/'restored' — DELETE /v1/teams/{id} stamps
--     graphs.archived_at, and POST /v1/teams/{id}/restore clears it. Both
--     are TEAM-level events, not member-level: like 'created',
--     target_user_id is set to the acting user themselves (there is no
--     other "target" to name), and old_role/new_role stay NULL — they
--     describe role transitions only, and archiving/restoring a team has
--     no role to report.
--   * 'renamed' — PATCH /v1/teams/{id} changes teams.name (and
--     graphs.name — see Store::rename_team's doc comment for why the two
--     track each other). Same self-referencing target_user_id / NULL-role
--     convention as 'archived'/'restored'; the before/after NAME itself is
--     deliberately NOT recorded in this table — widening old_role/new_role
--     to double as a generic old/new value pair would blur their one
--     documented meaning (a team_members.role transition) for a detail
--     this design doesn't ask this table to carry.
--   * 'left' — DELETE /v1/teams/{id}/members/{user_id} now permits
--     user_id == caller without team-admin/org-admin rights (a plain
--     member leaving a workspace they were added to). Deliberately a
--     DISTINCT value from 'removed': 'removed' in this table's history has
--     always meant "someone else acted on this row" (an admin's roster
--     write), and collapsing a self-initiated departure into that same
--     value would make every future audit read of 'removed' ambiguous.
--     target_user_id and actor_user_id are equal for a 'left' event;
--     old_role still carries the departing member's role (mirrors
--     'removed'), new_role stays NULL.
--
-- The constraint name is the auto-generated one Postgres assigned to the
-- inline column CHECK in 0125's CREATE TABLE (<table>_<column>_check).

ALTER TABLE membership_events
    DROP CONSTRAINT membership_events_action_check;

ALTER TABLE membership_events
    ADD CONSTRAINT membership_events_action_check
    CHECK (action IN ('added', 'removed', 'role_changed', 'cascade_removed',
                      'created', 'archived', 'restored', 'renamed', 'left'));

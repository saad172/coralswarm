-- Migration: 0204_google_chat_mailbox_space_owner_resolver
-- (renumbered from the originally-announced 0203 after #806 merged
--  0203_mcp_tenant_selection.sql first — merged wins.)
-- Description: gsuite Train G4 PR 5 (G4.5) — the DM/group mailbox opt-in
--   (`docs/connectors/google-workspace.md` §9's "Deferred to G4.5/G4.6" note,
--   contract §7's Mailbox archetype: owner's own private ocean, no fan-out
--   ever). No new table and no column changes — `google_chat_watched_spaces`
--   (migration `0198`) already supports one row per `(account_id, space_id,
--   owner_user_id)` for this archetype, and its archetype CHECK constraint
--   already enforces the invariant a mailbox opt-in write must satisfy
--   (`space_type <> 'SPACE' AND mode = 'single' AND owner_user_id IS NOT
--   NULL`). This migration adds exactly one control-only `SECURITY DEFINER`
--   resolver function.
--
--   ── `resolve_chat_mailbox_space_owners(text)` — the SAME cross-account
--   bootstrap problem `resolve_chat_space_account` (0198) solves for the
--   container archetype, for the mailbox archetype instead ──
--   A Workspace Events push notification for a `message.v1.created`/
--   `.deleted` event on a `GROUP_CHAT`/`DIRECT_MESSAGE` space arrives with
--   only a bare Chat space resource name — no `account_id`, no
--   `owner_user_id` — same bootstrap gap `routes::google_push`'s container
--   arm already has for `SPACE` messages. `routes::google_chat`'s mailbox
--   push-dispatch arm (this PR) needs to resolve every `(account_id,
--   owner_user_id)` pair that has independently opted this physical space
--   into their own private ocean, so it can run one bounded ingest pass per
--   opt-in.
--
--   Returns a SET of pairs, not a single row — deliberately NOT fail-closed
--   on more than one match, for the SAME reason `resolve_chat_space_account`
--   is not (see that function's own header): TWO different colleagues (even
--   under two different CoralSwarm accounts) may each independently opt the
--   SAME physical DM/group chat into their OWN private ocean — the whole
--   point of the mailbox archetype's per-owner partial unique index
--   (`uq_google_chat_watched_spaces_mailbox`, 0198) — and a push for it must
--   reach every one of them, never just one.
--
--   `owner_user_id IS NOT NULL` (rather than filtering on `space_type <>
--   'SPACE'`) is the correct predicate here: 0198's archetype CHECK already
--   guarantees a non-NULL `owner_user_id` implies a `GROUP_CHAT`/
--   `DIRECT_MESSAGE` row in `mode = 'single'` with a non-NULL `graph_id` —
--   there is no "mailbox row with an unset destination" state to filter out
--   (unlike the container archetype's `mode IS NOT NULL` filter, which
--   excludes a real "unrouted" state that DOES exist there). `active = true`
--   is intentionally NOT filtered here, mirroring `resolve_chat_space_account`
--   leaving deactivation-fan-out to the caller rather than baking a snapshot
--   of "active" into the resolver itself; `routes::google_chat`'s ingest path
--   re-checks `active` via `ingestable_mailbox_space` before doing any work.
--
--   Control-only by the SAME necessity `resolve_chat_space_account`'s own
--   header documents: G4.5 stays Personal-plane-only, so every account
--   capable of holding a MAILBOX row today has its content in THIS SAME
--   control database. **G4.6 (org-plane gate lift, merged) deliberately did
--   NOT extend `ORG_GOOGLE_CHAT_ENABLED` to the mailbox archetype's opt-in
--   routes** — they always resolve the caller's own account-wide primary
--   ocean via `resolve_primary_graph(AccountPlane::Personal, ..)`, which
--   would behave wrong against a real org tenant pool — so this function
--   stays control-only for now. A FUTURE PR that lifts the mailbox
--   archetype's own org gate must revisit this function first, exactly as
--   G4.6 had to revisit `resolve_chat_space_account` for the container
--   archetype.
--
-- Depends on: 0198_google_chat_watched_spaces (the table this reads,
--             including its archetype CHECK constraint this resolver relies
--             on rather than re-deriving).
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_chat_mailbox_space_owners(text) FROM coralswarm_app, CURRENT_USER;
--   REVOKE EXECUTE ON FUNCTION resolve_chat_mailbox_space_owners(text) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_chat_mailbox_space_owners(text);

CREATE OR REPLACE FUNCTION resolve_chat_mailbox_space_owners(p_space text)
RETURNS TABLE(account_id uuid, owner_user_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT DISTINCT gcws.account_id, gcws.owner_user_id
    FROM google_chat_watched_spaces gcws
    WHERE gcws.space_id = p_space
      AND p_space <> ''
      AND gcws.owner_user_id IS NOT NULL
$$;

REVOKE EXECUTE ON FUNCTION resolve_chat_mailbox_space_owners(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_chat_mailbox_space_owners(text) TO coralswarm_app, CURRENT_USER;

-- Migration 0076: google_chat_mailbox_owner_trigger (tenant tree)
-- Description: Tenant-tree half of the paired
--   `migrations_control/0205_google_chat_mailbox_owner_trigger.sql` — see
--   that file for the full incident (CodeRabbit round 1, PR #811) and
--   rationale. `google_chat_watched_spaces` is a SHARED table
--   (`check_schema_parity.sh` diffs a whole-schema `pg_dump` of both trees),
--   so the trigger FUNCTION body below is BYTE-IDENTICAL to the control
--   side's — deliberately, so this migration needs no
--   `CONTROL_FUNCTIONS`/`TENANT_FUNCTIONS` exclusion-list entry at all. The
--   `kind='personal'` pass-through arm is inert here today (no code creates
--   a `kind='personal'` graph on the tenant tree — Personal-plane accounts
--   never provision a tenant database, ADR-0007), but keeping the function
--   identical rather than tenant-trimmed is what makes the parity guarantee
--   automatic rather than a second thing to keep in sync by hand.
--
--   On the TENANT tree this closes the actually-reachable version of the
--   bug the moment G4.6 lifts the org gate: an org tenant DB's `graphs`
--   table genuinely holds MULTIPLE `kind='member'` oceans (one per member),
--   which is exactly the shape where a mismatched `(graph_id,
--   owner_user_id)` pair on a mailbox row would let one member's private
--   mailbox opt-in write into a DIFFERENT member's ocean using that other
--   member's own OAuth credential.
--
-- Depends on: 0073_google_chat_watched_spaces (the table and its archetype
--             CHECK this trigger complements), 0005_member_ocean_columns
--             (graphs.kind/owner_user_id, graphs_member_requires_owner).
--
-- Paired with: migrations_control/0205_google_chat_mailbox_owner_trigger.sql.
--
-- Down:
--   DROP TRIGGER IF EXISTS google_chat_watched_spaces_validate_mailbox_owner_trg ON google_chat_watched_spaces;
--   DROP FUNCTION IF EXISTS google_chat_watched_spaces_validate_mailbox_owner();

CREATE OR REPLACE FUNCTION google_chat_watched_spaces_validate_mailbox_owner()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    g_kind  text;
    g_owner uuid;
BEGIN
    -- Container archetype (owner_user_id NULL) — nothing to check here; the
    -- archetype CHECK constraint already governs that shape.
    IF NEW.owner_user_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT kind, owner_user_id INTO g_kind, g_owner FROM graphs WHERE id = NEW.graph_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'google_chat_watched_spaces: graph_id % does not exist', NEW.graph_id;
    END IF;

    -- Personal-plane primary ocean: no per-graph owner to check against
    -- (see this migration's header for the scope this deliberately does not
    -- cover, and why).
    IF g_kind = 'personal' THEN
        RETURN NEW;
    END IF;

    -- Every other kind capable of carrying a mailbox destination must be
    -- 'member', and that graph's OWN owner_user_id must be the SAME person
    -- as this row's — otherwise the credential this row's owner supplies
    -- would write into a DIFFERENT member's private ocean.
    IF g_kind <> 'member' OR g_owner IS DISTINCT FROM NEW.owner_user_id THEN
        RAISE EXCEPTION 'google_chat_watched_spaces: a mailbox row''s graph_id must be a member ocean owned by the SAME owner_user_id as the row itself (graph kind=%, graph owner=%, row owner=%)',
            g_kind, g_owner, NEW.owner_user_id;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS google_chat_watched_spaces_validate_mailbox_owner_trg ON google_chat_watched_spaces;
CREATE TRIGGER google_chat_watched_spaces_validate_mailbox_owner_trg
    BEFORE INSERT OR UPDATE ON google_chat_watched_spaces
    FOR EACH ROW EXECUTE FUNCTION google_chat_watched_spaces_validate_mailbox_owner();

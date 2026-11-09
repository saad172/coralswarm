-- Migration: 0205_google_chat_mailbox_owner_trigger
-- (renumbered from the originally-announced 0204 after this same train's
--  own 0203_google_chat_mailbox_space_owner_resolver.sql was itself
--  renumbered to 0204 — see that migration's own header.)
-- Description: gsuite Train G4 PR 5 (G4.5) fix-in-passing, adversarial review
--   (CodeRabbit round 1 on PR #811) — the archetype CHECK constraint (0198)
--   guarantees a mailbox row's `mode`/`owner_user_id`/`graph_id` are all
--   non-NULL together, but it says NOTHING about whether `graph_id` and
--   `owner_user_id` actually agree with each other. Nothing before this
--   migration stopped a `GROUP_CHAT`/`DIRECT_MESSAGE` row from naming ONE
--   member's `graph_id` (a `kind='member'` ocean) while claiming a
--   DIFFERENT member's `owner_user_id`.
--
--   ── Why that is exploitable, not merely untidy ──
--   `workers::google_chat_sweep::sync_mailbox_space_locked` loads the
--   CREDENTIAL for `owner_user_id` (`load_creds_bg(.., TokenOwner::
--   Member(owner_user_id))`) and writes ingested content into `graph_id`. A
--   row with `graph_id` = member A's private ocean but `owner_user_id` =
--   member B would use B's own Google OAuth token to read B's private DMs
--   and write them into A's ocean — A pulling B's private messages into her
--   own ocean using B's own credentials, with neither party's consent
--   matching what actually happened. `record_mailbox_optin`'s own two
--   parameters (`owner_user_id`, `graph_id`) always agree TODAY because
--   `routes::google_chat::opt_in_to_mailbox_space` derives both from the
--   SAME authenticated `user.user_id` — but nothing stopped a future or
--   different caller (a bug, a bulk-migration script, a member-scoped write
--   path G4.6 might add) from passing a mismatched pair, and the table's own
--   RLS `WITH CHECK` does not catch it either: `pick_space_destination`'s/
--   `record_mailbox_optin`'s writer runs under `begin_tenant_system_scoped`,
--   under which `tenant_rls_visible_graph_ids()` (0009) returns EVERY graph
--   as "visible" — the policy's `graph_id IN (visible)` arm is vacuously
--   true for ANY graph_id under system scope, so it was never a backstop
--   for THIS invariant in the first place. The control-plane
--   `account_isolation` policy (0198) never even referenced `owner_user_id`
--   at all. This turns #672/0198's "make it a database constraint, not a
--   code-review instruction" instruction — already applied once to this
--   same table for the SPACE-vs-owner archetype shape — into the SAME
--   instruction applied to the graph_id-vs-owner_user_id shape a CHECK
--   constraint cannot express (Postgres CHECK constraints cannot reference
--   another table at all), which is why this needs a TRIGGER rather than a
--   widened CHECK.
--
--   ── Scope: `kind='member'` graphs only, `kind='personal'` passed through
--   unconditionally ──
--   `graphs_member_requires_owner` (0090) already guarantees `owner_user_id`
--   is NULL on every non-`member` graph — a `kind='personal'` graph (the
--   ONLY destination `routes::slack::resolve_primary_graph`'s Personal arm
--   ever resolves, which is the actual production path G4.5 uses today) has
--   no per-graph owner column at all; a personal account's one implicit
--   owner lives on `accounts.owner_user_id`, a CONTROL-plane-ONLY table.
--   This function deliberately does NOT join `accounts` — doing so would
--   make its body diverge between the control and tenant trees (the tenant
--   tree has no `accounts` table to join, ADR-0007), which would fail
--   `check_schema_parity.sh`'s whole-schema diff on this SHARED table's
--   trigger. Keeping the function body BYTE-IDENTICAL on both trees (this
--   migration's tenant twin, `migrations_tenant/0076`, is a literal copy)
--   means no `CONTROL_FUNCTIONS`/`TENANT_FUNCTIONS` exclusion-list entry is
--   needed at all — the simplest correct shape, not a shortcut.
--
--   This closes the CONCRETE two-member-same-account scenario CodeRabbit
--   described (the only shape where `graph_id` and `owner_user_id` can
--   legitimately disagree at all, since a `kind='member'` graph's owner is
--   a real per-row fact to check against). It does NOT add a NEW check for
--   the personal-plane case beyond what already existed: `load_creds_bg`
--   scopes the credential read by BOTH `account_id` AND `owner_user_id`, so
--   a personal-plane mailbox row naming a member_user_id who is not a real,
--   `google_chat`-connected member of THAT account already fails closed
--   (no credential, sweep pauses) — and the product's own standing
--   invariant is that a personal account has exactly one member. Recorded
--   here rather than silently assumed: a personal-plane version of this
--   exact bug remains reachable if that invariant is ever violated, and
--   G4.6 (which lifts the org gate this train keeps closed) is the PR that
--   must revisit this the moment a genuine member-scoped mailbox write path
--   exists on a database where `kind='personal'` graphs and multiple
--   `owner_user_id`s could coexist.
--
--   ── BEFORE INSERT OR UPDATE, not INSERT-only ──
--   `opt_out_of_mailbox_space`'s `UPDATE ... SET active = false` and the
--   idempotent re-opt-in's `ON CONFLICT DO UPDATE` (`record_mailbox_optin`)
--   never touch `graph_id`/`owner_user_id` on an existing row, so the extra
--   re-validation on those UPDATEs is a no-op in practice, not a behaviour
--   change — but it is what makes this a real constraint rather than an
--   INSERT-only trapdoor a future UPDATE could walk around.
--
-- Depends on: 0198_google_chat_watched_spaces (the table and its archetype
--             CHECK this trigger complements), 0090_member_ocean_columns
--             (graphs.kind/owner_user_id, graphs_member_requires_owner).
--
-- Paired with: migrations_tenant/0076_google_chat_mailbox_owner_trigger.sql
--             — byte-identical function body (see header above for why).
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

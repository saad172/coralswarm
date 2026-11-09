-- Rename the auto-provisioned consumer-plane ocean from "Primary" to "Personal".
--
-- `clerk_provision.rs` created every new account's first ocean as "Primary".
-- That name is internal vocabulary — it describes the `is_primary` flag, not
-- anything a user can act on — and it became user-visible when the Ask scope
-- picker stopped special-casing that ocean behind a "Private" label and
-- started rendering its actual name like every other workspace.
--
-- Deliberately narrow. Only rows still carrying the provisioned default are
-- touched, so an account that renamed its ocean to something meaningful keeps
-- that name; `kind = 'personal' AND is_primary` additionally excludes team,
-- org, and secondary personal oceans that happen to be called "Primary".
--
-- Control plane only. A personal ocean can never exist inside an org tenant
-- (ADR-0007 D2 — the backend refuses to create one under an org account), and
-- a member's own ocean in a tenant DB is `kind = 'member'`, named after the
-- member's display name. There is nothing for a tenant migration to do.
UPDATE graphs
   SET name = 'Personal'
 WHERE kind = 'personal'
   AND is_primary
   AND name = 'Primary';

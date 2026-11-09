-- 0268_addin_microsoft_link_method_clerk_oauth.sql — a third way to create an
-- add-in Microsoft link: the user signed in to the add-in with Clerk OAuth and
-- `POST /v1/addin/microsoft/link` bound their verified Microsoft identity to the
-- workspace their token names (`link_method = 'clerk_oauth'`).
--
-- Control-only: `addin_microsoft_links` has no tenant-tree twin (see 0264), so
-- there is no migrations_tenant counterpart.
--
-- 0264 declared the CHECK inline, so Postgres auto-named it
-- `addin_microsoft_links_link_method_check`. Drop by that name (IF EXISTS keeps
-- a re-run safe) and re-add with the widened set.
ALTER TABLE addin_microsoft_links
    DROP CONSTRAINT IF EXISTS addin_microsoft_links_link_method_check;
ALTER TABLE addin_microsoft_links
    ADD CONSTRAINT addin_microsoft_links_link_method_check
    CHECK (link_method IN ('pairing', 'auto', 'clerk_oauth'));

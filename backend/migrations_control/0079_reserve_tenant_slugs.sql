-- Migration: 0079_reserve_tenant_slugs
-- Description: Reserve the subdomain labels the binding gate routes to the apex
--   plane ('app', 'www'). classify_host() sends app.<apex> / www.<apex> to the
--   apex, so a tenant provisioned with either slug would be unreachable on its
--   own subdomain (every request would hit the apex path and 403). Forbid them
--   at the DB level too — defense in depth with tenancy::gate::is_valid_slug /
--   RESERVED_SLUGS. New reserved labels must be added in BOTH places.
ALTER TABLE tenant_databases
    ADD CONSTRAINT tenant_databases_slug_not_reserved
    CHECK (slug NOT IN ('app', 'www'));

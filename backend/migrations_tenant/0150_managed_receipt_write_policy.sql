DROP POLICY member_ocean_isolation ON managed_attempt_receipts;

CREATE POLICY member_ocean_isolation ON managed_attempt_receipts FOR ALL
  USING (tenant_rls_is_system())
  WITH CHECK (tenant_rls_is_system());

CREATE POLICY member_ocean_read ON managed_attempt_receipts FOR SELECT
  USING (tenant_rls_is_system() OR graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

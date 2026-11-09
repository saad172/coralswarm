\set ON_ERROR_STOP on
\set app_role coralswarm_tenant_app
INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_source_delete_fixture');
INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES
 ('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000001','Alice delete fixture','member','00000000-0000-0000-0000-000000000011'),
 ('00000000-0000-0000-0000-000000000102','00000000-0000-0000-0000-000000000001','Bob delete fixture','member','00000000-0000-0000-0000-000000000012'),
 ('00000000-0000-0000-0000-000000000103','00000000-0000-0000-0000-000000000001','Alice graph cascade fixture','member','00000000-0000-0000-0000-000000000011');
\ir source_delete_core.sql

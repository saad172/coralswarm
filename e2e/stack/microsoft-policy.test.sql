-- Regression assertions over real provisioned acceptance accounts. Every write
-- rolls back; this creates no captures and is not capture provenance evidence.
\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE a uuid; u uuid; g uuid; foreign_u uuid; foreign_g uuid;
BEGIN
 SELECT accounts.id, accounts.owner_user_id, graphs.id INTO STRICT a,u,g
 FROM accounts JOIN graphs ON graphs.account_id=accounts.id
 WHERE accounts.clerk_org_id IS NULL AND graphs.kind='personal' LIMIT 1;
 SELECT accounts.owner_user_id,graphs.id INTO STRICT foreign_u,foreign_g
 FROM accounts JOIN graphs ON graphs.account_id=accounts.id
 WHERE accounts.id<>a AND graphs.kind='personal' LIMIT 1;
 PERFORM set_config('app.account_id',a::text,true);
 PERFORM set_config('app.user_id',u::text,true);
 SET LOCAL ROLE coralswarm_app;
 INSERT INTO outlook_everything_mode(account_id,owner_user_id,graph_id,enabled)
 VALUES(a,u,g,true) ON CONFLICT(account_id,owner_user_id) DO UPDATE SET enabled=true;
 RAISE NOTICE 'PASS own personal mailbox destination';
 BEGIN
   INSERT INTO outlook_everything_mode(account_id,owner_user_id,graph_id,enabled)
   VALUES(a,foreign_u,g,true);
   RAISE EXCEPTION 'FAIL foreign owner admitted';
 EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'PASS foreign mailbox owner refused'; END;
 BEGIN
   UPDATE outlook_everything_mode SET graph_id=foreign_g WHERE account_id=a AND owner_user_id=u;
   RAISE EXCEPTION 'FAIL foreign graph admitted';
 EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'PASS foreign graph refused'; END;
 RESET ROLE;
END $$;
ROLLBACK;

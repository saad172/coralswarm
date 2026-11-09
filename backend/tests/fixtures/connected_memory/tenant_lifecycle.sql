\set ON_ERROR_STOP on
INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_p2_lifecycle_fixture');
INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES
 ('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000001','Alice fixture','member','00000000-0000-0000-0000-000000000011'),
 ('00000000-0000-0000-0000-000000000102','00000000-0000-0000-0000-000000000001','Bob fixture','member','00000000-0000-0000-0000-000000000012');
INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id) VALUES
 ('00000000-0000-0000-0000-000000000201','00000000-0000-0000-0000-000000000101','jira','site:fixture','same-native-id'),
 ('00000000-0000-0000-0000-000000000202','00000000-0000-0000-0000-000000000102','jira','site:fixture','same-native-id');
INSERT INTO query_history(account_id,graph_id,user_id,query_text,answer_summary)
 SELECT account_id,id,owner_user_id,'fixture query','sensitive derived fixture' FROM graphs;
INSERT INTO journal_rollups(account_id,user_id,graph_id,hour_start,period_start,summary,input_fingerprint,model)
 SELECT account_id,owner_user_id,id,now()-interval '90 days',now()-interval '90 days','sensitive old fixture','fixture','mock' FROM graphs;
INSERT INTO queries(account_id,user_id,text,answer)
 SELECT account_id,owner_user_id,'fixture query','sensitive answer fixture' FROM graphs;
INSERT INTO conversations(account_id,user_id,graph_id,title)
 SELECT account_id,owner_user_id,id,'sensitive title fixture' FROM graphs;
INSERT INTO conversation_messages(conversation_id,seq,role,text,citations,sources,scope_graph_ids)
 SELECT id,0,'assistant','sensitive assistant fixture','["fixture"]','["fixture"]',jsonb_build_array(graph_id::text) FROM conversations;
-- Org workers share this nil-account cache across the tenant. Graph account
-- filters alone cannot reach it, so permission loss must clear it explicitly.
INSERT INTO extraction_cache(account_id,content_hash,task,model,prompt_version,response_content)
 VALUES('00000000-0000-0000-0000-000000000000','fixture','fixture','mock',1,'sensitive extraction fixture');
BEGIN;
SET LOCAL ROLE coralswarm_tenant_app;
SELECT set_config('app.account_id','00000000-0000-0000-0000-000000000001',true),set_config('app.user_id','00000000-0000-0000-0000-000000000011',true);
DO $$ DECLARE n BIGINT; BEGIN
 IF (SELECT rolbypassrls FROM pg_roles WHERE rolname=current_user) THEN RAISE EXCEPTION 'proof must enforce RLS'; END IF;
 SELECT count(*) INTO n FROM source_items; IF n<>1 THEN RAISE EXCEPTION 'private source visibility %',n; END IF;
 UPDATE source_items SET access_state='restricted' WHERE source_item_id='00000000-0000-0000-0000-000000000202';
 GET DIAGNOSTICS n=ROW_COUNT; IF n<>0 THEN RAISE EXCEPTION 'foreign source changed'; END IF;
 UPDATE source_items SET access_state='restricted' WHERE source_item_id='00000000-0000-0000-0000-000000000201';
 GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'own source not reached'; END IF;
 SELECT count(*) INTO n FROM source_retraction_outbox; IF n<>1 THEN RAISE EXCEPTION 'restricted outbox not visible %',n; END IF;
 SELECT count(*) INTO n FROM query_history; IF n<>0 THEN RAISE EXCEPTION 'own derived history survived %',n; END IF;
 SELECT count(*) INTO n FROM journal_rollups; IF n<>0 THEN RAISE EXCEPTION 'old own journal survived %',n; END IF;
 SELECT count(*) INTO n FROM extraction_cache; IF n<>0 THEN RAISE EXCEPTION 'tenant sentinel extraction cache survived %',n; END IF;
 IF EXISTS(SELECT 1 FROM queries WHERE user_id='00000000-0000-0000-0000-000000000011' AND answer IS NOT NULL) THEN RAISE EXCEPTION 'own persisted query answer survived'; END IF;
 IF EXISTS(SELECT 1 FROM conversation_messages WHERE text<>'[Evidence no longer available]' OR citations<>'[]'::jsonb OR sources<>'[]'::jsonb) THEN RAISE EXCEPTION 'own assistant prose survived'; END IF;
 IF EXISTS(SELECT 1 FROM conversations WHERE title<>'') THEN RAISE EXCEPTION 'own derived conversation title survived'; END IF;
 DELETE FROM source_items WHERE source_item_id='00000000-0000-0000-0000-000000000201';
 GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'actual source delete was suppressed'; END IF;
 IF EXISTS(SELECT 1 FROM source_items WHERE source_item_id='00000000-0000-0000-0000-000000000201') THEN RAISE EXCEPTION 'deleted source row survived'; END IF;
 SELECT count(*) INTO n FROM source_retraction_outbox; IF n<>2 THEN RAISE EXCEPTION 'durable deleted outbox missing %',n; END IF;
END $$;
COMMIT;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM source_items WHERE source_item_id='00000000-0000-0000-0000-000000000202' AND access_state='permitted') THEN RAISE EXCEPTION 'other private source did not survive'; END IF;
 IF (SELECT count(*) FROM query_history WHERE graph_id='00000000-0000-0000-0000-000000000102')<>1 THEN RAISE EXCEPTION 'foreign private history changed'; END IF;
 IF (SELECT count(*) FROM journal_rollups WHERE graph_id='00000000-0000-0000-0000-000000000102')<>1 THEN RAISE EXCEPTION 'foreign private journal changed'; END IF;
 IF NOT EXISTS(SELECT 1 FROM conversation_messages m JOIN conversations c ON c.id=m.conversation_id WHERE c.graph_id='00000000-0000-0000-0000-000000000102' AND m.text='sensitive assistant fixture') THEN RAISE EXCEPTION 'foreign private assistant text changed'; END IF;
END $$;

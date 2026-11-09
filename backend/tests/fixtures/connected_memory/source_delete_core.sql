-- Shared real-role proof, invoked only after applying the full appropriate
-- migration tree. This seeds complete retained and legacy evidence chains.
INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id)
 SELECT ('00000000-0000-0000-0000-000000000'||(200+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(100+x)::text)::uuid,'note','fixture:delete','item-'||x FROM generate_series(1,3) x;
INSERT INTO source_versions(version_id,source_item_id,graph_id,source_type,provider_namespace,source_id,raw_inline)
 SELECT ('00000000-0000-0000-0000-000000000'||(500+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(200+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(100+x)::text)::uuid,'note','fixture:delete','item-'||x,'synthetic retained evidence' FROM generate_series(1,3) x;
UPDATE source_items s SET current_version_id=v.version_id FROM source_versions v WHERE v.source_item_id=s.source_item_id;
INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp)
 SELECT ('00000000-0000-0000-0000-000000000'||(700+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(100+x)::text)::uuid,'note','item-'||x||':chunk:0','fixture','synthetic retained evidence',now() FROM generate_series(1,3) x;
INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id)
 SELECT ('00000000-0000-0000-0000-000000000'||(100+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(500+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(700+x)::text)::uuid FROM generate_series(1,3) x;
INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model)
 SELECT ('00000000-0000-0000-0000-000000000'||(800+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(100+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(500+x)::text)::uuid,0,'fact','synthetic fixture fact','source-atoms-v1','mock' FROM generate_series(1,3) x;
INSERT INTO semantic_atom_passages(passage_id,graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote)
 SELECT ('00000000-0000-0000-0000-000000000'||(900+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(100+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(500+x)::text)::uuid,
 ('00000000-0000-0000-0000-000000000'||(800+x)::text)::uuid,0,27,'synthetic retained evidence' FROM generate_series(1,3) x;
BEGIN;
SET LOCAL ROLE :app_role;
SELECT set_config('app.account_id','00000000-0000-0000-0000-000000000001',true),set_config('app.user_id','00000000-0000-0000-0000-000000000011',true);
DO $$ DECLARE n BIGINT; BEGIN
 IF (SELECT rolbypassrls OR rolsuper FROM pg_roles WHERE rolname=current_user) THEN RAISE EXCEPTION 'real NOBYPASSRLS role required'; END IF;
 DELETE FROM source_items WHERE source_item_id='00000000-0000-0000-0000-000000000202';
 GET DIAGNOSTICS n=ROW_COUNT; IF n<>0 THEN RAISE EXCEPTION 'foreign source deleted'; END IF;
 DELETE FROM source_items WHERE source_item_id='00000000-0000-0000-0000-000000000201';
 GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'BEFORE trigger suppressed actual source deletion'; END IF;
 IF EXISTS(SELECT 1 FROM source_items WHERE source_item_id='00000000-0000-0000-0000-000000000201') THEN RAISE EXCEPTION 'deleted item survived'; END IF;
 IF EXISTS(SELECT 1 FROM source_versions WHERE version_id='00000000-0000-0000-0000-000000000501') THEN RAISE EXCEPTION 'deleted version survived'; END IF;
 IF EXISTS(SELECT 1 FROM source_version_chunks WHERE source_version_id='00000000-0000-0000-0000-000000000501') THEN RAISE EXCEPTION 'deleted chunk mapping survived'; END IF;
 IF EXISTS(SELECT 1 FROM semantic_atoms WHERE source_version_id='00000000-0000-0000-0000-000000000501') THEN RAISE EXCEPTION 'deleted semantic outcome survived'; END IF;
 IF EXISTS(SELECT 1 FROM semantic_atom_passages WHERE source_version_id='00000000-0000-0000-0000-000000000501') THEN RAISE EXCEPTION 'deleted passage survived'; END IF;
 SELECT count(*) INTO n FROM source_retraction_outbox WHERE source_item_id='00000000-0000-0000-0000-000000000201';
 IF n<>1 THEN RAISE EXCEPTION 'minimal durable deletion event missing'; END IF;
 -- Graph deletion must not insert an outbox row referencing its vanished
 -- graph; the same repaired trigger participates in that FK cascade.
 DELETE FROM graphs WHERE id='00000000-0000-0000-0000-000000000103';
 GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'graph cascade suppressed'; END IF;
END $$;
COMMIT;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM source_items WHERE source_item_id='00000000-0000-0000-0000-000000000202') THEN RAISE EXCEPTION 'unrelated private item lost'; END IF;
 IF NOT EXISTS(SELECT 1 FROM semantic_atom_passages WHERE passage_id='00000000-0000-0000-0000-000000000902') THEN RAISE EXCEPTION 'unrelated private evidence lost'; END IF;
 IF EXISTS(SELECT 1 FROM source_items WHERE graph_id='00000000-0000-0000-0000-000000000103') OR EXISTS(SELECT 1 FROM source_versions WHERE graph_id='00000000-0000-0000-0000-000000000103') OR EXISTS(SELECT 1 FROM semantic_atom_passages WHERE graph_id='00000000-0000-0000-0000-000000000103') THEN RAISE EXCEPTION 'graph cascade left retained source content'; END IF;
 IF EXISTS(SELECT 1 FROM source_retraction_outbox WHERE graph_id='00000000-0000-0000-0000-000000000103') THEN RAISE EXCEPTION 'graph cascade left orphan deletion event'; END IF;
END $$;

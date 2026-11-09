\set ON_ERROR_STOP on
-- Run after the complete selected plane's migrations. Synthetic owner fixtures.
INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES
 ('80000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000001','P8 private','member','00000000-0000-0000-0000-000000000011'),
 ('80000000-0000-0000-0000-000000000102','00000000-0000-0000-0000-000000000001','P8 shared','team',NULL);
INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id) VALUES
 ('80000000-0000-0000-0000-000000000201','80000000-0000-0000-0000-000000000101','fixture','synthetic:p8','private');
INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,raw_inline,raw_blob_key) VALUES
 ('80000000-0000-0000-0000-000000000301','80000000-0000-0000-0000-000000000101','80000000-0000-0000-0000-000000000201','private','P8 SECRET ORIGINAL',(:'blob_prefix'||'80000000-0000-0000-0000-000000000101/80000000-0000-0000-0000-000000000901'));
INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_inline,title,content_blob_key,timestamp) VALUES
 ('80000000-0000-0000-0000-000000000401','80000000-0000-0000-0000-000000000101','fixture','original','P8 SECRET HEADER','P8 SECRET TITLE',(:'blob_prefix'||'80000000-0000-0000-0000-000000000101/80000000-0000-0000-0000-000000000901'),now());
INSERT INTO source_version_chunks VALUES('80000000-0000-0000-0000-000000000101','80000000-0000-0000-0000-000000000301','80000000-0000-0000-0000-000000000401');
INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_inline,title,content_blob_key,timestamp,promoted_from_atom_id) VALUES
 ('80000000-0000-0000-0000-000000000402','80000000-0000-0000-0000-000000000102','fixture','copy','P8 SECRET COPY','P8 SECRET COPY TITLE',(:'blob_prefix'||'80000000-0000-0000-0000-000000000101/80000000-0000-0000-0000-000000000901'),now(),'80000000-0000-0000-0000-000000000401');
INSERT INTO answer_support_receipts(receipt_id,graph_id,owner_user_id,support,evidence_complete) VALUES('80000000-0000-0000-0000-000000000701','80000000-0000-0000-0000-000000000102','00000000-0000-0000-0000-000000000011','[{"kind":"legacy_chunk","graph_id":"80000000-0000-0000-0000-000000000102","atom_id":"80000000-0000-0000-0000-000000000402"}]',true);
BEGIN;
SET LOCAL ROLE :app_role;
SELECT set_config('app.account_id','00000000-0000-0000-0000-000000000001',true),set_config('app.user_id','00000000-0000-0000-0000-000000000011',true);
DO $$BEGIN
 IF (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user) THEN RAISE EXCEPTION 'invalid app role';END IF;
 IF library_atom_permitted('80000000-0000-0000-0000-000000000102','80000000-0000-0000-0000-000000000402') THEN RAISE EXCEPTION 'legacy copy trusted';END IF;
END$$;
DELETE FROM source_items WHERE source_item_id='80000000-0000-0000-0000-000000000201';
DO $$BEGIN
 IF EXISTS(SELECT 1 FROM source_items WHERE source_item_id='80000000-0000-0000-0000-000000000201') THEN RAISE EXCEPTION 'delete suppressed';END IF;
 IF NOT EXISTS(SELECT 1 FROM source_purge_events WHERE source_item_id='80000000-0000-0000-0000-000000000201') THEN RAISE EXCEPTION 'lost outbox';END IF;
 IF EXISTS(SELECT 1 FROM answer_support_receipts WHERE receipt_id='80000000-0000-0000-0000-000000000701' AND evidence_complete) THEN RAISE EXCEPTION 'uncited copy receipt retained';END IF;
END$$;
SELECT library_drain_purge(100);SELECT library_drain_purge(100);SELECT library_drain_purge(100);
DO $$BEGIN
 IF EXISTS(SELECT 1 FROM atoms WHERE atom_id IN('80000000-0000-0000-0000-000000000401','80000000-0000-0000-0000-000000000402')) THEN RAISE EXCEPTION 'copied content retained';END IF;
 IF current_setting('app.account_id')<>'00000000-0000-0000-0000-000000000001' THEN RAISE EXCEPTION 'drain widened caller scope';END IF;
 IF NOT EXISTS(SELECT 1 FROM jobs WHERE kind='DeleteSourceBlob' AND right(payload->>'blob_key',73)='80000000-0000-0000-0000-000000000101/80000000-0000-0000-0000-000000000901') THEN RAISE EXCEPTION 'ciphertext cleanup lost';END IF;
END$$;
COMMIT;

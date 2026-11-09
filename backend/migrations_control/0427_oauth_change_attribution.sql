-- Diagnostic only: preserve 0426's authority/freshness decisions, including
-- repeated invalid-state observations. No token, reason, scope, row id, SQL,
-- caller-supplied label or value is retained. Unknown schema names fold to one
-- sentinel. At most 24 slots per account / fixed column / semantic class.
CREATE TABLE read_model_oauth_changes (
 account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
 column_name text NOT NULL CHECK(column_name IN ('id','account_id','provider','scope','access_token','refresh_token','token_type','expires_at','provider_user_id','provider_workspace_id','created_at','updated_at','user_token_encrypted','owner_user_id','dek_migrated_at','credential_invalid_at','credential_invalid_reason','_unknown','_noop')),
 semantic_class text NOT NULL CHECK(semantic_class IN ('authority','operational','credential_observation','unknown','noop')),
 slot smallint NOT NULL CHECK(slot>=0 AND slot<24),
 bucket_start timestamptz NOT NULL,
 statements bigint NOT NULL CHECK(statements>0),
 first_at timestamptz NOT NULL,
 last_at timestamptz NOT NULL,
 PRIMARY KEY(account_id,column_name,semantic_class,slot)
);
CREATE INDEX read_model_oauth_changes_expiry_idx ON read_model_oauth_changes(bucket_start);
ALTER TABLE read_model_oauth_changes ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE read_model_oauth_changes FROM PUBLIC;
DO $$ DECLARE role_name text; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
   EXECUTE format('REVOKE ALL ON TABLE public.read_model_oauth_changes FROM %I',role_name);
  END IF;
 END LOOP;
END $$;

CREATE FUNCTION read_model_oauth_change_attribution() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE bucket timestamptz := date_trunc('hour',statement_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
BEGIN
 -- AFTER STATEMENT transition tables see only this statement, including bulk
 -- updates. Full join handles a changed primary key; each account/column/class
 -- counts once per statement, not once per token or changed value.
 WITH pairs AS MATERIALIZED (
  SELECT to_jsonb(o) old_row,to_jsonb(n) new_row
  FROM old_rows o FULL JOIN new_rows n ON o.id=n.id
 ), changes AS (
  SELECT p.*, k.name,false is_noop
  FROM pairs p CROSS JOIN LATERAL (
   SELECT jsonb_object_keys(COALESCE(p.old_row,'{}') || COALESCE(p.new_row,'{}')) name
  ) k WHERE (p.old_row->k.name) IS DISTINCT FROM (p.new_row->k.name)
  UNION ALL
  SELECT p.*,'_noop',true FROM pairs p WHERE p.old_row=p.new_row
 ), dimensions AS (
  SELECT DISTINCT a.account_id,
   CASE WHEN is_noop THEN '_noop' WHEN name=ANY(ARRAY['id','account_id','provider','scope','access_token','refresh_token','token_type','expires_at','provider_user_id','provider_workspace_id','created_at','updated_at','user_token_encrypted','owner_user_id','dek_migrated_at','credential_invalid_at','credential_invalid_reason']) THEN name ELSE '_unknown' END column_name,
   CASE WHEN is_noop OR name='updated_at' THEN 'noop'
    WHEN name=ANY(ARRAY['access_token','refresh_token','token_type','expires_at','user_token_encrypted','dek_migrated_at']) THEN 'operational'
    -- Classification is descriptive, NOT suppression. Preserve latest-signal
    -- timestamp/reason writes and their existing authority invalidation.
    WHEN name=ANY(ARRAY['credential_invalid_at','credential_invalid_reason'])
     AND old_row->>'credential_invalid_at' IS NOT NULL
     AND new_row->>'credential_invalid_at' IS NOT NULL THEN 'credential_observation'
    WHEN name=ANY(ARRAY['id','account_id','provider','scope','provider_user_id','provider_workspace_id','created_at','owner_user_id','credential_invalid_at','credential_invalid_reason']) THEN 'authority'
    ELSE 'unknown' END semantic_class
  FROM changes c CROSS JOIN LATERAL (
   SELECT NULLIF(c.old_row->>'account_id','')::uuid account_id
   UNION SELECT NULLIF(c.new_row->>'account_id','')::uuid
  ) a WHERE a.account_id IS NOT NULL
 )
 INSERT INTO public.read_model_oauth_changes(account_id,column_name,semantic_class,slot,bucket_start,statements,first_at,last_at)
 SELECT d.account_id,d.column_name,d.semantic_class,mod(floor(extract(epoch FROM bucket)/3600)::bigint,24)::smallint,bucket,1,statement_timestamp(),statement_timestamp()
 FROM dimensions d JOIN public.accounts a ON a.id=d.account_id
 ORDER BY d.account_id,d.column_name,d.semantic_class
 ON CONFLICT(account_id,column_name,semantic_class,slot) DO UPDATE SET
  bucket_start=EXCLUDED.bucket_start,
  statements=CASE WHEN read_model_oauth_changes.bucket_start=EXCLUDED.bucket_start THEN read_model_oauth_changes.statements+1 ELSE 1 END,
  first_at=CASE WHEN read_model_oauth_changes.bucket_start=EXCLUDED.bucket_start THEN LEAST(read_model_oauth_changes.first_at,EXCLUDED.first_at) ELSE EXCLUDED.first_at END,
  last_at=CASE WHEN read_model_oauth_changes.bucket_start=EXCLUDED.bucket_start THEN GREATEST(read_model_oauth_changes.last_at,EXCLUDED.last_at) ELSE EXCLUDED.last_at END;
 DELETE FROM public.read_model_oauth_changes c USING (
  SELECT account_id,column_name,semantic_class,slot FROM public.read_model_oauth_changes
  WHERE bucket_start<=bucket-interval '24 hours' ORDER BY bucket_start LIMIT 64 FOR UPDATE SKIP LOCKED
 ) expired WHERE (c.account_id,c.column_name,c.semantic_class,c.slot)=
  (expired.account_id,expired.column_name,expired.semantic_class,expired.slot);
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION read_model_oauth_change_attribution() FROM PUBLIC;
CREATE TRIGGER read_model_oauth_change_attribution
 AFTER UPDATE ON oauth_tokens REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
 FOR EACH STATEMENT EXECUTE FUNCTION read_model_oauth_change_attribution();
DO $$ DECLARE role_name text; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
   EXECUTE format('REVOKE ALL ON FUNCTION public.read_model_oauth_change_attribution() FROM %I',role_name);
  END IF;
 END LOOP;
END $$;

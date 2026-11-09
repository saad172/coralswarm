-- Control only. Bounded authority-bump attribution, with no row contents.
-- Preserve 0414's authority/freshness decisions exactly; no invalidation is
-- suppressed. Counters roll into 24 UTC hourly slots, with bounded lazy expiry.
CREATE TABLE read_model_control_bumps (
 account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
 table_name text NOT NULL,
 operation text NOT NULL CHECK(operation IN ('INSERT','UPDATE','DELETE')),
 slot smallint NOT NULL CHECK(slot>=0 AND slot<24),
 bucket_start timestamptz NOT NULL,
 bumps bigint NOT NULL CHECK(bumps>0),
 first_at timestamptz NOT NULL,
 last_at timestamptz NOT NULL,
 PRIMARY KEY(account_id,table_name,operation,slot)
);
CREATE INDEX read_model_control_bumps_expiry_idx ON read_model_control_bumps(bucket_start);
ALTER TABLE read_model_control_bumps ENABLE ROW LEVEL SECURITY;
-- No policies: operations/migration owner only, including account identifiers.
-- Explicit revokes also defeat the app grants installed by default privileges.
REVOKE ALL ON TABLE read_model_control_bumps FROM PUBLIC;
DO $$ DECLARE role_name text; BEGIN
 FOREACH role_name IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN
   EXECUTE format('REVOKE ALL ON TABLE public.read_model_control_bumps FROM %I',role_name);
  END IF;
 END LOOP;
END $$;

CREATE OR REPLACE FUNCTION read_model_control_fact_changed() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE changed_rows jsonb; affected uuid[]; unchanged boolean; authority boolean := true; ignored text[]; bucket timestamptz:=date_trunc('hour',statement_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
BEGIN
 -- Strip only known operational fields when comparing authority. Everything
 -- else (including future columns) conservatively remains authority-bearing.
 -- Ordinary telemetry is durable freshness even on INSERT/DELETE, never a key.
 ignored := ARRAY['updated_at'];
 -- L4 known ordinary billing observations do not supersede model addresses.
 -- Account/user binding and every unknown future field remain conservative.
 IF TG_TABLE_NAME='credit_ledger' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['id','entry_kind','credits','source_job_id','source_query_id','graph_id','journal_period','journal_granularity','cost_usd','created_at','attributed_user_id','stripe_checkout_session_id','source_receipt_id','query_revision'];
 ELSIF TG_TABLE_NAME='credit_balances' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['balance','job_axis_cursor_finished_at','job_axis_cursor_job_id','query_axis_cursor_created_at','query_axis_cursor_usage_id','journal_axis_cursor_period','journal_axis_cursor_usage_id','receipt_cursor_at','receipt_cursor_id','managed_debited_credits'];
 ELSIF TG_TABLE_NAME='member_allowance_periods' THEN
  ignored := ignored || ARRAY['consumed_credits','managed_consumed_credits'];
 ELSIF TG_TABLE_NAME='slack_unmatched_email_notices' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['last_notified_at'];
 ELSIF TG_TABLE_NAME='mcp_tool_calls' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['id','client_id','token_jti','tool','scope_used','graph_id','target_id','args_digest','args_summary','outcome','error_code','duration_ms','request_id','created_at'];
 ELSIF TG_TABLE_NAME='google_meet_sync_state' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['last_synced_end_time','claimed_until','claim_id','last_attempted_at','last_synced_at','last_error','last_error_at'];
 ELSIF TG_TABLE_NAME='google_watch_channels' THEN
  authority := TG_OP='UPDATE';
  ignored := ignored || ARRAY['resource_id','channel_id','channel_resource_id','topic_name','channel_token','expires_at','last_renewed_at','last_error','last_error_at','sync_token','backfill_page_token','backfill_complete','push_pending_at'];
 ELSIF TG_TABLE_NAME='oauth_tokens' THEN
  ignored := ignored || ARRAY['access_token','refresh_token','user_token_encrypted','token_type','expires_at','dek_migrated_at'];
 ELSIF TG_TABLE_NAME='addin_tokens' THEN
  ignored := ignored || ARRAY['last_used_at'];
 ELSIF TG_TABLE_NAME='composio_connections' THEN
  ignored := ignored || ARRAY['self_check','self_check_reason','self_checked_at','namespace_checked_at'];
 ELSIF TG_TABLE_NAME='gmail_everything_mode' THEN
  ignored := ignored || ARRAY['resume_cursor','claimed_until','claim_id','last_synced_at','last_attempted_at','last_error','last_error_at','high_water_at','walk_started_at','walk_after','last_full_walk_at','page_failures','unreadable_ids','given_up_ids','page_failing_id','walk_skip_ids'];
 ELSIF TG_TABLE_NAME='outlook_everything_mode' THEN
  ignored := ignored || ARRAY['resume_cursor','claimed_until','claim_id','last_synced_at','last_attempted_at','last_error','last_error_at'];
 ELSIF TG_TABLE_NAME='microsoft_calendar_sync_state' THEN
  ignored := ignored || ARRAY['last_synced_at','last_error','last_error_at','delta_link','delta_resume_link','delta_window_end','delta_reconciled_at','delta_window_start','delta_verify_cursor','delta_verify_lap_end'];
 END IF;
 IF TG_OP='UPDATE' THEN
  -- Exact no-ops (and timestamp-only heartbeats) need no notification at all.
  EXECUTE 'SELECT NOT EXISTS(SELECT 1 FROM ((SELECT to_jsonb(n)-''updated_at'' j FROM new_rows n EXCEPT ALL SELECT to_jsonb(o)-''updated_at'' FROM old_rows o) UNION ALL (SELECT to_jsonb(o)-''updated_at'' FROM old_rows o EXCEPT ALL SELECT to_jsonb(n)-''updated_at'' FROM new_rows n)) changed)' INTO unchanged;
  IF unchanged THEN RETURN NULL; END IF;
  IF authority THEN
   EXECUTE 'SELECT EXISTS(SELECT 1 FROM ((SELECT to_jsonb(n)-$1 j FROM new_rows n EXCEPT ALL SELECT to_jsonb(o)-$1 FROM old_rows o) UNION ALL (SELECT to_jsonb(o)-$1 FROM old_rows o EXCEPT ALL SELECT to_jsonb(n)-$1 FROM new_rows n)) changed)' INTO authority USING ignored;
  END IF;
 END IF;
 IF TG_OP='INSERT' THEN EXECUTE 'SELECT jsonb_agg(to_jsonb(n)) FROM new_rows n' INTO changed_rows;
 ELSIF TG_OP='DELETE' THEN EXECUTE 'SELECT jsonb_agg(to_jsonb(o)) FROM old_rows o' INTO changed_rows;
 ELSE EXECUTE 'SELECT jsonb_agg(j) FROM (SELECT to_jsonb(n) j FROM new_rows n UNION ALL SELECT to_jsonb(o) FROM old_rows o) s' INTO changed_rows;
 END IF;
 WITH changes AS (SELECT value j FROM jsonb_array_elements(COALESCE(changed_rows,'[]'::jsonb)))
 SELECT array_agg(DISTINCT account_id) INTO affected FROM (
  SELECT NULLIF(j->>'account_id','')::uuid account_id FROM changes
  UNION SELECT NULLIF(j->>'id','')::uuid FROM changes WHERE TG_TABLE_NAME='accounts'
  UNION SELECT a.id FROM changes c JOIN public.accounts a ON a.clerk_org_id=c.j->>'clerk_org_id' WHERE TG_TABLE_NAME IN ('org_connector_flags','org_revocations','org_membership_tombstones','personal_orgs')
  UNION SELECT m.account_id FROM changes c JOIN public.account_members m ON m.user_id=CASE WHEN TG_TABLE_NAME='users' THEN NULLIF(c.j->>'id','')::uuid ELSE NULLIF(c.j->>'user_id','')::uuid END
  UNION SELECT a.id FROM changes c JOIN public.accounts a ON a.owner_user_id=CASE WHEN TG_TABLE_NAME='users' THEN NULLIF(c.j->>'id','')::uuid ELSE NULLIF(c.j->>'user_id','')::uuid END
  -- Applicants need not be members yet: their identity still labels each target org's queue/history.
  UNION SELECT r.account_id FROM changes c JOIN public.org_join_requests r ON r.user_id=CASE WHEN TG_TABLE_NAME='users' THEN NULLIF(c.j->>'id','')::uuid END
  UNION SELECT g.account_id FROM changes c JOIN public.grants g ON g.id=NULLIF(c.j->>'grant_id','')::uuid WHERE TG_TABLE_NAME='grant_seat_tiers'
  UNION SELECT m.account_id FROM changes c JOIN public.users u ON u.email=c.j->>'email' JOIN public.account_members m ON m.user_id=u.id WHERE TG_TABLE_NAME='slack_unmatched_email_notices'
  UNION SELECT m.account_id FROM changes c JOIN public.users u ON u.clerk_user_id=c.j->>'clerk_user_id' JOIN public.account_members m ON m.user_id=u.id WHERE TG_TABLE_NAME='personal_orgs'
  UNION SELECT id FROM public.accounts WHERE TG_TABLE_NAME IN ('plans','seat_tiers','model_rates','credit_conversion')
 ) a WHERE account_id IS NOT NULL;
 INSERT INTO public.read_model_control_revisions(account_id,revision,freshness)
 SELECT a.id,CASE WHEN authority THEN 1 ELSE 0 END,1 FROM public.accounts a WHERE a.id=ANY(affected)
 ON CONFLICT(account_id) DO UPDATE SET revision=read_model_control_revisions.revision+CASE WHEN authority THEN 1 ELSE 0 END,
 freshness=read_model_control_revisions.freshness+1;
 -- Record only actual authority increments, never operational freshness.
 -- The revision write above already serializes each account. Use its same
 -- account order for counters. Fixed hourly slots bound storage by dimensions,
 -- not write rate; no transition-row data or credentials enter this table.
 IF authority THEN
  INSERT INTO public.read_model_control_bumps(account_id,table_name,operation,slot,bucket_start,bumps,first_at,last_at)
  SELECT a.id,TG_TABLE_NAME,TG_OP,mod(floor(extract(epoch FROM bucket)/3600)::bigint,24)::smallint,
   bucket,1,statement_timestamp(),statement_timestamp()
  FROM public.accounts a WHERE a.id=ANY(affected) ORDER BY a.id
  ON CONFLICT(account_id,table_name,operation,slot) DO UPDATE SET
   bucket_start=EXCLUDED.bucket_start,
   bumps=CASE WHEN read_model_control_bumps.bucket_start=EXCLUDED.bucket_start
    THEN read_model_control_bumps.bumps+1 ELSE 1 END,
   first_at=CASE WHEN read_model_control_bumps.bucket_start=EXCLUDED.bucket_start
    THEN LEAST(read_model_control_bumps.first_at,EXCLUDED.first_at) ELSE EXCLUDED.first_at END,
   last_at=CASE WHEN read_model_control_bumps.bucket_start=EXCLUDED.bucket_start
    THEN GREATEST(read_model_control_bumps.last_at,EXCLUDED.last_at) ELSE EXCLUDED.last_at END;
  -- Lazy bounded cleanup: one indexed probe, at most 64 expired rows per
  -- authority statement. SKIP LOCKED avoids waiting on another cleanup/writer.
  DELETE FROM public.read_model_control_bumps b USING (
   SELECT account_id,table_name,operation,slot FROM public.read_model_control_bumps
   WHERE bucket_start<=bucket-interval '24 hours' ORDER BY bucket_start
   LIMIT 64 FOR UPDATE SKIP LOCKED
  ) expired
  WHERE (b.account_id,b.table_name,b.operation,b.slot)=
        (expired.account_id,expired.table_name,expired.operation,expired.slot);
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION read_model_control_fact_changed() FROM PUBLIC;

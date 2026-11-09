-- Control-only identity metadata: durable authority fence for CONTROL_FACTS models.
-- No content or per-model cache; the certified revision enters the shared scope key.
CREATE TABLE read_model_control_revisions (
 account_id uuid PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
 revision bigint NOT NULL DEFAULT 1,
 freshness bigint NOT NULL DEFAULT 1
);
ALTER TABLE read_model_control_revisions ENABLE ROW LEVEL SECURITY;
CREATE POLICY read_model_control_account ON read_model_control_revisions FOR SELECT
 USING(account_id=NULLIF(current_setting('app.account_id',true),'')::uuid);
CREATE FUNCTION read_model_control_fact_changed() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE changed_rows jsonb; affected uuid[]; unchanged boolean; authority boolean := true; ignored text[];
BEGIN
 -- Strip only known operational fields when comparing authority. Everything
 -- else (including future columns) conservatively remains authority-bearing.
 -- Ordinary telemetry is durable freshness even on INSERT/DELETE, never a key.
 ignored := ARRAY['updated_at'];
 IF TG_TABLE_NAME='mcp_tool_calls' THEN
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
 ) a WHERE account_id IS NOT NULL;
 INSERT INTO public.read_model_control_revisions(account_id,revision,freshness)
 SELECT a.id,CASE WHEN authority THEN 1 ELSE 0 END,1 FROM public.accounts a WHERE a.id=ANY(affected)
 ON CONFLICT(account_id) DO UPDATE SET revision=read_model_control_revisions.revision+CASE WHEN authority THEN 1 ELSE 0 END,
 freshness=read_model_control_revisions.freshness+1;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION read_model_control_fact_changed() FROM PUBLIC;
DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
 -- 0083 grants default DML on newly created tables. These counters are
 -- authority metadata: only the owner-backed trigger may advance them.
 REVOKE INSERT, UPDATE, DELETE ON read_model_control_revisions FROM coralswarm_app;
 GRANT SELECT ON read_model_control_revisions TO coralswarm_app;
END IF; END $$;
CREATE TRIGGER read_model_control_personal_orgs_insert AFTER INSERT ON personal_orgs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_personal_orgs_update AFTER UPDATE ON personal_orgs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_personal_orgs_delete AFTER DELETE ON personal_orgs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_oauth_tokens_insert AFTER INSERT ON oauth_tokens REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_oauth_tokens_update AFTER UPDATE ON oauth_tokens REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_oauth_tokens_delete AFTER DELETE ON oauth_tokens REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_composio_connections_insert AFTER INSERT ON composio_connections REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_composio_connections_update AFTER UPDATE ON composio_connections REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_composio_connections_delete AFTER DELETE ON composio_connections REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_connector_flags_insert AFTER INSERT ON org_connector_flags REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_connector_flags_update AFTER UPDATE ON org_connector_flags REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_connector_flags_delete AFTER DELETE ON org_connector_flags REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_github_installations_insert AFTER INSERT ON github_installations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_github_installations_update AFTER UPDATE ON github_installations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_github_installations_delete AFTER DELETE ON github_installations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_connector_sources_insert AFTER INSERT ON connector_sources REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_connector_sources_update AFTER UPDATE ON connector_sources REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_connector_sources_delete AFTER DELETE ON connector_sources REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_integrations_insert AFTER INSERT ON slack_integrations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_integrations_update AFTER UPDATE ON slack_integrations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_integrations_delete AFTER DELETE ON slack_integrations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_google_watch_channels_insert AFTER INSERT ON google_watch_channels REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_google_watch_channels_update AFTER UPDATE ON google_watch_channels REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_google_watch_channels_delete AFTER DELETE ON google_watch_channels REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_gmail_everything_mode_insert AFTER INSERT ON gmail_everything_mode REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_gmail_everything_mode_update AFTER UPDATE ON gmail_everything_mode REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_gmail_everything_mode_delete AFTER DELETE ON gmail_everything_mode REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_outlook_everything_mode_insert AFTER INSERT ON outlook_everything_mode REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_outlook_everything_mode_update AFTER UPDATE ON outlook_everything_mode REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_outlook_everything_mode_delete AFTER DELETE ON outlook_everything_mode REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_microsoft_calendar_sync_state_insert AFTER INSERT ON microsoft_calendar_sync_state REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_microsoft_calendar_sync_state_update AFTER UPDATE ON microsoft_calendar_sync_state REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_microsoft_calendar_sync_state_delete AFTER DELETE ON microsoft_calendar_sync_state REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_google_meet_sync_state_insert AFTER INSERT ON google_meet_sync_state REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_google_meet_sync_state_update AFTER UPDATE ON google_meet_sync_state REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_google_meet_sync_state_delete AFTER DELETE ON google_meet_sync_state REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_mcp_tool_calls_insert AFTER INSERT ON mcp_tool_calls REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_mcp_tool_calls_update AFTER UPDATE ON mcp_tool_calls REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_mcp_tool_calls_delete AFTER DELETE ON mcp_tool_calls REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_addin_tokens_insert AFTER INSERT ON addin_tokens REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_addin_tokens_update AFTER UPDATE ON addin_tokens REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_addin_tokens_delete AFTER DELETE ON addin_tokens REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_addin_microsoft_links_insert AFTER INSERT ON addin_microsoft_links REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_addin_microsoft_links_update AFTER UPDATE ON addin_microsoft_links REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_addin_microsoft_links_delete AFTER DELETE ON addin_microsoft_links REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_users_insert AFTER INSERT ON users REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_users_update AFTER UPDATE ON users REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_users_delete AFTER DELETE ON users REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_user_prefs_insert AFTER INSERT ON user_prefs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_user_prefs_update AFTER UPDATE ON user_prefs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_user_prefs_delete AFTER DELETE ON user_prefs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_user_account_prefs_insert AFTER INSERT ON user_account_prefs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_user_account_prefs_update AFTER UPDATE ON user_account_prefs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_user_account_prefs_delete AFTER DELETE ON user_account_prefs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_account_prefs_insert AFTER INSERT ON account_prefs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_account_prefs_update AFTER UPDATE ON account_prefs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_account_prefs_delete AFTER DELETE ON account_prefs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_accounts_insert AFTER INSERT ON accounts REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_accounts_update AFTER UPDATE ON accounts REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_accounts_delete AFTER DELETE ON accounts REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_account_members_insert AFTER INSERT ON account_members REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_account_members_update AFTER UPDATE ON account_members REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_account_members_delete AFTER DELETE ON account_members REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_revocations_insert AFTER INSERT ON org_revocations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_revocations_update AFTER UPDATE ON org_revocations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_revocations_delete AFTER DELETE ON org_revocations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_membership_tombstones_insert AFTER INSERT ON org_membership_tombstones REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_membership_tombstones_update AFTER UPDATE ON org_membership_tombstones REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_org_membership_tombstones_delete AFTER DELETE ON org_membership_tombstones REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_member_channels_insert AFTER INSERT ON slack_member_channels REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_member_channels_update AFTER UPDATE ON slack_member_channels REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();
CREATE TRIGGER read_model_control_slack_member_channels_delete AFTER DELETE ON slack_member_channels REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION read_model_control_fact_changed();

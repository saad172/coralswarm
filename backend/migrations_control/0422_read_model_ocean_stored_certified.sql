-- PAIRED WITH: tenant 0233 (read_model_ocean_stored_certified).
-- Successor of read_model_ocean_stored (0414) for the OceansIndex personal
-- fan-in. Stored reads now serve a row that is behind a reduction as stale
-- instead of withholding it, and `read_models/store.rs` `decode` needs two
-- more certification signals beside fence_state to do that safely:
--   failed    the last compute failed (`failed_attempts>0`, which only
--             publish resets; the dirty triggers reset retry_after on every
--             write, so retry_after alone cannot say this). A behind row is
--             withheld again while its recompute fails, because the owed
--             recompute is what bounds how long a reduced item can show.
--   identity  the row is certified for this request: a compute snapshot
--             exists, and the account and (when windowed) the viewer match
--             the request. These are the refusals read_model_fence_state
--             folds into 0; they are authority, not "behind", and withhold.
-- Additive only: the previous function stays for the release before this one
-- and is retired by a later contract migration. Paired bodies are identical
-- after this header.
CREATE FUNCTION read_model_ocean_stored_certified(g uuid,k text,v int)
RETURNS TABLE(payload text,computed_at timestamptz,dirty_at timestamptz,computed_control_freshness bigint,fence_state int,eta timestamptz,retry_after timestamptz,valid boolean,failed boolean,identity boolean)
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE a uuid; prior text:=current_setting('app.account_id',true);
BEGIN
 IF to_regclass('public.tenant_meta') IS NOT NULL THEN RAISE EXCEPTION 'personal fan-in unavailable on tenant plane' USING ERRCODE='42501'; END IF;
 a:=public.read_model_graph_account(g);
 IF a IS NULL OR EXISTS(SELECT 1 FROM public.tenant_databases t WHERE t.account_id=a) THEN RETURN; END IF;
 PERFORM set_config('app.account_id',a::text,true);
 RETURN QUERY SELECT r.payload::text,r.computed_at,r.dirty_at,r.computed_control_freshness,
  public.read_model_fence_state(r.account_id,r.graph_ids,r.model,r.viewer_user_id,r.computed_snapshot),
  LEAST(r.dirty_bumped_at+make_interval(secs=>r.debounce_seconds),r.dirty_at+make_interval(secs=>r.max_delay_seconds)),r.retry_after,
  (r.invalidated_xid IS NULL OR pg_visible_in_snapshot(r.invalidated_xid,r.computed_snapshot)),
  r.failed_attempts>0,
  (r.computed_snapshot IS NOT NULL AND (current_setting('app.tenant_actor',true) IS NOT DISTINCT FROM 'system'
   OR (r.account_id IS NOT DISTINCT FROM NULLIF(current_setting('app.account_id',true),'')::uuid
       AND (r.viewer_user_id IS NULL OR r.viewer_user_id IS NOT DISTINCT FROM NULLIF(current_setting('app.user_id',true),'')::uuid))))
 FROM public.read_models r WHERE r.account_id=a AND r.model='oceans_index' AND r.scope_key=k AND r.version=v;
 PERFORM set_config('app.account_id',COALESCE(prior,''),true);
EXCEPTION WHEN OTHERS THEN PERFORM set_config('app.account_id',COALESCE(prior,''),true); RAISE;
END $$;
REVOKE ALL ON FUNCTION read_model_ocean_stored_certified(uuid,text,int) FROM PUBLIC;
DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN GRANT EXECUTE ON FUNCTION read_model_ocean_stored_certified(uuid,text,int) TO coralswarm_app; END IF; END $$;

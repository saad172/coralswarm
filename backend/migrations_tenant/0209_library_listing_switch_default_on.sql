-- PAIRED WITH: control 0394 (library_listing_switch_default_on).
-- The Library listing program is the default. A plane whose listing switch is
-- still Off (a freshly provisioned tenant database, or a plane never flipped)
-- comes up On with a new generation. A plane already On keeps its generation:
-- the UPDATE matches no row, so the guard trigger never fires and no graph
-- rebuilds.
--
-- This is the operator flip (workers/library_evidence/listing_facts.rs,
-- flip_switch) run once by the release. The worker stamps the barrier on its
-- next tick, as it does after a crash between the operator's two
-- transactions. sqlx runs this file in its own transaction, so the flip
-- setting is scoped to it.
SET LOCAL lock_timeout = '3s';
SELECT set_config('coralswarm.library_listing_switch_flip','operator',true);

UPDATE library_listing_switch
 SET enabled=true, enabled_since=now(), enabled_snapshot=NULL, snapshot_since=NULL,
     barrier_xid=NULL, barrier_since=NULL, changed_at=now()
 WHERE NOT enabled;

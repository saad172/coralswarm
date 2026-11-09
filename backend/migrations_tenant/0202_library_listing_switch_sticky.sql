-- PAIRED WITH: control 0383 (library_listing_switch_sticky).
-- The listing switch becomes sticky, and its flip barrier stamps an xid.
--
-- 1. Sticky switch. Until now every backend mirrored its LIBRARY_LISTING_FACTS
--    value into this row every tick, so two instances that disagreed (a
--    rolling deploy) flapped enabled_since, and every flip to On is a new
--    generation that rebuilds every graph. The row is now the only source of
--    truth and only the operator command flips it
--    (workers/library_evidence/listing_facts.rs, flip_switch, run by the
--    library_listing_switch binary); the env var is ignored with a warning.
--    The guard trigger below refuses any UPDATE that changes enabled or
--    enabled_since unless the transaction set
--    coralswarm.library_listing_switch_flip to 'operator' (set_config(...,
--    true), transaction-local). It is not a security boundary: it makes the
--    flip an explicit act that no other code path performs, and it refuses
--    the per-tick mirror of a binary that predates this migration, so an old
--    instance still running during the deploy cannot revert an operator flip
--    (its mirror UPDATE fails; when the row already matches its env value
--    the UPDATE matches no row and the trigger never fires).
--
-- 2. Flip barrier. 0201/0380 stamped enabled_snapshot=pg_current_snapshot()
--    after the flip and settled once pg_snapshot_xmin(pg_current_snapshot())
--    reached its xmax. A snapshot's xmax is one past the highest COMPLETED
--    xid, not the next xid to be assigned. A writer that took its xid after
--    the flip's, and whose capture trigger read the switch before the flip
--    committed (so read Off and logged nothing), can have an xid above every
--    completed one at the stamp: it is neither below the stamp's xmax nor in
--    its in-progress list, so the barrier could settle with it still open
--    and the rebuild would miss its write. barrier_xid is instead the stamp
--    transaction's own pg_current_xact_id(), assigned after a snapshot that
--    sees the flip committed; every writer that read Off was assigned its
--    xid before that, and the barrier settles only once
--    pg_snapshot_xmin(pg_current_snapshot()) > barrier_xid. barrier_since
--    names the generation the stamp belongs to, as snapshot_since did.
--    enabled_snapshot/snapshot_since stay for binaries that predate this
--    migration; nothing here reads them. A plane already On has no
--    barrier_xid, so it waits until the next worker tick stamps one (a new
--    stamp in the same generation: no rebuild).
SET LOCAL lock_timeout = '3s';

ALTER TABLE library_listing_switch
 ADD COLUMN barrier_xid xid8,
 ADD COLUMN barrier_since TIMESTAMPTZ;

CREATE FUNCTION library_listing_switch_guard() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
BEGIN
 IF current_setting('coralswarm.library_listing_switch_flip',true) IS DISTINCT FROM 'operator' THEN
  RAISE EXCEPTION 'library_listing_switch is flipped only by the library_listing_switch operator command'
   USING ERRCODE='insufficient_privilege';
 END IF;
 RETURN NEW;
END $$;

CREATE TRIGGER library_listing_switch_guard
 BEFORE UPDATE OF enabled,enabled_since ON library_listing_switch
 FOR EACH ROW
 WHEN (OLD.enabled IS DISTINCT FROM NEW.enabled OR OLD.enabled_since IS DISTINCT FROM NEW.enabled_since)
 EXECUTE FUNCTION library_listing_switch_guard();

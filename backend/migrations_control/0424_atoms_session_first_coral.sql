-- no-transaction
-- Home "Continue" (GET /v1/home/continue): a session is the viewer's when its
-- FIRST coral by creation time (created_at, atom_id) is theirs. A session
-- coral's source_id is "cc:{session_id}:{checkpoint_id}[:chunk:N]", and both
-- ids may contain colons, so the only key every coral of a session shares is
-- the first segment of its session id. With only the (graph_id, source_id)
-- pattern index the read had to sort the session's whole cc:{id}: range
-- (14 to 21 ms for the read with one 5,000-checkpoint session on a 228k-coral
-- seed, growing with the session); with this index it walks the session's
-- corals in time order and stops at the first one (about 3 ms). Partial:
-- session corals only.
--
-- CREATE INDEX CONCURRENTLY cannot run in a transaction, so it is alone in a
-- `-- no-transaction` file. A failed build leaves an INVALID index that
-- IF NOT EXISTS would then skip on retry, so drop it first:
--   DROP INDEX CONCURRENTLY IF EXISTS atoms_session_first_coral;
-- The DDL is identical in both trees (tenant 0235 is the twin): `atoms` is a
-- shared table and `check_schema_parity.sh` diffs its schema.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS atoms_session_first_coral;

CREATE INDEX CONCURRENTLY IF NOT EXISTS atoms_session_first_coral
    ON atoms (graph_id, split_part(source_id, ':', 2), created_at, atom_id)
    WHERE left(source_id, 3) = 'cc:';

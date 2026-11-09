-- 0011_coding_session_source_type.sql — paired half of
-- `migrations_control/0098_coding_session_source_type.sql`. See that
-- migration's header for the full rationale: why `'coding_session'` is a
-- DERIVED source type rather than an `add_context` allowlist entry, why the
-- relabel needs no re-embed (chunk strategy and the coding-session chunk
-- header are both independent of `source_type`), why the natural-key
-- collision it guards against cannot occur, why relabelling in place is what
-- keeps re-ingest idempotent across the cutover, and why desktop replicas are
-- deliberately left alone.
--
-- Why this tree needs it at all. `atoms` is a SHARED table (present in both
-- trees; NOT in `check_schema_parity.sh`'s `CONTROL_TABLES` roster), so the
-- DDL below must stay BYTE-IDENTICAL to the control migration or the
-- schema-only `pg_dump` diff fails. Beyond parity, the backfill has real work
-- to do here: ingest lands in the caller's own private ocean on whichever
-- plane they are on (ADR-0007 D7 as amended by member-private-oceans PR 5),
-- which for an org member is their `kind='member'` ocean in THIS database.
-- Coding-session corals therefore exist on the tenant plane too, and skipping
-- this tree would leave org members' captures labelled as notes forever.
--
-- APPLY NOTE: the control tree auto-applies via the release command; the
-- tenant tree does not. This must be applied to every provisioned org tenant
-- database (the same manual step every tenant migration has needed), and
-- until it is, that tenant's schema-version gate will 503 the API plane.
--
-- The partial index lives in 0012 as a `-- no-transaction` `CREATE INDEX
-- CONCURRENTLY`, mirroring control 0099 — see control 0098's header for the
-- split rationale and for why the backfill below is deliberately unbatched.
--
-- Depends on: 0001_tenant_baseline (atoms, and the `cc:` convention's
-- coding_sessions/projects tables).
-- Down: see control 0098 (same statements, same lossiness caveat).

UPDATE atoms AS a
   SET source_type = 'coding_session'
 WHERE a.source_type IN ('note', 'chat')
   AND a.source_id LIKE 'cc:%'
   AND NOT EXISTS (
         SELECT 1
           FROM atoms AS clash
          WHERE clash.graph_id    = a.graph_id
            AND clash.source_type = 'coding_session'
            AND clash.source_id   = a.source_id
       );

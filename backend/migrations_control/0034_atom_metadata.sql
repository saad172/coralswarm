-- Structured provenance on atoms (corals). Until now a coral's channel, author,
-- and time lived only as a text header prepended to the body
-- ("<author> in #<channel> · <when>"), forcing brittle string-parsing
-- (graphs.rs channel_label_from_preview). These typed columns make that
-- provenance queryable and indexable. The body header stays for now (synthesis
-- + embeddings are tuned around it); consumers migrate to these columns.
--
-- All nullable: non-Slack sources (e.g. meeting huddles) have no channel, and
-- historical atoms are backfilled separately (author_id is unrecoverable for
-- those, so it stays NULL on old rows).
ALTER TABLE atoms
    ADD COLUMN channel_id   TEXT,
    ADD COLUMN channel_name TEXT,
    ADD COLUMN author_id    TEXT,
    ADD COLUMN author_name  TEXT;

-- Backs grouping/filtering the Sources view by channel within an ocean.
CREATE INDEX idx_atoms_graph_channel ON atoms (graph_id, channel_id);

-- Library text search: btree_gin lets one GIN index key on graph_id beside the
-- trigram, so the outcome-text match reads only the scope graph's candidates
-- (issue #1524 blocker 5). Paired control/tenant DDL is identical.
-- btree_gin is a trusted contrib extension: the database owner can create it.
-- Its support functions are called by the index, never by a query, so the
-- tenant default-privilege revoke (tenant 0009) does not reach the app role.
-- Down: none. A later index may depend on the extension.

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;

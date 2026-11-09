\set ON_ERROR_STOP on
\timing on
SET search_path = public;
SELECT setseed(0.42);

-- The caller MUST supply acct/u1/u2/g_org/g_team/g_mem as SQL-quoted UUID
-- psql variables (or substitute them in the PG test harness). Never override
-- CLI variables here: the nightly uses real API-provisioned actor/account IDs.
-- This is the single authoritative 600k/60k distribution for PG and browser
-- tests. Existing graph identities are verified by the nightly adapter.

ALTER TABLE atoms DISABLE TRIGGER USER;
ALTER TABLE nodes DISABLE TRIGGER USER;
ALTER TABLE node_atoms DISABLE TRIGGER USER;
ALTER TABLE semantic_atoms DISABLE TRIGGER USER;
ALTER TABLE node_semantic_atoms DISABLE TRIGGER USER;
ALTER TABLE source_items DISABLE TRIGGER USER;
ALTER TABLE source_versions DISABLE TRIGGER USER;

INSERT INTO graphs(id, account_id, name, kind, owner_user_id) VALUES
 (:g_org, :acct, 'Org ocean', 'org', NULL),
 (:g_team, :acct, 'Team ocean', 'team', NULL),
 (:g_mem, :acct, 'My ocean', 'member', :u1)
 ON CONFLICT (id) DO NOTHING;

INSERT INTO teams(id, account_id, graph_id, name, created_by)
 VALUES ('33333333-0000-0000-0000-000000000001', :acct, :g_team, 'Platform', :u1);
INSERT INTO team_members(team_id, user_id, role)
 VALUES ('33333333-0000-0000-0000-000000000001', :u1, 'member'),
        ('33333333-0000-0000-0000-000000000001', :u2, 'member');
INSERT INTO team_seat_grants(account_id, team_id, user_id, source, history_from)
 VALUES (:acct, '33333333-0000-0000-0000-000000000001', :u1, 'seed', NULL),
        (:acct, '33333333-0000-0000-0000-000000000001', :u2, 'seed', now() - interval '20 days');

-- Atoms: 600k, skewed to recent days, six source types, meetings chunked.
CREATE TEMP TABLE seed_atoms AS
SELECT i,
       md5('atom'||i)::uuid AS atom_id,
       CASE WHEN i % 20 < 15 THEN :g_org::uuid WHEN i % 20 < 19 THEN :g_team::uuid ELSE :g_mem::uuid END AS graph_id,
       CASE (i % 100)
         WHEN 0 THEN 'meeting_transcript' WHEN 1 THEN 'meeting_transcript' WHEN 2 THEN 'meeting_transcript'
         WHEN 3 THEN 'meeting_transcript' WHEN 4 THEN 'meeting_transcript' WHEN 5 THEN 'meeting_transcript'
         WHEN 6 THEN 'meeting_transcript' WHEN 7 THEN 'meeting_transcript' WHEN 8 THEN 'meeting_transcript'
         WHEN 9 THEN 'meeting_transcript' WHEN 10 THEN 'meeting_transcript' WHEN 11 THEN 'meeting_transcript'
         WHEN 12 THEN 'meeting_transcript' WHEN 13 THEN 'meeting_transcript' WHEN 14 THEN 'meeting_transcript'
         WHEN 15 THEN 'github_pr' WHEN 16 THEN 'github_pr' WHEN 17 THEN 'github_pr' WHEN 18 THEN 'github_pr' WHEN 19 THEN 'github_pr'
         WHEN 20 THEN 'github_pr' WHEN 21 THEN 'github_pr' WHEN 22 THEN 'github_pr' WHEN 23 THEN 'github_pr' WHEN 24 THEN 'github_pr'
         WHEN 25 THEN 'github_pr' WHEN 26 THEN 'github_pr' WHEN 27 THEN 'github_pr' WHEN 28 THEN 'github_pr' WHEN 29 THEN 'github_pr'
         WHEN 30 THEN 'coding_session' WHEN 31 THEN 'coding_session' WHEN 32 THEN 'coding_session' WHEN 33 THEN 'coding_session'
         WHEN 34 THEN 'coding_session' WHEN 35 THEN 'coding_session' WHEN 36 THEN 'coding_session' WHEN 37 THEN 'coding_session'
         WHEN 38 THEN 'coding_session' WHEN 39 THEN 'coding_session'
         WHEN 40 THEN 'email' WHEN 41 THEN 'email' WHEN 42 THEN 'email' WHEN 43 THEN 'email' WHEN 44 THEN 'email'
         WHEN 45 THEN 'email' WHEN 46 THEN 'email' WHEN 47 THEN 'email' WHEN 48 THEN 'email' WHEN 49 THEN 'email'
         WHEN 50 THEN 'teams_chat' WHEN 51 THEN 'teams_chat' WHEN 52 THEN 'teams_chat' WHEN 53 THEN 'teams_chat' WHEN 54 THEN 'teams_chat'
         WHEN 55 THEN 'teams_chat' WHEN 56 THEN 'teams_chat' WHEN 57 THEN 'teams_chat' WHEN 58 THEN 'teams_chat' WHEN 59 THEN 'teams_chat'
         ELSE 'slack' END AS source_type,
       now() - (power(random(), 2) * interval '180 days') AS created_at,
       CASE WHEN i % 2 = 0 THEN :u1::uuid ELSE :u2::uuid END AS ingested_by,
       random() AS r
FROM generate_series(1, 600000) i;

INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, participants, "timestamp", ingested_by,
                  scope, created_at, content_inline, title, author_name, channel_name,
                  source_evidence_bound, derived_copy_bound, evidence_retracted)
SELECT atom_id, graph_id, source_type,
       CASE WHEN source_type = 'meeting_transcript' THEN 'mtg-' || (i / 20) || ':chunk:' || (i % 20)
            ELSE source_type || '-' || i END,
       'blob/' || atom_id,
       ARRAY['Person ' || (i % 500), 'Person ' || ((i * 7) % 500)],
       created_at - (random() * interval '6 hours'),
       ingested_by,
       'shared',
       created_at,
       'Discussion about Topic ' || (i % 60000) || ' and Topic ' || ((i * 7) % 60000) || ' ' || repeat('lorem ipsum dolor sit amet ', 8),
       'Title ' || i, 'Person ' || (i % 500), '#chan-' || (i % 40),
       r < 0.06,            -- bound to a source version
       r >= 0.06 AND r < 0.065,  -- derived copy
       r >= 0.065 AND r < 0.075  -- retracted
FROM seed_atoms;

-- Source items: one per bound atom's "document" (every 10 bound atoms share a version), 10% restricted.
-- Plus 300 denied items whose (source_type, source_id) match unbound atoms exactly.
CREATE TEMP TABLE seed_bound AS
SELECT a.atom_id, a.graph_id, a.source_type, s.i, (s.i / 10) AS doc
FROM seed_atoms s JOIN atoms a USING (atom_id) WHERE a.source_evidence_bound;

INSERT INTO source_items(source_item_id, graph_id, source_type, provider_namespace, source_id, access_state)
SELECT DISTINCT ON (doc, graph_id) md5('item'||doc||graph_id)::uuid, graph_id, source_type, 'seed', 'doc-' || doc,
       CASE WHEN doc % 10 = 0 THEN 'restricted' ELSE 'permitted' END
FROM seed_bound ORDER BY doc, graph_id;

INSERT INTO source_versions(version_id, graph_id, source_id, source_item_id, source_type, provider_namespace, captured_at)
SELECT md5('ver'||si.source_item_id)::uuid, si.graph_id, si.source_id, si.source_item_id, si.source_type, 'seed', now() - interval '1 day'
FROM source_items si WHERE si.provider_namespace = 'seed';
UPDATE source_items si SET current_version_id = v.version_id
FROM source_versions v WHERE v.source_item_id = si.source_item_id AND si.provider_namespace = 'seed';

INSERT INTO source_version_chunks(graph_id, source_version_id, atom_id)
SELECT b.graph_id, v.version_id, b.atom_id
FROM seed_bound b
JOIN source_items si ON si.graph_id = b.graph_id AND si.source_id = 'doc-' || b.doc AND si.provider_namespace='seed'
JOIN source_versions v ON v.source_item_id = si.source_item_id;

-- Denied items for unbound atoms (the NOT EXISTS branch of library_permitted_atoms).
INSERT INTO source_items(source_item_id, graph_id, source_type, provider_namespace, source_id, access_state)
SELECT md5('deny'||a.atom_id)::uuid, a.graph_id, a.source_type, 'seed-deny', a.source_id, 'restricted'
FROM atoms a JOIN seed_atoms s USING (atom_id)
WHERE NOT a.source_evidence_bound AND s.r > 0.9995 AND a.source_type <> 'meeting_transcript';

-- Nodes: 60k reefs across the three oceans.
CREATE TEMP TABLE seed_nodes AS
SELECT i, md5('node'||i)::uuid AS node_id,
       CASE WHEN i % 20 < 15 THEN :g_org::uuid WHEN i % 20 < 19 THEN :g_team::uuid ELSE :g_mem::uuid END AS graph_id,
       (ARRAY['entity','entity','entity','thread','decision','reference'])[1 + i % 6] AS type,
       now() - (power(random(), 2) * interval '180 days') AS created_at
FROM generate_series(1, 60000) i;

INSERT INTO nodes(node_id, graph_id, type, title, body, scope, created_at, updated_at, coral_count)
SELECT node_id, graph_id, type,
       'Topic ' || i,
       CASE WHEN type = 'decision' THEN jsonb_build_object('decided_at', created_at::text) ELSE '{}'::jsonb END,
       'shared', created_at, created_at + interval '1 hour', 0
FROM seed_nodes;

-- Links: every atom links to two nodes of its ocean; node choice is Zipf-like so a few reefs are hubs.
INSERT INTO node_atoms(node_id, atom_id)
SELECT DISTINCT n.node_id, a.atom_id
FROM seed_atoms a
CROSS JOIN LATERAL (
  SELECT floor(power(((a.i::bigint * 2654435761) % 1000003) / 1000003.0, 3) * 60000)::int AS k
  UNION ALL SELECT ((a.i::bigint * 7919) % 60000)::int
) pick
JOIN seed_nodes n ON n.i = 1 + pick.k AND n.graph_id = a.graph_id;

UPDATE nodes n SET coral_count = c.n FROM (SELECT node_id, count(*) n FROM node_atoms GROUP BY 1) c WHERE c.node_id = n.node_id;

-- Semantic atoms on the seeded versions, linked to nodes (semantic admission branch).
INSERT INTO semantic_atoms(semantic_atom_id, graph_id, source_version_id, ordinal, kind, statement, extraction_version, model, entities)
SELECT md5('sem'||v.version_id||o)::uuid, v.graph_id, v.version_id, o, 'decision', 'We decided thing ' || o, 'v1', 'seed',
       jsonb_build_array(jsonb_build_object('mention', 'Topic ' || ((o * 37) % 3000)))
FROM source_versions v CROSS JOIN generate_series(1, 5) o WHERE v.provider_namespace = 'seed';

INSERT INTO node_semantic_atoms(node_id, semantic_atom_id, graph_id)
SELECT DISTINCT n.node_id, sa.semantic_atom_id, sa.graph_id
FROM semantic_atoms sa
JOIN seed_nodes n ON n.graph_id = sa.graph_id AND n.i = 1 + (abs(hashtext(sa.semantic_atom_id::text)) % 60000);

-- Questions and agent sessions.
INSERT INTO query_history(account_id, graph_id, user_id, query_text, created_at)
SELECT :acct, :g_org, CASE WHEN i % 2 = 0 THEN :u1::uuid ELSE :u2::uuid END, 'q ' || i, now() - (random() * interval '120 days')
FROM generate_series(1, 3000) i;
INSERT INTO coding_sessions(graph_id, session_id, started_at, session_kind)
SELECT CASE WHEN i % 4 = 0 THEN :g_team::uuid ELSE :g_org::uuid END, 'sess-' || i, now() - (random() * interval '120 days'), 'coding'
FROM generate_series(1, 6000) i;

ALTER TABLE atoms ENABLE TRIGGER USER;
ALTER TABLE nodes ENABLE TRIGGER USER;
ALTER TABLE node_atoms ENABLE TRIGGER USER;
ALTER TABLE semantic_atoms ENABLE TRIGGER USER;
ALTER TABLE node_semantic_atoms ENABLE TRIGGER USER;
ALTER TABLE source_items ENABLE TRIGGER USER;
ALTER TABLE source_versions ENABLE TRIGGER USER;

VACUUM ANALYZE atoms; VACUUM ANALYZE nodes; VACUUM ANALYZE node_atoms; VACUUM ANALYZE source_items;
VACUUM ANALYZE source_versions; VACUUM ANALYZE source_version_chunks; VACUUM ANALYZE semantic_atoms; VACUUM ANALYZE node_semantic_atoms;

SELECT (SELECT count(*) FROM atoms) atoms, (SELECT count(*) FROM nodes) nodes, (SELECT count(*) FROM node_atoms) links,
       (SELECT count(*) FROM source_items) items, (SELECT count(*) FROM source_version_chunks) chunks,
       (SELECT count(*) FROM semantic_atoms) sems, (SELECT max(coral_count) FROM nodes) hub;

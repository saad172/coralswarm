#!/usr/bin/env python3
"""Synthetic final-tree consumer admission audit; no production/grant changes.

Requires ORGT_TEST_BASE_URL at the disposable localhost postgres database.
Uses an existing coralswarm_app login; its public local-fixture password is not
set or changed by this probe. Account RLS and HTTP graph admission are separate.
"""
from pathlib import Path
from urllib.parse import urlparse, urlunparse
import json
import os
import subprocess
import uuid

configured = os.environ['ORGT_TEST_BASE_URL']
parsed = urlparse(configured)
assert parsed.hostname in ('localhost', '127.0.0.1', '::1')
assert parsed.path == '/postgres' and not parsed.query
root = Path(__file__).resolve().parents[4]
db = 'consumer_private_audit_' + uuid.uuid4().hex
owner_base = configured.rsplit('/', 1)[0] + '/'
host = '[' + parsed.hostname + ']' if ':' in parsed.hostname else parsed.hostname
app_url = urlunparse((parsed.scheme, f'coralswarm_app@{host}:{parsed.port or 5432}', '/' + db, '', '', ''))

def run(database, sql=None, file=None, app=False):
    command = ['psql', app_url if app else owner_base + database, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1']
    if file:
        if not file.read_text().startswith('-- no-transaction'):
            command.append('-1')
        command += ['-f', str(file)]
    env = os.environ.copy()
    if app:
        # Public test constant, matching support/rls_probe.rs; never a real key.
        env['PGPASSWORD'] = 'orgt_rls_probe_test_only_pw'
    result = subprocess.run(command, input=sql, text=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()

account, creator, member, outsider = [str(uuid.uuid4()) for _ in range(4)]
private_creator, private_member, shared, team, archived = [str(uuid.uuid4()) for _ in range(5)]
run('postgres', f'CREATE DATABASE {db};')
try:
    run(db, 'CREATE TABLE _sqlx_migrations(version BIGINT PRIMARY KEY,description TEXT NOT NULL,installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),success BOOLEAN NOT NULL,checksum BYTEA NOT NULL,execution_time BIGINT NOT NULL);')
    for migration in sorted((root / 'backend/migrations_control').glob('*.sql')):
        run(db, file=migration)
    run(db, f"""
        INSERT INTO users(id,email) VALUES('{creator}','creator@private-probe.test'),('{member}','member@private-probe.test'),('{outsider}','outsider@private-probe.test');
        INSERT INTO accounts(id,owner_user_id) VALUES('{account}','{creator}');
        INSERT INTO account_members(account_id,user_id,role) VALUES('{account}','{creator}','owner'),('{account}','{member}','admin');
        INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES
          ('{private_creator}','{account}','CREATOR PRIVATE','member','{creator}'),
          ('{private_member}','{account}','MEMBER PRIVATE','member','{member}'),
          ('{shared}','{account}','Shared commons','org',NULL),
          ('{team}','{account}','Team control','team',NULL),
          ('{archived}','{account}','ARCHIVED PRIVATE','member','{member}');
        UPDATE graphs SET archived_at=now() WHERE id='{archived}';
        INSERT INTO teams(id,account_id,graph_id,name,created_by) VALUES('{team}','{account}','{team}','Team fixture','{creator}');
        INSERT INTO team_members(team_id,user_id,role) VALUES('{team}','{member}','member');
        INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin)
          SELECT id,'note','synthetic:private-probe',id::text,jsonb_build_object('service_id',CASE WHEN id='{private_member}' THEN 'PRIVATE_MEMBER_ONLY' WHEN id='{private_creator}' THEN 'PRIVATE_CREATOR_ONLY' ELSE 'SHARED_ALLOWED' END)
          FROM graphs;
        INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline)
          SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,'BODY: '||(SELECT name FROM graphs WHERE id=graph_id) FROM source_items;
        UPDATE source_items s SET current_version_id=v.version_id FROM source_versions v WHERE v.source_item_id=s.source_item_id AND v.graph_id=s.graph_id;
    """)
    role = json.loads(run(db, "SELECT jsonb_build_array(current_user,(SELECT rolsuper FROM pg_roles WHERE rolname=current_user),(SELECT rolbypassrls FROM pg_roles WHERE rolname=current_user),(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user));", app=True))
    assert role == ['coralswarm_app', False, False, 0], role
    functions = run(db, "SELECT count(*) FROM pg_proc WHERE proname IN('resolve_graph_reachable_by_user','resolve_reachable_graphs') AND prosecdef AND proconfig @> ARRAY['search_path=public, pg_temp'];", app=True)
    assert functions == '2'
    for actor, expected in [(creator, {private_creator, shared}), (member, {private_member, shared, team}), (outsider, set())]:
        visible = json.loads(run(db, f"SELECT COALESCE(jsonb_agg(id),'[]') FROM resolve_reachable_graphs('{actor}');", app=True))
        assert set(visible) == expected, (actor, visible, expected)
        for graph in [private_creator, private_member, shared, team, archived]:
            reachable = run(db, f"SELECT resolve_graph_reachable_by_user('{graph}','{actor}');", app=True)
            assert reachable == ('t' if graph in expected else 'f')
        value = json.loads(run(db, f"""BEGIN;
          SET LOCAL app.account_id='{account}'; SET LOCAL app.user_id='{actor}';
          SELECT jsonb_build_object('count',count(*),'facets',COALESCE(jsonb_agg(s.origin->>'service_id'),'[]'),
            'bodies',COALESCE(jsonb_agg(v.raw_inline),'[]'))
          FROM source_items s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.version_id=s.current_version_id
          WHERE s.access_state='permitted' AND s.graph_id IN(SELECT id FROM resolve_reachable_graphs('{actor}'));
          COMMIT;""", app=True))
        assert value['count'] == len(expected)
        hidden = 'MEMBER' if actor == creator else 'CREATOR'
        assert f'PRIVATE_{hidden}_ONLY' not in value['facets']
        assert all(f'{hidden} PRIVATE' not in body for body in value['bodies'])
        assert all('ARCHIVED' not in body for body in value['bodies'])
    # Account-wide RLS alone intentionally does not confer end-user admission.
    rows = run(db, f"BEGIN; SET LOCAL app.account_id='{account}'; SET LOCAL app.user_id='{creator}'; SELECT count(*) FROM source_items WHERE graph_id='{private_member}'; COMMIT;", app=True)
    assert rows == '1'
    run(db, f"UPDATE team_members SET departing_at=now() WHERE team_id='{team}' AND user_id='{member}';")
    assert run(db, f"SELECT resolve_graph_reachable_by_user('{team}','{member}');", app=True) == 'f'
    print('PASS: final control tree; existing actual NOBYPASS/nonowner app login; representable multi-member consumer account; private owner/admin/creator isolation; shared/team/archived controls; admitted counts/facets/body; account-wide RLS distinction')
finally:
    run('postgres', f'DROP DATABASE {db} WITH (FORCE);')

#!/usr/bin/env python3
"""Populated additive-migration/backfill proof under real app roles.

Run with ORGT_TEST_BASE_URL pointing ONLY at a disposable local base database.
Creates/drops library_p4_sql_control and library_p4_sql_tenant. This SQL-layer
probe complements the actual-login Rust HTTP-handler regression fixture. It
prepares the exact module query strings rather than maintaining a second copy.
No production/model calls or application credentials are used.
"""
from pathlib import Path
import re,subprocess,json
import os
from urllib.parse import urlparse
root=Path(__file__).resolve().parents[4]
configured=os.environ.get('ORGT_TEST_BASE_URL','postgres://alex@localhost:5432/postgres')
parsed=urlparse(configured)
assert parsed.hostname in ('localhost','127.0.0.1','::1'), 'disposable local PostgreSQL only'
assert parsed.query == '', 'fixture base URL must have no query parameters'
base=configured.rsplit('/',1)[0]+'/'
def scope_cte():
 return (const("SCOPE_CTE").replace("__OUTCOME_VERSION_MODE__","true")
  .replace("__SELECTED_EXTRACTION_STATUS__",const("EXTRACTION_STATUS_SQL").replace("__VERSION__","v"))
  .replace("__CURRENT_EXTRACTION_STATUS__",const("EXTRACTION_STATUS_SQL").replace("__VERSION__","current_v")))
def run(db,sql=None,file=None,transaction=False):
 cmd=['psql',base+db,'-X','-v','ON_ERROR_STOP=1','-q','-At']
 if file:cmd+=['-1'] if transaction else [];cmd+=['-f',str(file)]
 p=subprocess.run(cmd,input=sql,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if p.returncode:raise RuntimeError(p.stdout)
 return p.stdout
s=(root/'backend/src/library_read.rs').read_text();const=lambda n:re.search(r'(?:pub\(crate\) )?const '+n+r':\s*&str\s*=\s*r#"(.*?)"#;',s,re.S).group(1)
search=scope_cte()+const('SEARCH_SQL').replace('__NODE_PROJECTION__',const('NODE_PROJECTION')).replace('__OUTCOME_PROJECTION__',const('OUTCOME_PROJECTION'))
neighbor=re.search(r'let sql\s*=\s*format!\(r#"\{(?:SCOPE_CTE|cte)\}(.*?)"#\);',s,re.S).group(1).replace('{{','{').replace('}}','}')
for plane in ['control','tenant']:
 db='library_p4_sql_'+plane
 run('postgres',f'DROP DATABASE IF EXISTS {db} WITH (FORCE); CREATE DATABASE {db};')
 run(db,'CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY,description TEXT NOT NULL,installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),success BOOLEAN NOT NULL,checksum BYTEA NOT NULL,execution_time BIGINT NOT NULL);')
 files=sorted((root/f'backend/migrations_{plane}').glob('*.sql'))
 migration=next(f for f in files if 'library_read_boundary' in f.name)
 for f in files:
  if f == migration:break
  run(db,file=f,transaction=not f.read_text().startswith('-- no-transaction'))
 fixture=root/'backend/tests/fixtures/connected_memory'
 wrapper=(fixture/f'source_delete_{"consumer" if plane=="control" else "tenant"}.sql').read_text().split('\\ir')[0]
 run(db,wrapper)
 run(db,(fixture/'source_delete_core.sql').read_text().split('BEGIN;')[0])
 run(db,"""INSERT INTO nodes(node_id,graph_id,type,title) VALUES('00000000-0000-0000-0000-000000000901','00000000-0000-0000-0000-000000000101','entity','Atlas');
 INSERT INTO nodes(node_id,graph_id,type,title) VALUES('00000000-0000-0000-0000-000000000902','00000000-0000-0000-0000-000000000101','entity','synthetic');
 INSERT INTO node_atoms(node_id,atom_id) VALUES('00000000-0000-0000-0000-000000000902','00000000-0000-0000-0000-000000000701');
 UPDATE semantic_atoms SET entities='[{"mention":"Atlas","type":"project"}]' WHERE semantic_atom_id='00000000-0000-0000-0000-000000000801';
 INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000901','00000000-0000-0000-0000-000000000801');
 INSERT INTO node_atoms(node_id,atom_id) VALUES('00000000-0000-0000-0000-000000000901','00000000-0000-0000-0000-000000000701');
 UPDATE source_items SET origin='{"service_id":"slack","transport":"native","content_kind":"message"}' WHERE source_item_id='00000000-0000-0000-0000-000000000201';
 INSERT INTO relationship_edge(graph_id,src_node_id,dst_node_id,predicate,status,provenance) VALUES('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000901','00000000-0000-0000-0000-000000000902','supported','confirmed','{"atom_ids":["00000000-0000-0000-0000-000000000701"]}');""")
 run(db,file=migration,transaction=True)
 role='coralswarm_app' if plane=='control' else 'coralswarm_tenant_app'
 prefix=f"SET ROLE {role}; BEGIN; SET LOCAL app.account_id='00000000-0000-0000-0000-000000000001'; SET LOCAL app.user_id='00000000-0000-0000-0000-000000000011';"
 sql=prefix+f'PREPARE library_search(uuid[],jsonb,uuid,uuid,uuid,text,text,timestamptz,uuid,timestamptz,uuid,bigint,bigint,uuid,bigint) AS {search};'
 sql+="EXECUTE library_search(ARRAY['00000000-0000-0000-0000-000000000101'::uuid],'{}',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,21);"
 sql+=f'PREPARE library_neighbors(uuid[],jsonb,uuid,uuid,uuid,text,uuid,bigint) AS {scope_cte()+neighbor};'
 sql+="EXECUTE library_neighbors(ARRAY['00000000-0000-0000-0000-000000000101'::uuid],'{}',NULL,NULL,'00000000-0000-0000-0000-000000000901',NULL,NULL,21); ROLLBACK;"
 lines=run(db,sql).splitlines();payload=json.loads(lines[0]);assert payload['sources']['total']==1 and payload['outcomes']['total']==1 and payload['nodes']['total']==2;assert any(n['title']=='Atlas' for n in payload['nodes']['items']);assert len(payload['prominent_work'])==1;assert json.loads(lines[1])['support'][0]['source_item_id'].endswith('201')
 sql=prefix+"""DO $$ BEGIN
 IF (SELECT rolbypassrls OR rolsuper FROM pg_roles WHERE rolname=current_user) THEN RAISE EXCEPTION 'actual low privilege role required'; END IF;
 IF NOT (SELECT source_evidence_bound FROM atoms WHERE atom_id='00000000-0000-0000-0000-000000000701') THEN RAISE EXCEPTION 'backfill lost historical chunk binding'; END IF;
 UPDATE atoms SET source_evidence_bound=false WHERE atom_id='00000000-0000-0000-0000-000000000701';
 IF NOT (SELECT source_evidence_bound FROM atoms WHERE atom_id='00000000-0000-0000-0000-000000000701') THEN RAISE EXCEPTION 'binding marker not monotonic'; END IF;
 UPDATE source_items SET access_state='unknown' WHERE source_item_id='00000000-0000-0000-0000-000000000201';
 IF library_atom_permitted('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000701') THEN RAISE EXCEPTION 'unknown legacy leaked'; END IF;
 IF library_node_permitted('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000901') THEN RAISE EXCEPTION 'unknown node leaked'; END IF;
 DELETE FROM source_items WHERE source_item_id='00000000-0000-0000-0000-000000000201';
 IF library_atom_permitted('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000701') THEN RAISE EXCEPTION 'deleted chunk resurrected as unmapped'; END IF;
 END $$; COMMIT;"""
 run(db,sql)
 run('postgres',f'DROP DATABASE {db} WITH (FORCE);')
 print(plane+': populated query + actual role + existing mapping backfill + monotonic marker + unknown denial + direct FK deletion guard PASS',flush=True)

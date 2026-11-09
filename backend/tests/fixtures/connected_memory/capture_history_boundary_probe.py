#!/usr/bin/env python3
"""Prospective capture-context migrations and invoker masks on disposable planes."""
from pathlib import Path
from urllib.parse import urlparse
import os,subprocess
root=Path(__file__).resolve().parents[4]
base=os.environ.get('ORGT_TEST_BASE_URL','postgres://alex@localhost:5432/postgres')
assert urlparse(base).hostname in ('localhost','127.0.0.1','::1')
base=base.rsplit('/',1)[0]+'/'
def run(db,sql=None,file=None):
 args=['psql',base+db,'-X','-v','ON_ERROR_STOP=1','-q','-At']
 if file:args+=['-1','-f',str(file)]
 p=subprocess.run(args,input=sql,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if p.returncode:raise RuntimeError(p.stdout)
 return p.stdout
for plane,role in [('control','coralswarm_app'),('tenant','coralswarm_tenant_app')]:
 db='library_capture_sql_'+plane
 run('postgres',f'DROP DATABASE IF EXISTS {db} WITH(FORCE);CREATE DATABASE {db};')
 run(db,'CREATE TABLE _sqlx_migrations(version BIGINT PRIMARY KEY,description TEXT NOT NULL,installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),success BOOLEAN NOT NULL,checksum BYTEA NOT NULL,execution_time BIGINT NOT NULL);')
 for f in sorted((root/f'backend/migrations_{plane}').glob('*.sql')):
  if f.read_text().startswith('-- no-transaction'):
   p=subprocess.run(['psql',base+db,'-X','-v','ON_ERROR_STOP=1','-q','-f',str(f)],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
   if p.returncode:raise RuntimeError(p.stdout)
  else:run(db,file=f)
 fixture=root/'backend/tests/fixtures/connected_memory'
 wrapper=(fixture/f'source_delete_{"consumer" if plane=="control" else "tenant"}.sql').read_text().split('\\ir')[0]
 run(db,wrapper)
 run(db,(fixture/'source_delete_core.sql').read_text().split('BEGIN;')[0])
 run(db,"UPDATE source_versions SET source_metadata='{\"session_id\":\"root\",\"session_kind\":\"agent\",\"session_title\":\"Historical reported title\",\"branch\":\"recorded-main\",\"model\":\"uncontracted model\"}' WHERE version_id='00000000-0000-0000-0000-000000000501';")
 prefix=f"SET ROLE {role};BEGIN;SET LOCAL app.account_id='00000000-0000-0000-0000-000000000001';SET LOCAL app.user_id='00000000-0000-0000-0000-000000000011';"
 run(db,prefix+"""
 INSERT INTO coding_sessions(graph_id,session_id,session_kind,title,model,branch,task,root_session_id) VALUES
 ('00000000-0000-0000-0000-000000000101','root','agent','PRIVATE MUTABLE TITLE','PRIVATE MUTABLE MODEL','PRIVATE MUTABLE BRANCH','PRIVATE MUTABLE TASK','root');
 DO $$ DECLARE c JSONB; BEGIN
 IF (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user) THEN RAISE EXCEPTION 'actual app role required'; END IF;
 c:=library_capture_context('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000501');
 IF c->>'session_title'<>'Historical reported title' OR c->>'branch'<>'recorded-main' OR c->>'model' IS NOT NULL OR c->>'task' IS NOT NULL OR c->>'root_session_id' IS NOT NULL THEN RAISE EXCEPTION 'historical context fabricated from mutable session row'; END IF;
 IF (SELECT title FROM library_sessions('00000000-0000-0000-0000-000000000101') WHERE session_id='root')<>'Historical reported title' THEN RAISE EXCEPTION 'legacy descriptor mask failed'; END IF;
 IF library_capture_context('00000000-0000-0000-0000-000000000102','00000000-0000-0000-0000-000000000502') IS NOT NULL THEN RAISE EXCEPTION 'cross-graph context leaked'; END IF;
 BEGIN
 UPDATE source_versions SET capture_context='{"revision":1}',source_metadata=source_metadata||'{"capture_context":{"revision":1}}' WHERE version_id='00000000-0000-0000-0000-000000000501';
 RAISE EXCEPTION 'retrospective snapshot accepted';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='retrospective snapshot accepted' THEN RAISE; END IF; END;
 UPDATE source_items SET access_state='unknown' WHERE source_item_id='00000000-0000-0000-0000-000000000201';
 IF EXISTS(SELECT 1 FROM library_sessions('00000000-0000-0000-0000-000000000101')) THEN RAISE EXCEPTION 'unknown session descriptor remained visible'; END IF;
 IF library_session_supported('00000000-0000-0000-0000-000000000101','fixture:delete','root') THEN RAISE EXCEPTION 'unknown lineage remained supported'; END IF;
 END $$;COMMIT;
 """)
 run(db,prefix+"""
 INSERT INTO coding_sessions(graph_id,session_id,session_kind,parent_session_id,root_session_id) VALUES
 ('00000000-0000-0000-0000-000000000101','amb','agent',NULL,'amb'),
 ('00000000-0000-0000-0000-000000000101','amb:detail','agent',NULL,'amb:detail'),
 ('00000000-0000-0000-0000-000000000101','amb-child','agent','amb','amb');
 INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id) VALUES
 ('00000000-0000-0000-0000-000000000361','00000000-0000-0000-0000-000000000101','agent_session','fixture:ambiguous','amb-legacy'),
 ('00000000-0000-0000-0000-000000000362','00000000-0000-0000-0000-000000000101','agent_session','fixture:ambiguous','amb-child');
 INSERT INTO source_versions(version_id,source_item_id,graph_id,source_type,provider_namespace,source_id,raw_inline,source_metadata,capture_context) VALUES
 ('00000000-0000-0000-0000-000000000661','00000000-0000-0000-0000-000000000361','00000000-0000-0000-0000-000000000101','agent_session','fixture:ambiguous','amb-legacy','synthetic ambiguous evidence','{}',NULL),
 ('00000000-0000-0000-0000-000000000662','00000000-0000-0000-0000-000000000362','00000000-0000-0000-0000-000000000101','agent_session','fixture:ambiguous','amb-child','synthetic child evidence','{"capture_context":{"revision":1,"session_id":"amb-child","session_kind":"agent"}}','{"revision":1,"session_id":"amb-child","session_kind":"agent","parent_session_id":"amb","root_session_id":"amb"}');
 INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES
 ('00000000-0000-0000-0000-000000000961','00000000-0000-0000-0000-000000000101','agent_session','cc:amb:detail:0','fixture','synthetic ambiguous evidence',now());
 INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES
 ('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000661','00000000-0000-0000-0000-000000000961');
 DO $$ DECLARE c JSONB; BEGIN
 IF (SELECT session_id FROM library_session_versions('00000000-0000-0000-0000-000000000101') WHERE version_id='00000000-0000-0000-0000-000000000661') IS NOT NULL THEN RAISE EXCEPTION 'ambiguous legacy attribution became a session'; END IF;
 IF library_session_supported('00000000-0000-0000-0000-000000000101','fixture:ambiguous','amb') THEN RAISE EXCEPTION 'ambiguous legacy prefix incorrectly certifies lineage'; END IF;
 c:=library_capture_context('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000662');
 IF c->>'parent_session_id' IS NOT NULL OR c->>'root_session_id' IS NOT NULL OR c->>'lineage_status'<>'partial' THEN RAISE EXCEPTION 'unsupported ambiguous lineage identifiers leaked'; END IF;
 END $$;
 INSERT INTO coding_sessions(graph_id,session_id,session_kind,root_session_id) VALUES
 ('00000000-0000-0000-0000-000000000101','unique-root','agent','unique-root');
 INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id) VALUES
 ('00000000-0000-0000-0000-000000000363','00000000-0000-0000-0000-000000000101','agent_session','fixture:ambiguous','unique-legacy'),
 ('00000000-0000-0000-0000-000000000364','00000000-0000-0000-0000-000000000101','agent_session','fixture:other','amb-proof'),
 ('00000000-0000-0000-0000-000000000365','00000000-0000-0000-0000-000000000101','agent_session','fixture:ambiguous','amb-proof');
 INSERT INTO source_versions(version_id,source_item_id,graph_id,source_type,provider_namespace,source_id,raw_inline,source_metadata,capture_context) VALUES
 ('00000000-0000-0000-0000-000000000663','00000000-0000-0000-0000-000000000363','00000000-0000-0000-0000-000000000101','agent_session','fixture:ambiguous','unique-legacy','synthetic unique evidence','{}',NULL),
 ('00000000-0000-0000-0000-000000000664','00000000-0000-0000-0000-000000000364','00000000-0000-0000-0000-000000000101','agent_session','fixture:other','amb-proof','synthetic other namespace root','{"capture_context":{"revision":1,"session_id":"amb","session_kind":"agent"}}','{"revision":1,"session_id":"amb","session_kind":"agent","root_session_id":"amb"}');
 INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES
 ('00000000-0000-0000-0000-000000000963','00000000-0000-0000-0000-000000000101','agent_session','cc:unique-root:0','fixture','synthetic unique evidence',now());
 INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES
 ('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000663','00000000-0000-0000-0000-000000000963');
 DO $$ DECLARE c JSONB; BEGIN
 IF NOT library_session_supported('00000000-0000-0000-0000-000000000101','fixture:ambiguous','unique-root') THEN RAISE EXCEPTION 'uniquely mapped legacy support unavailable'; END IF;
 c:=library_capture_context('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000662');
 IF c->>'parent_session_id' IS NOT NULL OR c->>'root_session_id' IS NOT NULL THEN RAISE EXCEPTION 'another namespace certified lineage'; END IF;
 END $$;
 INSERT INTO source_versions(version_id,source_item_id,graph_id,source_type,provider_namespace,source_id,raw_inline,source_metadata,capture_context) VALUES
 ('00000000-0000-0000-0000-000000000665','00000000-0000-0000-0000-000000000365','00000000-0000-0000-0000-000000000101','agent_session','fixture:ambiguous','amb-proof','synthetic same namespace root','{"capture_context":{"revision":1,"session_id":"amb","session_kind":"agent"}}','{"revision":1,"session_id":"amb","session_kind":"agent","root_session_id":"amb"}');
 DO $$ DECLARE c JSONB; BEGIN
 c:=library_capture_context('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000662');
 IF c->>'parent_session_id'<>'amb' OR c->>'root_session_id'<>'amb' OR c->>'lineage_status'<>'captured' THEN RAISE EXCEPTION 'exact captured support unavailable'; END IF;
 END $$;
 UPDATE source_items SET access_state='restricted' WHERE source_item_id='00000000-0000-0000-0000-000000000365';
 DO $$ DECLARE c JSONB; BEGIN
 c:=library_capture_context('00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-000000000662');
 IF c->>'parent_session_id' IS NOT NULL OR c->>'root_session_id' IS NOT NULL OR c->>'lineage_status'<>'partial' THEN RAISE EXCEPTION 'revoked canonical support survived ambiguity'; END IF;
 END $$;COMMIT;
 """)
 run('postgres',f'DROP DATABASE {db} WITH(FORCE);')
 print(plane+': immutable prospective context + historical unknown + source descriptor mask + unknown/cross-graph denial PASS',flush=True)

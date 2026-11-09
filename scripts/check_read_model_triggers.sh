#!/usr/bin/env bash
# Database-free read-model trigger coverage and request/compute boundary guard.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - "$@" <<'PY'
import re,sys
from pathlib import Path

def check(model,tenant,control,page,store,worker):
    tables=set(re.findall(r'DirtyTrigger::Table\("([a-z_]+)"\)',model))
    for table in tables|{'atoms','source_items','source_versions','source_version_chunks','team_members','team_seat_grants','graphs'}:
        for event in ['INSERT','UPDATE','DELETE']:
            pattern=rf'CREATE TRIGGER \w+ AFTER {event} ON {table}\s+REFERENCING .*?FOR EACH STATEMENT EXECUTE FUNCTION read_models_mark_dirty\(\)'
            for plane,sql in [('tenant',tenant),('control',control)]:
                assert re.search(pattern,sql),f'{plane}: missing statement trigger {table}/{event}'
    assert 'rm::store::read::<M>' in page and 'rm::store::enqueue::<M>' in page
    assert not re.search(r'(?:compute_json|M::compute|graph_workspace_counts_compute|spawn_workspace_counts_refresh|begin_tenant_system_scoped)\s*\(',page),'request compute/system bypass'
    assert 'REPEATABLE READ READ ONLY' in store,'compute snapshot must be read only/repeatable'
    assert 'claim_token=$7' in store and 'dirty_generation=$6' in store,'publish must fence claim and preserve bumps'
    assert 'model.verify_scope' in worker,'worker must reverify actor/class'
    assert 'live_org_membership' in worker,'worker must reverify current control-plane org membership'
    assert 'LEAST(dirty_bumped_at+' in store,'due is debounce OR max-delay'
    for sql in [tenant,control]:
        assert 'CREATE TABLE read_model_graph_fences' in sql and 'CREATE TABLE read_model_user_fences' in sql,'scope-independent fences missing'
        # Check the effective definition: a later copied function must not silently
        # replace the first-fill fence with the older row-only implementation.
        dirty=re.findall(r'CREATE(?: OR REPLACE)? FUNCTION read_models_mark_dirty\(\).*?END \$\$;',sql,re.S)[-1]
        cert=re.findall(r'CREATE(?: OR REPLACE)? FUNCTION read_model_fence_state\(.*?END \$\$;',sql,re.S)[-1]
        assert dirty.index('INSERT INTO public.read_model_graph_fences') < dirty.index('UPDATE public.read_models r SET'),'effective fence must precede cached-row update'
        assert 'f.changed_xid=pg_current_xact_id()' in dirty and 'NOT reducing OR f.invalidated_xid=pg_current_xact_id()' in dirty,'effective trigger lost XID coalescing/upgrade'
        assert 'n.graph_id,n.timestamp' in dirty and 'o.graph_id,o.timestamp' in dirty,'effective trigger lost history timestamp reduction'
        assert 'public.read_model_graph_fences' in cert and 'public.read_model_user_fences' in cert and 'pg_visible_in_snapshot' in cert,'effective certifier lost durable fences'
        assert "current_setting('app.tenant_actor',true)" in cert,'effective certifier lost caller guard'
        if 'ALTER TABLE read_model_user_fences ADD COLUMN changed_xid' in sql:
            assert 'INSERT INTO public.read_model_user_fences' in dirty and "'*'" in dirty,'effective trigger lost graph-independent owner fences'
            assert re.search(r"f.model IN\s*\(m,'\*'\)",cert),'effective certifier lost owner wildcard'
            reductions=dirty[dirty.index('reducing :='):dirty.index('-- Serialize each graph fence')]
            assert "'node_atoms','node_semantic_atoms'" in reductions,'effective trigger lost link-support reductions'
            assert 'n.content_inline,n.participants' in reductions,'effective trigger lost text/participant support reductions'
            assert 'n.graph_id,n.title,n.type' in reductions,'effective trigger lost node reassignment reductions'
            prose=dirty[dirty.index('IF clear_prose'):]
            assert "'node_atoms','node_semantic_atoms'" in prose,'link-support loss must retract compiled prose'
        assert 'read_model_fence_state(uuid,uuid[],text,uuid,pg_snapshot)' in sql,'fence oracle must have explicit execute grants/revokes'
        assert "current_setting('app.tenant_actor',true) IS DISTINCT FROM 'system'" in sql,'event invalidation requires system scope'
    assert store.count('read_model_fence_state(') >= 3,'read and publication must certify current fences'
    return tables
if '--self-test' in sys.argv:
    root=Path('.')
    args=['\n'.join(p.read_text() for p in root.glob('backend/src/read_models/*.rs')),*['\n'.join(f.read_text() for f in sorted(root.glob(f'backend/migrations_{p}/*.sql'))) for p in ['tenant','control']],Path('backend/src/routes/page_read.rs').read_text(),Path('backend/src/read_models/store.rs').read_text(),Path('backend/src/workers/read_model_refresh.rs').read_text()]
    check(*args)
    for i,bad in [(1,args[1].replace('AFTER INSERT ON atoms','AFTER INSERT ON omitted')),(3,args[3]+'\n M::compute('),(4,args[4].replace('READ ONLY','READ WRITE')),(5,args[5].replace('model.verify_scope','unchecked')),(1,args[1].replace('CREATE TABLE read_model_graph_fences','CREATE TABLE missing_graph_fences')),(4,args[4].replace('read_model_fence_state(', 'uncertified(')),(1,args[1]+'\nCREATE OR REPLACE FUNCTION read_models_mark_dirty() RETURNS trigger AS $$ BEGIN UPDATE public.read_models r SET dirty_at=now(); RETURN NULL; END $$;'),(2,args[2]+'\nCREATE OR REPLACE FUNCTION read_model_fence_state(a uuid, gs uuid[],m text,u uuid,s pg_snapshot) RETURNS int AS $$ BEGIN RETURN 2; END $$;'),(1,args[1]+'\n'+re.findall(r'CREATE(?: OR REPLACE)? FUNCTION read_models_mark_dirty\(\).*?END \$\$;',args[1],re.S)[0])]:
        altered=args.copy();altered[i]=bad
        try:check(*altered)
        except (AssertionError,ValueError):continue
        raise AssertionError(f'guard accepted invalid fixture {i}')
    for plane in [1,2]:
        for before,after in [("'node_atoms','node_semantic_atoms','reef_identity_corrections'", "'reef_identity_corrections'"),('n.content_inline,n.participants','n.content_revision'),('n.graph_id,n.title,n.type','n.updated_at')]:
            altered=args.copy();altered[plane]=args[plane].replace(before,after)
            try:check(*altered)
            except (AssertionError,ValueError):continue
            raise AssertionError(f'guard accepted support-loss fixture {plane}: {before}')
    print('read-model guard self-test: 15 invalid fixtures rejected')
else:
    tables=check('\n'.join(p.read_text() for p in Path('backend/src/read_models').glob('*.rs')),*['\n'.join(f.read_text() for f in sorted(Path(f'backend/migrations_{p}').glob('*.sql'))) for p in ['tenant','control']],Path('backend/src/routes/page_read.rs').read_text(),Path('backend/src/read_models/store.rs').read_text(),Path('backend/src/workers/read_model_refresh.rs').read_text())
    assert 'spawn_workspace_counts_refresh' not in Path('backend/src/store/mod.rs').read_text()
    print(f'read-model guard: {len(tables)} subscriptions covered on both planes; request/compute boundary intact')
PY

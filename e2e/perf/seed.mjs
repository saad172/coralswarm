// One authoritative SQL fixture, shared with the existing PG benchmarks.
// Owner SQL is fixture setup only; timed reads/worker compute use the stack's
// actual nonowner serving roles and normal HTTP/auth/RLS/fence path.
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { CONTAINERS, REMOTE, STACK_ID } from '../stack/config.mjs';

export const SEED_URL = new URL('../../backend/tests/fixtures/page_models_600k.sql', import.meta.url);
export const BULK_TABLES = ['atoms', 'nodes', 'node_atoms', 'semantic_atoms', 'node_semantic_atoms', 'source_items', 'source_versions'];
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export function seedVariables(ids) {
  const keys = ['acct', 'u1', 'u2', 'g_org', 'g_team', 'g_mem'];
  if (Object.keys(ids).sort().join() !== [...keys].sort().join()) throw new Error('seed requires exactly six canonical identity variables');
  for (const key of keys) if (!UUID.test(ids[key])) throw new Error(`invalid seed UUID: ${key}`);
  if (ids.u1 === ids.u2 || new Set([ids.g_org, ids.g_team, ids.g_mem]).size !== 3) throw new Error('seed users/graphs must be distinct');
  return keys.flatMap((key) => ['-v', `${key}='${ids[key]}'`]);
}
export function loadCanonicalSeed(db, ids) {
  if (REMOTE || !process.env.E2E_STACK_ID || !/^(pageloadl6|page-load-speed-)/.test(STACK_ID)) {
    throw new Error('600k seed requires an explicitly owned local page-load stack');
  }
  if (!/^coralswarm_tenant_[a-z0-9_]+$/.test(db)) throw new Error('seed requires the API-provisioned local tenant database');
  const variables = seedVariables(ids);
  const sql = readFileSync(SEED_URL, 'utf8');
  if (/^\\set\s+(acct|u1|u2|g_org|g_team|g_mem)\b/m.test(sql)) throw new Error('canonical SQL must not override caller identities');
  const psql = (query, args = []) => execFileSync('docker', ['exec', '-i', CONTAINERS.pg, 'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db, ...args],
    { input: query, encoding: 'utf8', timeout: 600_000, maxBuffer: 8 * 1024 * 1024 });
  const triggers = `SELECT t.tgname,c.relname,t.tgenabled FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal AND c.relname IN (${BULK_TABLES.map((t) => `'${t}'`).join(',')}) ORDER BY c.relname,t.tgname`;
  const before = psql(triggers).trim();
  // ENABLE TRIGGER USER cannot preserve a prior replica/always/disabled mode.
  // Reject such input instead of accidentally changing another fixture's state.
  if (!before || before.split('\n').some((line) => !line.endsWith('|O'))) throw new Error('bulk fixture requires all affected user triggers initially enabled normally');
  const identity = psql(`SELECT account_id FROM graphs WHERE id='${ids.g_mem}' AND kind='member' AND owner_user_id='${ids.u1}'; SELECT account_id FROM graphs WHERE id='${ids.g_org}' AND kind='org'`)
    .trim().split('\n');
  if (identity.length !== 2 || identity.some((account) => account !== ids.acct)) throw new Error('API graph/account/actor binding mismatch');
  if (psql(`SELECT count(*) FROM graphs WHERE id='${ids.g_team}'`).trim() !== '0') throw new Error('synthetic team graph must be a fresh owned identity');
  const baseline = JSON.parse(psql("SELECT json_build_object('atoms',(SELECT count(*) FROM atoms),'nodes',(SELECT count(*) FROM nodes))"));
  // L5's shared representative fixture supplies a small real capture/topic for
  // routes that require an address. Disclose these separately from the exact
  // canonical synthetic population; never count them as synthetic scale rows.
  if (baseline.atoms > 20 || baseline.nodes > 20) throw new Error('expected a fresh representative-only fixture before bulk load');
  let loadError;
  try { psql(sql, variables); }
  catch (error) { loadError = new Error(`canonical bulk seed failed (${error.status ?? 'process error'}); triggers restored before reporting`); }
  finally {
    psql(BULK_TABLES.map((table) => `ALTER TABLE ${table} ENABLE TRIGGER USER;`).join('\n'));
    if (psql(triggers).trim() !== before) throw new Error('bulk fixture user trigger restoration mismatch');
  }
  if (loadError) throw loadError;
  // Separate statements: VACUUM cannot execute inside a transaction block.
  for (const table of [...BULK_TABLES, 'source_version_chunks', 'query_history', 'coding_sessions']) psql(`VACUUM ANALYZE ${table};`);
  const counts = JSON.parse(psql(`SELECT json_build_object('atoms',(SELECT count(*) FROM atoms),'nodes',(SELECT count(*) FROM nodes),'links',(SELECT count(*) FROM node_atoms),'disabled_user_triggers',(SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='D'))`));
  if (counts.atoms - baseline.atoms !== 600_000 || counts.nodes - baseline.nodes !== 60_000 || counts.disabled_user_triggers !== 0) throw new Error(`canonical fixture count/trigger violation: ${JSON.stringify(counts)}`);
  const fingerprints = {};
  for (const [table, key] of [['atoms','atom_id'],['nodes','node_id'],['node_atoms','node_id,atom_id'],['source_items','source_item_id'],['source_versions','version_id'],['source_version_chunks','source_version_id,atom_id'],['semantic_atoms','semantic_atom_id'],['node_semantic_atoms','node_id,semantic_atom_id']]) {
    fingerprints[table] = JSON.parse(psql(`SELECT json_build_object('count',count(*),'md5',md5(string_agg(md5(to_jsonb(t)::text),'' ORDER BY ${key}))) FROM ${table} t`));
  }
  return { canonical_sql: 'backend/tests/fixtures/page_models_600k.sql', sql_sha256: createHash('sha256').update(sql).digest('hex'),
    ids, counts, representative_baseline: baseline, synthetic_counts: { atoms: 600_000, nodes: 60_000 },
    fingerprints, triggers_restored: true, vacuum_analyze: true, setseed: 0.42,
    method: 'owner-only synthetic bulk setup; registered worker and timed HTTP use normal serving roles' };
}

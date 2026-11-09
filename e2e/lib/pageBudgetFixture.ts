import { execFileSync, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { expect, type APIRequestContext } from '@playwright/test';
import { personalUser, createReadyOrg } from './isolation';
import { CONTAINERS, CONTROL_DB, REMOTE } from '../stack/config.mjs';
import type { Api } from './api';

/** Owner SQL is confined to isolated fixture setup and explicit worker
 * barriers. All observations use the real serving role and HTTP/browser. */
export function fixtureSql(db: string, query: string): string {
  if (REMOTE) throw new Error('page budget fixtures require the isolated local stack');
  return execFileSync('docker', ['exec', '-i', CONTAINERS.pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db, '-c', query],
    { encoding: 'utf8', timeout: 15_000 }).trim().split('\n')[0];
}
export interface PageBudgetFixture {
  api: Api; email: string; userId: string; orgId: string | null;
  db: string; account: string; actor: string; graph: string; sharedGraph: string;
  node: string; atom: string; source: string; sourceItem: string;
  conversations: string[]; shareToken: string;
  values: Record<string, string>;
}
export async function seedPageBudgetFixture(request: APIRequestContext, plane: 'personal' | 'org'): Promise<PageBudgetFixture> {
  const org = plane === 'org' ? await createReadyOrg(request, 'l5-budget') : null;
  const user = org?.admin ?? await personalUser(request, 'l5-budget');
  const api = org?.adminInOrg ?? user.api;
  const me = await api.json('GET', '/v1/me');
  const graph = (await api.graphs()).find((g) => g.kind === 'member' || g.kind === 'personal')!.id;
  const db = org ? fixtureSql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${org.orgId}'`) : CONTROL_DB;
  const shared = await api.json('POST', '/v1/teams', { name: 'Budget shared workspace' });
  const source = `l5-budget-${randomUUID()}`;
  await api.seedSource(graph, 'The Orchid page budget rollout is owned by Priya. The launch is on March 14.', source, { sourceType: 'transcript' });
  const atom = fixtureSql(db, `SELECT atom_id FROM atoms WHERE graph_id='${graph}' AND source_id LIKE '${source}%' ORDER BY timestamp DESC LIMIT 1`);
  expect(atom).toMatch(/^[a-f0-9-]{36}$/);
  const sourceItem = fixtureSql(db, `SELECT source_item_id FROM source_items WHERE graph_id='${graph}' AND source_id='${source}' LIMIT 1`);
  const node = fixtureSql(db, `INSERT INTO nodes(graph_id,type,title) VALUES('${graph}','entity','Orchid rollout') RETURNING node_id`);
  fixtureSql(db, `INSERT INTO node_atoms(node_id,atom_id) VALUES('${node}','${atom}')`);
  const conversations: string[] = [];
  for (let i = 0; i < 4; i++) {
    const id = fixtureSql(db, `INSERT INTO conversations(account_id,graph_id,user_id,title) VALUES('${me.account_id}','${graph}','${me.user_id}','Budget chat ${i}') RETURNING id`);
    fixtureSql(db, `INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES('${id}',1,'user','Budget chat ${i}: When is the Orchid launch?')`);
    conversations.unshift(id);
  }
  // Legacy whole-original share endpoints intentionally refuse selected-evidence bypasses.
  const share = { token: "unavailable-legacy-share-fixture" };
  const values = { graph, sharedGraph: shared.graph_id, node, atom, source, sourceItem, conversation: conversations[3],
    shareToken: share.token, prefillToken: 'unavailable-prefill-fixture', pairCode: 'unavailable-pair-fixture', orgSlug: org?.orgId ?? 'unavailable-org-fixture' };
  return { api, email: user.email, userId: user.user.id, orgId: org?.orgId ?? null, db, account: me.account_id, actor: me.user_id,
    graph, sharedGraph: shared.graph_id, node, atom, source, sourceItem, conversations, shareToken: share.token, values };
}
export function representativePath(template: string, fixture: PageBudgetFixture): string {
  return template.replace(/\{([^}]+)\}/g, (_, name: string) => {
    const value = fixture.values[name]; if (!value) throw new Error(`Missing route representative ${name}`);
    return encodeURIComponent(value);
  });
}
export async function holdPageWorkers(db: string): Promise<() => Promise<void>> {
  const child = spawn('docker', ['exec', '-i', CONTAINERS.pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db], { stdio: ['pipe', 'pipe', 'pipe'] });
  await new Promise<void>((resolve, reject) => {
    child.stdout.on('data', (chunk) => { if (String(chunk).includes('l5-worker-barrier')) resolve(); });
    child.once('error', reject);
    child.once('exit', (code) => { if (code !== null && code !== 0) reject(new Error(`worker barrier exited ${code}`)); });
    child.stdin.write("BEGIN; SET LOCAL lock_timeout='3s'; LOCK TABLE nodes IN ACCESS EXCLUSIVE MODE; SELECT 'l5-worker-barrier';\n");
  });
  return async () => { const ended = new Promise<void>((resolve) => child.once('exit', () => resolve())); child.stdin.end('ROLLBACK;\n'); await ended; };
}

import crypto from "node:crypto";
import { selectRows } from "../lib/stack-db";
import { expect, test } from '../lib/fixtures';
import { personalUser, createReadyOrg, addMember } from '../lib/isolation';
import { API_BASE, PORTS } from '../stack/config.mjs';
import { freshPayload } from '../lib/readModels';

// Only GitHub's external transport is simulated; catalog, callback, credential
// persistence and repository authorization run through the authenticated backend.
test('GitHub successful personal install preserves setup id and lists granted repositories', async ({ request }) => {
  test.setTimeout(180000);
  const { api } = await personalUser(request, 'ghn');
  await api.graphs();
  const installationId = Math.floor(Date.now() / 1000);
  const user = `gh-${installationId}`;
  const fake = `http://127.0.0.1:${PORTS.fakes}`;
  expect((await request.post(`${fake}/__github/accounts`, { data: { user, installationId } })).status()).toBe(201);
  const start = await api.json('POST', '/v1/connectors/github/connect', { return_to: 'wizard' });
  const state = new URL(start.authorize_url).searchParams.get('state');
  expect(state).toBeTruthy();
  const consent = new URL(`${fake}/github/apps/e2e-github/installations/new`);
  consent.searchParams.set('state', state!); consent.searchParams.set('user', user);
  const grant = await request.get(consent.href, { maxRedirects: 0 });
  expect(grant.status()).toBe(302);
  expect(grant.headers().location).toContain(`${API_BASE}/v1/oauth/github/callback`);
  const callback = await request.get(grant.headers().location, { maxRedirects: 0 });
  expect(callback.status()).toBe(307);
  expect(callback.headers().location).toContain(`installation_id=${installationId}`);
  const repos = await api.json('GET', `/v1/connectors/github/repos?installation_id=${installationId}`);
  expect(JSON.stringify(repos)).toContain(`${user}/repo1`);
  expect(JSON.stringify(repos)).toContain(`${user}/repo2`);
  await api.json('PATCH', '/v1/connectors/github/sources/repos', { enabled: true });
  await api.json('PUT', '/v1/connectors/github/sources/repos/scope', { mode: 'pick', item_ids: [String(installationId * 10 + 1)] });
  expect((await freshPayload(api, '/v1/connectors/github')).sources[0].scope.count).toBe(1);
  await expect.poll(async () => (await api.json('GET', '/v1/connectors/github/captures')).items.length, { timeout: 60000 }).toBeGreaterThan(0);
  const captures = await api.json('GET', '/v1/connectors/github/captures');
  const atom = captures.items[0].atom_id;
  await expect.poll(async () => (await api.call('GET', `/v1/atoms/${atom}`)).status()).toBe(200);
  const body = await api.json('GET', `/v1/atoms/${atom}`);
  expect(body.content).toContain('acceptance evidence');
  const settled = () => expect.poll(() => selectRows(`SELECT count(*) FROM jobs j JOIN github_watched_repos r ON r.id::text=j.payload->>'repo_row_id' WHERE j.kind='GithubBackfill' AND r.installation_id=${installationId} AND j.status::text IN ('pending','running')`)[0][0], { timeout: 60000 }).toBe('0');
  await settled();
  const automatic = await (await request.get(`${fake}/__github/requests?user=${user}`)).json();
  expect(automatic.some((r: any) => r.path.endsWith('/issues/4/comments'))).toBe(true); // 100 days: automatic remains unbounded.
  for (const days of [7, 30, 90, 0]) {
    await request.delete(`${fake}/__github/requests?user=${user}`);
    await api.json('POST', '/v1/connectors/github/sources/repos/backfill', { days });
    await settled();
    const bounded = await (await request.get(`${fake}/__github/requests?user=${user}`)).json();
    const comments = bounded.filter((r: any) => /\/issues\/\d+\/comments$/.test(r.path));
    expect(comments.length, `explicit ${days} days fetched issue bodies beyond its bound`).toBe(days === 7 ? 1 : days === 30 ? 2 : days === 90 ? 3 : 0);
  }
  // ACK means enqueue; hold the pause until the real worker settles.
  async function event(number: number) {
    const delivery = crypto.randomUUID();
    const now = new Date().toISOString();
    const payload = JSON.stringify({ action: 'opened', installation: { id: installationId }, repository: { id: installationId * 10 + 1, name: 'repo1', owner: { login: user } }, issue: { number, title: `Webhook ${number}`, body: `Webhook acceptance evidence ${number}`, state: 'open', user: { login: user }, created_at: now, updated_at: now, html_url: `https://github.com/${user}/repo1/issues/${number}` } });
    const signature = 'sha256=' + crypto.createHmac('sha256', 'e2e-github-webhook').update(payload).digest('hex');
    expect((await request.post(`${API_BASE}/v1/github/events`, { data: payload, headers: { 'content-type': 'application/json', 'x-github-event': 'issues', 'x-github-delivery': delivery, 'x-hub-signature-256': signature } })).ok()).toBe(true);
    await expect.poll(() => selectRows(`SELECT status::text FROM jobs WHERE kind='GithubEventIngest' AND payload->>'delivery_id'='${delivery}'`).map(r => r[0]), { timeout: 30000 }).toEqual(['done']);
  }
  await api.json('PATCH', '/v1/connectors/github/sources/repos', { paused: true });
  await request.delete(`${fake}/__github/requests?user=${user}`);
  await api.json('PUT', '/v1/connectors/github/repos', { installation_id: installationId, selections: [{ repo_id: installationId * 10 + 2, active: true, destination: { kind: 'personal' } }] });
  await settled();
  const pausedWork = await (await request.get(`${fake}/__github/requests?user=${user}`)).json();
  expect(pausedWork.some((r: any) => r.path.endsWith('/repo2/issues'))).toBe(false);
  await event(901);
  const pausedCaptures = await api.json('GET', '/v1/connectors/github/captures');
  expect(JSON.stringify(pausedCaptures)).not.toContain('Webhook 901');
  await api.json('PATCH', '/v1/connectors/github/sources/repos', { paused: false });
  await expect.poll(async () => {
    const calls = await (await request.get(`${fake}/__github/requests?user=${user}`)).json();
    return calls.some((r: any) => r.path.endsWith('/repo2/issues'));
  }, { timeout: 15000 }).toBe(true);
  await event(902);
  await expect.poll(async () => JSON.stringify(await api.json('GET', '/v1/connectors/github/captures'))).toContain('Webhook 902');
  const outsider = await personalUser(request, 'gh-out');
  await outsider.api.graphs();
  expect((await outsider.api.call('GET', `/v1/connectors/github/repos?installation_id=${installationId}`)).status()).toBe(404);
  expect((await outsider.api.call('GET', `/v1/atoms/${atom}`)).status()).toBe(404);
  expect(automatic.some((r: any) => r.path.endsWith('/access_tokens'))).toBe(true);
  expect(automatic.some((r: any) => r.path.endsWith('/login/oauth/access_token'))).toBe(true);
  await api.json('DELETE', '/v1/connectors/github');
  expect((await api.json('GET', `/v1/atoms/${atom}`)).content).toBe(body.content);
  await event(903);
  expect(JSON.stringify(await api.json('GET', '/v1/connectors/github/captures'))).not.toContain('Webhook 903');
  const replay = await request.get(grant.headers().location, { maxRedirects: 0 });
  expect(replay.headers().location).not.toContain(`installation_id=${installationId}`);
  const reconnect = await api.json('POST', '/v1/connectors/github/connect', { return_to: 'wizard' });
  consent.searchParams.set('state', new URL(reconnect.authorize_url).searchParams.get('state')!);
  const reconnectGrant = await request.get(consent.href, { maxRedirects: 0 });
  const reconnected = await request.get(reconnectGrant.headers().location, { maxRedirects: 0 });
  expect(reconnected.headers().location).toContain(`installation_id=${installationId}`);
  expect((await api.json('GET', '/v1/connectors/github/installs')).installs).toHaveLength(1);
  expect((await api.call('DELETE', `/v1/connectors/github/installs/${installationId}`)).ok()).toBe(true);
  const upstream = await (await request.get(`${fake}/__github/requests?user=${user}`)).json();
  expect(upstream.some((r: any) => r.method === 'DELETE' && r.path.endsWith(`/app/installations/${installationId}`))).toBe(false); // Local disconnect preserves the upstream App installation.
  expect((await api.json('GET', `/v1/atoms/${atom}`)).content).toBe(body.content);

});

test('GitHub browser installation, Everything and None complete setup with retained worker capture', async ({ page, signedIn, request }) => {
  test.setTimeout(120000);
  const api = signedIn.api;
  const installationId = Math.floor(Date.now() / 1000) + 10000;
  const user = `browser-${installationId}`;
  const fake = `http://127.0.0.1:${PORTS.fakes}`;
  expect((await request.post(`${fake}/__github/accounts`, { data: { user, installationId } })).status()).toBe(201);
  await page.goto('/connectors');
  await page.getByRole('button', { name: 'Connect GitHub', exact: true }).click();
  await page.getByRole('button', { name: 'Sign in to GitHub', exact: true }).click();
  await page.getByRole('button', { name: user, exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`installation_id=${installationId}`));
  await expect(page.getByRole('heading', { name: 'Choose scope: Repositories', exact: true })).toBeVisible();
  await page.getByRole('tab', { name: 'Everything I can see' }).click();
  await page.getByRole('button', { name: 'Continue', exact: true }).click();
  await expect(page.getByText('None', { exact: true })).toBeVisible();
  await page.reload();
  await expect(page.getByText('None', { exact: true })).toBeVisible();
  await page.getByText('None', { exact: true }).click();
  await page.getByRole('button', { name: 'Finish', exact: true }).click();
  await freshPayload(api, '/v1/connectors/github', (c: any) => c.status === 'active', 60000);
  await expect.poll(async () => (await api.json('GET', '/v1/connectors/github/captures')).items.length, { timeout: 60000 }).toBeGreaterThan(0);
  const captures = await api.json('GET', '/v1/connectors/github/captures');
  const atom = captures.items[0].atom_id;
  await expect.poll(async () => (await api.call('GET', `/v1/atoms/${atom}`)).status()).toBe(200);
  const body = await api.json('GET', `/v1/atoms/${atom}`);
  expect(body.content).toContain('acceptance evidence');
  const selected = await api.json('GET', `/v1/connectors/github/repos?installation_id=${installationId}`);
  expect(selected.repos.filter((r: any) => r.active).map((r: any) => r.repo_id).sort()).toEqual([installationId * 10 + 1, installationId * 10 + 2]);
  for (const n of [1, 2]) {
    await expect.poll(async () => {
      const calls = await (await request.get(`${fake}/__github/requests?user=${user}`)).json();
      return calls.some((r: any) => r.path.endsWith(`/repo${n}/issues/4/comments`));
    }).toBe(true);
  }
  // New upstream grants enter the existing pending-selection path, even after Everything.
  expect((await request.post(`${fake}/__github/config`, { data: { user, repositories: 3 } })).ok()).toBe(true);
  const payload = JSON.stringify({ action: 'added', installation: { id: installationId }, repositories_added: [{ id: installationId * 10 + 3, full_name: `${user}/repo3` }] });
  const signature = 'sha256=' + crypto.createHmac('sha256', 'e2e-github-webhook').update(payload).digest('hex');
  expect((await request.post(`${API_BASE}/v1/github/events`, { data: payload, headers: { 'content-type': 'application/json', 'x-github-event': 'installation_repositories', 'x-github-delivery': crypto.randomUUID(), 'x-hub-signature-256': signature } })).ok()).toBe(true);
  const discovered = await api.json('GET', `/v1/connectors/github/repos?installation_id=${installationId}`);
  const future = discovered.repos.find((r: any) => r.repo_id === installationId * 10 + 3);
  expect(future).toBeTruthy(); expect(future.active).toBe(false);
  expect(selectRows(`SELECT active::text, paused_reason, graph_id::text FROM github_watched_repos WHERE installation_id=${installationId} AND repo_id=${installationId * 10 + 3}`)).toEqual([['false', 'pending_selection', '']]);
  const available = await api.json('GET', '/v1/connectors/github/sources/repos/scope-items');
  expect(JSON.stringify(available)).toContain(`${user}/repo3`);
  // Explicitly selecting Everything again includes all three CURRENT grants.
  await api.json('PUT', '/v1/connectors/github/sources/repos/scope', { mode: 'everything' });
  expect((await api.json('GET', `/v1/connectors/github/repos?installation_id=${installationId}`)).repos.filter((r: any) => r.active)).toHaveLength(3);
  await expect.poll(async () => {
    const calls = await (await request.get(`${fake}/__github/requests?user=${user}`)).json();
    return calls.some((r: any) => r.path.endsWith('/repo3/issues/4/comments'));
  }).toBe(true);
  await api.json('DELETE', '/v1/connectors/github');
  expect((await freshPayload(api, '/v1/connectors/github')).status).toBe('not_connected');
  expect((await api.json('GET', `/v1/atoms/${atom}`)).content).toBe(body.content);
});


test('GitHub org installation is owned by the connecting member and survives member disconnect', async ({ request }) => {
  test.setTimeout(90000);
  const org = await createReadyOrg(request, 'ghorg');
  const member = await addMember(request, org, 'ghmember');
  const installationId = Math.floor(Date.now() / 1000) + 20000;
  const user = `org-${installationId}`;
  const fake = `http://127.0.0.1:${PORTS.fakes}`;
  expect((await request.post(`${fake}/__github/accounts`, { data: { user, installationId, type: 'Organization' } })).status()).toBe(201);
  const start = await org.adminInOrg.json('POST', '/v1/connectors/github/connect', { return_to: 'wizard' });
  const consent = new URL(`${fake}/github/apps/e2e-github/installations/new`);
  consent.searchParams.set('state', new URL(start.authorize_url).searchParams.get('state')!);
  consent.searchParams.set('user', user);
  const grant = await request.get(consent.href, { maxRedirects: 0 });
  const callback = await request.get(grant.headers().location, { maxRedirects: 0 });
  expect(callback.headers().location).toContain(`installation_id=${installationId}`);
  const detail = await freshPayload(org.adminInOrg, '/v1/connectors/github');
  expect(detail.status).toBe('setting_up');
  expect((await freshPayload(member.inOrg, '/v1/connectors/github')).status).toBe('not_connected');
  expect((await member.inOrg.call('DELETE', '/v1/connectors/github')).status()).toBe(404);
  expect((await member.inOrg.json('GET', '/v1/connectors/github/captures')).items).toEqual([]);
  await org.adminInOrg.json('PATCH', '/v1/connectors/github/sources/repos', { enabled: true });
  await org.adminInOrg.json('PUT', '/v1/connectors/github/sources/repos/scope', { mode: 'pick', item_ids: [String(installationId * 10 + 1)] });
  await expect.poll(async () => (await org.adminInOrg.json('GET', '/v1/connectors/github/captures')).items.length, { timeout: 60000 }).toBeGreaterThan(0);
  const populated = await org.adminInOrg.json('GET', '/v1/connectors/github/captures');
  const privateAtom = populated.items[0].atom_id;
  await expect.poll(async () => (await org.adminInOrg.call('GET', `/v1/atoms/${privateAtom}`)).status()).toBe(200);
  expect((await org.adminInOrg.json('GET', `/v1/atoms/${privateAtom}`)).content).toContain('acceptance evidence');
  // Both sessions belong to this same account; the feed now really is populated.
  expect((await member.inOrg.json('GET', '/v1/connectors/github/captures')).items).toEqual([]);
  expect((await member.inOrg.call('GET', `/v1/atoms/${privateAtom}`)).status()).toBe(404);
  const installs = await org.adminInOrg.json('GET', '/v1/connectors/github/installs');
  expect(installs.installs[0].account_type).toBe('Organization');
  await org.adminInOrg.json('DELETE', '/v1/connectors/github');
  expect((await freshPayload(org.adminInOrg, '/v1/connectors/github')).status).toBe('not_connected');
  expect((await org.adminInOrg.json('GET', '/v1/connectors/github/installs')).installs).toHaveLength(1);
  const logs = await (await request.get(`${fake}/__github/requests?user=${user}`)).json();
  expect(logs.some((r: any) => r.method === 'DELETE')).toBe(false);
});

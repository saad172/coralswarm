import { expect, test } from '../lib/fixtures';
import { personalUser, createReadyOrg, addMember } from '../lib/isolation';
import { execFileSync } from 'node:child_process';
import { CONTAINERS, CONTROL_DB, PORTS } from '../stack/config.mjs';
import { freshPayload } from '../lib/readModels';

function sql(query: string) {
  return execFileSync('docker', ['exec', CONTAINERS.pg, 'psql', '-U', 'postgres', '-d', CONTROL_DB, '-At', '-v', 'ON_ERROR_STOP=1', '-c', query], { encoding: 'utf8' }).trim();
}
// Real authenticated routes and tenant databases; only the upstream provider is fake.
for (const provider of ['linear']) {
  test(`${provider}: failed recipe recovery admits real capture`, async ({ request }) => {
    test.setTimeout(240_000);
    const { api } = await personalUser(request, `recovery-${provider}`);
    const { api: other } = await personalUser(request, `recovery-other-${provider}`);
    const root = `/v1/connectors/${provider}`;
    const source = `${root}/sources/workspace`;
    const expired = await api.json('POST', `${root}/connect`, { return_to: 'wizard' });
    const expiredAccount = expired.authorize_url.split('/').pop();
    await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${expiredAccount}`, { data: { status: 'EXPIRED' } });
    const ended = await api.call('POST', `${root}/connect/complete`, { pending_id: expired.pending_id });
    expect(ended.status()).toBe(410);
    expect((await ended.json()).code).toBe('sign_in_expired');
    const start = await api.json('POST', `${root}/connect`, { return_to: 'wizard' });
    expect(Date.parse(start.expires_at)).toBeGreaterThan(Date.now());
    const pending = { pending_id: start.pending_id };
    expect((await api.call('POST', `${root}/connect/complete`, pending)).status()).toBe(409);
    expect((await other.call('POST', `${root}/connect/complete`, pending)).status()).toBe(404);
    const account = start.authorize_url.split('/').pop();
    expect((await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`, { data: { recipeProbe: "failed" } })).ok()).toBeTruthy();
    const connected = await api.json('POST', `${root}/connect/complete`, pending);
    expect(connected.problem.code).toBe('capture_unavailable');
    await api.json('PUT', `${source}/scope`, { mode: 'everything' });
    await api.json('PATCH', source, { paused: true });
    const before = (await freshPayload(api, root)).sources;
    const recover = () => api.json('POST', `${root}/recover`, { problem: 'capture_unavailable' });
    expect((await other.call('POST', `${root}/recover`, { problem: 'capture_unavailable' })).status()).toBe(404);
    expect((await recover()).problem.code).toBe('capture_unavailable');
    await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`, { data: { recipeProbe: 'inconclusive' } });
    expect((await recover()).problem.code).toBe('capture_unavailable');
    expect(sql(`SELECT state FROM composio_capture_scopes WHERE connection_id='${start.pending_id}'::uuid`)).toBe('needs_attention');
    await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`, { data: { recipeProbe: 'passed' } });
    const repaired = await recover();
    expect(repaired.sources).toEqual(before);
    expect(repaired.status).toBe('active');
    expect(repaired.problem).toBeFalsy();
    expect(await freshPayload(api, root)).toMatchObject(repaired);
    const captureWhere = `connection_id='${start.pending_id}'::uuid`;
    const activation = sql(`SELECT id::text || ':' || watermark::text FROM composio_capture_scopes WHERE ${captureWhere}`);
    const requestsBefore = await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__requests`)).json();
    // Simulate cancellation after the passed control-plane commit but before
    // the tenant upsert: retry must complete activation without another probe.
    sql(`UPDATE composio_capture_scopes SET state='needs_attention' WHERE ${captureWhere}`);
    expect((await recover()).problem).toBeFalsy();
    expect(sql(`SELECT state FROM composio_capture_scopes WHERE ${captureWhere}`)).toBe('active');
    expect((await freshPayload(api, root)).sources).toEqual(before);
    const requestsAfter = await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__requests`)).json();
    const toolCalls = (rows: any[]) => rows.filter(r => r.path.includes('/tools/execute/')).length;
    expect(toolCalls(requestsAfter)).toBe(toolCalls(requestsBefore));
    expect(sql(`SELECT id::text || ':' || watermark::text FROM composio_capture_scopes WHERE ${captureWhere}`)).toBe(activation);
    expect(sql(`SELECT count(*) FROM composio_capture_scopes WHERE ${captureWhere}`)).toBe('1');
    expect(connected.sources[0].scope_shape).toBe('everything_only');
    expect((await api.call('PUT', `${source}/scope`, { mode: 'pick', item_ids: ['x'] })).status()).toBe(422);
    await api.json('PUT', `${source}/scope`, { mode: 'everything' });
    await api.json('PATCH', source, { enabled: true });
    const history = await api.call('POST', `${source}/backfill`, { days: 7 });
    expect(history.status()).toBe(409);
    expect((await history.json()).code).toBe('history_unsupported');
    await api.json('PATCH', source, { paused: true });
    expect((await freshPayload(api, root)).sources[0].paused).toBe(true);
    expect(start.pending_id).toMatch(/^[0-9a-f-]{36}$/);
    const where = `connection_id='${start.pending_id}'::uuid`;
    const due = () => sql(`UPDATE composio_capture_scopes SET next_poll_at=now()-interval '1 minute' WHERE ${where}`);
    due();
    await expect.poll(() => sql(`SELECT next_poll_at>now() FROM composio_capture_scopes WHERE ${where}`), { timeout: 90_000, intervals: [1000] }).toBe('t');
    expect((await api.json('GET', `${root}/captures`)).items).toHaveLength(0);
    await api.json('PATCH', source, { paused: false });
    due();
    await expect.poll(async () => (await api.json('GET', `${root}/captures`)).items.length, { timeout: 90_000, intervals: [1000] }).toBeGreaterThan(0);
    const retained = (await api.json('GET', `${root}/captures`)).items.map((x: any) => x.atom_id);
    await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`, { data: { status: 'INACTIVE' } });
    due();
    await freshPayload(api, root, (c: any) => c.status === 'reconnect_needed', 90_000);
    expect((await freshPayload(api, root)).sources[0].paused).toBe(false);
    const refused = await api.call('POST', `${root}/recover`, { problem: 'capture_unavailable' });
    expect(refused.status()).toBe(409);
    expect((await refused.json()).code).toBe('recovery_not_applicable');
    expect((await api.call('DELETE', root)).status()).toBe(204);
    expect((await freshPayload(api, root)).status).toBe('not_connected');
    const seen = await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__requests`)).json();
    expect(seen.some((r: any) => r.method === 'DELETE' && r.path.endsWith(`/connected_accounts/${account}`))).toBe(true);
    expect((await api.json('GET', `${root}/captures`)).items.map((x: any) => x.atom_id)).toEqual(expect.arrayContaining(retained));
  });
}

// Same-account member isolation is distinct from personal-account isolation above.
test('recovery resolves the member within a shared account', async ({ request }) => {
  test.setTimeout(120_000);
  const org = await createReadyOrg(request, 'recovery-team');
  const member = await addMember(request, org, 'recovery-member');
  const api = org.adminInOrg;
  const root = '/v1/connectors/linear';
  const start = await api.json('POST', `${root}/connect`, { return_to: 'wizard' });
  const account = start.authorize_url.split('/').pop();
  await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`, { data: { recipeProbe: 'failed' } });
  expect((await api.json('POST', `${root}/connect/complete`, { pending_id: start.pending_id })).problem.code).toBe('capture_unavailable');
  expect((await member.inOrg.call('POST', `${root}/recover`, { problem: 'capture_unavailable' })).status()).toBe(404);
  expect((await freshPayload(api, root)).problem.code).toBe('capture_unavailable');
  await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`);
  expect((await api.json('POST', `${root}/recover`, { problem: 'capture_unavailable' })).problem).toBeFalsy();
  expect((await freshPayload(member.inOrg, root)).status).toBe('not_connected');
  await api.call('DELETE', root);
});

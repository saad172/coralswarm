import { expect, test } from '../lib/fixtures';
import { personalUser } from '../lib/isolation';
import { execFileSync } from 'node:child_process';
import { CONTAINERS, CONTROL_DB, PORTS } from '../stack/config.mjs';
import { freshPayload } from '../lib/readModels';

function sql(query: string) {
  return execFileSync('docker', ['exec', CONTAINERS.pg, 'psql', '-U', 'postgres', '-d', CONTROL_DB, '-At', '-v', 'ON_ERROR_STOP=1', '-c', query], { encoding: 'utf8' }).trim();
}
// Real authenticated routes and tenant databases; only the upstream provider is fake.
for (const provider of ['jira', 'linear', 'confluence', 'hubspot', 'zendesk', 'dart']) {
  test(`${provider}: hosted ownership, Everything, history, pause and disconnect`, async ({ request }) => {
    test.setTimeout(30_000);
    const { api } = await personalUser(request, `six-${provider}`);
    const { api: other } = await personalUser(request, `six-other-${provider}`);
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
    expect((await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`)).ok()).toBeTruthy();
    const connected = await api.json('POST', `${root}/connect/complete`, pending);
    expect(connected.status).toBe('active');
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
    await expect.poll(() => sql(`SELECT next_poll_at>now() FROM composio_capture_scopes WHERE ${where}`), { timeout: 10_000, intervals: [100, 250] }).toBe('t');
    expect((await api.json('GET', `${root}/captures`)).items).toHaveLength(0);
    await api.json('PATCH', source, { paused: false });
    due();
    await expect.poll(async () => (await api.json('GET', `${root}/captures`)).items.length, { timeout: 10_000, intervals: [100, 250] }).toBeGreaterThan(0);
    // Captures become visible before the worker commits its watermark and
    // releases the lease. Wait for that commit before advancing its deadline,
    // otherwise the completed poll can overwrite due() with its next interval.
    await expect.poll(() => sql(`SELECT lease_id IS NULL AND next_poll_at>now() FROM composio_capture_scopes WHERE ${where}`), { timeout: 10_000, intervals: [100, 250] }).toBe('t');
    const retained = (await api.json('GET', `${root}/captures`)).items.map((x: any) => x.atom_id);
    await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`, { data: { status: 'INACTIVE' } });
    due();
    await freshPayload(api, root, (c: any) => c.status === 'reconnect_needed', 10_000);
    expect((await freshPayload(api, root)).sources[0].paused).toBe(false);
    expect((await api.call('DELETE', root)).status()).toBe(204);
    expect((await freshPayload(api, root)).status).toBe('not_connected');
    const seen = await (await request.get(`http://127.0.0.1:${PORTS.fakes}/__requests`)).json();
    expect(seen.some((r: any) => r.method === 'DELETE' && r.path.endsWith(`/connected_accounts/${account}`))).toBe(true);
    expect((await api.json('GET', `${root}/captures`)).items.map((x: any) => x.atom_id)).toEqual(expect.arrayContaining(retained));
  });
}

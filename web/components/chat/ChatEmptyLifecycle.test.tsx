// @vitest-environment jsdom
import { act, cleanup, render, screen } from '@testing-library/react';
import { QueryClientProvider } from '@tanstack/react-query';
import { afterEach, expect, it, vi } from 'vitest';
import { makeQueryClient } from '@/lib/queryClient';
import { ChatEmpty } from './ChatEmpty';
import { FirstRunChecklist } from './FirstRunChecklist';
const state = vi.hoisted(() => ({ orgLoaded: false, read: vi.fn(), token: () => 'token' }));
vi.mock('@clerk/nextjs', () => ({ useUser: () => ({ user: { firstName: 'Dana' } }) }));
vi.mock('@/lib/useEvidenceAdmission', () => ({ useEvidenceAdmission: () => ({ queryScope: 'actor:org', ready: true, getToken: state.token }) }));
vi.mock('@/lib/providers', () => ({ useApp: () => ({ authReady: true, getAuthToken: state.token, queryScope: 'actor:org', accountId: 'account', internalUserId: 'actor' }) }));
vi.mock('@/components/org/useOrgAdmin', () => ({ useOrgAdmin: () => ({ loaded: state.orgLoaded, hasOrg: true, isAdmin: true, orgId: 'org', orgName: state.orgLoaded ? 'Acme' : null }) }));
vi.mock('@/lib/api', async (actual) => ({ ...await actual<typeof import('@/lib/api')>(), apiRequest: (...args: unknown[]) => state.read(...args) }));
vi.mock('./RevealAnswer', () => ({ RevealAnswer: ({ children }: { children: React.ReactNode }) => children }));
const payload = { workspace_activity: null, activity: [], connections: [], connections_status: 'available', teammate_count: 1, own_conversation_count: 0 };
const pending = () => ({ model: 'chat_empty', version: 1, computed_at: null, stale: true, refresh_eta: new Date(Date.now() + 5000).toISOString(), payload });
const computed = (privateTopic = false) => ({ model: 'chat_empty', version: 1, computed_at: new Date().toISOString(), stale: false, refresh_eta: null,
  payload: privateTopic ? { ...payload, workspace_activity: { workspaces: [{ graph_id: 'graph', name: 'Previously certified topic', headline: { reef: null } }] } } : payload });
const tree = () => <><FirstRunChecklist /><ChatEmpty onAsk={() => {}} /></>;
const clients: ReturnType<typeof makeQueryClient>[] = [];
afterEach(() => { cleanup(); clients.splice(0).forEach((client) => client.clear()); vi.useRealTimers(); vi.resetAllMocks(); state.orgLoaded = false; });

for (const phase of ['cold', 'withheld'] as const) for (const rollback of [false, true]) {
  it(`${phase}: late org presentation keeps one shared address and polls after two monotonic seconds${rollback ? ' despite a backward wall-clock step' : ''}`, async () => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] });
    vi.setSystemTime(new Date('2026-10-31T23:19:49Z'));
    const starts: { wall: number; elapsed: number }[] = [];
    const client = makeQueryClient(); clients.push(client);
    let readsPending = phase === 'cold';
    state.read.mockImplementation(() => {
      starts.push({ wall: Date.now(), elapsed: performance.now() });
      if (phase === 'withheld' && !readsPending) return Promise.resolve(computed(true));
      const body = pending(); readsPending = false;
      state.read.mockImplementation(() => { starts.push({ wall: Date.now(), elapsed: performance.now() }); return Promise.resolve(computed()); });
      return Promise.resolve(body);
    });
    const view = render(<QueryClientProvider client={client}>{tree()}</QueryClientProvider>);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(starts).toHaveLength(1);
    if (phase === 'withheld') {
      expect(screen.getByRole('button', { name: /Previously certified topic/ })).toBeTruthy();
      readsPending = true;
      await act(async () => { await client.refetchQueries({ queryKey: ['chatEmpty'], type: 'active' }); await vi.advanceTimersByTimeAsync(0); });
      expect(starts).toHaveLength(2);
      expect(screen.queryByRole('button', { name: /Previously certified topic/ })).toBeNull();
    }
    const pendingRead = starts[starts.length - 1];
    const initialCount = starts.length;
    expect(screen.getByRole('status').textContent).toContain('Finding recent');
    expect(screen.queryByRole('region', { name: 'Getting started' })).toBeNull();
    await act(async () => { await vi.advanceTimersByTimeAsync(1800); });
    state.orgLoaded = true;
    view.rerender(<QueryClientProvider client={client}>{tree()}</QueryClientProvider>);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(starts).toHaveLength(initialCount);
    if (rollback) vi.setSystemTime(Date.now() - 200);
    await act(async () => { await vi.advanceTimersByTimeAsync(199); });
    expect(starts).toHaveLength(initialCount);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(starts).toHaveLength(initialCount + 1);
    const poll = starts[starts.length - 1];
    expect(poll.elapsed - pendingRead.elapsed).toBe(2000);
    expect(poll.wall - pendingRead.wall).toBe(rollback ? 1800 : 2000);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(screen.getByRole('status').textContent).not.toContain('Finding recent');
    expect(state.read.mock.calls.every(([path]) => path === '/v1/load/chats/empty?limit=6&since_days=7')).toBe(true);
    expect(state.read.mock.calls.every((call) => call[4].priority === 'critical')).toBe(true);
    expect(client.getQueryCache().getAll()).toHaveLength(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(4000); });
    expect(starts).toHaveLength(initialCount + 1);
  });
}

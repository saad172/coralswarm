import { afterEach, expect, it, vi } from 'vitest';
import { apiRequest } from './api';
import { refreshPageSection } from './pageSectionRefresh';
vi.mock('./api', async (original) => ({ ...await original<typeof import('./api')>(), apiRequest: vi.fn() }));
afterEach(() => { vi.useRealTimers(); vi.resetAllMocks(); });
const stored = (computed: string | null, eta: string | null, payload: unknown) => ({ model: 'connector_detail', version: 1, payload, computed_at: computed, stale: eta !== null, refresh_eta: eta });
it('waits for a real worker answer after refresh and keeps owed polls critical', async () => {
  vi.useFakeTimers();
  vi.mocked(apiRequest).mockResolvedValueOnce({ refresh_eta: '2026-10-31T00:00:02Z' })
    .mockResolvedValueOnce(stored(null, '2026-10-31T00:00:02Z', {}))
    .mockResolvedValueOnce(stored('2026-10-31T00:00:00Z', '2026-10-31T00:00:02Z', { key: 'stale' }))
    .mockResolvedValueOnce(stored('2026-10-31T00:00:03Z', null, { key: 'slack' }));
  const result = refreshPageSection('token', 'connector_detail', { provider: 'slack' });
  await vi.advanceTimersByTimeAsync(4_000);
  // Finish must reconcile an owed stale answer promptly too, rather than
  // waiting for the passive page's 30s cadence before requesting history.
  expect(apiRequest).toHaveBeenCalledTimes(4);
  await expect(result).resolves.toEqual({ key: 'slack' });
  expect(vi.mocked(apiRequest).mock.calls.map((call) => [call[0], call[4]?.priority])).toEqual([
    ['/v1/connectors/slack/refresh', 'normal'], ['/v1/connectors/slack', 'critical'],
    ['/v1/connectors/slack', 'critical'], ['/v1/connectors/slack', 'critical'],
  ]);
});
it('cancels a pending owed wait instead of retaining an action poll after unmount', async () => {
  vi.useFakeTimers(); const controller = new AbortController();
  vi.mocked(apiRequest).mockResolvedValueOnce({ refresh_eta: '2026-10-31T00:00:02Z' })
    .mockResolvedValue(stored(null, '2026-10-31T00:00:02Z', {}));
  const result = refreshPageSection('token', 'connector_detail', { provider: 'slack' }, controller.signal);
  await Promise.all([expect(result).rejects.toMatchObject({ name: 'AbortError' }), (async () => { await vi.advanceTimersByTimeAsync(1); controller.abort(); })()]);
  await vi.advanceTimersByTimeAsync(10_000); expect(apiRequest).toHaveBeenCalledTimes(2);
});
it('does not turn malformed or refused refresh responses into connector facts', async () => {
  vi.mocked(apiRequest).mockResolvedValueOnce({ refresh_eta: null }).mockResolvedValueOnce({ key: 'slack' });
  await expect(refreshPageSection('token', 'connector_detail', { provider: 'slack' })).rejects.toThrow('Could not verify');
});

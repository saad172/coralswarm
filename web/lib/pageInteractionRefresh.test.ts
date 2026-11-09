// @vitest-environment jsdom
import { QueryObserver } from '@tanstack/react-query';
import { afterEach, expect, it, vi } from 'vitest';
import { apiRequest } from './api';
import { connectorQueryOptions, meetingCapturesQueryOptions } from './appQueries';
import { makeQueryClient } from './queryClient';
import { pageSectionQuery } from './pageSections';

vi.mock('./api', async (actual) => ({ ...await actual<typeof import('./api')>(), apiRequest: vi.fn() }));
const read = vi.mocked(apiRequest);
const client = makeQueryClient();
const ctx = { scope: 'member', getAuthToken: () => 'token' };
afterEach(() => { client.clear(); vi.useRealTimers(); vi.resetAllMocks(); });

it('connector detail controls reconcile stale followed threads within the short recovery cadence', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-11-04T21:07:08Z'));
  read.mockResolvedValueOnce({ model: 'connector_detail', version: 1, computed_at: '2026-11-04T21:07:06Z',
    stale: true, refresh_eta: '2026-11-04T21:07:13Z', payload: { watched: { threads: [] } } })
    .mockResolvedValue({ model: 'connector_detail', version: 1, computed_at: '2026-11-04T21:07:09Z',
      stale: false, refresh_eta: null, payload: { watched: { threads: [{ id: 'followed' }] } } });
  // ConnectorDetail uses the section hook with the shared connector factory key.
  const observer = new QueryObserver(client, pageSectionQuery('connector_detail', ctx.scope,
    { provider: 'microsoft' }, ctx.getAuthToken, { queryKey: connectorQueryOptions(ctx, 'microsoft').queryKey }));
  const stop = observer.subscribe(() => {});
  try {
    await vi.advanceTimersByTimeAsync(2_001);
    expect(read).toHaveBeenCalledTimes(2);
    expect(observer.getCurrentResult().data?.payload).toEqual({ watched: { threads: [{ id: 'followed' }] } });
  } finally { stop(); }
});

it('Teams capture action fetch reconciles a stale computed-empty list without hiding the selected meeting', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-11-04T20:56:30Z'));
  const empty = { model: 'teams_transcript_captures', version: 1, computed_at: '2026-11-04T20:56:29Z',
    stale: false, refresh_eta: null, payload: { captures: [] } };
  read.mockResolvedValueOnce(empty);
  const options = meetingCapturesQueryOptions(ctx);
  const observer = new QueryObserver(client, options);
  const stop = observer.subscribe(() => {});
  try {
    await vi.advanceTimersByTimeAsync(1);
    read.mockResolvedValueOnce({ ...empty, stale: true, refresh_eta: '2026-11-04T20:57:00Z' })
      .mockResolvedValue({ ...empty, computed_at: '2026-11-04T20:56:31Z', payload: {
        captures: [{ capture_id: 'selected', meeting_id: 'meeting', created_at: '2026-11-04T20:56:30Z', status: 'active' }],
      } });
    await client.fetchQuery({ ...options, staleTime: 0 });
    await vi.advanceTimersByTimeAsync(2_001);
    expect(read).toHaveBeenCalledTimes(3);
    expect(observer.getCurrentResult().data?.[0]?.capture_id).toBe('selected');
    await vi.advanceTimersByTimeAsync(120_000);
    expect(read).toHaveBeenCalledTimes(3);
  } finally { stop(); }
});

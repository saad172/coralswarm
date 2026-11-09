// @vitest-environment jsdom
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { focusManager, QueryClientProvider, useQuery, type QueryClient } from "@tanstack/react-query";
import { createElement, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { criticalReadCount } from "./transport";
import { makeQueryClient } from "./queryClient";
import { apiRequest, ApiError } from "./api";
import { canonicalSectionParams, pageSectionPath, pageSectionQuery, typedStored, usePageSection, type Stored } from "./pageSections";

const identity = vi.hoisted(() => ({ queryScope: 'u:personal', authReady: true, getAuthToken: () => 'token' }));
vi.mock('./providers', () => ({ useApp: () => identity }));
vi.mock('./api', async (actual) => ({ ...await actual<typeof import('./api')>(), apiRequest: vi.fn() }));
const call = vi.mocked(apiRequest);
const body = (payload: string[], changes: Partial<Stored<string[]>> = {}): Stored<string[]> => ({
  model: 'journal_gist', version: 1, payload, computed_at: '2026-10-31T00:00:00Z', stale: false, refresh_eta: null, ...changes,
});
let qc: QueryClient;
function wrapper({ children }: { children: ReactNode }) { return createElement(QueryClientProvider, { client: qc }, children); }
beforeEach(() => { qc = makeQueryClient(); call.mockReset(); identity.queryScope = 'u:personal'; });
afterEach(() => { cleanup(); qc.clear(); focusManager.setFocused(undefined); vi.useRealTimers(); });

it('plain mode returns the original body without inventing an envelope', async () => {
  call.mockResolvedValueOnce({ role: 'member' });
  const options = pageSectionQuery<{ role: string }>('/v1/me', 'u:personal', {}, () => 'token', { plain: true });
  await expect(options.queryFn({ signal: new AbortController().signal })).resolves.toEqual({ role: 'member' });
  expect(call.mock.calls[0][4]).toMatchObject({ priority: 'critical' });
});

it('validates real certificates and keeps query/path keys canonical', () => {
  expect(canonicalSectionParams({ tz: 'America/Chicago', days: 30, nil: null })).toEqual({ days: '30', tz: 'America/Chicago' });
  expect(pageSectionPath('connector_detail', { provider: 'slack' })).toBe('/v1/connectors/slack');
  expect(pageSectionPath('home_metrics', { tz: 'UTC', days: 30 }, true)).toBe('/v1/home/metrics/refresh?days=30&tz=UTC');
  expect(() => typedStored([], 'journal_gist')).toThrow();
  expect(() => typedStored(body([], { version: 99 }), 'journal_gist')).toThrow();
  const a = pageSectionQuery('home_metrics', 'u:o', { tz: 'UTC', days: 30 }, () => 'A');
  const b = pageSectionQuery('home_metrics', 'u:o', { days: 30, tz: 'UTC' }, () => 'B');
  expect(a.queryKey).toEqual(b.queryKey);
});

describe('stored observers', () => {
  it('shows pending until real compute, then distinguishes computed-empty', async () => {
    call.mockResolvedValueOnce(body([], { computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:01Z' })).mockResolvedValue(body([]));
    const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
    await waitFor(() => expect(call).toHaveBeenCalledTimes(1));
    expect(hook.result.current.isPending).toBe(true); expect(hook.result.current.data).toBeUndefined();
    await act(async () => { await qc.invalidateQueries(); });
    await waitFor(() => expect(hook.result.current.isPending).toBe(false));
    expect(hook.result.current.data).toEqual([]); expect(hook.result.current.computedAt).toBeInstanceOf(Date);
  });

  it('keeps safe stale payload while owed, waits for ETA, then uses slower polling until resolved', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-31T00:00:00Z'));
    call.mockResolvedValueOnce(body(['safe'], { stale: true, refresh_eta: '2026-10-31T00:00:30Z' }))
      .mockResolvedValueOnce(body(['safe'], { stale: true, refresh_eta: '2026-10-31T00:00:30Z' }))
      .mockResolvedValue(body(['new'], { computed_at: '2026-10-31T00:00:31Z' }));
    const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(hook.result.current.data).toEqual(['safe']); expect(hook.result.current.isRefreshing).toBe(true);
    await act(async () => { await vi.advanceTimersByTimeAsync(29_000); }); expect(call).toHaveBeenCalledTimes(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(1_000); }); expect(call).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(30_000); }); expect(call).toHaveBeenCalledTimes(3);
    expect(hook.result.current.data).toEqual(['new']); expect(hook.result.current.isRefreshing).toBe(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(10_000); }); expect(call).toHaveBeenCalledTimes(3);
  });

  it('bounds empty retries, backs off failures, and permits an explicit recovery without inventing data', async () => {
    vi.useFakeTimers();
    call.mockResolvedValueOnce(body([], { computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:30Z' }))
      .mockRejectedValue(new ApiError('unavailable', 503));
    const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
    await act(async () => { await vi.advanceTimersByTimeAsync(2_001); }); expect(call).toHaveBeenCalledTimes(2);
    expect(hook.result.current.data).toBeUndefined();
    await act(async () => { await vi.advanceTimersByTimeAsync(3_000); }); expect(call).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(1_000); }); expect(call).toHaveBeenCalledTimes(3);
    await act(async () => { await vi.advanceTimersByTimeAsync(110_000); });
    const stopped = call.mock.calls.length;
    expect(hook.result.current.error?.message).toContain('Automatic checks have stopped');
    await act(async () => { await vi.advanceTimersByTimeAsync(120_000); }); expect(call).toHaveBeenCalledTimes(stopped);
    call.mockResolvedValue(body([]));
    await act(async () => { await qc.invalidateQueries(); await vi.advanceTimersByTimeAsync(1); });
    expect(hook.result.current.error).toBeNull(); expect(hook.result.current.data).toEqual([]);
  });

  it('pauses stale polling while hidden and stops it after the last observer unmounts', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-31T00:00:00Z'));
    call.mockResolvedValue(body(['safe'], { stale: true, refresh_eta: '2026-10-31T00:00:30Z' }));
    const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    act(() => focusManager.setFocused(false));
    await act(async () => { await vi.advanceTimersByTimeAsync(30_000); });
    expect(call).toHaveBeenCalledTimes(1); expect(hook.result.current.data).toEqual(['safe']);
    act(() => focusManager.setFocused(true));
    await act(async () => { await vi.advanceTimersByTimeAsync(30_000); }); expect(call).toHaveBeenCalledTimes(2);
    hook.unmount();
    await act(async () => { await vi.advanceTimersByTimeAsync(120_000); }); expect(call).toHaveBeenCalledTimes(2);
  });

  it('drops a warmed payload immediately when the server withholds it and never resurrects it on failure', async () => {
    call.mockResolvedValueOnce(body(['revoked']));
    const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
    await waitFor(() => expect(hook.result.current.data).toEqual(['revoked']));
    call.mockResolvedValueOnce(body([], { computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:30Z' }));
    await act(async () => { await qc.invalidateQueries(); });
    await waitFor(() => expect(hook.result.current.data).toBeUndefined()); expect(hook.result.current.isPending).toBe(true);
    call.mockRejectedValue(new ApiError('unavailable', 503));
    await act(async () => { await qc.invalidateQueries(); });
    await waitFor(() => expect(hook.result.current.error).toBeTruthy()); expect(hook.result.current.data).toBeUndefined();
  });

  it('posts manual refresh as normal, reads the real ETA and dedupes shared observers', async () => {
    call.mockResolvedValue(body(['initial']));
    const hook = renderHook(() => [usePageSection<string[]>('journal_gist'), usePageSection<string[]>('journal_gist')], { wrapper });
    await waitFor(() => expect(hook.result.current[0].data).toEqual(['initial'])); expect(call).toHaveBeenCalledTimes(1);
    call.mockResolvedValueOnce({ refresh_eta: '2026-10-31T00:00:30Z' }).mockResolvedValue(body(['initial'], { stale: true, refresh_eta: '2026-10-31T00:00:30Z' }));
    await act(async () => { await hook.result.current[0].refresh(); });
    expect(call.mock.calls[1][0]).toBe('/v1/journal/gist/refresh');
    expect(call.mock.calls[1][4]).toMatchObject({ priority: 'normal' });
    expect(call.mock.calls[2][4]).toMatchObject({ priority: 'critical' });
    await waitFor(() => expect(hook.result.current[1].isRefreshing).toBe(true));
  });

  it('isolates account/org keys and cancels the final observer on unmount', async () => {
    let signal: AbortSignal | undefined;
    call.mockImplementation((_path, _token, _init, _retry, opts) => { signal = opts?.signal; return new Promise(() => {}); });
    const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
    await waitFor(() => expect(signal).toBeDefined());
    const oldSignal = signal;
    identity.queryScope = 'u:org'; hook.rerender();
    await waitFor(() => expect(call).toHaveBeenCalledTimes(2));
    expect(oldSignal?.aborted).toBe(true); expect(hook.result.current.data).toBeUndefined();
    hook.unmount(); expect(signal?.aborted).toBe(true);
    expect(criticalReadCount()).toBe(0);
  });

  it('plain errors remain errors and no fallback payload or zeros are invented', async () => {
    call.mockRejectedValue(new ApiError('denied', 403));
    const hook = renderHook(() => useQuery(pageSectionQuery<string[]>('journal_gist', 'u', {}, () => 'token')), { wrapper });
    await waitFor(() => expect(hook.result.current.isError).toBe(true));
    expect(hook.result.current.data).toBeUndefined(); expect(call).toHaveBeenCalledTimes(1);
  });
});

describe('authority refusal is permanent until a new verified read', () => {
  it.each([401, 403, 404])('does not resurrect a payload after %s then 503 across hook and payload observers', async (status) => {
    call.mockResolvedValue(body(['secret']));
    const hook = renderHook(() => ({ section: usePageSection<string[]>('journal_gist'),
      payload: useQuery({ ...pageSectionQuery<string[]>('journal_gist', identity.queryScope, {}, identity.getAuthToken),
        select: (stored) => stored.computed_at === null ? undefined : stored.payload }) }), { wrapper });
    await waitFor(() => expect(hook.result.current.payload.data).toEqual(['secret']));
    call.mockRejectedValue(new ApiError('refused', status));
    await act(async () => { await qc.invalidateQueries(); });
    await waitFor(() => expect(hook.result.current.section.data).toBeUndefined());
    expect(hook.result.current.payload.data).toBeUndefined();
    call.mockRejectedValue(new ApiError('offline', 503));
    await act(async () => { await qc.invalidateQueries(); });
    expect(hook.result.current.section.data).toBeUndefined();
    expect(hook.result.current.payload.data).toBeUndefined();
    call.mockResolvedValue(body(['new authority']));
    await act(async () => { await qc.invalidateQueries(); });
    await waitFor(() => expect(hook.result.current.section.data).toEqual(['new authority']));
  });

  it('invalid certificates revoke old data across different factory keys and cancel late peers', async () => {
    call.mockResolvedValue(body(['secret']));
    const a = pageSectionQuery<string[]>('journal_gist', 'u:personal', {}, () => 'token', { queryKey: ['A'] });
    const b = pageSectionQuery<string[]>('journal_gist', 'u:personal', {}, () => 'token', { queryKey: ['B'] });
    const hook = renderHook(() => [useQuery(a), useQuery(b)], { wrapper });
    await waitFor(() => expect(hook.result.current[1].data?.payload).toEqual(['secret']));
    let late!: (value: unknown) => void;
    call.mockImplementationOnce(() => new Promise((resolve) => { late = resolve; })).mockResolvedValueOnce(body(['invalid'], { version: 999 }));
    act(() => { void qc.invalidateQueries(); });
    await waitFor(() => expect(hook.result.current[1].isError).toBe(true));
    await act(async () => { late(body(['late secret'])); });
    expect(hook.result.current.map((q) => q.data)).toEqual([undefined, undefined]);
  });

  it('fences protected plain reads after refusal too', async () => {
    call.mockResolvedValue({ name: 'private' });
    const hook = renderHook(() => useQuery(pageSectionQuery<{ name: string }>('/v1/me', 'u', {}, () => 'token', { plain: true })), { wrapper });
    await waitFor(() => expect(hook.result.current.data?.name).toBe('private'));
    call.mockRejectedValue(new ApiError('denied', 403));
    await act(async () => { await qc.invalidateQueries(); });
    await waitFor(() => expect(hook.result.current.data).toBeUndefined());
    call.mockRejectedValue(new ApiError('offline', 503));
    await act(async () => { await qc.invalidateQueries(); });
    expect(hook.result.current.data).toBeUndefined();
  });

  it('stops owed polling when authority is refused', async () => {
    vi.useFakeTimers();
    call.mockResolvedValueOnce(body([], { computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:02Z' }))
      .mockRejectedValue(new ApiError('denied', 403));
    const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
    await act(async () => { await vi.advanceTimersByTimeAsync(2_100); });
    expect(hook.result.current.error).toBeTruthy();
    await act(async () => { await vi.advanceTimersByTimeAsync(4_200); });
    expect(call).toHaveBeenCalledTimes(2);
  });

  it('resumes bounded empty-page polling after an explicit retry following refusal', async () => {
    vi.useFakeTimers();
    const pending = body([], { computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:02Z' });
    call.mockResolvedValueOnce(pending).mockRejectedValue(new ApiError('denied', 403));
    const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
    await act(async () => { await vi.advanceTimersByTimeAsync(2_100); });
    expect(hook.result.current.error).toBeTruthy();
    expect(qc.getQueryData(['journal_gist', identity.queryScope, {}])).toBeUndefined();
    call.mockResolvedValue(pending);
    await act(async () => { await qc.invalidateQueries(); await vi.advanceTimersByTimeAsync(1); });
    expect(hook.result.current.isPending).toBe(true);
    expect(hook.result.current.error).toBeNull();
    const retried = call.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(2_000); });
    expect(call).toHaveBeenCalledTimes(retried + 1);
    await act(async () => { await vi.advanceTimersByTimeAsync(120_000); });
    expect(hook.result.current.error?.message).toContain('Automatic checks have stopped');
    const stopped = call.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(120_000); });
    expect(call).toHaveBeenCalledTimes(stopped);
  });
});

it('propagates an actual withheld envelope across alternate keys before payload selection', async () => {
  call.mockResolvedValue(body(['private']));
  const a = pageSectionQuery<string[]>('journal_gist', 'u', {}, () => 'token', { queryKey: ['variant-a'] });
  const b = pageSectionQuery<string[]>('journal_gist', 'u', {}, () => 'token', { queryKey: ['variant-b'] });
  const select = (stored: Stored<string[]>) => stored.computed_at === null ? undefined : stored.payload;
  const hook = renderHook(() => [useQuery({ ...a, select }), useQuery({ ...b, select })], { wrapper });
  await waitFor(() => expect(hook.result.current.map((q) => q.data)).toEqual([['private'], ['private']]));
  call.mockResolvedValue(body([], { computed_at: null, stale: true, refresh_eta: '2026-10-31T00:00:02Z' }));
  await act(async () => { await qc.invalidateQueries({ queryKey: ['variant-a'], exact: true }); });
  await waitFor(() => expect(hook.result.current.map((q) => q.data)).toEqual([undefined, undefined]));
  call.mockRejectedValue(new ApiError('offline', 503));
  await act(async () => { await qc.invalidateQueries(); });
  expect(hook.result.current.map((q) => q.data)).toEqual([undefined, undefined]);
});

it('never erases or restores another account while fencing a refused resource', async () => {
  call.mockResolvedValue(body(['account A']));
  const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
  await waitFor(() => expect(hook.result.current.data).toEqual(['account A']));
  identity.queryScope = 'u:org'; call.mockResolvedValue(body(['account B'])); hook.rerender();
  await waitFor(() => expect(hook.result.current.data).toEqual(['account B']));
  call.mockRejectedValue(new ApiError('revoked A', 403));
  await act(async () => { await qc.refetchQueries({ queryKey: ['journal_gist', 'u:personal'], type: 'all' }); });
  expect(hook.result.current.data).toEqual(['account B']);
  call.mockReturnValue(new Promise(() => {})); identity.queryScope = 'u:personal'; hook.rerender();
  expect(hook.result.current.data).toBeUndefined();
});

it('does not let an obsolete peer refusal clear authority established by a later verified read', async () => {
  call.mockResolvedValue(body(['old']));
  const a = pageSectionQuery<string[]>('journal_gist', 'u', {}, () => 'token', { queryKey: ['late-A'] });
  const b = pageSectionQuery<string[]>('journal_gist', 'u', {}, () => 'token', { queryKey: ['late-B'] });
  const hook = renderHook(() => [useQuery(a), useQuery(b)], { wrapper });
  await waitFor(() => expect(hook.result.current[1].data?.payload).toEqual(['old']));
  let rejectOld!: (reason: unknown) => void;
  call.mockImplementationOnce(() => new Promise((_resolve, reject) => { rejectOld = reject; })).mockRejectedValueOnce(new ApiError('denied', 403));
  act(() => { void qc.invalidateQueries(); });
  await waitFor(() => expect(hook.result.current.every((q) => q.data === undefined)).toBe(true));
  call.mockResolvedValue(body(['new verified authority']));
  await act(async () => { await qc.refetchQueries({ queryKey: ['late-B'], exact: true }); });
  await waitFor(() => expect(hook.result.current.every((q) => q.data?.payload[0] === 'new verified authority')).toBe(true));
  await act(async () => { rejectOld(new ApiError('obsolete denial', 403)); });
  expect(hook.result.current.map((q) => q.data?.payload)).toEqual([['new verified authority'], ['new verified authority']]);
});

it('treats malformed successful JSON as an invalid answer and never restores its previous payload on 503', async () => {
  call.mockResolvedValue(body(['private']));
  const hook = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
  await waitFor(() => expect(hook.result.current.data).toEqual(['private']));
  call.mockRejectedValue(new SyntaxError('Unexpected end of JSON input'));
  await act(async () => { await qc.invalidateQueries(); });
  await waitFor(() => expect(hook.result.current.data).toBeUndefined());
  call.mockRejectedValue(new ApiError('offline', 503));
  await act(async () => { await qc.invalidateQueries(); });
  expect(hook.result.current.data).toBeUndefined();
});

it('declares disabled incomplete selectors without inventing a request path', async () => {
  const hook = renderHook(({ id }) => useQuery({ ...pageSectionQuery('atlas', 'u', { graph_id: id }, () => 'token'), enabled: !!id }), { wrapper, initialProps: { id: '' } });
  expect(call).not.toHaveBeenCalled();
  call.mockResolvedValue({ model: 'atlas', version: 1, payload: {}, computed_at: '2026-10-31T00:00:00Z', stale: false, refresh_eta: null });
  hook.rerender({ id: '11111111-1111-4111-8111-111111111111' });
  await waitFor(() => expect(hook.result.current.data?.computed_at).toBe('2026-10-31T00:00:00Z'));
  expect(call).toHaveBeenCalledTimes(1);
  expect(call.mock.calls[0][0]).toBe('/v1/graphs/11111111-1111-4111-8111-111111111111/atlas');
});

it('fences every observer when the manual refresh itself refuses authority', async () => {
  call.mockResolvedValue(body(['private']));
  const hook = renderHook(() => [usePageSection<string[]>('journal_gist'), usePageSection<string[]>('journal_gist')], { wrapper });
  await waitFor(() => expect(hook.result.current[1].data).toEqual(['private']));
  call.mockRejectedValueOnce(new ApiError('refused refresh', 403));
  await act(async () => { await hook.result.current[0].refresh(); });
  await waitFor(() => expect(hook.result.current.map((s) => s.data)).toEqual([undefined, undefined]));
  call.mockRejectedValue(new ApiError('offline', 503));
  await act(async () => { await qc.invalidateQueries(); });
  expect(hook.result.current.map((s) => s.data)).toEqual([undefined, undefined]);
});

it.each(['stored', 'plain'])('rejects a late %s body after the credential changes without a render', async (kind) => {
  let token = 'old-actor';
  let finish!: (value: unknown) => void;
  call.mockResolvedValue(kind === 'stored' ? body(['private']) : { name: 'private' });
  const hook = renderHook(() => {
    const stored = useQuery({ ...pageSectionQuery('journal_gist', 'u', {}, () => token), enabled: kind === 'stored' });
    const plain = useQuery({ ...pageSectionQuery('/v1/me', 'u', {}, () => token, { plain: true }), enabled: kind === 'plain' });
    return kind === 'stored' ? stored : plain;
  }, { wrapper });
  await waitFor(() => expect(hook.result.current.data).toBeDefined());
  call.mockImplementationOnce(() => new Promise((resolve) => { finish = resolve; }));
  act(() => { void qc.invalidateQueries(); });
  await waitFor(() => expect(call).toHaveBeenCalledTimes(2));
  token = 'another-actor';
  await act(async () => finish(kind === 'stored' ? body(['late private']) : { name: 'late private' }));
  await waitFor(() => expect(hook.result.current.data).toBeUndefined());
  call.mockRejectedValue(new ApiError('offline', 503));
  await act(async () => { await qc.invalidateQueries(); });
  expect(hook.result.current.data).toBeUndefined();
});

it('fences cached section data in the render that disables admission', async () => {
  call.mockResolvedValue(body(['private']));
  const hook = renderHook(({ enabled }) => usePageSection<string[]>('journal_gist', {}, enabled), { wrapper, initialProps: { enabled: true } });
  await waitFor(() => expect(hook.result.current.data).toEqual(['private']));
  hook.rerender({ enabled: false });
  expect(hook.result.current.data).toBeUndefined(); expect(hook.result.current.computedAt).toBeNull();
  await act(async () => { await hook.result.current.refresh(); });
  expect(call).toHaveBeenCalledTimes(1);
});

it('deduplicates simultaneous manual refresh actions and keeps the shared action alive while another observer remains', async () => {
  call.mockResolvedValue(body(['initial']));
  const first = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
  const second = renderHook(() => usePageSection<string[]>('journal_gist'), { wrapper });
  await waitFor(() => expect(first.result.current.data).toEqual(['initial']));
  let complete!: (value: unknown) => void;
  let signal: AbortSignal | undefined;
  call.mockImplementationOnce((_path, _token, _init, _retry, options) => { signal = options?.signal; return new Promise((resolve) => { complete = resolve; }); });
  let one!: Promise<void>; let two!: Promise<void>;
  act(() => { one = first.result.current.refresh(); two = second.result.current.refresh(); });
  await waitFor(() => expect(call.mock.calls.filter((row) => row[2]?.method === 'POST')).toHaveLength(1));
  expect(second.result.current.isRefreshing).toBe(true);
  first.unmount();
  expect(signal?.aborted).toBe(false);
  await act(async () => { complete({ refresh_eta: null }); await Promise.all([one, two]); });
  expect(second.result.current.data).toEqual(['initial']);
});

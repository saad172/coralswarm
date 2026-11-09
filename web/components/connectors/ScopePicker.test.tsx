// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { connectorsApi, type SourceSummary } from "@/lib/connectorsApi";
import { SCOPE_PAGES_PER_ACTION } from "@/lib/connectorActionPolicy";
import { ScopePicker } from "./ScopePicker";
const source: SourceSummary = { key: 'channels', label: 'Channels', enabled: true, paused: false, status: 'active', scope_shape: 'items', scope: null, history: { supported: true, max_days: 90 }, last_captured_at: null, week: [0,0,0,0,0,0,0], history_days: null, history_requested_at: null, setup_completed_at: null, backfill: null };
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.useRealTimers(); });
function mount(actionDeadline?: number) {
  const changed = vi.fn(); const ready = vi.fn();
  const view = render(<ScopePicker getAuthToken={() => 'token'} provider="slack" source={source} value={{ mode: 'pick', item_ids: [] }} onChange={changed} onReady={ready} actionDeadline={actionDeadline} />);
  return { ...view, changed, ready };
}
it('refuses an expired callback phase before a request and permits an explicit retry', async () => {
  const read = vi.spyOn(connectorsApi, 'scopeItems').mockResolvedValue({ items: [], next_cursor: null, total: 0 });
  mount(Date.now() - 1);
  await screen.findByRole('alert'); expect(read).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
  await screen.findByText(/No items found/); expect(read).toHaveBeenCalledTimes(1);
});
it('stops a repeated cursor with a recoverable error without publishing partial selection', async () => {
  const read = vi.spyOn(connectorsApi, 'scopeItems').mockResolvedValueOnce({ items: [{ id:'selected',label:'Selected elsewhere',meta:null,selected:true }], next_cursor: '50', total: 100 }).mockResolvedValue({ items: [], next_cursor: '50', total: 100 });
  const view = mount(); await screen.findByRole('alert');
  expect(read).toHaveBeenCalledTimes(2); expect(view.changed).not.toHaveBeenCalled(); expect(view.ready).not.toHaveBeenCalledWith(true);
});
it('bounds one discovery action and resumes its verified cursor on explicit retry without discarding earlier selections', async () => {
  let calls = 0;
  const read = vi.spyOn(connectorsApi, 'scopeItems').mockImplementation(async () => {
    const index = ++calls;
    return { items: [{ id:String(index),label:`Choice ${index}`,meta:null,selected:true }], next_cursor: index <= SCOPE_PAGES_PER_ACTION ? String(index * 50) : null, total: 501 };
  });
  const view = mount(); await screen.findByRole('alert');
  expect(read).toHaveBeenCalledTimes(SCOPE_PAGES_PER_ACTION); expect(view.changed).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
  await waitFor(() => expect(view.ready).toHaveBeenLastCalledWith(true));
  expect(read.mock.calls.at(-1)?.[3]).toEqual({ cursor: String(SCOPE_PAGES_PER_ACTION * 50) });
  expect(view.changed.mock.calls.at(-1)?.[0].item_ids).toHaveLength(SCOPE_PAGES_PER_ACTION + 1);
});
it('cancels discovery at the fixed deadline even when mocked I/O ignores abort', async () => {
  vi.useFakeTimers(); let signal: AbortSignal | undefined;
  vi.spyOn(connectorsApi, 'scopeItems').mockImplementation(async (_token,_provider,_source,_params,value) => { signal = value; await new Promise(() => {}); return { items:[],next_cursor:null,total:0 }; });
  mount(Date.now() + 20);
  await act(async () => { await vi.advanceTimersByTimeAsync(20); });
  expect(signal?.aborted).toBe(true); expect(screen.getByRole('alert')).toBeInTheDocument(); expect(screen.queryByRole('status')).not.toBeInTheDocument();
});
it('aborts the exact picker request on unmount', async () => {
  let signal: AbortSignal | undefined;
  vi.spyOn(connectorsApi, 'scopeItems').mockImplementation(async (_token,_provider,_source,_params,value) => { signal = value; await new Promise(() => {}); return { items:[],next_cursor:null,total:0 }; });
  const view = mount(); await waitFor(() => expect(signal).toBeDefined()); view.unmount(); expect(signal?.aborted).toBe(true);
});

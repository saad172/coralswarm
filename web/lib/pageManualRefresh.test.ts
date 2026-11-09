import { expect, it, vi } from "vitest";
import { createManualRefresh } from "./pageManualRefresh";

it("deduplicates the action and aborts only when its last admitted observer leaves, even if I/O ignores abort", async () => {
  const state = createManualRefresh(); const first = state.observe(); const second = state.observe();
  let signal: AbortSignal | undefined;
  const work = vi.fn(async (value: AbortSignal) => { signal = value; await new Promise(() => {}); });
  const job = state.run(work); const peer = state.run(work);
  expect(job).toBe(peer);
  await Promise.resolve(); expect(work).toHaveBeenCalledTimes(1);
  first(); expect(signal?.aborted).toBe(false); expect(state.snapshot().busy).toBe(true);
  second(); await job;
  expect(signal?.aborted).toBe(true); expect(state.snapshot()).toEqual({ busy: false, error: null });
  const leave = state.observe(); await state.run(async () => {}); leave();
  expect(state.snapshot().busy).toBe(false);
});
it("shares a recoverable action error and clears it for an explicit verified retry", async () => {
  const state = createManualRefresh(); const leave = state.observe();
  await state.run(async () => { throw new Error('temporarily unavailable'); });
  expect(state.snapshot().error?.message).toBe('temporarily unavailable');
  await state.run(async () => {});
  expect(state.snapshot()).toEqual({ busy: false, error: null }); leave();
});

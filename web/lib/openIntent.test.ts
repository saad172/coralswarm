import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { OPEN_INTENT_MS, openIntent } from "./openIntent";

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { vi.useRealTimers(); });

it("a sweep fires once, for the row the pointer settled on", () => {
  const onFire = vi.fn();
  const intent = openIntent(onFire);
  intent.arm("A");
  vi.advanceTimersByTime(OPEN_INTENT_MS / 2);
  intent.arm("B");
  vi.advanceTimersByTime(OPEN_INTENT_MS);
  expect(onFire.mock.calls).toEqual([["B"]]);
  vi.advanceTimersByTime(OPEN_INTENT_MS * 10);
  expect(onFire).toHaveBeenCalledTimes(1);
});

it("leaving the row before the delay fires nothing", () => {
  const onFire = vi.fn();
  const intent = openIntent(onFire);
  intent.arm("A");
  vi.advanceTimersByTime(OPEN_INTENT_MS - 1);
  intent.disarm();
  vi.advanceTimersByTime(OPEN_INTENT_MS * 10);
  expect(onFire).not.toHaveBeenCalled();
});

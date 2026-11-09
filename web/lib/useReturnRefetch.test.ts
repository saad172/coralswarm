// @vitest-environment jsdom
import { cleanup, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RETURN_REFETCH_FLOOR_MS, useReturnRefetch } from "./useReturnRefetch";

let visibility: DocumentVisibilityState = "visible";
beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-23T12:00:00Z"));
  visibility = "visible";
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

function leaveAndReturn({ focus = true }: { focus?: boolean } = {}) {
  visibility = "hidden";
  document.dispatchEvent(new Event("visibilitychange"));
  visibility = "visible";
  document.dispatchEvent(new Event("visibilitychange"));
  if (focus) window.dispatchEvent(new Event("focus"));
}

describe("useReturnRefetch", () => {
  it("runs once for a single return that fires visibilitychange and focus together", () => {
    const run = vi.fn();
    renderHook(() => useReturnRefetch(run));
    vi.advanceTimersByTime(RETURN_REFETCH_FLOOR_MS);
    leaveAndReturn();
    expect(run).toHaveBeenCalledTimes(1);
    // No trailing call armed by the second event of the same return.
    vi.advanceTimersByTime(RETURN_REFETCH_FLOOR_MS * 4);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("still runs once when it also listens to focus (the Slack wrapper's mode)", () => {
    const run = vi.fn();
    renderHook(() => useReturnRefetch(run, { focus: true, floorMs: 15_000 }));
    vi.advanceTimersByTime(15_000);
    leaveAndReturn();
    vi.advanceTimersByTime(60_000);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("defers a return inside the floor to the floor boundary instead of dropping it", () => {
    const run = vi.fn();
    renderHook(() => useReturnRefetch(run));
    vi.advanceTimersByTime(RETURN_REFETCH_FLOOR_MS);
    leaveAndReturn({ focus: false });
    expect(run).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(2_000);
    leaveAndReturn({ focus: false });
    expect(run).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(RETURN_REFETCH_FLOOR_MS - 2_000);
    expect(run).toHaveBeenCalledTimes(2);
  });

  it("drops a return while a request is in flight", () => {
    const run = vi.fn();
    renderHook(() => useReturnRefetch(run, { inFlight: () => true }));
    vi.advanceTimersByTime(RETURN_REFETCH_FLOOR_MS);
    leaveAndReturn();
    window.dispatchEvent(new Event("online"));
    expect(run).not.toHaveBeenCalled();
  });

  it("ignores events while hidden, and runs the interval only while visible", () => {
    const run = vi.fn();
    renderHook(() => useReturnRefetch(run, { intervalMs: 60_000 }));
    visibility = "hidden";
    window.dispatchEvent(new Event("online"));
    vi.advanceTimersByTime(60_000);
    expect(run).not.toHaveBeenCalled();
    visibility = "visible";
    vi.advanceTimersByTime(60_000);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("measures the floor from a caller-stamped request, and detaches when disabled", () => {
    const run = vi.fn();
    const lastRunAtRef = { current: Date.now() };
    const hook = renderHook(({ enabled }) => useReturnRefetch(run, { lastRunAtRef, enabled }), { initialProps: { enabled: true } });
    vi.advanceTimersByTime(2_000);
    leaveAndReturn();
    expect(run).not.toHaveBeenCalled();
    hook.rerender({ enabled: false });
    vi.advanceTimersByTime(RETURN_REFETCH_FLOOR_MS);
    expect(run).not.toHaveBeenCalled();
    leaveAndReturn();
    expect(run).not.toHaveBeenCalled();
  });
});

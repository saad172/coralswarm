// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, act } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { JumpToLatest } from "./JumpToLatest";
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it.each([false, true])("uses the strict threshold and respects reduced motion %s", reduced => {
  vi.stubGlobal("ResizeObserver", class { observe() {} disconnect() {} });
  vi.stubGlobal("matchMedia", () => ({ matches: reduced }));
  let callback: FrameRequestCallback = () => {};
  vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => { callback = cb; return 1; });
  const el = document.createElement("div");
  Object.defineProperties(el, { scrollHeight: { value: 1000 }, clientHeight: { value: 400 } });
  el.scrollTop = 280;
  el.scrollTo = vi.fn();
  render(<JumpToLatest scrollRef={{ current: el }} />);
  expect(screen.queryByRole("button")).toBeNull();
  el.scrollTop = 279;
  fireEvent.scroll(el);
  act(() => callback(0));
  fireEvent.click(screen.getByRole("button", { name: /Jump to latest/ }));
  expect(el.scrollTo).toHaveBeenCalledWith({ top: 1000, behavior: reduced ? "auto" : "smooth" });
});

// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { SwarmStream } from "./SwarmStream";

function stubCanvas() {
  const ctx = new Proxy({} as Record<string, unknown>, {
    get: (t, k) => (k in t ? t[k as string] : (t[k as string] = vi.fn())),
    set: (t, k, v) => ((t[k as string] = v), true),
  });
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(ctx as unknown as CanvasRenderingContext2D);
  return ctx;
}

let raf: ReturnType<typeof vi.fn>;
let caf: ReturnType<typeof vi.fn>;

beforeEach(() => {
  raf = vi.fn().mockReturnValue(7);
  caf = vi.fn();
  vi.stubGlobal("requestAnimationFrame", raf);
  vi.stubGlobal("cancelAnimationFrame", caf);
  vi.stubGlobal("ResizeObserver", class { observe() {} disconnect() {} });
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const media = (reduce: boolean) =>
  vi.stubGlobal("matchMedia", (q: string) => ({ matches: reduce && q.includes("reduce") }));

it("is hidden from assistive tech", () => {
  media(false);
  stubCanvas();
  const { container } = render(<SwarmStream />);
  expect(container.querySelector("canvas")).toHaveAttribute("aria-hidden", "true");
});

it("animates, and cancels its frame on unmount", () => {
  media(false);
  stubCanvas();
  const view = render(<SwarmStream />);
  expect(raf).toHaveBeenCalledTimes(1);
  view.unmount();
  expect(caf).toHaveBeenCalledWith(7);
});

it("draws a static frame and schedules nothing under reduced motion", () => {
  media(true);
  const ctx = stubCanvas();
  render(<SwarmStream />);
  expect(raf).not.toHaveBeenCalled();
  expect(ctx.arc).toBeDefined();
  expect((ctx.arc as ReturnType<typeof vi.fn>).mock.calls.length).toBeGreaterThan(0);
});

it("does nothing when the browser gives no 2d context", () => {
  media(false);
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(null);
  render(<SwarmStream />);
  expect(raf).not.toHaveBeenCalled();
});

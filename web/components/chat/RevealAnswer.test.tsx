// @vitest-environment jsdom
import { cleanup, render } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { RevealAnswer } from "./RevealAnswer";
const deps = vi.hoisted(() => ({ reduced: false, animate: vi.fn(() => ({ complete: vi.fn() })) }));
vi.mock("motion", () => ({ animate: deps.animate }));
vi.mock("@/lib/motion", async (original) => ({ ...await original<typeof import("@/lib/motion")>(), useReducedMotion: () => deps.reduced }));
afterEach(() => { cleanup(); deps.animate.mockClear(); deps.reduced = false; });
it("staggers completed blocks and keeps the original citation control", () => {
  const view = render(<RevealAnswer active><div><p>One <a className="cite" href="/source" aria-label="Open source 1">1</a></p><p>Two</p></div></RevealAnswer>);
  expect(deps.animate.mock.calls.map((call: unknown[]) => call[2])).toEqual([
    { duration: .32, ease: [.22, 1, .36, 1], delay: 0 }, { duration: .32, ease: [.22, 1, .36, 1], delay: .04 },
    { duration: .18, delay: .46, ease: [.34, 1.32, .64, 1] },
  ]);
  expect(view.getByRole("link", { name: "Open source 1" }).getAttribute("href")).toBe("/source");
});
it("renders history and reduced motion without delayed animation", () => {
  const view = render(<RevealAnswer active={false}><p>History</p></RevealAnswer>);
  expect(deps.animate).not.toHaveBeenCalled();
  deps.reduced = true;
  view.rerender(<RevealAnswer active delay={760}><button type="button">Copy</button></RevealAnswer>);
  expect(view.getByRole("button", { name: "Copy" })).toBeTruthy();
  expect(deps.animate).not.toHaveBeenCalled();
});

it("finishes delayed ancestors and citations when keyboard focus enters", () => {
  const view = render(<RevealAnswer active><div><p><a className="cite" href="/source">Source</a></p></div></RevealAnswer>);
  const link = view.getByRole("link");
  link.focus();
  expect(document.activeElement).toBe(link);
  for (const result of deps.animate.mock.results) expect(result.value.complete).toHaveBeenCalled();
});
it("finishes delayed action controls on focus", () => {
  const view = render(<RevealAnswer active delay={760}><button type="button">Copy</button></RevealAnswer>);
  view.getByRole("button").focus();
  expect(deps.animate.mock.results.at(-1)?.value.complete).toHaveBeenCalled();
});

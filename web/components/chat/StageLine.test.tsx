// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { StageLine, type AnswerStage } from "./StageLine";
vi.mock("@/lib/motion", async (actual) => ({ ...await actual<typeof import("@/lib/motion")>(), useReducedMotion: () => true }));
afterEach(() => { cleanup(); vi.useRealTimers(); });
it.each([
  [{ kind: "searching" }, "Searching your workspaces"],
  [{ kind: "retrieved", sources: 1 }, "Reading 1 source"],
  [{ kind: "retrieved", sources: 2 }, "Reading 2 sources"],
  [{ kind: "writing" }, "Writing answer"],
  [{ kind: "checking" }, "Checking citations"],
] as [AnswerStage, string][])("announces %j", (stage, label) => {
  const { container } = render(<StageLine state="pending" stage={stage} startedAt={Date.now()} />);
  expect(screen.getByText(label)).toBeTruthy();
  expect(container.querySelector('[aria-live="polite"]')?.textContent).toBe(label);
  expect(container.querySelector(".animate-pulse")).toBeNull();
});
it("updates elapsed time and settles without inventing history timing", () => {
  vi.useFakeTimers(); vi.setSystemTime(1000);
  const view = render(<StageLine state="pending" stage={null} startedAt={1000} />);
  act(() => vi.advanceTimersByTime(200));
  expect(screen.getByText("0.2s")).toBeTruthy();
  view.rerender(<StageLine state="answered" sources={1} elapsedMs={200} />);
  expect(screen.getByText("answered from 1 source · 0.2s")).toBeTruthy();
  expect(vi.getTimerCount()).toBe(0);
  view.rerender(<StageLine state="answered" sources={2} />);
  expect(screen.getByText("answered from 2 sources")).toBeTruthy();
});
it("offers an explicit retry after stop", () => {
  const retry = vi.fn();
  render(<StageLine state="stopped" elapsedMs={1200} onAskAgain={retry} />);
  expect(screen.getByText("stopped · 1.2s")).toBeTruthy();
  expect(screen.getByText(/Stopped before it wrote anything/)).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Check saved history" }));
  expect(retry).toHaveBeenCalledOnce();
});

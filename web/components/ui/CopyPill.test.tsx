// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { CopyPill } from "./CopyPill";

beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const flush = () => act(async () => {});

it("copies the value, shows copied for 1.8s, then reverts", async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { clipboard: { writeText } });
  render(<CopyPill value="https://api.example.test/mcp" />);
  const pill = screen.getByRole("button", { name: "Copy https://api.example.test/mcp" });
  expect(pill).toHaveTextContent("copy");

  fireEvent.click(pill);
  await flush();
  expect(writeText).toHaveBeenCalledExactlyOnceWith("https://api.example.test/mcp");
  expect(pill).toHaveTextContent("copied ✓");
  // The accessible name does not change; the announcement is a live region.
  expect(screen.getByRole("status")).toHaveTextContent("Copied");

  act(() => void vi.advanceTimersByTime(1799));
  expect(pill).toHaveTextContent("copied ✓");
  act(() => void vi.advanceTimersByTime(2));
  expect(pill).not.toHaveTextContent("copied ✓");
});

it("does not claim success when the clipboard refuses", async () => {
  vi.stubGlobal("navigator", { clipboard: { writeText: vi.fn().mockRejectedValue(new Error("denied")) } });
  render(<CopyPill value="x" />);
  fireEvent.click(screen.getByRole("button"));
  await flush();
  expect(screen.getByRole("button")).not.toHaveTextContent("copied");
});

it("tells the caller once per successful copy, and never when the clipboard refuses", async () => {
  const onCopied = vi.fn();
  vi.stubGlobal("navigator", { clipboard: { writeText: vi.fn().mockResolvedValue(undefined) } });
  render(<CopyPill value="x" onCopied={onCopied} />);
  fireEvent.click(screen.getByRole("button"));
  await flush();
  expect(onCopied).toHaveBeenCalledTimes(1);
  cleanup();

  const refused = vi.fn();
  vi.stubGlobal("navigator", { clipboard: { writeText: vi.fn().mockRejectedValue(new Error("denied")) } });
  render(<CopyPill value="x" onCopied={refused} />);
  fireEvent.click(screen.getByRole("button"));
  await flush();
  expect(refused).not.toHaveBeenCalled();
});

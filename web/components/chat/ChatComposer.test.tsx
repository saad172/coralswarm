// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ChatComposer } from "./ChatComposer";
afterEach(cleanup);
it("preserves multiline and IME input, sends Enter, and exposes Stop", async () => {
  const send = vi.fn();
  const stop = vi.fn();
  const props = { model: "smart" as const, effort: "medium" as const, style: "detailed" as const, setModel: vi.fn(), setEffort: vi.fn(), setStyle: vi.fn(), onSend: send, onStop: stop };
  const { rerender } = render(<ChatComposer {...props} busy={false} />);
  const input = screen.getByRole("textbox", { name: "Question" });
  fireEvent.change(input, { target: { value: "First\nSecond" } });
  fireEvent.keyDown(input, { key: "Enter", shiftKey: true });
  fireEvent.keyDown(input, { key: "Enter", isComposing: true });
  expect(send).not.toHaveBeenCalled();
  fireEvent.keyDown(input, { key: "Enter" });
  expect(send).toHaveBeenCalledWith("First\nSecond");
  await waitFor(() => expect(input).toHaveValue(""));
  rerender(<ChatComposer {...props} busy />);
  fireEvent.click(screen.getByRole("button", { name: "Stop" }));
  expect(stop).toHaveBeenCalledOnce();
});

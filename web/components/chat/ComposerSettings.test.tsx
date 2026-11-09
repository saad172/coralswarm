// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ComposerSettings } from "./ComposerSettings";
afterEach(cleanup);
const props = { model: "smart" as const, effort: "medium" as const, style: "detailed" as const, setModel: vi.fn(), setEffort: vi.fn(), setStyle: vi.fn() };
it("offers supported options, changes style, and dismisses by Escape and outside click", async () => {
  render(<ComposerSettings {...props} />);
  const trigger = screen.getByRole("button", { name: "Answer settings" });
  fireEvent.click(trigger);
  for (const name of ["Fast", "Smart", "Max", "Concise", "Detailed", "Sources only"]) expect(screen.getByRole("tab", { name })).toBeVisible();
  fireEvent.click(screen.getByRole("tab", { name: "Concise" }));
  expect(props.setStyle).toHaveBeenCalledWith("concise");
  expect(screen.getByRole("radiogroup")).toBeVisible();
  fireEvent.keyDown(document, { key: "Escape" });
  expect(screen.queryByRole("tab")).toBeNull();
  fireEvent.click(trigger);
  await new Promise(resolve => setTimeout(resolve, 10));
  fireEvent.mouseDown(document.body);
  expect(screen.queryByRole("tab")).toBeNull();
});
it("hides scoped effort and sources-only model, summarizing only effective values", () => {
  const { rerender } = render(<ComposerSettings {...props} scoped />);
  fireEvent.click(screen.getByRole("button"));
  expect(screen.queryByRole("radiogroup")).toBeNull();
  rerender(<ComposerSettings {...props} scoped style="sources-only" model="max" />);
  expect(screen.queryByRole("tab", { name: "Max" })).toBeNull();
  expect(screen.getByText("sources only")).toBeVisible();
  rerender(<ComposerSettings {...props} disabled />);
  expect(screen.getByRole("button")).toBeDisabled();
  expect(screen.queryByRole("tab")).toBeNull();
});

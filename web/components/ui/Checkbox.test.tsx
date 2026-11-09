// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { Checkbox } from "./Checkbox";

afterEach(cleanup);

it("is a labelled native checkbox that reports the next value", () => {
  const onChange = vi.fn();
  render(<Checkbox checked={false} onChange={onChange} label="general" />);
  const box = screen.getByRole("checkbox", { name: "general" });
  expect(box).not.toBeChecked();
  fireEvent.click(box);
  expect(onChange).toHaveBeenCalledExactlyOnceWith(true);
});

it("reflects checked and takes an aria-label when it has no visible text", () => {
  render(<Checkbox checked onChange={() => {}} ariaLabel="Select #random" />);
  expect(screen.getByRole("checkbox", { name: "Select #random" })).toBeChecked();
});

it("is natively disabled when disabled", () => {
  render(<Checkbox checked={false} onChange={() => {}} disabled ariaLabel="x" />);
  expect(screen.getByRole("checkbox")).toBeDisabled();
});

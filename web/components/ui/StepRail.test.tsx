// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { StepRail } from "./StepRail";

afterEach(cleanup);

const STEPS = ["Connect", "Sources", "Scope", "History"];

it("marks done, current and upcoming steps", () => {
  const { container } = render(<StepRail steps={STEPS} currentIndex={1} />);
  expect(container.textContent).toContain("✓");
  expect(screen.getByText("Sources").closest("[aria-current]")).toHaveAttribute("aria-current", "step");
  expect(container.querySelectorAll("[aria-current]")).toHaveLength(1);
  expect(screen.getByText("Sources")).toHaveClass("font-medium");
});

it("labels every step by default", () => {
  render(<StepRail steps={STEPS} currentIndex={0} />);
  for (const s of STEPS) expect(screen.getByText(s)).not.toHaveClass("sr-only");
});

it("compact labels only the current step visibly", () => {
  render(<StepRail steps={STEPS} currentIndex={2} compact />);
  expect(screen.getByText("Scope")).not.toHaveClass("sr-only");
  for (const s of ["Connect", "Sources", "History"]) expect(screen.getByText(s)).toHaveClass("sr-only");
});

// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { Sparkline } from "./Sparkline";

afterEach(cleanup);

const bars = (c: HTMLElement) => Array.from(c.querySelectorAll<HTMLElement>("[data-bar]"));

it("scales bars to the busiest day and names the weekly total", () => {
  const { container } = render(<Sparkline values={[0, 2, 4, 0, 1, 4, 2]} />);
  expect(screen.getByRole("img", { name: "13 this week" })).toBeInTheDocument();
  expect(bars(container).map((b) => b.style.height)).toEqual(["0%", "50%", "100%", "0%", "25%", "100%", "50%"]);
});

it("uses the spark-bar class for past days, full teal today and line for empty days", () => {
  const { container } = render(<Sparkline values={[0, 3, 3]} />);
  const [zero, past, today] = bars(container);
  expect(zero).toHaveClass("bg-line");
  expect(past).toHaveClass("spark-bar");
  expect(today).toHaveClass("bg-hue-teal");
});

it("renders seven bars for a week and tooltips from labels", () => {
  const { container } = render(<Sparkline values={[1, 1, 1, 1, 1, 1, 1]} labels={["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]} />);
  expect(bars(container)).toHaveLength(7);
  expect(bars(container)[0]).toHaveAttribute("title", "Mon · 1");
});

it("survives an all-zero week", () => {
  const { container } = render(<Sparkline values={[0, 0, 0, 0, 0, 0, 0]} />);
  expect(bars(container).every((b) => b.style.height === "0%")).toBe(true);
});
